"""
    FastAPI 依赖注入模块
    按照 FastAPI 最佳实践实现认证和授权
"""
from typing import Annotated, Optional, List
from fastapi import Depends, HTTPException, Header, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session, selectinload
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN

from database import get_db
from redisbase import RedisBase
from apps.user.models import User
from apps.user.rbac_models import Role, Permission
from apps.menu.models import Menu
from users.auth.utils import verify_token, extract_device_info, parse_location_from_ip

security = HTTPBearer(auto_error=False)


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db)
) -> Optional[User]:
    """
    可选的当前用户获取
    如果没有认证信息则返回 None
    """
    if not credentials:
        return None

    try:
        token = credentials.credentials
        is_valid, payload, error = verify_token(token, expected_type="access")

        if not is_valid:
            return None

        jti = payload.get("jti")
        user_id = int(payload.get("sub"))

        cached_user_id = RedisBase.get_current_token(jti)
        if not cached_user_id or cached_user_id != user_id:
            return None

        user = db.query(User).options(
            selectinload(User.roles).selectinload(Role.permissions),
            selectinload(User.roles).selectinload(Role.menus)
        ).filter(User.id == user_id).first()

        if not user or user.is_deleted or not user.is_active:
            return None

        return user

    except Exception:
        return None


def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
    db: Session = Depends(get_db)
) -> User:
    """
    获取当前认证用户 (必需)
    基于 RFC 8725 JWT 最佳实践验证 token
    """
    if not credentials:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="未提供认证凭据",
            headers={"WWW-Authenticate": "Bearer"}
        )

    token = credentials.credentials

    is_valid, payload, error = verify_token(token, expected_type="access")

    if not is_valid:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail=error,
            headers={"WWW-Authenticate": "Bearer"}
        )

    jti = payload.get("jti")
    user_id = int(payload.get("sub"))

    cached_user_id = RedisBase.get_current_token(jti)
    if not cached_user_id or cached_user_id != user_id:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="令牌已失效，请重新登录",
            headers={"WWW-Authenticate": "Bearer"}
        )

    user = db.query(User).options(
        selectinload(User.roles).selectinload(Role.permissions),
        selectinload(User.roles).selectinload(Role.menus)
    ).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="用户不存在",
            headers={"WWW-Authenticate": "Bearer"}
        )

    if user.is_deleted:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="用户已被删除",
            headers={"WWW-Authenticate": "Bearer"}
        )

    if not user.is_active:
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="用户账号已停用",
            headers={"WWW-Authenticate": "Bearer"}
        )

    return user


class PermissionChecker:
    """
    权限检查器
    用于依赖注入中检查用户权限
    """

    def __init__(self, required_permissions: List[str], require_all: bool = False):
        self.required_permissions = required_permissions
        self.require_all = require_all

    async def __call__(
        self,
        current_user: Annotated[User, Depends(get_current_user)]
    ) -> User:
        if not self.required_permissions:
            return current_user

        user_permissions = set(current_user.get_permissions())

        if 'admins:manage' in user_permissions or 'admins:view' in user_permissions:
            return current_user

        if self.require_all:
            missing = [p for p in self.required_permissions if p not in user_permissions]
            if missing:
                raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN,
                    detail={
                        "code": "PERMISSION_DENIED",
                        "message": f"缺少必要权限: {', '.join(missing)}",
                        "required": self.required_permissions,
                        "allowed": list(user_permissions)
                    }
                )
        else:
            has_any = any(p in user_permissions for p in self.required_permissions)
            if not has_any:
                raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN,
                    detail={
                        "code": "PERMISSION_DENIED",
                        "message": f"需要以下任一权限: {', '.join(self.required_permissions)}",
                        "required": self.required_permissions,
                        "allowed": list(user_permissions)
                    }
                )

        return current_user


class RoleChecker:
    """
    角色检查器
    用于依赖注入中检查用户角色
    """

    def __init__(self, required_roles: List[str]):
        self.required_roles = required_roles

    async def __call__(
        self,
        current_user: Annotated[User, Depends(get_current_user)]
    ) -> User:
        user_roles = set(current_user.get_role_codes())

        if 'superadmin' in user_roles:
            return current_user

        has_role = any(r in user_roles for r in self.required_roles)
        if not has_role:
            raise HTTPException(
                status_code=HTTP_403_FORBIDDEN,
                detail={
                    "code": "ROLE_DENIED",
                    "message": f"需要以下角色: {', '.join(self.required_roles)}",
                    "required": self.required_roles,
                    "user_roles": list(user_roles)
                }
            )

        return current_user


def require_permission(*permissions, require_all: bool = False):
    """
    权限依赖装饰器
    用法: @require_permission('users:view')
          @require_permission('users:view', 'users:edit', require_all=True)
    """
    return PermissionChecker(list(permissions), require_all)


def require_role(*roles):
    """
    角色依赖装饰器
    用法: @require_role('admin', 'superadmin')
    """
    return RoleChecker(list(roles))


def get_client_ip(request: Request) -> str:
    """获取客户端 IP 地址"""
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def get_user_agent(request: Request) -> str:
    """获取 User-Agent"""
    return request.headers.get("user-agent", "")


CurrentUser = Annotated[User, Depends(get_current_user)]
OptionalUser = Annotated[Optional[User], Depends(get_current_user_optional)]
