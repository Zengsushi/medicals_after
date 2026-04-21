"""
    认证中间件和权限装饰器
"""
import functools
import logging
from typing import Callable, List, Optional, Set
from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session, selectinload
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN

from apps.user.models import User
from apps.user.rbac_models import Role, Permission
from apps.menu.models import Menu
from database import get_db
from redisbase import RedisBase

security = HTTPBearer(auto_error=False)

logger = logging.getLogger(__name__)


class AuthMiddleware:
    """认证中间件"""

    @staticmethod
    def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: Session = Depends(get_db)
    ) -> User:
        """
        获取当前登录用户
        """
        if not credentials:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="未提供认证凭据"
            )

        token = credentials.credentials
        user_id = RedisBase.get_current_token(token)

        if not user_id:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="令牌已过期或无效"
            )

        user = db.query(User).options(
            selectinload(User.roles).selectinload(Role.permissions),
            selectinload(User.roles).selectinload(Role.menus)
        ).filter(User.id == user_id).first()

        if not user:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="用户不存在"
            )

        if not user.is_active:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="用户账号已停用"
            )

        if user.is_deleted:
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="用户已被删除"
            )

        return user

    @staticmethod
    def get_current_user_optional(
        credentials: HTTPAuthorizationCredentials = Depends(security),
        db: Session = Depends(get_db)
    ) -> Optional[User]:
        """
        可选的认证 - 如果没有凭据则返回None
        """
        if not credentials:
            return None

        try:
            return AuthMiddleware.get_current_user(credentials, db)
        except HTTPException:
            return None


def require_permission(*permission_codes: str):
    """
    权限检查装饰器
    用法: @require_permission('users:view', 'users:edit')
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            user: User = kwargs.get('current_user')
            if not user:
                for arg in args:
                    if isinstance(arg, User):
                        user = arg
                        break

            if not user:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail="需要登录后才能访问"
                )

            required_perms = set(permission_codes)
            user_perms = set(user.get_permissions())

            if not required_perms.intersection(user_perms):
                logger.warning(f"用户 {user.username} 缺少权限 {required_perms - user_perms}")
                raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN,
                    detail=f"缺少必要权限: {', '.join(required_perms - user_perms)}"
                )

            return await func(*args, **kwargs)
        return wrapper
    return decorator


def require_role(*role_codes: str):
    """
    角色检查装饰器
    用法: @require_role('admin', 'superadmin')
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            user: User = kwargs.get('current_user')
            if not user:
                for arg in args:
                    if isinstance(arg, User):
                        user = arg
                        break

            if not user:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail="需要登录后才能访问"
                )

            user_roles = set(user.get_role_codes())
            required_roles = set(role_codes)

            if not required_roles.intersection(user_roles):
                logger.warning(f"用户 {user.username} 缺少角色 {required_roles - user_roles}")
                raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN,
                    detail=f"需要角色: {', '.join(required_roles)}"
                )

            return await func(*args, **kwargs)
        return wrapper
    return decorator


def require_menu(path: str):
    """
    菜单权限检查装饰器
    用法: @require_menu('/admin/user')
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            user: User = kwargs.get('current_user')
            if not user:
                for arg in args:
                    if isinstance(arg, User):
                        user = arg
                        break

            if not user:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail="需要登录后才能访问"
                )

            user_menus = user.get_menus()
            menu_paths = {menu.path for menu in user_menus if menu.path}

            if path not in menu_paths:
                logger.warning(f"用户 {user.username} 无权访问菜单 {path}")
                raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN,
                    detail="无权访问该菜单"
                )

            return await func(*args, **kwargs)
        return wrapper
    return decorator


class PermissionChecker:
    """权限检查工具类"""

    @staticmethod
    def has_permission(user: User, permission_code: str) -> bool:
        """检查用户是否拥有指定权限"""
        return user.has_permission(permission_code)

    @staticmethod
    def has_any_permission(user: User, permission_codes: List[str]) -> bool:
        """检查用户是否拥有任一指定权限"""
        user_perms = set(user.get_permissions())
        return any(perm in user_perms for perm in permission_codes)

    @staticmethod
    def has_all_permissions(user: User, permission_codes: List[str]) -> bool:
        """检查用户是否拥有所有指定权限"""
        user_perms = set(user.get_permissions())
        return all(perm in user_perms for perm in permission_codes)

    @staticmethod
    def has_role(user: User, role_code: str) -> bool:
        """检查用户是否拥有指定角色"""
        return role_code in user.get_role_codes()

    @staticmethod
    def has_any_role(user: User, role_codes: List[str]) -> bool:
        """检查用户是否拥有任一指定角色"""
        user_roles = set(user.get_role_codes())
        return any(role in user_roles for role in role_codes)

    @staticmethod
    def can_access_menu(user: User, path: str) -> bool:
        """检查用户是否可以访问指定菜单路径"""
        user_menus = user.get_menus()
        return any(menu.path == path for menu in user_menus)


async def refresh_user_permissions(user_id: int, db: Session):
    """
    刷新用户权限缓存
    用于权限变更后立即生效
    """
    user = db.query(User).options(
        selectinload(User.roles).selectinload(Role.permissions),
        selectinload(User.roles).selectinload(Role.menus)
    ).filter(User.id == user_id).first()

    if user:
        perms = user.get_permissions()
        menus = user.get_menus()
        role_codes = user.get_role_codes()

        return {
            "permissions": perms,
            "menus": [
                {
                    "id": menu.id,
                    "name": menu.name,
                    "path": menu.path,
                    "component": menu.component,
                    "icon": menu.icon,
                    "order": menu.order,
                    "parent_id": menu.parent_id
                }
                for menu in menus
            ],
            "roles": role_codes
        }
    return None
