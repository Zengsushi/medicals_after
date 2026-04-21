"""
优化后的用户 API 路由

改进点：
1. 统一认证依赖注入
2. 完整的日志记录
3. 速率限制（防暴力破解）
4. 输入验证和安全检查
5. 错误处理标准化
6. 性能优化（缓存、查询优化）
"""

import os
import time
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from functools import wraps

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    UploadFile,
    File,
    Query
)
from pydantic import BaseModel, Field, validator
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import or_, func
from starlette.responses import FileResponse

# 导入统一认证中间件
from utils.auth_middleware import (
    rate_limiter,
    SecurityMiddleware,
    get_auth_dependency,
    extract_request_metadata,
    log_api_call,
    login_tracker
)

from apps.user.models import User
from apps.user.rbac_models import Role, Permission
from apps.menu.models import Menu
from database import get_db
from utils.response_helper import ResponseHelper, Result
from utils.security import Security
from redisbase import RedisBase
from users.db import UserService
from users.dependencies import get_current_user, get_client_ip
from users.services.auth_service import AuthService, SessionService, AuditService, PasswordService
from users.auth.utils import generate_device_fingerprint, verify_password_strength

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["users"])
user_router = router


# ============================
# 请求模型 (Pydantic 验证)
# ============================

class UserLoginRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50, description="用户名")
    password: str = Field(..., min_length=6, max_length=128, description="密码")
    login_mode: str = Field(default="password", description="登录模式")

class TokenRefreshRequest(BaseModel):
    refresh_token: str = Field(..., min_length=10, description="刷新令牌")

class RegisterUserRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=8, max_length=128)
    phone: str = Field(..., regex=r'^1[3-9]\d{9}$')
    email: str = Field(..., regex=r'^[^@]+@[^@]+\.[^@]+$')
    first_name: str = Field(default="", max_length=50)
    last_name: str = Field(default="", max_length=50)
    avatar: str = Field(default="")
    
    @validator('password')
    def validate_password(cls, v):
        is_valid, error = verify_password_strength(v)
        if not is_valid:
            raise ValueError(error)
        return v

class UserUpdateRequest(BaseModel):
    phone: Optional[str] = Field(None, regex=r'^1[3-9]\d{9}$')
    email: Optional[str] = Field(None, regex=r'^[^@]+@[^@]+\.[^@]+$')
    first_name: Optional[str] = Field(None, max_length=50)
    last_name: Optional[str] = Field(None, max_length=50)
    introduce: Optional[str] = Field(None, max_length=500)

class PasswordChangeRequest(BaseModel):
    old_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8, max_length=128)
    
    @validator('new_password')
    def validate_new_password(cls, v):
        is_valid, error = verify_password_strength(v)
        if not is_valid:
            raise ValueError(error)
        return v


# ============================
# 响应构建器 (减少重复代码)
# ============================

class UserResponseBuilder:
    """统一的响应数据构建"""
    
    @staticmethod
    def build_user_info(user: User) -> Dict[str, Any]:
        """构建用户信息响应（带缓存提示）"""
        primary_role = user.get_primary_role()
        
        return {
            "id": user.id,
            "username": user.username,
            "email": getattr(user, 'email', ''),
            "phone": getattr(user, 'phone', ''),
            "first_name": getattr(user, 'first_name', ''),
            "last_name": getattr(user, 'last_name', ''),
            "avatar": getattr(user, 'avatar', ''),
            "is_active": user.is_active,
            "role": primary_role.code if primary_role else "guest",
            "role_name": primary_role.name if primary_role else "访客",
            "roles": [
                {"id": r.id, "code": r.code, "name": r.name}
                for r in (user.roles or [])
            ],
            "introduce": getattr(user, 'introduce', ''),
            "date_joined": user.date_joined.isoformat() if hasattr(user, 'date_joined') and user.date_joined else None,
            "last_login": user.last_login.isoformat() if hasattr(user, 'last_login') and user.last_login else None
        }
    
    @staticmethod
    def build_permissions_list(user: User) -> List[str]:
        """构建权限列表"""
        return user.get_permissions()
    
    @staticmethod
    def build_menu_tree(menus: List[Menu]) -> List[Dict[str, Any]]:
        """构建菜单树结构（优化版：使用字典推导）"""
        menu_map = {
            m.id: {
                **{
                    "id": m.id,
                    "name": m.name,
                    "path": m.path,
                    "component": m.component,
                    "icon": m.icon,
                    "order": m.order,
                    "parent_id": m.parent_id,
                    "permission_code": m.permission_code,
                    "is_visible": m.is_visible,
                    "is_cached": m.is_cached
                },
                "children": []
            }
            for m in menus
        }
        
        # 构建树形结构
        tree = []
        for menu in menus:
            if menu.parent_id is None:
                tree.append(menu_map[menu.id])
            elif menu.parent_id in menu_map:
                menu_map[menu.parent_id]["children"].append(menu_map[menu.id])
        
        return tree


response_builder = UserResponseBuilder()


# ============================
# 认证接口 (带速率限制和暴力破解防护)
# ============================

@router.post("/login", summary="用户登录")
@rate_limiter.limit("login:{ip}", max_requests=10, window=60)
@log_api_call("user_login")
async def user_login(
    request: Request,
    body: UserLoginRequest,
    db: Session = Depends(get_db)
):
    """
    用户登录
    
    安全特性：
    - 速率限制：每 IP 每分钟最多 10 次
    - 暴力破解防护：5次失败后锁定15分钟
    - 设备指纹记录
    - 完整审计日志
    """
    # 提取请求元数据
    metadata = await extract_request_metadata(request)
    
    # 检查账户是否被锁定
    if login_tracker.is_locked(body.username):
        lock_info = login_tracker.record_failed_attempt(body.username, metadata["ip_address"])
        return Result.error(
            423,
            "账户已锁定，请稍后再试",
            data={"lockout_remaining": lock_info["lockout_remaining"]}
        )
    
    device_fingerprint = generate_device_fingerprint(
        user_agent=metadata["user_agent"],
        ip_address=metadata["ip_address"]
    )

    result = AuthService.login(
        db=db,
        username=body.username,
        password=body.password,
        ip_address=metadata["ip_address"],
        user_agent=metadata["user_agent"],
        device_fingerprint=device_fingerprint
    )

    if not result["success"]:
        # 记录失败尝试
        attempt_info = login_tracker.record_failed_attempt(
            body.username, 
            metadata["ip_address"]
        )
        
        logger.warning(
            f"Failed login attempt for {body.username} "
            f"- IP: {metadata['ip_address']} "
            f"- Attempts: {attempt_info['attempts']}"
        )
        
        status_code = 401 if "密码" in result["message"] or "不存在" in result["message"] else 400
        return Result.error(status_code, result["message"])

    # 登录成功，重置失败计数
    login_tracker.record_successful_login(body.username)
    
    response_data = result["data"]

    return Result.success(200, "用户登录成功", {
        "user": response_builder.build_user_info(response_data["user"]),
        "token": response_data["access_token"],
        "refresh_token": response_data["refresh_token"],
        "permissions": response_data["permissions"],
        "menus": response_data["menus"],
        "role": response_data["role"]
    })


@router.post("/token/refresh", summary="刷新令牌")
@rate_limiter.limit("refresh:{ip}", max_requests=20, window=60)
@log_api_call("token_refresh")
async def refresh_token(
    request: Request,
    body: TokenRefreshRequest,
    db: Session = Depends(get_db)
):
    """
    刷新访问令牌
    
    使用 Refresh Token Rotation 机制：
    - 每次刷新后旧 token 立即失效
    - 颁发新的 access_token 和 refresh_token
    - 支持检测 token 重放攻击
    """
    metadata = await extract_request_metadata(request)

    result = AuthService.refresh(
        db=db,
        refresh_token=body.refresh_token,
        ip_address=metadata["ip_address"],
        user_agent=metadata["user_agent"]
    )

    if not result["success"]:
        logger.warning(f"Token refresh failed from {metadata['ip_address']}")
        return Result.error(401, result["message"])

    return Result.success(200, "令牌刷新成功", result["data"])


@router.post("/logout", summary="用户登出")
@log_api_call("user_logout")
async def user_logout(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """用户登出 - 撤销当前会话的所有 token"""
    metadata = await extract_request_metadata(request)
    
    authorization = request.headers.get("authorization", "")
    token = authorization.replace("Bearer ", "") if authorization else None

    result = AuthService.logout(
        db=db,
        user_id=current_user.id,
        jti=token,
        ip_address=metadata["ip_address"],
        user_agent=metadata["user_agent"]
    )

    if result["success"]:
        return Result.success(msg=result["message"])
    return Result.error(msg=result["message"])


@router.post("/logout/all", summary="登出所有设备")
@log_api_call("logout_all_devices")
async def logout_all_devices(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """登出所有设备 - 撤销该用户所有活跃的 session"""
    metadata = await extract_request_metadata(request)

    result = AuthService.logout(
        db=db,
        user_id=current_user.id,
        revoke_all=True,
        ip_address=metadata["ip_address"],
        user_agent=metadata["user_agent"]
    )

    if result["success"]:
        return Result.success(msg=result["message"], data=result["data"])
    return Result.error(msg=result["message"])


# ============================
# 用户信息接口
# ============================

@router.get("/user/info", summary="获取当前用户信息")
@log_api_call("get_user_info")
async def user_info(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取当前用户的详细信息"""
    try:
        user_data = response_builder.build_user_info(current_user)
        return Result.success(200, "用户详情获取成功", data=user_data)
    except Exception as e:
        logger.error(f"Get user info failed: {e}", exc_info=True)
        return Result.error(500, "获取用户信息失败")


@router.patch("/user/info", summary="修改用户信息")
@log_api_call("update_user_info")
async def update_user_info(
    request: Request,
    body: UserUpdateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """修改当前用户的基本信息"""
    service = UserService(db)
    
    try:
        update_data = body.dict(exclude_unset=True)
        user = service.user_info_update(update_data, current_user.id)
        
        AuditService.log(
            db, "USER_UPDATE",
            user_id=current_user.id,
            username=current_user.username,
            ip_address=get_client_ip(request)
        )
        
        return Result.success(msg="用户信息修改成功")
    except Exception as e:
        logger.error(f"Update user info failed: {e}", exc_info=True)
        return Result.error(500, str(e))


@router.get("/user/permissions", summary="获取用户权限列表")
@log_api_call("get_permissions")
async def get_user_permissions(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取当前用户的所有权限"""
    try:
        permissions = response_builder.build_permissions_list(current_user)
        return Result.success(200, "权限获取成功", data=permissions)
    except Exception as e:
        logger.error(f"Get permissions failed: {e}", exc_info=True)
        return Result.error(500, "获取权限失败")


@router.get("/user/menus", summary="获取用户菜单")
@log_api_call("get_menus")
async def get_user_menus(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取当前用户的菜单树（带缓存）"""
    try:
        menus = current_user.get_menus()
        menu_tree = response_builder.build_menu_tree(menus)
        return Result.success(200, "菜单获取成功", data=menu_tree)
    except Exception as e:
        logger.error(f"Get menus failed: {e}", exc_info=True)
        return Result.error(500, "获取菜单失败")


@router.post("/user/password/change", summary="修改密码")
@rate_limiter.limit("pwd_change:{user_id}", max_requests=3, window=3600)
@log_api_call("change_password")
async def change_password(
    request: Request,
    body: PasswordChangeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    修改当前用户密码
    
    安全措施：
    - 验证旧密码
    - 新密码强度检查
    - 每小时限制 3 次
    - 修改后撤销所有 token（强制重新登录）
    """
    metadata = await extract_request_metadata(request)

    # 验证旧密码
    if not Security.verify_password(body.old_password, current_user.password):
        AuditService.log(
            db, "PASSWORD_CHANGE_FAILED",
            user_id=current_user.id,
            username=current_user.username,
            ip_address=metadata["ip_address"],
            status="failed",
            error_message="Wrong old password"
        )
        return Result.error(401, "原密码错误")

    # 更新密码
    current_user.password = Security.get_password_hash(body.new_password)
    db.commit()

    # 撤销所有 token（强制重新登录）
    AuthService.logout(db, current_user.id, revoke_all=True, **metadata)

    AuditService.log(
        db, "PASSWORD_CHANGE",
        user_id=current_user.id,
        username=current_user.username,
        ip_address=metadata["ip_address"]
    )

    return Result.success(msg="密码修改成功，请重新登录")


# ============================
# 用户头像接口 (安全增强)
# ============================

ALLOWED_IMAGE_TYPES = ['image/jpeg', 'image/png', 'image/gif', 'image/webp']

@router.post("/user/avatar", summary="上传用户头像")
@log_api_call("upload_avatar")
async def upload_avatar(
    current_user: User = Depends(get_current_user),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """上传用户头像（带安全验证）"""
    # 验证文件安全性
    is_valid, error_msg = SecurityMiddleware.validate_file_upload(
        file, 
        allowed_types=ALLOWED_IMAGE_TYPES,
        max_size_mb=2
    )
    
    if not is_valid:
        return Result.error(400, error_msg)
    
    # 清理文件名
    safe_filename = SecurityMiddleware.sanitize_filename(file.filename)
    
    service = UserService(db)
    try:
        avatar_name = f"avatar_{current_user.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{safe_filename}"
        avatar_path = os.path.join(os.getenv("USER_STATIC_FILE_PATH", "static/uploads"), avatar_name)
        
        # 确保目录存在
        os.makedirs(os.path.dirname(avatar_path), exist_ok=True)
        
        with open(avatar_path, "wb") as f:
            content = await file.read()
            f.write(content)
        
        # 删除旧头像
        if current_user.avatar and os.path.exists(current_user.avatar):
            try:
                os.remove(current_user.avatar)
            except OSError:
                pass
        
        user = service.user_search(current_user.id)
        user.avatar = avatar_path
        db.commit()
        db.refresh(user)
        
        # 构建可访问的 URL
        server_url = f"http://{os.getenv('SERVER_IP', '127.0.0.1')}:{os.getenv('SERVER_PORT', '8000')}"
        clean_path = avatar_path.replace("\\", "/").replace("B:\\3_after_end\\medicalBs\\", "")
        user.avatar = f"{server_url}/{clean_path}"
        
        return Result.success(200, "用户头像更新成功!", data={"avatar": user.avatar})
        
    except Exception as e:
        logger.error(f"Upload avatar failed: {e}", exc_info=True)
        return Result.error(500, "头像上传失败")


@router.get("/user/avatar/download", summary="下载用户头像")
@log_api_call("download_avatar")
async def download_avatar(
    filename: str,
    current_user: User = Depends(get_current_user)
):
    """下载用户头像（路径穿越防护）"""
    # 安全处理文件路径
    safe_filename = SecurityMiddleware.sanitize_filename(filename)
    
    # 基础目录
    base_dir = os.getenv("USER_STATIC_FILE_PATH", "static/uploads")
    
    # 构建完整路径并确保在允许的目录内
    full_path = os.path.normpath(os.path.join(base_dir, safe_filename))
    
    # 安全检查：确保最终路径仍在基础目录内
    if not full_path.startswith(os.path.normpath(base_dir)):
        return Result.error(403, "非法的文件路径")
    
    if not os.path.exists(full_path):
        return Result.error(404, "文件不存在")

    return FileResponse(
        full_path,
        filename=safe_filename,
        media_type='application/octet-stream'
    )


# ============================
# 用户管理接口 (管理员)
# ============================

@router.get("/user/list", summary="获取用户列表")
@log_api_call("get_user_list")
async def get_user_list(
    current_user: User = Depends(get_auth_dependency(['user:view', 'admin:view'])),
    search: Optional[str] = Query(default=None, max_length=100),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    获取用户列表（分页、搜索）
    
    优化：
    - 使用 selectinload 预加载角色关系
    - 分页参数校验
    - 搜索关键词过滤
    """
    service = UserService(db)
    try:
        total, users = service.get_user_list(page, page_size, search)
        
        return Result.success(200, "列表数据获取成功", data={
            "user_list": [response_builder.build_user_info(u) for u in users],
            "total": total,
            "page": page,
            "page_size": page_size
        })
    except Exception as e:
        logger.error(f"Get user list failed: {e}", exc_info=True)
        return Result.error(500, str(e))


@router.post("/user/register", summary="用户注册")
@rate_limiter.limit("register:{ip}", max_requests=5, window=3600)
@log_api_call("user_register")
async def user_register(
    request: Request,
    register_data: RegisterUserRequest,
    db: Session = Depends(get_db)
):
    """
    用户注册
    
    安全特性：
    - Pydantic 数据验证
    - 密码强度检查
    - 速率限制：每小时每 IP 5 次
    - 用户名唯一性检查
    """
    metadata = await extract_request_metadata(request)
    
    service = UserService(db)
    
    # 检查用户是否已存在
    if service.user_exists(register_data.username):
        return Result.error(400, "此用户名已被注册")
    
    if service.phone_exists(register_data.phone):
        return Result.error(400, "此手机号已被注册")
    
    try:
        hash_pass = Security.get_password_hash(register_data.password)
        user = User(
            username=register_data.username,
            password=hash_pass,
            phone=register_data.phone,
            email=register_data.email,
            first_name=register_data.first_name or "",
            last_name=register_data.last_name or "",
            avatar=register_data.avatar or ""
        )
        service.user_info_save(user)
        
        AuditService.log(
            db, "USER_CREATE",
            username=register_data.username,
            ip_address=metadata["ip_address"]
        )
        
        return Result.success(msg="用户注册成功")
        
    except Exception as e:
        logger.error(f"Registration failed: {e}", exc_info=True)
        return Result.error(500, "注册失败，请稍后再试")


@router.delete("/user/{user_id}", summary="删除用户(软删除)")
@log_api_call("delete_user")
async def delete_user(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_auth_dependency(['user:delete', 'admin:manage'])),
    db: Session = Depends(get_db)
):
    """软删除用户"""
    metadata = await extract_request_metadata(request)
    
    # 不允许删除自己
    if user_id == current_user.id:
        return Result.error(400, "不能删除自己的账号")
    
    service = UserService(db)
    try:
        user = service.user_del(user_id)
        
        AuditService.log(
            db, "USER_DELETE",
            operator_id=current_user.id,
            target_user_id=user_id,
            ip_address=metadata["ip_address"]
        )
        
        return Result.success(msg="删除成功", data={"deleted_id": user_id})
        
    except Exception as e:
        logger.error(f"Delete user failed: {e}", exc_info=True)
        return Result.error(500, str(e))


@router.patch("/user/delete/{user_id}", summary="删除用户(软删除)")
@log_api_call("delete_user_patch")
async def delete_user_patch(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_auth_dependency(['user:delete', 'admin:manage'])),
    db: Session = Depends(get_db)
):
    """软删除用户（PATCH 版本）"""
    metadata = await extract_request_metadata(request)
    
    # 不允许删除自己
    if user_id == current_user.id:
        return Result.error(400, "不能删除自己的账号")
    
    service = UserService(db)
    try:
        user = service.user_del(user_id)
        
        AuditService.log(
            db, "USER_DELETE",
            operator_id=current_user.id,
            target_user_id=user_id,
            ip_address=metadata["ip_address"]
        )
        
        return Result.success(msg="删除成功", data={"deleted_id": user_id})
        
    except Exception as e:
        logger.error(f"Delete user failed: {e}", exc_info=True)
        return Result.error(500, str(e))


@router.put("/admin/user/{user_id}", summary="管理员更新用户")
@log_api_call("admin_update_user")
async def admin_update_user(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_auth_dependency(['user:update', 'admin:manage'])),
    db: Session = Depends(get_db)
):
    """管理员更新用户信息"""
    try:
        # 获取请求体
        body = await request.json()
        
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")
        
        # 更新字段
        if "phone" in body and body["phone"] is not None:
            user.phone = body["phone"]
        if "email" in body and body["email"] is not None:
            user.email = body["email"]
        if "first_name" in body and body["first_name"] is not None:
            user.first_name = body["first_name"]
        if "last_name" in body and body["last_name"] is not None:
            user.last_name = body["last_name"]
        if "avatar" in body and body["avatar"] is not None:
            user.avatar = body["avatar"]
        if "is_active" in body and body["is_active"] is not None:
            user.is_active = bool(body["is_active"])
        if "is_staff" in body and body["is_staff"] is not None:
            user.is_staff = bool(body["is_staff"])
        if "is_superuser" in body and body["is_superuser"] is not None:
            user.is_superuser = bool(body["is_superuser"])
        if "introduce" in body and body["introduce"] is not None:
            user.introduce = body["introduce"]
        
        db.commit()
        db.refresh(user)
        
        return Result.success(200, "用户更新成功", {
            "id": user.id,
            "username": user.username,
            "phone": user.phone,
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "avatar": user.avatar,
            "is_active": user.is_active,
            "is_staff": user.is_staff,
            "is_superuser": user.is_superuser
        })
    except Exception as e:
        db.rollback()
        logger.error(f"Update user failed: {e}", exc_info=True)
        return Result.error(500, str(e))


@router.patch("/user/permissions/{user_id}", summary="更新用户权限")
@log_api_call("update_user_permissions")
async def update_user_permissions(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_auth_dependency(['user:authorize', 'admin:manage'])),
    db: Session = Depends(get_db)
):
    """更新用户权限"""
    try:
        from pydantic import BaseModel
        
        class PermissionUpdate(BaseModel):
            role: Optional[str] = None
            is_staff: Optional[bool] = None
            is_superuser: Optional[bool] = None
        
        body = await request.json()
        
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")
        
        # 更新角色和权限
        if "role" in body:
            role = body["role"]
            if role == 'admin':
                user.is_staff = True
                user.is_superuser = False
            elif role == 'superadmin':
                user.is_staff = True
                user.is_superuser = True
            else:
                user.is_staff = False
                user.is_superuser = False
        
        if "is_staff" in body and body["is_staff"] is not None:
            user.is_staff = body["is_staff"]
        
        if "is_superuser" in body and body["is_superuser"] is not None:
            user.is_superuser = body["is_superuser"]
        
        db.commit()
        db.refresh(user)
        
        return Result.success(200, "用户权限更新成功", {
            "id": user.id,
            "username": user.username,
            "is_staff": user.is_staff,
            "is_superuser": user.is_superuser
        })
    except Exception as e:
        db.rollback()
        logger.error(f"Update user permissions failed: {e}", exc_info=True)
        return Result.error(500, str(e))


# ============================
# 角色和权限管理
# ============================

@router.get("/roles", summary="获取角色列表")
@log_api_call("get_roles")
async def get_roles(
    current_user: User = Depends(get_auth_dependency(['role:view', 'admin:view'])),
    db: Session = Depends(get_db)
):
    """获取角色列表（带权限预加载）"""
    roles = db.query(Role).options(
        selectinload(Role.permissions),
        selectinload(Role.menus)
    ).filter(Role.is_active == True).all()
    
    return Result.success(200, "角色列表获取成功", data=[
        {
            "id": role.id,
            "name": role.name,
            "code": role.code,
            "description": role.description,
            "is_system": role.is_system,
            "permission_count": len(role.permissions) if role.permissions else 0,
            "created_at": role.created_at.isoformat() if role.created_at else None
        }
        for role in roles
    ])


@router.get("/permissions", summary="获取权限列表")
@log_api_call("get_permissions")
async def get_permissions(
    current_user: User = Depends(get_auth_dependency(['permission:view', 'admin:view'])),
    db: Session = Depends(get_db)
):
    """获取权限列表"""
    permissions = db.query(Permission).filter(Permission.is_active == True).all()
    
    return Result.success(200, "权限列表获取成功", data=[
        {
            "id": p.id,
            "name": p.name,
            "code": p.code,
            "description": p.description,
            "module": p.module
        }
        for p in permissions
    ])


# ============================
# 设备管理
# ============================

@router.get("/user/devices", summary="获取登录设备列表")
@log_api_call("get_devices")
async def get_user_devices(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取当前用户的活跃设备列表"""
    devices = AuthService.get_user_devices(db, current_user.id)
    return Result.success(200, "设备列表获取成功", data=devices)


@router.delete("/user/devices/{session_id}", summary="登出指定设备")
@log_api_call("logout_device")
async def logout_device(
    session_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """登出指定的远程设备"""
    result = AuthService.logout_device(db, current_user.id, session_id)

    if result["success"]:
        return Result.success(msg=result["message"])
    return Result.error(msg=result["message"])


@router.patch("/user/status/{user_id}", summary="用户启用/禁用")
@log_api_call("toggle_user_status")
async def toggle_user_status(
    user_id: int,
    request: Request,
    current_user: User = Depends(get_auth_dependency(['user:update', 'admin:manage'])),
    db: Session = Depends(get_db)
):
    """启用或禁用用户"""
    try:
        # 获取请求体
        body = await request.json()
        is_active = body.get("is_active")
        
        if is_active is None:
            return Result.error(400, "is_active 参数是必需的")
        
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")
        
        user.is_active = is_active
        db.commit()
        db.refresh(user)
        
        return Result.success(200, f"用户{user.is_active and '启用' or '禁用'}成功", {
            "id": user.id,
            "username": user.username,
            "is_active": user.is_active
        })
    except Exception as e:
        db.rollback()
        return Result.error(500, str(e))


@router.post("/user/force_logout/{user_id}", summary="强制用户下线")
@log_api_call("force_logout_user")
async def force_logout_user(
    user_id: int,
    current_user: User = Depends(get_auth_dependency(['user:update', 'admin:manage'])),
    db: Session = Depends(get_db)
):
    """强制用户下线（撤销所有会话）"""
    try:
        from users.services.auth_service import SessionService
        count = SessionService.revoke_all_sessions(db, user_id)
        
        return Result.success(200, f"强制下线成功，已撤销 {count} 个会话", {"revoked_count": count})
    except Exception as e:
        return Result.error(500, str(e))
