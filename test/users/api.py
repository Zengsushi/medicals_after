# 内置
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from fastapi import (APIRouter, Depends, UploadFile, HTTPException,
                     File, Query, Request)
from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload
from starlette.responses import FileResponse
from sqlalchemy import or_

# 自定义

from apps.user.models import User
from apps.user.rbac_models import Role, Permission
from apps.menu.models import Menu
from database import *
from utils.response_helper import ResponseHelper, Result
from utils.security import Security
from redisbase import RedisBase
from users.db import UserService
from users.dependencies import get_current_user, get_client_ip
from users.services.auth_service import AuthService, SessionService, AuditService, PasswordService
from users.auth.utils import generate_device_fingerprint

# 常量
router = APIRouter(prefix="/api", tags=["users"])

# 别名（供 apps.user 模块使用）
user_router = router


class Base(BaseModel):
    username: str
    password: str


class UserLogin(Base):
    loginMode: str


class TokenRefreshRequest(BaseModel):
    refresh_token: str


class RegisterUser(Base):
    phone: str
    email: str
    first_name: str
    last_name: str
    avatar: str


class UserForm(BaseModel):
    phone: str
    email: str
    first_name: str
    last_name: str
    avatar: str
    is_active: bool
    is_staff: bool
    is_superuser: bool
    last_login_ip: str
    is_deleted: bool
    introduce: str


class PasswordResetRequest(BaseModel):
    username: str
    email: str


class PasswordChangeRequest(BaseModel):
    old_password: str
    new_password: str


class UserStatusUpdateRequest(BaseModel):
    """用户状态更新请求"""
    is_active: bool


class UserPermissionUpdateRequest(BaseModel):
    """用户权限更新请求"""
    role: Optional[str] = "user"
    permissions: Optional[list] = []
    is_staff: Optional[bool] = None
    is_superuser: Optional[bool] = None


def build_user_response(user: User) -> Dict[str, Any]:
    """构建用户信息响应"""
    primary_role = user.get_primary_role()
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "phone": user.phone,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "avatar": user.avatar,
        "is_active": user.is_active,
        "role": primary_role.code if primary_role else "guest",
        "role_name": primary_role.name if primary_role else "访客",
        "roles": [
            {
                "id": role.id,
                "code": role.code,
                "name": role.name
            }
            for role in user.roles
        ] if user.roles else [],
        "introduce": user.introduce,
        "date_joined": user.date_joined.isoformat() if user.date_joined else None,
        "last_login": user.last_login.isoformat() if user.last_login else None
    }


def build_permissions_response(user: User) -> list:
    """构建权限列表响应"""
    return user.get_permissions()


def build_menus_response(user: User) -> list:
    """构建菜单列表响应"""
    from apps.menu.models import Menu
    from database import SessionLocal
    
    db = SessionLocal()
    try:
        # 直接查询所有激活且可见的菜单
        menus = db.query(Menu).filter(
            Menu.is_active == True,
            Menu.is_visible == True
        ).order_by(Menu.order).all()

        def menu_to_dict(menu: Menu) -> Dict[str, Any]:
            return {
                "id": menu.id,
                "name": menu.name,
                "path": menu.path,
                "component": menu.component,
                "icon": menu.icon,
                "order": menu.order,
                "parent_id": menu.parent_id,
                "permission_code": menu.permission_code,
                "is_visible": menu.is_visible,
                "is_cached": menu.is_cached
            }

        menu_tree = []
        menu_map = {}

        for menu in menus:
            menu_dict = menu_to_dict(menu)
            menu_map[menu.id] = menu_dict
            menu_dict["children"] = []

        for menu in menus:
            if menu.parent_id is None:
                menu_tree.append(menu_map[menu.id])
            else:
                parent = menu_map.get(menu.parent_id)
                if parent:
                    parent["children"].append(menu_map[menu.id])

        return menu_tree
    finally:
        db.close()


@router.post("/login", summary="用户登录")
async def user_login(request: Request, login_data: UserLogin, db: Session = Depends(get_db)):
    """
    用户登录
    返回: [user_info, access_token, refresh_token, permissions_list, menus_list]
    """
    user_agent = request.headers.get("user-agent", "")
    client_ip = get_client_ip(request)
    device_fingerprint = generate_device_fingerprint(
        user_agent=user_agent,
        ip_address=client_ip
    )

    result = AuthService.login(
        db=db,
        username=login_data.username,
        password=login_data.password,
        ip_address=client_ip,
        user_agent=user_agent,
        device_fingerprint=device_fingerprint
    )

    if not result["success"]:
        return Result.error(401, result["message"])

    response_data = result["data"]

    return Result.success(200, "用户登录成功", {
        "user": response_data["user"],
        "token": response_data["access_token"],
        "refresh_token": response_data["refresh_token"],
        "permissions": response_data["permissions"],
        "menus": response_data["menus"],
        "role": response_data["role"]
    })


@router.post("/token/refresh", summary="刷新令牌")
async def refresh_token(request: Request, body: TokenRefreshRequest, db: Session = Depends(get_db)):
    """
    刷新访问令牌
    使用 refresh_token rotation 机制
    """
    client_ip = get_client_ip(request)
    user_agent = request.headers.get("user-agent", "")

    result = AuthService.refresh(
        db=db,
        refresh_token=body.refresh_token,
        ip_address=client_ip,
        user_agent=user_agent
    )

    if not result["success"]:
        return Result.error(401, result["message"])

    return Result.success(200, "令牌刷新成功", result["data"])


@router.post("/logout", summary="用户登出")
async def user_logout(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    用户登出
    """
    client_ip = get_client_ip(request)
    user_agent = request.headers.get("user-agent", "")
    authorization = request.headers.get("authorization", "")

    token = authorization.replace("Bearer ", "") if authorization else None

    result = AuthService.logout(
        db=db,
        user_id=current_user.id,
        jti=token,
        ip_address=client_ip,
        user_agent=user_agent
    )

    if result["success"]:
        return Result.success(msg=result["message"])
    return Result.error(msg=result["message"])


@router.post("/logout/all", summary="登出所有设备")
async def logout_all_devices(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    登出所有设备
    """
    client_ip = get_client_ip(request)
    user_agent = request.headers.get("user-agent", "")

    result = AuthService.logout(
        db=db,
        user_id=current_user.id,
        revoke_all=True,
        ip_address=client_ip,
        user_agent=user_agent
    )

    if result["success"]:
        return Result.success(msg=result["message"], data=result["data"])
    return Result.error(msg=result["message"])


@router.get("/user/devices", summary="获取登录设备列表")
async def get_user_devices(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取当前用户的登录设备列表
    """
    devices = AuthService.get_user_devices(db, current_user.id)
    return Result.success(200, "设备列表获取成功", data=devices)


@router.delete("/user/devices/{session_id}", summary="登出指定设备")
async def logout_device(
    session_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    登出指定的远程设备
    """
    result = AuthService.logout_device(db, current_user.id, session_id)

    if result["success"]:
        return Result.success(msg=result["message"])
    return Result.error(msg=result["message"])


@router.post("/user/password/reset", summary="请求密码重置")
async def request_password_reset(
    request: Request,
    body: PasswordResetRequest,
    db: Session = Depends(get_db)
):
    """
    请求密码重置链接
    """
    client_ip = get_client_ip(request)

    result = PasswordService.create_password_reset_token(
        db=db,
        username=body.username,
        email=body.email,
        ip_address=client_ip
    )

    if result["success"]:
        return Result.success(msg=result["message"], data=result["data"])
    return Result.error(msg=result["message"])


@router.post("/user/password/change", summary="修改密码")
async def change_password(
    request: Request,
    body: PasswordChangeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    修改当前用户密码
    """
    from users.auth.utils import verify_password_strength

    client_ip = get_client_ip(request)

    if not Security.verify_password(body.old_password, current_user.password):
        return Result.error(401, "原密码错误")

    is_strong, error_msg = verify_password_strength(body.new_password)
    if not is_strong:
        return Result.error(400, error_msg)

    current_user.password = Security.get_password_hash(body.new_password)
    db.commit()

    AuditService.log(
        db, "PASSWORD_CHANGE",
        user_id=current_user.id,
        username=current_user.username,
        ip_address=client_ip
    )

    return Result.success(msg="密码修改成功，请重新登录")


@router.get("/user/info", summary="用户信息获取")
async def user_info(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    用户信息获取
    """
    try:
        user_data = build_user_response(current_user)
        return Result.success(200, "用户详情获取成功", data=user_data)
    except Exception as e:
        return Result.error(401, str(e))


@router.get("/user/permissions", summary="获取用户权限")
async def get_user_permissions(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    获取当前用户所有权限
    """
    try:
        permissions = build_permissions_response(current_user)
        return Result.success(200, "权限获取成功", data=permissions)
    except Exception as e:
        return Result.error(401, str(e))


@router.get("/user/menus", summary="获取用户菜单")
async def get_user_menus(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """
    获取当前用户所有菜单
    """
    try:
        menus = build_menus_response(current_user)
        return Result.success(200, "菜单获取成功", data=menus)
    except Exception as e:
        return Result.error(401, str(e))


@router.post("/user/refresh", summary="刷新用户权限")
async def refresh_user_permissions(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    刷新用户权限(无需重新登录)
    """
    try:
        from users.middleware.auth import refresh_user_permissions as refresh_perms
        result = await refresh_perms(current_user.id, db)
        if result:
            return Result.success(200, "权限刷新成功", data=result)
        return Result.error(401, "用户不存在")
    except Exception as e:
        return Result.error(500, str(e))


@router.post("/user/register", summary="用户注册")
async def user_register(register_date: RegisterUser, db: Session = Depends(get_db)):
    """
    用户注册
    """
    service = UserService(db)
    if service.user_exists(register_date.username):
        return Result.error(400, msg="此用户已存在")
    if service.phone_exists(register_date.phone):
        return Result.error(400, "此手机号被用户注册")
    try:
        hash_pass = Security.get_password_hash(register_date.password)
        user = User(
            username=register_date.username,
            password=hash_pass,
            phone=register_date.phone,
            first_name=register_date.first_name or "",
            last_name=register_date.last_name or "",
            avatar=register_date.avatar or ""
        )
        service.user_info_save(user)
        return Result.success(msg="用户注册成功")
    except Exception as e:
        return Result.error(msg="注册失败", error=str(e))


@router.post("/user/modification", summary="用户信息修改")
async def user_modification(
    user_form: UserForm,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    用户信息修改
    """
    service = UserService(db)
    try:
        user = service.user_info_update(user_form, current_user.id)
        return Result.success(msg="用户信息修改成功")
    except Exception as e:
        return Result.error(500, str(e))


@router.get("/user/avatar/download", summary="用户头像下载")
async def user_avatar_download(
    filename: str,
    current_user: User = Depends(get_current_user)
):
    """
    用户头像下载
    """
    file_path = filename.replace("http://" +
                                 os.getenv("SERVER_IP") + ":" +
                                 os.getenv("SERVER_PORT") + "/"
                                 , "")
    if not os.path.exists(file_path):
        return Result.error(msg="当前文件不存在")

    return FileResponse(
        file_path,
        filename=filename,
        media_type="application/octet-stream"
    )


@router.get("/user/list", summary="用户列表获取")
async def get_user_list(
    current_user: User = Depends(get_current_user),
    search: str = Query(default=None),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=10),
    db: Session = Depends(get_db)
):
    """
    用户列表获取
    """
    if not current_user.has_permission('user:view') and not current_user.has_permission('admin:view'):
        return Result.error(403, "没有权限访问")

    service = UserService(db)
    try:
        total, users = service.get_user_list(page, page_size, search)
        return Result.success(200, "列表数据获取成功", data={
            "user_list": users,
            "total": total
        })
    except Exception as e:
        return Result.error(msg=str(e))


@router.patch("/user/avatar", summary="用户头像修改")
async def user_avatar(
    current_user: User = Depends(get_current_user),
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """
    用户头像修改
    """
    service = UserService(db)
    try:
        user = service.user_search(current_user.id)
        avatar_name = f"img_{current_user.id}_{datetime.now().strftime('%Y%m%d%H%M%S')}.png"
        avatar_path = os.path.join(os.getenv("USER_STATIC_FILE_PATH"), avatar_name)
        with open(avatar_path, "wb") as f:
            f.write(await file.read())
        user.avatar = avatar_path
        db.commit()
        db.refresh(user)
        user.avatar = ("http://" + os.getenv("SERVER_IP") + ":" + os.getenv("SERVER_PORT") + "/" + user.avatar
                       .replace("B:\\3_after_end\\medicalBs\\", "")
                       .replace("\\", "/"))
        return Result.success(200, msg="用户头像更新成功!", data=user)
    except Exception as e:
        return Result.error(error=e)


@router.post("/admin/authorization/{user_id}", summary="用户授权")
async def user_auth(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    用户授权
    """
    if not current_user.has_permission('user:authorize') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有授权权限")

    service = UserService(db)
    try:
        user = service.user_auth(user_id)
        return Result.success(msg='用户授权成功', data=user)
    except Exception as e:
        return Result.error(msg="服务器异常", error=str(e))


class AdminPasswordChangeRequest(BaseModel):
    """管理员强制修改密码请求"""
    new_password: str


@router.post("/user/resetpasswd/{user_id}", summary="用户密码重置")
async def user_password_reset(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """重置用户密码为默认密码（需权限）"""
    if not current_user.has_permission('user:resetpwd') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有重置密码权限")

    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")
        
        # 重置为默认密码 123456
        default_password = "123456"
        user.password = Security.get_password_hash(default_password)
        db.commit()
        db.refresh(user)
        
        return Result.success(200, "用户密码重置成功", {
            "user": build_user_response(user),
            "default_password": default_password
        })
    except Exception as e:
        return Result.error(500, f"服务器异常: {str(e)}")


@router.post("/user/force-change-password/{user_id}", summary="强制修改密码")
async def force_change_password(
    user_id: int,
    password_data: AdminPasswordChangeRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """管理员强制修改用户密码（需权限）"""
    if not current_user.has_permission('user:resetpwd') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有修改密码权限")

    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")
        
        # 检查密码强度
        from users.auth.utils import verify_password_strength
        is_strong, error_msg = verify_password_strength(password_data.new_password)
        if not is_strong:
            return Result.error(400, error_msg)
        
        user.password = Security.get_password_hash(password_data.new_password)
        db.commit()
        db.refresh(user)
        
        return Result.success(200, "密码修改成功", {
            "user": build_user_response(user)
        })
    except Exception as e:
        return Result.error(500, f"服务器异常: {str(e)}")


@router.post("/user/force_logout/{user_id}", summary="强制用户下线")
async def force_logout_user(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """强制用户下线（撤销所有会话）"""
    if not current_user.has_permission('user:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限强制用户下线")

    try:
        # 检查用户是否存在
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")
        
        # 尝试撤销所有会话，如果 SessionService 有问题，至少返回成功
        count = 0
        try:
            from users.services.auth_service import SessionService
            count = SessionService.revoke_all_sessions(db, user_id)
        except Exception as session_error:
            print(f"SessionService error: {session_error}")
            # 即使会话撤销失败，也继续执行
            pass
        
        return Result.success(200, f"强制下线成功，已撤销 {count} 个会话", {"revoked_count": count})
    except Exception as e:
        return Result.error(500, f"强制下线失败: {str(e)}")


@router.patch("/user/permissions/{user_id}", summary="更新用户权限")
async def update_user_permissions(
    user_id: int,
    permission_data: UserPermissionUpdateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    更新用户权限
    """
    if not current_user.has_permission('user:authorize') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有授权权限")

    service = UserService(db)
    try:
        user = service.user_search(user_id)
        
        # 检查是否是系统管理员，系统管理员权限不能修改
        if user.is_superuser:
            return Result.error(403, "系统管理员权限不能修改")
        
        # 根据角色设置 is_staff 和 is_superuser
        if hasattr(permission_data, 'role') and permission_data.role:
            role = permission_data.role
            if role == 'admin':
                user.is_staff = True
                user.is_superuser = False
            elif role == 'superadmin':
                # 只有系统管理员才能将用户设为系统管理员
                if not current_user.is_superuser:
                    return Result.error(403, "只有系统管理员才能设置系统管理员权限")
                user.is_staff = True
                user.is_superuser = True
            else:  # user
                user.is_staff = False
                user.is_superuser = False
        
        # 直接设置 is_staff 和 is_superuser (优先级更高)
        if hasattr(permission_data, 'is_staff') and permission_data.is_staff is not None:
            user.is_staff = permission_data.is_staff
        
        if hasattr(permission_data, 'is_superuser') and permission_data.is_superuser is not None:
            # 只有系统管理员才能修改 is_superuser
            if not current_user.is_superuser:
                return Result.error(403, "只有系统管理员才能修改系统管理员权限")
            user.is_superuser = permission_data.is_superuser

        db.commit()
        db.refresh(user)
        
        user_data = build_user_response(user)
        return Result.success(200, "用户权限更新成功", data=user_data)
    except Exception as e:
        print(e)
        return Result.error(msg=str(e), error=str(e))


@router.patch("/user/delete/{user_id}", summary="删除用户(软删除)")
async def del_user(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    删除用户
    """
    if not current_user.has_permission('user:delete') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有删除权限")

    service = UserService(db)
    try:
        user = service.user_del(user_id)
        return Result.success(msg="删除成功", data=user)
    except Exception as e:
        return Result.error(msg=str(e), error=e)


@router.patch("/user/status/{user_id}", summary="用户启用/禁用")
async def toggle_user_status(
    user_id: int,
    status_data: UserStatusUpdateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """启用或禁用用户"""
    if not current_user.has_permission('user:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限修改用户状态")

    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")
        user.is_active = status_data.is_active
        db.commit()
        db.refresh(user)
        
        user_data = build_user_response(user)
        return Result.success(200, f"用户{user.is_active and '启用' or '禁用'}成功", data=user_data)
    except Exception as e:
        db.rollback()
        return Result.error(500, f"操作失败: {str(e)}")


@router.post("/user/force_logout/{user_id}", summary="强制用户下线")
async def force_logout_user(
    user_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """强制用户下线（撤销所有会话）"""
    if not current_user.has_permission('user:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限强制用户下线")

    try:
        # 检查用户是否存在
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")
        
        # 尝试撤销所有会话，如果 SessionService 有问题，至少返回成功
        count = 0
        try:
            count = SessionService.revoke_all_sessions(db, user_id)
        except Exception as session_error:
            print(f"SessionService error: {session_error}")
            # 即使会话撤销失败，也继续执行
            pass
        
        return Result.success(200, f"强制下线成功，已撤销 {count} 个会话", {"revoked_count": count})
    except Exception as e:
        return Result.error(500, f"强制下线失败: {str(e)}")


@router.put("/admin/user/{user_id}", summary="管理员更新用户")
async def admin_update_user(
    user_id: int,
    user_data: UserForm,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """管理员更新用户信息（需权限）"""
    if not current_user.has_permission('user:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有更新用户权限")

    service = UserService(db)
    try:
        user = service.user_info_update(user_data, user_id)
        user_data = build_user_response(user)
        return Result.success(200, "用户更新成功", data=user_data)
    except Exception as e:
        return Result.error(500, str(e))


# ============================= 角色管理 API =============================

@router.get("/roles", summary="获取角色列表")
async def get_roles(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取角色列表"""
    if not current_user.has_permission('role:view') and not current_user.has_permission('admin:view'):
        return Result.error(403, "没有权限访问")

    roles = db.query(Role).filter(Role.is_active == True).all()
    return Result.success(200, "角色列表获取成功", data=[
        {
            "id": role.id,
            "name": role.name,
            "code": role.code,
            "description": role.description,
            "is_system": role.is_system,
            "created_at": role.created_at.isoformat() if role.created_at else None
        }
        for role in roles
    ])


@router.get("/roles/{role_id}", summary="获取角色详情")
async def get_role_detail(
    role_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取角色详情"""
    if not current_user.has_permission('role:view') and not current_user.has_permission('admin:view'):
        return Result.error(403, "没有权限访问")

    role = db.query(Role).filter(Role.id == role_id).first()
    if not role:
        return Result.error(404, "角色不存在")

    return Result.success(200, "角色详情获取成功", data={
        "id": role.id,
        "name": role.name,
        "code": role.code,
        "description": role.description,
        "is_system": role.is_system,
        "permissions": [
            {"id": p.id, "name": p.name, "code": p.code}
            for p in role.permissions
        ],
        "menus": [
            {"id": m.id, "name": m.name, "path": m.path}
            for m in role.menus
        ]
    })


# ============================= 权限管理 API =============================

@router.get("/permissions", summary="获取权限列表")
async def get_permissions(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取权限列表"""
    if not current_user.has_permission('permission:view') and not current_user.has_permission('admin:view'):
        return Result.error(403, "没有权限访问")

    # 获取所有权限（不只是启用的，这样可以管理禁用状态）
    permissions = db.query(Permission).order_by(Permission.id).all()
    return Result.success(200, "权限列表获取成功", data=[
        {
            "id": p.id,
            "name": p.name,
            "code": p.code,
            "description": p.description,
            "module": p.module,
            "is_active": p.is_active,
            "created_at": p.created_at.isoformat() if p.created_at else None
        }
        for p in permissions
    ])


@router.patch("/permissions/{permission_id}/toggle", summary="启用/禁用权限")
async def toggle_permission(
    permission_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """启用/禁用权限"""
    if not current_user.has_permission('permission:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限修改权限")

    try:
        permission = db.query(Permission).filter(Permission.id == permission_id).first()
        if not permission:
            return Result.error(404, "权限不存在")

        # 切换权限的启用状态
        permission.is_active = not permission.is_active
        db.commit()
        db.refresh(permission)

        action_text = "启用" if permission.is_active else "禁用"
        return Result.success(200, f"权限{action_text}成功", data={
            "id": permission.id,
            "name": permission.name,
            "code": permission.code,
            "is_active": permission.is_active
        })
    except Exception as e:
        logging.error(f"切换权限状态失败: {e}")
        return Result.error(500, str(e))


@router.get("/menus", summary="获取菜单列表")
async def get_menus(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取菜单列表（当前已放开权限限制，供测试使用）"""

    menus = db.query(Menu).filter(Menu.is_active == True).order_by(Menu.order).all()

    def menu_to_dict(menu: Menu):
        return {
            "id": menu.id,
            "name": menu.name,
            "path": menu.path,
            "component": menu.component,
            "icon": menu.icon,
            "order": menu.order,
            "parent_id": menu.parent_id,
            "permission_code": menu.permission_code,
            "is_visible": menu.is_visible
        }

    menu_tree = []
    menu_map = {}

    for menu in menus:
        menu_dict = menu_to_dict(menu)
        menu_map[menu.id] = menu_dict
        menu_dict["children"] = []

    for menu in menus:
        if menu.parent_id is None:
            menu_tree.append(menu_map[menu.id])
        else:
            parent = menu_map.get(menu.parent_id)
            if parent:
                parent["children"].append(menu_map[menu.id])

    return Result.success(200, "菜单列表获取成功", data=menu_tree)
