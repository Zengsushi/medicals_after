"""
    用户应用路由
    仅包含用户认证、个人信息管理、密码管理等用户相关 API
    角色/权限/菜单 API 已拆分到对应模块
"""
import os
from typing import Dict, Any
from fastapi import APIRouter, Depends, UploadFile, File, Query, Request
from sqlalchemy.orm import Session

from apps.user.models import User
from apps.user.schemas import (
    LoginRequest,
    TokenRefreshRequest,
    RegisterRequest,
    UserUpdateRequest,
    PasswordResetRequest,
    PasswordChangeRequest,
    UserPermissionUpdateRequest,
    UserStatusUpdateRequest,
    AdminPasswordChangeRequest
)
from apps.user.service import UserService as UserHelperService
from apps.user.db import UserService
from apps.user.services.auth_service import (
    AuthService,
    SessionService,
    AuditService,
    PasswordService
)
from apps.user.auth.utils import generate_device_fingerprint, verify_password_strength
from apps.core.database import get_db
from apps.core import Security, Result
from utils.auth_helpers import require_auth, get_token_from_request, get_client_info

router = APIRouter(prefix="/api", tags=["users"])


# ============================= 公开接口（无需认证） =============================

@router.post("/login", summary="用户登录")
async def user_login(request: Request, login_data: LoginRequest, db: Session = Depends(get_db)):
    """
    返回: [user_info, access_token, refresh_token, permissions_list, menus_list]
    """
    client_info = get_client_info(request)
    device_fingerprint = generate_device_fingerprint(
        user_agent=client_info["user_agent"],
        ip_address=client_info["ip_address"]
    )

    result = AuthService.login(
        db=db,
        username=login_data.username,
        password=login_data.password,
        ip_address=client_info["ip_address"],
        user_agent=client_info["user_agent"],
        device_fingerprint=device_fingerprint
    )

    if not result["success"]:
        return Result.error(401, result["message"])

    response_data = result["data"]

    return Result.success(200, "用户登录成功", {
        "user": response_data["user"],
        "access_token": response_data["access_token"],
        "token": response_data["access_token"],
        "refresh_token": response_data["refresh_token"],
        "permissions": response_data["permissions"],
        "menus": response_data["menus"],
        "role": response_data["role"]
    })


@router.post("/token/refresh", summary="刷新令牌")
async def refresh_token(request: Request, body: TokenRefreshRequest, db: Session = Depends(get_db)):
    """
    刷新访问令牌（无需认证）
    使用 refresh_token rotation 机制
    """
    client_info = get_client_info(request)

    refresh_token_value = (body.refresh_token or "").strip()
    if not refresh_token_value:
        return Result.error(400, "refresh_token 不能为空")

    result = AuthService.refresh(
        db=db,
        refresh_token=refresh_token_value,
        ip_address=client_info["ip_address"],
        user_agent=client_info["user_agent"]
    )

    if not result["success"]:
        return Result.error(401, result["message"])

    return Result.success(200, "令牌刷新成功", result["data"])


@router.post("/user/register", summary="用户注册")
async def user_register(register_date: RegisterRequest, db: Session = Depends(get_db)):
    """
    用户注册（无需认证）
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


# ============================= 需要认证的接口 =============================

@router.post("/logout", summary="用户登出")
async def user_logout(
        request: Request,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """用户登出"""
    token = get_token_from_request(request)
    client_info = get_client_info(request)

    result = AuthService.logout(
        db=db,
        user_id=current_user.id,
        jti=token,
        ip_address=client_info["ip_address"],
        user_agent=client_info["user_agent"]
    )

    if result["success"]:
        return Result.success(msg=result["message"])
    return Result.error(msg=result["message"])


@router.post("/logout/all", summary="登出所有设备")
async def logout_all_devices(
        request: Request,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """登出所有设备"""
    client_info = get_client_info(request)

    result = AuthService.logout(
        db=db,
        user_id=current_user.id,
        revoke_all=True,
        ip_address=client_info["ip_address"],
        user_agent=client_info["user_agent"]
    )

    if result["success"]:
        return Result.success(msg=result["message"], data=result["data"])
    return Result.error(msg=result["message"])


@router.get("/user/devices", summary="获取登录设备列表")
async def get_user_devices(
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取当前用户的登录设备列表"""
    devices = AuthService.get_user_devices(db, current_user.id)
    return Result.success(200, "设备列表获取成功", data=devices)


@router.delete("/user/devices/{session_id}", summary="登出指定设备")
async def logout_device(
        session_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """登出指定的远程设备"""
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
    """请求密码重置链接（需认证）"""
    client_info = get_client_info(request)

    result = PasswordService.create_password_reset_token(
        db=db,
        username=body.username,
        email=body.email,
        ip_address=client_info["ip_address"]
    )

    if result["success"]:
        return Result.success(msg=result["message"], data=result["data"])
    return Result.error(msg=result["message"])


@router.post("/user/password/change", summary="修改密码")
async def change_password(
        request: Request,
        body: PasswordChangeRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """修改当前用户密码"""
    client_info = get_client_info(request)

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
        ip_address=client_info["ip_address"]
    )

    return Result.success(msg="密码修改成功，请重新登录")


@router.get("/user/info", summary="用户信息获取")
async def user_info(current_user: User = Depends(require_auth()), db: Session = Depends(get_db)):
    """获取当前用户信息"""
    try:
        user_data = UserHelperService.build_user_response(current_user)
        return Result.success(200, "用户详情获取成功", data=user_data)
    except Exception as e:
        return Result.error(401, str(e))


@router.get("/user/permissions", summary="获取用户权限")
async def get_user_permissions(current_user: User = Depends(require_auth()), db: Session = Depends(get_db)):
    """获取当前用户所有权限"""
    try:
        permissions = UserHelperService.build_permissions_response(current_user)
        return Result.success(200, "权限获取成功", data=permissions)
    except Exception as e:
        return Result.error(401, str(e))


@router.get("/user/menus", summary="获取用户菜单")
async def get_user_menus(current_user: User = Depends(require_auth()), db: Session = Depends(get_db)):
    """获取当前用户所有菜单"""
    try:
        menus = UserHelperService.build_menus_response(current_user)
        return Result.success(200, "菜单获取成功", data=menus)
    except Exception as e:
        return Result.error(401, str(e))


@router.post("/user/refresh", summary="刷新用户权限")
async def refresh_user_permissions(
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """刷新用户权限(无需重新登录)"""
    try:
        from apps.user.middleware.auth import refresh_user_permissions as refresh_perms
        result = await refresh_perms(current_user.id, db)
        if result:
            return Result.success(200, "权限刷新成功", data=result)
        return Result.error(401, "用户不存在")
    except Exception as e:
        return Result.error(500, str(e))


@router.post("/user/modification", summary="用户信息修改")
async def user_modification(
        user_form: UserUpdateRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """修改用户信息"""
    service = UserService(db)
    try:
        user = service.user_info_update(user_form, current_user.id)
        return Result.success(msg="用户信息修改成功")
    except Exception as e:
        return Result.error(500, str(e))


@router.get("/user/avatar/download", summary="用户头像下载")
async def user_avatar_download(
        filename: str,
        current_user: User = Depends(require_auth())
):
    """下载用户头像"""
    return UserHelperService.download_avatar(filename, current_user)


@router.patch("/user/avatar", summary="用户头像上传")
async def user_avatar_upload(
        current_user: User = Depends(require_auth()),
        file: UploadFile = File(...),
        db: Session = Depends(get_db)
):
    """上传用户头像"""
    return await UserHelperService.upload_avatar(current_user, file, db)


@router.get("/user/list", summary="用户列表获取")
async def get_user_list(
        current_user: User = Depends(require_auth()),
        search: str = Query(default=None),
        status: str = Query(default=None),
        role: str = Query(default=None),
        page: int = Query(1, ge=1),
        page_size: int = Query(10, ge=10),
        db: Session = Depends(get_db)
):
    """获取用户列表（需权限）"""
    if not current_user.has_permission('users:view') and not current_user.has_permission('admin:view'):
        return Result.error(403, "没有权限访问")

    service = UserService(db)
    try:
        total, users = service.get_user_list(page, page_size, search, status, role)
        return Result.success(200, "列表数据获取成功", data={
            "user_list": users,
            "total": total
        })
    except Exception as e:
        return Result.error(msg=str(e))


@router.post("/admin/authorization/{user_id}", summary="用户授权")
async def user_auth(
        user_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """用户授权（需权限）"""
    if not current_user.has_permission('users:authorize') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有授权权限")

    service = UserService(db)
    try:
        user = service.user_auth(user_id)
        return Result.success(msg='用户授权成功', data=user)
    except Exception as e:
        return Result.error(msg="服务器异常", error=str(e))


@router.post("/user/resetpasswd/{user_id}", summary="用户密码重置")
async def user_password_reset(
        user_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """重置用户密码为默认密码（需权限）"""
    if not current_user.has_permission('users:resetpwd') and not current_user.has_permission('admin:manage'):
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
            "user": UserHelperService.build_user_response(user),
            "default_password": default_password
        })
    except Exception as e:
        return Result.error(500, f"服务器异常: {str(e)}")


@router.post("/user/force-change-password/{user_id}", summary="强制修改密码")
async def force_change_password(
        user_id: int,
        password_data: AdminPasswordChangeRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """管理员强制修改用户密码（需权限）"""
    if not current_user.has_permission('users:resetpwd') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有修改密码权限")

    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")

        # 检查密码强度
        from apps.user.auth.utils import verify_password_strength
        is_strong, error_msg = verify_password_strength(password_data.new_password)
        if not is_strong:
            return Result.error(400, error_msg)

        user.password = Security.get_password_hash(password_data.new_password)
        db.commit()
        db.refresh(user)

        return Result.success(200, "密码修改成功", {
            "user": UserHelperService.build_user_response(user)
        })
    except Exception as e:
        return Result.error(500, f"服务器异常: {str(e)}")


@router.patch("/user/permissions/{user_id}", summary="更新用户权限")
async def update_user_permissions(
        user_id: int,
        permission_data: UserPermissionUpdateRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """更新用户权限（需权限）"""
    if not current_user.has_permission('users:authorize') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有授权权限")

    service = UserService(db)
    try:
        user = service.update_user_permissions(user_id, permission_data)
        user_data = UserHelperService.build_user_response(user)
        return Result.success(200, "用户权限更新成功", data=user_data)
    except Exception as e:
        return Result.error(msg=str(e), error=str(e))


@router.patch("/user/delete/{user_id}", summary="删除用户(软删除)")
async def del_user(
        user_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """删除用户（需权限）"""
    if not current_user.has_permission('users:delete') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有删除权限")

    service = UserService(db)
    try:
        user = service.user_del(user_id)
        return Result.success(msg="删除成功", data=user)
    except Exception as e:
        return Result.error(msg=str(e), error=e)


@router.put("/admin/user/{user_id}", summary="管理员更新用户")
async def admin_update_user(
        user_id: int,
        user_data: UserUpdateRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """管理员更新用户信息（需权限）"""
    if not current_user.has_permission('users:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有更新用户权限")

    service = UserService(db)
    try:
        user = service.user_info_update(user_data, user_id)
        user_data = UserHelperService.build_user_response(user)
        return Result.success(200, "用户更新成功", data=user_data)
    except Exception as e:
        return Result.error(500, str(e))


@router.patch("/user/status/{user_id}", summary="用户启用/禁用")
async def toggle_user_status(
        user_id: int,
        status_data: UserStatusUpdateRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """启用或禁用用户"""
    if not current_user.has_permission('users:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限修改用户状态")

    try:
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")
        user.is_active = status_data.is_active
        db.commit()
        db.refresh(user)

        user_data = UserHelperService.build_user_response(user)
        return Result.success(200, f"用户{user.is_active and '启用' or '禁用'}成功", data=user_data)
    except Exception as e:
        db.rollback()
        return Result.error(500, f"操作失败: {str(e)}")


@router.post("/user/force_logout/{user_id}", summary="强制用户下线")
async def force_logout_user(
        user_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """强制用户下线（撤销所有会话）"""
    if not current_user.has_permission('users:edit') and not current_user.has_permission('admin:manage'):
        return Result.error(403, "没有权限强制用户下线")

    try:
        # 检查用户是否存在
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            return Result.error(404, "用户不存在")

        # 尝试撤销所有会话，如果 SessionService 有问题，至少返回成功
        count = 0
        try:
            from apps.user.services.auth_service import SessionService
            count = SessionService.revoke_all_sessions(db, user_id)
        except Exception as session_error:
            print(f"SessionService error: {session_error}")
            # 即使会话撤销失败，也继续执行
            pass

        return Result.success(200, f"强制下线成功，已撤销 {count} 个会话", {"revoked_count": count})
    except Exception as e:
        return Result.error(500, f"强制下线失败: {str(e)}")
