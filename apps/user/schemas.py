"""
    用户模块 - Pydantic 请求/响应模型
"""
from pydantic import BaseModel
from pydantic import Field
from typing import Optional


class LoginRequest(BaseModel):
    """登录请求"""
    username: str
    password: str
    loginMode: str


class TokenRefreshRequest(BaseModel):
    """令牌刷新请求"""
    refresh_token: str = Field(..., alias="refreshToken")

    class Config:
        populate_by_name = True


class RegisterRequest(BaseModel):
    """注册请求"""
    username: str
    password: str
    phone: str
    email: Optional[str] = None
    first_name: Optional[str] = ""
    last_name: Optional[str] = ""
    avatar: Optional[str] = ""


class UserUpdateRequest(BaseModel):
    """用户信息更新请求"""
    phone: Optional[str] = None
    email: Optional[str] = None
    first_name: Optional[str] = ""
    last_name: Optional[str] = ""
    avatar: Optional[str] = ""
    is_active: Optional[bool] = None
    is_staff: Optional[bool] = None
    is_superuser: Optional[bool] = None
    last_login_ip: Optional[str] = ""
    is_deleted: Optional[bool] = None
    introduce: Optional[str] = ""


class PasswordResetRequest(BaseModel):
    """密码重置请求"""
    username: str
    email: str


class PasswordChangeRequest(BaseModel):
    """密码修改请求"""
    old_password: str
    new_password: str


class UserPermissionUpdateRequest(BaseModel):
    """用户权限更新请求"""
    role: Optional[str] = "user"
    permissions: Optional[list] = []
    is_staff: Optional[bool] = None
    is_superuser: Optional[bool] = None


class UserResponse(BaseModel):
    """用户信息响应"""
    id: int
    username: str
    email: Optional[str]
    phone: str
    first_name: Optional[str]
    last_name: Optional[str]
    avatar: Optional[str]
    is_active: bool
    role: str
    role_name: str
    roles: list
    introduce: Optional[str]
    date_joined: Optional[str]
    last_login: Optional[str]

    class Config:
        from_attributes = True


class UserStatusUpdateRequest(BaseModel):
    """用户状态更新请求"""
    is_active: bool


class AdminPasswordChangeRequest(BaseModel):
    """管理员强制修改密码请求"""
    new_password: str
