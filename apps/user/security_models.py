"""
    安全相关模型
    - DeviceSession: 设备会话管理
    - AuditLog: 审计日志
    - RefreshToken: 刷新令牌
"""
from sqlalchemy import Column, func, ForeignKey, Index
from sqlalchemy import Integer, DateTime, String, Boolean, Text, JSON
from sqlalchemy.orm import relationship
from apps.core.database import Base
from datetime import datetime


class DeviceSession(Base):
    """
        设备会话模型
    """
    __tablename__ = "device_sessions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, comment="用户ID")

    jti = Column(String(64), unique=True, nullable=False, index=True, comment="JWT ID")
    device_fingerprint = Column(String(512), nullable=True, comment="设备指纹")
    user_agent = Column(String(512), nullable=True, comment="User-Agent")
    ip_address = Column(String(45), nullable=True, comment="IP地址")
    login_location = Column(String(255), nullable=True, comment="登录位置")
    device_type = Column(String(50), nullable=True, comment="设备类型")
    browser = Column(String(100), nullable=True, comment="浏览器")
    os = Column(String(100), nullable=True, comment="操作系统")

    is_active = Column(Boolean, default=True, comment="是否激活")
    is_suspicious = Column(Boolean, default=False, comment="是否可疑")
    last_active_at = Column(DateTime(timezone=True), nullable=True, comment="最后活跃时间")
    expired_at = Column(DateTime(timezone=True), nullable=True, comment="过期时间")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")

    user = relationship("User", foreign_keys=[user_id])

    __table_args__ = (
        Index('idx_user_id', 'user_id'),
        Index('idx_jti', 'jti'),
    )

    def __repr__(self):
        return f"<DeviceSession(id={self.id}, user_id={self.user_id}, device_type='{self.device_type}')>"


class AuditLog(Base):
    """
        审计日志模型
    """
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True, comment="用户ID")
    username = Column(String(100), nullable=True, comment="用户名")

    action = Column(String(100), nullable=False, index=True, comment="操作类型")
    resource = Column(String(100), nullable=True, comment="资源类型")
    resource_id = Column(String(100), nullable=True, comment="资源ID")
    description = Column(Text, nullable=True, comment="操作描述")

    ip_address = Column(String(45), nullable=True, comment="IP地址")
    user_agent = Column(String(512), nullable=True, comment="User-Agent")
    device_fingerprint = Column(String(512), nullable=True, comment="设备指纹")

    status = Column(String(20), default="success", comment="操作状态: success/failed/warning")
    error_message = Column(Text, nullable=True, comment="错误信息")

    request_data = Column(JSON, nullable=True, comment="请求数据(脱敏)")
    response_data = Column(JSON, nullable=True, comment="响应数据")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True, comment="创建时间")

    user = relationship("User", foreign_keys=[user_id])

    __table_args__ = (
        Index('idx_audit_user_id', 'user_id'),
        Index('idx_audit_action', 'action'),
        Index('idx_audit_created_at', 'created_at'),
    )

    def __repr__(self):
        return f"<AuditLog(id={self.id}, user='{self.username}', action='{self.action}')>"


class RefreshToken(Base):
    """
        刷新令牌模型
    """
    __tablename__ = "refresh_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, comment="用户ID")
    device_session_id = Column(Integer, ForeignKey("device_sessions.id", ondelete="CASCADE"), nullable=True, comment="设备会话ID")

    jti = Column(String(64), unique=True, nullable=False, index=True, comment="JWT ID")
    token_hash = Column(String(128), nullable=False, comment="令牌哈希")
    token_type = Column(String(20), default="refresh", comment="令牌类型")

    device_fingerprint = Column(String(512), nullable=True, comment="设备指纹")
    ip_address = Column(String(45), nullable=True, comment="IP地址")

    is_used = Column(Boolean, default=False, comment="是否已使用")
    is_revoked = Column(Boolean, default=False, comment="是否已撤销")
    is_expired = Column(Boolean, default=False, comment="是否已过期")

    expires_at = Column(DateTime(timezone=True), nullable=False, comment="过期时间")
    used_at = Column(DateTime(timezone=True), nullable=True, comment="使用时间")
    revoked_at = Column(DateTime(timezone=True), nullable=True, comment="撤销时间")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")

    user = relationship("User", foreign_keys=[user_id])
    device_session = relationship("DeviceSession", foreign_keys=[device_session_id])

    __table_args__ = (
        Index('idx_rt_user_id', 'user_id'),
        Index('idx_rt_jti', 'jti'),
    )

    def __repr__(self):
        return f"<RefreshToken(id={self.id}, user_id={self.user_id}, is_used={self.is_used})>"


class PasswordResetToken(Base):
    """
        密码重置令牌模型
    """
    __tablename__ = "password_reset_tokens"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, comment="用户ID")

    jti = Column(String(64), unique=True, nullable=False, index=True, comment="JWT ID")
    token_hash = Column(String(128), nullable=False, comment="令牌哈希")

    ip_address = Column(String(45), nullable=True, comment="IP地址")
    is_used = Column(Boolean, default=False, comment="是否已使用")
    is_expired = Column(Boolean, default=False, comment="是否已过期")

    expires_at = Column(DateTime(timezone=True), nullable=False, comment="过期时间")
    used_at = Column(DateTime(timezone=True), nullable=True, comment="使用时间")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")

    user = relationship("User", foreign_keys=[user_id])

    def __repr__(self):
        return f"<PasswordResetToken(id={self.id}, user_id={self.user_id}, is_used={self.is_used})>"


AUDIT_ACTIONS = {
    "LOGIN": "用户登录",
    "LOGOUT": "用户登出",
    "LOGIN_FAILED": "登录失败",
    "PASSWORD_CHANGE": "密码修改",
    "PASSWORD_RESET": "密码重置",
    "USER_CREATE": "创建用户",
    "USER_UPDATE": "更新用户",
    "USER_DELETE": "删除用户",
    "USER_AUTHORIZE": "用户授权",
    "ROLE_CREATE": "创建角色",
    "ROLE_UPDATE": "更新角色",
    "ROLE_DELETE": "删除角色",
    "PERMISSION_CHANGE": "权限变更",
    "SESSION_REVOKED": "会话撤销",
    "SUSPICIOUS_LOGIN": "可疑登录",
    "NEW_DEVICE_LOGIN": "新设备登录",
}
