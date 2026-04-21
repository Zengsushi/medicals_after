"""
    RBAC 权限模型（角色、权限）
    从 users/rbac_models.py 迁移 Role 和 Permission 模型
    这些模型与用户模块紧密关联，因此放在 user 模块下
"""
from sqlalchemy import Column, func, ForeignKey, Table
from sqlalchemy import Integer, DateTime, String, Boolean, Text
from sqlalchemy.orm import relationship
from apps.core.database import Base
from datetime import datetime


role_permission = Table(
    "role_permission",
    Base.metadata,
    Column(
        "role_id",
        Integer,
        ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True
    ),
    Column(
        "permission_id",
        Integer,
        ForeignKey("permissions.id", ondelete="CASCADE"),
        primary_key=True
    ),
)


class Permission(Base):
    """
        权限模型
    """
    __tablename__ = "permissions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False, comment="权限名称")
    code = Column(String(100), unique=True, nullable=False, index=True, comment="权限代码(唯一)")
    description = Column(String(500), nullable=True, comment="权限描述")
    module = Column(String(100), nullable=True, comment="所属模块")
    is_active = Column(Boolean, default=True, comment="是否启用")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")

    roles = relationship(
        "Role",
        secondary=role_permission,
        back_populates="permissions"
    )

    def __repr__(self) -> str:
        return f"<Permission(id={self.id}, name='{self.name}', code='{self.code}')>"


class Role(Base):
    """
        角色模型 - 扩展原有 Role
    """
    __tablename__ = "roles"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), unique=True, nullable=False, comment="角色名称")
    code = Column(String(100), unique=True, nullable=False, index=True, comment="角色代码")
    description = Column(String(500), nullable=True, comment="角色描述")
    is_active = Column(Boolean, default=True, comment="是否启用")
    is_system = Column(Boolean, default=False, comment="是否系统角色(不可删除)")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")

    permissions = relationship(
        "Permission",
        secondary=role_permission,
        back_populates="roles"
    )

    menus = relationship(
        "Menu",
        secondary="role_menu",
        back_populates="roles"
    )

    users = relationship(
        "User",
        secondary="user_role",
        back_populates="roles"
    )

    def __repr__(self) -> str:
        return f"<Role(id={self.id}, name='{self.name}', code='{self.code}')>"
