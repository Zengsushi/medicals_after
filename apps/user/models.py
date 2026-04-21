"""
    用户模型
    包含：BaseModel, User, UserProfile, user_role 关联表
    从 users/models.py 完整迁移
"""
from sqlalchemy import Column, func, ForeignKey, Table
from sqlalchemy import Integer, DateTime, String, Boolean
from sqlalchemy.orm import relationship, declared_attr
from apps.core.database import Base
from datetime import datetime


class BaseModel(Base):
    """通用模型 字段"""
    __abstract__ = True

    @declared_attr
    def id(self):
        return Column(Integer,
                      primary_key=True,
                      index=True,
                      comment=f"{self.__name__} 主键ID")

    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), comment="修改时间")


user_role = Table(
    "user_role",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
    Column("role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE"), primary_key=True),
)


class User(BaseModel):
    """
    用户模型
    """

    __tablename__ = "users"
    username = Column(String(100), index=True, unique=True, nullable=False, comment="用户名(账号)")
    password = Column(String(2000), nullable=False, comment="用户密钥(密码)")
    email = Column(String(255), unique=True, nullable=True, comment="邮箱地址")
    phone = Column(String(11), index=True, unique=True, nullable=False, comment="手机号码")
    first_name = Column(String(500), nullable=True, comment="名")
    last_name = Column(String(500), nullable=True, comment="姓")
    avatar = Column(String(1000), nullable=True, comment="用户头像URL")
    is_active = Column(Boolean, default=True, comment="是否激活")
    is_staff = Column(Boolean, default=False, comment="是否员工")
    is_superuser = Column(Boolean, default=False, comment="是否超级管理员")
    last_login = Column(DateTime(timezone=True), nullable=True, comment="最后登录时间")
    last_login_ip = Column(String(15), nullable=True, comment="最后登录ip")
    is_deleted = Column(Boolean, default=False, comment="是否删除")
    date_joined = Column(DateTime(timezone=True), server_default=func.now(), comment="注册时间")
    introduce = Column(String(4000), nullable=True, comment="用户介绍")

    profile = relationship(
        "UserProfile",
        back_populates="user",
        uselist=False,
        cascade="all,delete-orphan",
    )

    roles = relationship(
        "Role",
        secondary=user_role,
        back_populates="users",
        viewonly=True
    )

    def get_role_codes(self):
        """获取用户所有角色代码"""
        return [role.code for role in self.roles] if self.roles else []

    def has_permission(self, permission_code: str) -> bool:
        """检查用户是否拥有指定权限"""
        # 系统管理员拥有所有权限
        if self.is_superuser:
            return True
        for role in self.roles:
            for permission in role.permissions:
                if permission.code == permission_code and permission.is_active:
                    return True
        return False

    def get_permissions(self):
        """获取用户所有权限"""
        # 系统管理员拥有所有权限（直接返回空列表表示全部拥有）
        if self.is_superuser:
            return []
        perms = set()
        for role in self.roles:
            for permission in role.permissions:
                if permission.is_active:
                    perms.add(permission.code)
        return list(perms)

    def get_menus(self):
        """获取用户所有可见菜单"""
        # 系统管理员获取所有菜单
        if self.is_superuser:
            from apps.menu.models import Menu
            from database import SessionLocal
            db = SessionLocal()
            try:
                all_menus = db.query(Menu).filter(
                    Menu.is_active == True
                ).order_by(Menu.order).all()
                return all_menus
            finally:
                db.close()

        # 普通用户按角色获取菜单
        menus = []
        for role in self.roles:
            for menu in role.menus:
                if menu.is_active:
                    if menu not in menus:
                        menus.append(menu)
        return sorted(menus, key=lambda x: x.order)

    def get_primary_role(self):
        """获取用户主角色(按角色代码排序)"""
        if not self.roles:
            return None
        role_priority = ['superadmin', 'admin', 'user', 'guest']
        sorted_roles = sorted(self.roles, key=lambda r: role_priority.index(r.code) if r.code in role_priority else 999)
        return sorted_roles[0] if sorted_roles else None

    def __repr__(self) -> str:
        return f"<User(id={self.id}, username='{self.username}')>"


class UserProfile(BaseModel):
    """
        用户拓展信息
    """
    __tablename__ = "user_profile"

    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, comment="关联的用户ID")
    real_name = Column(String(100), nullable=True, comment="真实姓名")
    role = Column(String(100), default="user", nullable=False, comment="用户角色(兼容旧字段)")
    login_count = Column(Integer, default=0, comment="登录次数")
    last_activity = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(),
                           comment="最后活动时间")

    user = relationship(
        "User",
        back_populates="profile"
    )

    def __repr__(self) -> str:
        return (f"<UserProfile(id={self.id}, user='{self.user.username}', real_name='{self.real_name}',"
                f" role='{self.role}')>")
