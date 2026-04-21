"""
    菜单模型
    从 users/rbac_models.py 迁移 Menu 模型和 role_menu 关联表
"""
from sqlalchemy import Column, func, ForeignKey, Table
from sqlalchemy import Integer, DateTime, String, Boolean
from sqlalchemy.orm import relationship
from apps.core.database import Base
from datetime import datetime


role_menu = Table(
    "role_menu",
    Base.metadata,
    Column(
        "role_id",
        Integer,
        ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True
    ),
    Column(
        "menu_id",
        Integer,
        ForeignKey("menus.id", ondelete="CASCADE"),
        primary_key=True
    ),
)


class Menu(Base):
    """
        菜单模型
    """
    __tablename__ = "menus"

    id = Column(Integer, primary_key=True, index=True)
    parent_id = Column(Integer, ForeignKey("menus.id", ondelete="CASCADE"), nullable=True, comment="父菜单ID")
    parent_path = Column(String(200), nullable=True, comment="父菜单路径，用于构建菜单层级关系")
    name = Column(String(100), nullable=False, comment="菜单名称")
    path = Column(String(200), nullable=True, comment="路由路径")
    component = Column(String(200), nullable=True, comment="组件路径")
    icon = Column(String(100), nullable=True, comment="图标")
    order = Column(Integer, default=0, comment="排序")
    permission_code = Column(String(100), ForeignKey("permissions.code", ondelete="SET NULL"), nullable=True, comment="关联权限代码")
    is_cached = Column(Boolean, default=False, comment="是否缓存")
    is_active = Column(Boolean, default=True, comment="是否启用")
    is_folder = Column(Boolean, default=False, comment="是否为文件夹")
    position = Column(Integer, default=0, comment="菜单位置: 0=顶部菜单, 1=顶部子菜单, 2=左侧菜单, 3=左侧子菜单")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")

    parent = relationship("Menu", remote_side=[id], backref="children")
    permission = relationship("Permission", foreign_keys=[permission_code], primaryjoin="Menu.permission_code==Permission.code", viewonly=True)
    roles = relationship(
        "Role",
        secondary=role_menu,
        back_populates="menus"
    )

    def __repr__(self) -> str:
        return f"<Menu(id={self.id}, name='{self.name}', path='{self.path}')>"
