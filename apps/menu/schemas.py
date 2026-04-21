"""
    菜单模块 - Pydantic 请求/响应模型
"""
from pydantic import BaseModel
from typing import Optional


class MenuCreateRequest(BaseModel):
    """创建菜单请求"""
    name: str
    path: Optional[str] = None
    component: Optional[str] = None
    icon: Optional[str] = None
    order: int = 0
    parent_id: Optional[int] = None
    permission_code: Optional[str] = None
    is_cached: bool = False
    is_folder: bool = False
    is_active: bool = True
    position: int = 0


class MenuUpdateRequest(BaseModel):
    """更新菜单请求"""
    name: Optional[str] = None
    path: Optional[str] = None
    component: Optional[str] = None
    icon: Optional[str] = None
    order: Optional[int] = None
    parent_id: Optional[int] = None
    permission_code: Optional[str] = None
    is_cached: Optional[bool] = None
    is_folder: Optional[bool] = None
    is_active: Optional[bool] = None
    position: Optional[int] = None


class MenuResponse(BaseModel):
    """菜单响应"""
    id: int
    name: str
    path: Optional[str]
    component: Optional[str]
    icon: Optional[str]
    order: int
    parent_id: Optional[int]
    permission_code: Optional[str]
    is_cached: bool
    is_folder: bool
    is_active: bool
    position: int
    children: list = []

    class Config:
        from_attributes = True
