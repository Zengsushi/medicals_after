"""
    分析模块 - Pydantic 请求/响应模型
"""
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class RecentUserResponse(BaseModel):
    """最近注册用户响应"""
    id: int
    username: str
    avatar: Optional[str] = None
    real_name: Optional[str] = None
    role: str
    role_name: str
    date_joined: str
    is_active: bool

    class Config:
        from_attributes = True
