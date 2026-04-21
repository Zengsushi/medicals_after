"""
    数据同步模块 - Pydantic 请求/响应模型
"""
from pydantic import BaseModel, Field
from pydantic.config import ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class SyncTaskStatus(str, Enum):
    """同步任务状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SyncMode(str, Enum):
    """同步模式"""
    FULL = "full"
    INCREMENTAL = "incremental"


class SyncTaskBase(BaseModel):
    """同步任务基础模型"""
    name: str
    source_id: int
    target_id: int
    source_table: str
    target_table: str
    sync_mode: str = SyncMode.FULL
    sync_condition: Optional[str] = None
    column_mapping: Optional[Dict[str, Any]] = None
    batch_size: int = 1000
    is_scheduled: bool = False
    cron_expression: Optional[str] = None
    description: Optional[str] = None


class SyncTaskCreate(SyncTaskBase):
    """创建同步任务请求"""
    pass


class SyncTaskUpdate(BaseModel):
    """更新同步任务请求"""
    name: Optional[str] = None
    source_id: Optional[int] = None
    target_id: Optional[int] = None
    source_table: Optional[str] = None
    target_table: Optional[str] = None
    sync_mode: Optional[str] = None
    sync_condition: Optional[str] = None
    column_mapping: Optional[Dict[str, Any]] = None
    batch_size: Optional[int] = None
    is_scheduled: Optional[bool] = None
    cron_expression: Optional[str] = None
    description: Optional[str] = None


class SyncTaskResponse(SyncTaskBase):
    """同步任务响应"""
    id: int
    status: str
    progress: int
    row_count: int
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class SyncLogResponse(BaseModel):
    """同步日志响应"""
    id: int
    task_id: int
    level: str
    message: str
    created_at: datetime

    class Config:
        from_attributes = True


class SyncTestRequest(BaseModel):
    """测试同步请求"""
    source_id: int
    target_id: int
    source_table: str
    target_table: str
    limit: int = 10


class SyncPreviewRequest(BaseModel):
    """前端页面预览同步数据（兼容字段命名）"""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    source_id: int = Field(..., alias="sourceId")
    source_database: str = Field(..., alias="sourceDatabase")
    source_table: str = Field(..., alias="sourceTable")
    target_id: int = Field(..., alias="targetId")
    target_database: str = Field(..., alias="targetDatabase")
    target_table: str = Field(..., alias="targetTable")
    limit: int = 10


class SyncExecuteNowRequest(BaseModel):
    """前端页面立即执行同步（兼容字段命名）"""
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    source_id: int = Field(..., alias="sourceId")
    source_database: str = Field(..., alias="sourceDatabase")
    source_table: str = Field(..., alias="sourceTable")
    target_id: int = Field(..., alias="targetId")
    target_database: str = Field(..., alias="targetDatabase")
    target_table: str = Field(..., alias="targetTable")
    column_mapping: Optional[Dict[str, Any]] = Field(None, alias="columnMapping")
    sync_mode: str = Field("full", alias="syncMode")


class SyncExecuteRequest(BaseModel):
    """执行同步请求"""
    task_id: int


class SyncTablePreviewRequest(BaseModel):
    """预览表数据请求"""
    datasource_id: int
    table_name: str
    limit: int = 50


class SyncTableInfo(BaseModel):
    """表信息"""
    table_name: str
    columns: List[Dict[str, Any]]
    row_count: Optional[int] = None
