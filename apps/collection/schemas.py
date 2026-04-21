"""
    数据采集模块 Schemas
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


# ========== 采集源 Schemas ==========

class CollectionSourceBase(BaseModel):
    """采集源基础模型"""
    name: str = Field(..., description="采集源名称")
    type: str = Field(..., description="采集源类型")
    datasource_id: Optional[int] = Field(None, description="关联的数据源ID")
    file_path: Optional[str] = Field(None, description="文件路径")
    file_format: Optional[str] = Field(None, description="文件格式")
    api_url: Optional[str] = Field(None, description="API地址")
    api_method: Optional[str] = Field(None, description="HTTP方法")
    api_headers: Optional[Dict[str, Any]] = Field(None, description="API请求头")
    api_body: Optional[Dict[str, Any]] = Field(None, description="API请求体")
    config: Optional[Dict[str, Any]] = Field(None, description="扩展配置")
    description: Optional[str] = Field(None, description="描述")


class CollectionSourceCreate(CollectionSourceBase):
    """创建采集源"""
    is_active: bool = Field(True, description="是否启用")


class CollectionSourceUpdate(BaseModel):
    """更新采集源"""
    name: Optional[str] = Field(None, description="采集源名称")
    type: Optional[str] = Field(None, description="采集源类型")
    datasource_id: Optional[int] = Field(None, description="关联的数据源ID")
    file_path: Optional[str] = Field(None, description="文件路径")
    file_format: Optional[str] = Field(None, description="文件格式")
    api_url: Optional[str] = Field(None, description="API地址")
    api_method: Optional[str] = Field(None, description="HTTP方法")
    api_headers: Optional[Dict[str, Any]] = Field(None, description="API请求头")
    api_body: Optional[Dict[str, Any]] = Field(None, description="API请求体")
    config: Optional[Dict[str, Any]] = Field(None, description="扩展配置")
    description: Optional[str] = Field(None, description="描述")
    is_active: Optional[bool] = Field(None, description="是否启用")


class CollectionSourceResponse(CollectionSourceBase):
    """采集源响应"""
    id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ========== 采集任务 Schemas ==========

class CollectionTaskBase(BaseModel):
    """采集任务基础模型"""
    name: str = Field(..., description="任务名称")
    type: str = Field("manual", description="任务类型")
    source_id: int = Field(..., description="采集源ID")
    target_datasource_id: Optional[int] = Field(None, description="目标数据源ID")
    target_table: Optional[str] = Field(None, description="目标表名")
    target_path: Optional[str] = Field(None, description="目标路径")
    query: Optional[str] = Field(None, description="SQL查询或文件筛选条件")
    schedule_cron: Optional[str] = Field(None, description="Cron表达式")
    incremental_field: Optional[str] = Field(None, description="增量字段")
    batch_size: int = Field(1000, description="批次大小")
    description: Optional[str] = Field(None, description="描述")


class CollectionTaskCreate(CollectionTaskBase):
    """创建采集任务"""
    is_active: bool = Field(True, description="是否启用")


class CollectionTaskUpdate(BaseModel):
    """更新采集任务"""
    name: Optional[str] = Field(None, description="任务名称")
    type: Optional[str] = Field(None, description="任务类型")
    source_id: Optional[int] = Field(None, description="采集源ID")
    target_datasource_id: Optional[int] = Field(None, description="目标数据源ID")
    target_table: Optional[str] = Field(None, description="目标表名")
    target_path: Optional[str] = Field(None, description="目标路径")
    query: Optional[str] = Field(None, description="SQL查询或文件筛选条件")
    schedule_cron: Optional[str] = Field(None, description="Cron表达式")
    incremental_field: Optional[str] = Field(None, description="增量字段")
    batch_size: Optional[int] = Field(None, description="批次大小")
    description: Optional[str] = Field(None, description="描述")
    is_active: Optional[bool] = Field(None, description="是否启用")


class CollectionTaskResponse(CollectionTaskBase):
    """采集任务响应"""
    id: int
    status: str
    last_run_at: Optional[datetime] = None
    next_run_at: Optional[datetime] = None
    total_records: int
    success_records: int
    failed_records: int
    duration: int
    error_message: Optional[str] = None
    is_active: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ========== 采集执行 Schemas ==========

class CollectionExecutionResponse(BaseModel):
    """采集执行响应"""
    id: int
    task_id: int
    status: str
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration: int
    total_records: int
    success_records: int
    failed_records: int
    error_message: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


# ========== 采集日志 Schemas ==========

class CollectionLogResponse(BaseModel):
    """采集日志响应"""
    id: int
    execution_id: int
    level: str
    message: str
    record_count: Optional[int] = None
    details: Optional[Dict[str, Any]] = None
    created_at: datetime

    class Config:
        from_attributes = True


# ========== 其他 Schemas ==========

class CollectionTaskExecuteRequest(BaseModel):
    """手动执行采集任务请求"""
    task_id: int


class CollectionTaskStatusResponse(BaseModel):
    """采集任务状态响应"""
    task_id: int
    status: str
    current_execution: Optional[CollectionExecutionResponse] = None
    message: str
