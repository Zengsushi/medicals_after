from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from pydantic.config import ConfigDict
from datetime import datetime
from enum import Enum


class DataSourceCategory(str, Enum):
    """数据源分类枚举"""
    RELATIONAL = "relational"
    DATA_WAREHOUSE = "data_warehouse"


class DataSourceType(str, Enum):
    """数据源类型枚举"""
    POSTGRESQL = "postgresql"
    HIVE = "hive"
    MYSQL = "mysql"
    ORACLE = "oracle"


class DataSourceBase(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    name: str = Field(..., description="数据源名称")
    category: Optional[DataSourceCategory] = Field(None, description="数据源分类: relational, data_warehouse")
    # 前端兼容字段：dbType -> type
    type: DataSourceType = Field(..., description="数据源类型: postgresql, hive, mysql, oracle", alias="dbType")
    host: str = Field(..., description="主机地址")
    port: int = Field(..., description="端口")
    database: Optional[str] = Field(None, description="数据库名")
    username: Optional[str] = Field(None, description="用户名")
    password: Optional[str] = Field(None, description="密码")
    # 前端兼容字段：params -> extra_config
    extra_config: Optional[str] = Field(None, description="额外配置(JSON格式)", alias="params")
    description: Optional[str] = Field(None, description="描述")
    # 前端兼容字段：isActive -> is_active
    is_active: bool = Field(True, description="是否启用", alias="isActive")
    # 前端兼容字段：isDefault -> is_default
    is_default: bool = Field(False, description="是否默认数据源", alias="isDefault")


class DataSourceCreate(DataSourceBase):
    pass


class DataSourceUpdate(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)
    name: Optional[str] = Field(None, description="数据源名称")
    category: Optional[DataSourceCategory] = Field(None, description="数据源分类: relational, data_warehouse")
    type: Optional[DataSourceType] = Field(None, description="数据源类型", alias="dbType")
    host: Optional[str] = Field(None, description="主机地址")
    port: Optional[int] = Field(None, description="端口")
    database: Optional[str] = Field(None, description="数据库名")
    username: Optional[str] = Field(None, description="用户名")
    password: Optional[str] = Field(None, description="密码")
    extra_config: Optional[str] = Field(None, description="额外配置(JSON格式)", alias="params")
    description: Optional[str] = Field(None, description="描述")
    is_active: Optional[bool] = Field(None, description="是否启用", alias="isActive")
    is_default: Optional[bool] = Field(None, description="是否默认数据源", alias="isDefault")


class DataSourceResponse(DataSourceBase):
    id: int
    is_connected: bool
    latency: Optional[int] = None
    last_connected_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ConnectionTestRequest(BaseModel):
    id: Optional[int] = Field(None, description="数据源ID(已有数据源测试时使用)")
    category: Optional[DataSourceCategory] = Field(None, description="数据源分类(新数据源测试时使用)")
    type: Optional[DataSourceType] = Field(None, description="数据源类型(新数据源测试时使用)")
    host: Optional[str] = Field(None, description="主机地址(新数据源测试时使用)")
    port: Optional[int] = Field(None, description="端口(新数据源测试时使用)")
    database: Optional[str] = Field(None, description="数据库名(新数据源测试时使用)")
    username: Optional[str] = Field(None, description="用户名(新数据源测试时使用)")
    password: Optional[str] = Field(None, description="密码(新数据源测试时使用)")
    extra_config: Optional[str] = Field(None, description="额外配置(新数据源测试时使用)")


class DataSourceTypeResponse(BaseModel):
    value: str = Field(..., description="类型值")
    label: str = Field(..., description="类型显示名称")


class DataSourceCategoryResponse(BaseModel):
    value: str = Field(..., description="分类值")
    label: str = Field(..., description="分类显示名称")


class ConnectionTestResponse(BaseModel):
    success: bool
    message: str
    latency: Optional[float] = None
    details: Optional[dict] = None


class DataSourceBatchCreate(BaseModel):
    """批量创建数据源请求"""
    datasources: List[DataSourceCreate] = Field(..., description="数据源列表")


class DataSourceBatchUpdate(BaseModel):
    """批量更新数据源请求"""
    updates: List[Dict[str, Any]] = Field(..., description="更新列表，每个元素包含 id 和要更新的字段")


class DataSourceBatchDelete(BaseModel):
    """批量删除数据源请求"""
    ids: List[int] = Field(..., description="数据源 ID 列表")


class BatchOperationResponse(BaseModel):
    """批量操作响应"""
    success: bool
    total: int
    successful: int
    failed: int
    details: Optional[List[Dict[str, Any]]] = None
