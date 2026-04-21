from typing import Optional
from pydantic import BaseModel, Field
from datetime import datetime


class ClusterBase(BaseModel):
    name: str = Field(..., description="集群名称")
    type: str = Field(..., description="集群类型: spark, hadoop")
    master_host: str = Field(..., description="主节点地址")
    master_port: Optional[int] = Field(None, description="主节点端口")
    web_ui_url: Optional[str] = Field(None, description="Web UI 地址")
    hdfs_host: Optional[str] = Field(None, description="HDFS 主节点地址")
    hdfs_port: Optional[int] = Field(9000, description="HDFS 端口")
    extra_config: Optional[str] = Field(None, description="额外配置(JSON格式)")
    description: Optional[str] = Field(None, description="描述")
    is_active: bool = Field(True, description="是否启用")


class ClusterCreate(ClusterBase):
    pass


class ClusterUpdate(BaseModel):
    name: Optional[str] = Field(None, description="集群名称")
    type: Optional[str] = Field(None, description="集群类型")
    master_host: Optional[str] = Field(None, description="主节点地址")
    master_port: Optional[int] = Field(None, description="主节点端口")
    web_ui_url: Optional[str] = Field(None, description="Web UI 地址")
    hdfs_host: Optional[str] = Field(None, description="HDFS 主节点地址")
    hdfs_port: Optional[int] = Field(None, description="HDFS 端口")
    extra_config: Optional[str] = Field(None, description="额外配置(JSON格式)")
    description: Optional[str] = Field(None, description="描述")
    is_active: Optional[bool] = Field(None, description="是否启用")


class ClusterResponse(ClusterBase):
    id: int
    is_connected: bool
    last_connected_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class ClusterTestRequest(BaseModel):
    id: Optional[int] = Field(None, description="集群ID(已有集群测试时使用)")
    type: Optional[str] = Field(None, description="集群类型(新集群测试时使用)")
    master_host: Optional[str] = Field(None, description="主节点地址(新集群测试时使用)")
    master_port: Optional[int] = Field(None, description="主节点端口(新集群测试时使用)")
    web_ui_url: Optional[str] = Field(None, description="Web UI 地址(新集群测试时使用)")
    extra_config: Optional[str] = Field(None, description="额外配置(新集群测试时使用)")


class ClusterTestResponse(BaseModel):
    success: bool
    message: str
    latency: Optional[float] = None
    details: Optional[dict] = None


class ClusterMetricsResponse(BaseModel):
    success: bool
    message: str
    metrics: Optional[dict] = None


class HDFSOperationRequest(BaseModel):
    cluster_id: int = Field(..., description="集群ID")
    path: str = Field(..., description="HDFS 路径")


class HDFSUploadRequest(HDFSOperationRequest):
    local_path: Optional[str] = Field(None, description="本地文件路径")
    content: Optional[str] = Field(None, description="文件内容")


class HDFSDirectoryCreateRequest(HDFSOperationRequest):
    permission: Optional[str] = Field("755", description="目录权限")


class HDFSOperationResponse(BaseModel):
    success: bool
    message: str
    data: Optional[dict] = None


class HDFSListResponse(BaseModel):
    success: bool
    message: str
    files: Optional[list] = None
