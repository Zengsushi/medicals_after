"""
监控模块 - Pydantic schemas
"""
from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime


class DatabaseMetricResponse(BaseModel):
    """数据库监控指标响应"""
    id: int
    datasource_id: int
    timestamp: datetime
    
    # 内存使用
    memory_used: Optional[float] = None
    memory_total: Optional[float] = None
    memory_percent: Optional[float] = None
    
    # 访问统计
    query_count: int = 0
    connection_count: int = 0
    max_connections: Optional[int] = None
    
    # 其他指标
    extra_info: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True


class ClusterMetricResponse(BaseModel):
    """集群监控指标响应"""
    id: int
    cluster_id: int
    timestamp: datetime
    
    # 内存使用
    memory_used: Optional[float] = None
    memory_total: Optional[float] = None
    memory_percent: Optional[float] = None
    
    # CPU使用
    cpu_usage: Optional[float] = None
    
    # 存储使用
    hdfs_used: Optional[float] = None
    hdfs_total: Optional[float] = None
    hdfs_percent: Optional[float] = None
    
    # 节点和任务
    active_nodes: int = 0
    dead_nodes: int = 0
    active_tasks: int = 0
    completed_tasks: int = 0
    
    # 其他指标
    extra_info: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True


class MonitorOverviewResponse(BaseModel):
    """监控概览响应"""
    database_metrics: Optional[DatabaseMetricResponse] = None
    cluster_metrics: Optional[ClusterMetricResponse] = None
    timestamp: datetime


class DatabaseMonitorRequest(BaseModel):
    """数据库监控请求"""
    datasource_id: int


class ClusterMonitorRequest(BaseModel):
    """集群监控请求"""
    cluster_id: int
