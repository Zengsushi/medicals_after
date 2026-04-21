"""
监控模块 - 数据模型
"""
from sqlalchemy import Column, func, ForeignKey
from sqlalchemy import Integer, DateTime, String, Boolean, Float, JSON
from sqlalchemy.orm import relationship
from apps.core.database import Base
from datetime import datetime


class DatabaseMetric(Base):
    """
    数据库监控指标
    """
    __tablename__ = "database_metrics"

    id = Column(Integer, primary_key=True, index=True)
    datasource_id = Column(Integer, ForeignKey("datasources.id"), nullable=False, comment="数据源ID")
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), comment="采集时间")
    
    # 内存使用
    memory_used = Column(Float, comment="已使用内存(MB)")
    memory_total = Column(Float, comment="总内存(MB)")
    memory_percent = Column(Float, comment="内存使用率(%)")
    
    # 访问统计
    query_count = Column(Integer, default=0, comment="查询次数")
    connection_count = Column(Integer, default=0, comment="当前连接数")
    max_connections = Column(Integer, comment="最大连接数")
    
    # 其他指标
    extra_info = Column(JSON, comment="额外信息")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")


class ClusterMetric(Base):
    """
    集群监控指标（Hadoop/Spark）
    """
    __tablename__ = "cluster_metrics"

    id = Column(Integer, primary_key=True, index=True)
    cluster_id = Column(Integer, ForeignKey("clusters.id"), nullable=False, comment="集群ID")
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), comment="采集时间")
    
    # 内存使用
    memory_used = Column(Float, comment="已使用内存(MB)")
    memory_total = Column(Float, comment="总内存(MB)")
    memory_percent = Column(Float, comment="内存使用率(%)")
    
    # CPU使用
    cpu_usage = Column(Float, comment="CPU使用率(%)")
    
    # 存储使用
    hdfs_used = Column(Float, comment="HDFS已使用(GB)")
    hdfs_total = Column(Float, comment="HDFS总容量(GB)")
    hdfs_percent = Column(Float, comment="HDFS使用率(%)")
    
    # 节点和任务
    active_nodes = Column(Integer, default=0, comment="活跃节点数")
    dead_nodes = Column(Integer, default=0, comment="死亡节点数")
    active_tasks = Column(Integer, default=0, comment="活跃任务数")
    completed_tasks = Column(Integer, default=0, comment="完成任务数")
    
    # 其他指标
    extra_info = Column(JSON, comment="额外信息")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
