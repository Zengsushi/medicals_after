"""
    数据采集模块模型
    支持：定时采集任务、手动采集、采集监控等
"""
from sqlalchemy import Column, func, ForeignKey, Index, Text
from sqlalchemy import Integer, DateTime, String, Boolean, JSON
from sqlalchemy.orm import relationship
from enum import Enum
from apps.core.database import Base


class CollectionTaskStatus(str, Enum):
    """采集任务状态枚举"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELLED = "cancelled"


class CollectionTaskType(str, Enum):
    """采集任务类型枚举"""
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    ON_DEMAND = "on_demand"


class CollectionSourceType(str, Enum):
    """采集源类型枚举"""
    DATABASE = "database"
    FILE = "file"
    API = "api"
    STREAM = "stream"


class CollectionSource(Base):
    """
    数据采集源模型
    定义数据从哪里采集
    """
    __tablename__ = "collection_sources"

    id = Column(Integer, primary_key=True, index=True, comment="采集源ID")
    name = Column(String(200), nullable=False, index=True, comment="采集源名称")
    type = Column(String(50), nullable=False, comment="采集源类型: database, file, api, stream")
    
    # 数据库源配置
    datasource_id = Column(Integer, ForeignKey("datasources.id"), nullable=True, comment="关联的数据源ID")
    
    # 文件源配置
    file_path = Column(String(500), nullable=True, comment="文件路径")
    file_format = Column(String(50), nullable=True, comment="文件格式: csv, json, parquet, excel")
    
    # API源配置
    api_url = Column(String(500), nullable=True, comment="API地址")
    api_method = Column(String(20), nullable=True, comment="HTTP方法: GET, POST")
    api_headers = Column(JSON, nullable=True, comment="API请求头")
    api_body = Column(JSON, nullable=True, comment="API请求体")
    
    # 通用配置
    config = Column(JSON, nullable=True, comment="扩展配置(JSON)")
    description = Column(Text, nullable=True, comment="描述")
    
    is_active = Column(Boolean, default=True, comment="是否启用")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")
    
    datasource = relationship("DataSource", foreign_keys=[datasource_id])
    
    __table_args__ = (
        Index('idx_source_type', 'type'),
        Index('idx_source_active', 'is_active'),
    )

    def __repr__(self) -> str:
        return f"<CollectionSource(id={self.id}, name='{self.name}', type='{self.type}')>"


class CollectionTask(Base):
    """
    数据采集任务模型
    定义数据采集任务
    """
    __tablename__ = "collection_tasks"

    id = Column(Integer, primary_key=True, index=True, comment="采集任务ID")
    name = Column(String(200), nullable=False, index=True, comment="任务名称")
    type = Column(String(50), nullable=False, default=CollectionTaskType.MANUAL, comment="任务类型: scheduled, manual, on_demand")
    status = Column(String(50), nullable=False, default=CollectionTaskStatus.PENDING, comment="任务状态")
    
    # 采集源关联
    source_id = Column(Integer, ForeignKey("collection_sources.id"), nullable=False, comment="采集源ID")
    
    # 目标配置
    target_datasource_id = Column(Integer, ForeignKey("datasources.id"), nullable=True, comment="目标数据源ID")
    target_table = Column(String(200), nullable=True, comment="目标表名")
    target_path = Column(String(500), nullable=True, comment="目标路径")
    
    # 采集配置
    query = Column(Text, nullable=True, comment="SQL查询或文件筛选条件")
    schedule_cron = Column(String(100), nullable=True, comment="Cron表达式(定时任务用)")
    incremental_field = Column(String(100), nullable=True, comment="增量字段")
    last_sync_value = Column(String(500), nullable=True, comment="上次同步值")
    batch_size = Column(Integer, default=1000, comment="批次大小")
    
    # 时间字段
    last_run_at = Column(DateTime(timezone=True), nullable=True, comment="上次运行时间")
    next_run_at = Column(DateTime(timezone=True), nullable=True, comment="下次运行时间")
    
    # 统计信息
    total_records = Column(Integer, default=0, comment="总记录数")
    success_records = Column(Integer, default=0, comment="成功记录数")
    failed_records = Column(Integer, default=0, comment="失败记录数")
    duration = Column(Integer, default=0, comment="执行时长(秒)")
    
    description = Column(Text, nullable=True, comment="描述")
    error_message = Column(Text, nullable=True, comment="错误信息")
    
    is_active = Column(Boolean, default=True, comment="是否启用")
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True, comment="创建人ID")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")
    
    source = relationship("CollectionSource", foreign_keys=[source_id])
    target_datasource = relationship("DataSource", foreign_keys=[target_datasource_id])
    creator = relationship("User", foreign_keys=[created_by])
    
    __table_args__ = (
        Index('idx_task_status', 'status'),
        Index('idx_task_type', 'type'),
        Index('idx_task_source', 'source_id'),
        Index('idx_task_next_run', 'next_run_at'),
    )

    def __repr__(self) -> str:
        return f"<CollectionTask(id={self.id}, name='{self.name}', status='{self.status}')>"


class CollectionExecution(Base):
    """
    数据采集执行记录模型
    记录每次采集任务的执行情况
    """
    __tablename__ = "collection_executions"

    id = Column(Integer, primary_key=True, index=True, comment="执行ID")
    task_id = Column(Integer, ForeignKey("collection_tasks.id"), nullable=False, comment="任务ID")
    
    status = Column(String(50), nullable=False, default=CollectionTaskStatus.PENDING, comment="执行状态")
    start_time = Column(DateTime(timezone=True), nullable=True, comment="开始时间")
    end_time = Column(DateTime(timezone=True), nullable=True, comment="结束时间")
    duration = Column(Integer, default=0, comment="执行时长(秒)")
    
    # 统计信息
    total_records = Column(Integer, default=0, comment="总记录数")
    success_records = Column(Integer, default=0, comment="成功记录数")
    failed_records = Column(Integer, default=0, comment="失败记录数")
    
    error_message = Column(Text, nullable=True, comment="错误信息")
    error_stacktrace = Column(Text, nullable=True, comment="错误堆栈")
    
    # 执行配置快照
    config_snapshot = Column(JSON, nullable=True, comment="配置快照")
    
    triggered_by = Column(Integer, ForeignKey("users.id"), nullable=True, comment="触发人ID")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    
    task = relationship("CollectionTask", foreign_keys=[task_id])
    trigger_user = relationship("User", foreign_keys=[triggered_by])
    
    __table_args__ = (
        Index('idx_exec_task', 'task_id'),
        Index('idx_exec_status', 'status'),
        Index('idx_exec_created', 'created_at'),
    )

    def __repr__(self) -> str:
        return f"<CollectionExecution(id={self.id}, task_id={self.task_id}, status='{self.status}')>"


class CollectionLog(Base):
    """
    数据采集日志模型
    记录采集过程中的详细日志
    """
    __tablename__ = "collection_logs"

    id = Column(Integer, primary_key=True, index=True, comment="日志ID")
    execution_id = Column(Integer, ForeignKey("collection_executions.id"), nullable=False, comment="执行ID")
    
    level = Column(String(20), nullable=False, default="INFO", comment="日志级别: DEBUG, INFO, WARNING, ERROR")
    message = Column(Text, nullable=False, comment="日志消息")
    
    record_count = Column(Integer, nullable=True, comment="涉及的记录数")
    details = Column(JSON, nullable=True, comment="详细信息(JSON)")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    
    execution = relationship("CollectionExecution", foreign_keys=[execution_id])
    
    __table_args__ = (
        Index('idx_log_execution', 'execution_id'),
        Index('idx_log_level', 'level'),
        Index('idx_log_created', 'created_at'),
    )

    def __repr__(self) -> str:
        return f"<CollectionLog(id={self.id}, execution_id={self.execution_id}, level='{self.level}')>"
