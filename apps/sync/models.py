"""
    数据同步模型
    支持从 Hive 到其他数据源的同步
"""
from sqlalchemy import Column, func
from sqlalchemy import Integer, DateTime, String, Boolean, Text, JSON
from enum import Enum
from apps.core.database import Base


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


class SyncTask(Base):
    """
        数据同步任务模型
    """
    __tablename__ = "sync_tasks"

    id = Column(Integer, primary_key=True, index=True, comment="同步任务ID")
    name = Column(String(200), nullable=False, index=True, comment="同步任务名称")
    
    # 源数据源和目标数据源
    source_id = Column(Integer, nullable=False, comment="源数据源ID")
    target_id = Column(Integer, nullable=False, comment="目标数据源ID")
    
    # 同步配置
    source_table = Column(String(255), nullable=False, comment="源表名")
    target_table = Column(String(255), nullable=False, comment="目标表名")
    sync_mode = Column(String(50), default=SyncMode.FULL, comment="同步模式: full/incremental")
    sync_condition = Column(Text, nullable=True, comment="增量同步条件(WHERE子句)")
    column_mapping = Column(JSON, nullable=True, comment="列映射(JSON格式)")
    batch_size = Column(Integer, default=1000, comment="批次大小")
    
    # 状态和进度
    status = Column(String(50), default=SyncTaskStatus.PENDING, comment="状态")
    progress = Column(Integer, default=0, comment="进度(0-100)")
    row_count = Column(Integer, default=0, comment="同步行数")
    error_message = Column(Text, nullable=True, comment="错误信息")
    
    # 时间
    started_at = Column(DateTime(timezone=True), nullable=True, comment="开始时间")
    completed_at = Column(DateTime(timezone=True), nullable=True, comment="完成时间")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")
    
    # 调度
    is_scheduled = Column(Boolean, default=False, comment="是否启用定时调度")
    cron_expression = Column(String(100), nullable=True, comment="Cron表达式")
    
    description = Column(Text, nullable=True, comment="描述")

    def __repr__(self) -> str:
        return f"<SyncTask(id={self.id}, name='{self.name}', status='{self.status}')>"


class SyncLog(Base):
    """
        同步日志模型
    """
    __tablename__ = "sync_logs"

    id = Column(Integer, primary_key=True, index=True, comment="日志ID")
    task_id = Column(Integer, nullable=False, index=True, comment="任务ID")
    level = Column(String(20), default="INFO", comment="日志级别")
    message = Column(Text, nullable=False, comment="日志内容")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")

    def __repr__(self) -> str:
        return f"<SyncLog(id={self.id}, task_id={self.task_id}, level='{self.level}')>"
