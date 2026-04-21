"""
    集群模型
    支持：Spark、Hadoop 等集群管理
"""
from sqlalchemy import Column, func
from sqlalchemy import Integer, DateTime, String, Boolean, Text
from apps.core.database import Base


class Cluster(Base):
    """
        集群模型
    """
    __tablename__ = "clusters"

    id = Column(Integer, primary_key=True, index=True, comment="集群ID")
    name = Column(String(200), nullable=False, index=True, unique=True, comment="集群名称")
    type = Column(String(50), nullable=False, comment="集群类型: spark, hadoop")
    master_host = Column(String(255), nullable=False, comment="主节点地址")
    master_port = Column(Integer, nullable=True, comment="主节点端口")
    web_ui_url = Column(String(500), nullable=True, comment="Web UI 地址")
    hdfs_host = Column(String(255), nullable=True, comment="HDFS 主节点地址")
    hdfs_port = Column(Integer, nullable=True, default=9000, comment="HDFS 端口")
    extra_config = Column(Text, nullable=True, comment="额外配置(JSON格式)")
    is_active = Column(Boolean, default=True, comment="是否启用")
    is_connected = Column(Boolean, default=False, comment="连接状态")
    last_connected_at = Column(DateTime(timezone=True), nullable=True, comment="最后连接时间")
    description = Column(Text, nullable=True, comment="描述")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")

    def __repr__(self) -> str:
        return f"<Cluster(id={self.id}, name='{self.name}', type='{self.type}')>"
