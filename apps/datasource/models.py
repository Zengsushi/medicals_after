"""
    数据源模型
    支持：PostgreSQL, Hive 等数据源
"""
from sqlalchemy import Column, func, ForeignKey
from sqlalchemy import Integer, DateTime, String, Boolean, Text
from sqlalchemy.orm import relationship
from enum import Enum
from apps.core.database import Base


class DataSourceCategory(str, Enum):
    """数据源分类枚举"""
    RELATIONAL = "relational"
    DATA_WAREHOUSE = "data_warehouse"

    @classmethod
    def get_all_categories(cls):
        """获取所有数据源分类列表"""
        return [
            {"value": cls.RELATIONAL, "label": "关系型数据库"},
            {"value": cls.DATA_WAREHOUSE, "label": "数仓"}
        ]

    @classmethod
    def get_types_by_category(cls, category):
        """根据分类获取对应的数据源类型"""
        if category == cls.RELATIONAL:
            return [
                {"value": DataSourceType.POSTGRESQL, "label": "PostgreSQL"},
                {"value": DataSourceType.MYSQL, "label": "MySQL"},
                {"value": DataSourceType.ORACLE, "label": "Oracle"}
            ]
        elif category == cls.DATA_WAREHOUSE:
            return [
                {"value": DataSourceType.HIVE, "label": "Hive"}
            ]
        return []


class DataSourceType(str, Enum):
    """数据源类型枚举"""
    POSTGRESQL = "postgresql"
    HIVE = "hive"
    MYSQL = "mysql"
    ORACLE = "oracle"

    @classmethod
    def get_category(cls, data_source_type):
        """根据数据源类型获取分类"""
        relational_types = [cls.POSTGRESQL, cls.MYSQL, cls.ORACLE]
        if data_source_type in relational_types:
            return DataSourceCategory.RELATIONAL
        elif data_source_type == cls.HIVE:
            return DataSourceCategory.DATA_WAREHOUSE
        return None

    @classmethod
    def get_all_types(cls):
        """获取所有数据源类型列表"""
        return [
            {"value": cls.POSTGRESQL, "label": "PostgreSQL"},
            {"value": cls.HIVE, "label": "Hive"},
            {"value": cls.MYSQL, "label": "MySQL"},
            {"value": cls.ORACLE, "label": "Oracle"}
        ]


class DataSource(Base):
    """
        数据源模型
    """
    __tablename__ = "datasources"

    id = Column(Integer, primary_key=True, index=True, comment="数据源ID")
    name = Column(String(200), nullable=False, index=True, unique=True, comment="数据源名称")
    category = Column(String(50), nullable=False, comment="数据源分类: relational, data_warehouse")
    type = Column(String(50), nullable=False, comment="数据源类型: postgresql, hive, mysql")
    host = Column(String(255), nullable=False, comment="主机地址")
    port = Column(Integer, nullable=False, comment="端口")
    database = Column(String(255), nullable=True, comment="数据库名")
    username = Column(String(255), nullable=True, comment="用户名")
    password = Column(String(2000), nullable=True, comment="密码(加密存储)")
    extra_config = Column(Text, nullable=True, comment="额外配置(JSON格式)")
    is_active = Column(Boolean, default=True, comment="是否启用")
    is_connected = Column(Boolean, default=False, comment="连接状态")
    is_default = Column(Boolean, default=False, comment="是否默认数据源")
    latency = Column(Integer, nullable=True, comment="最近一次连接延迟(毫秒)")
    last_connected_at = Column(DateTime(timezone=True), nullable=True, comment="最后连接时间")
    description = Column(Text, nullable=True, comment="描述")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")

    def set_category_from_type(self):
        """根据数据源类型自动设置分类"""
        self.category = DataSourceType.get_category(self.type)

    def get_connection_string(self) -> str:
        """获取连接字符串"""
        from utils.crypto import CryptoUtil
        password = CryptoUtil.decrypt(self.password) if self.password else None
        if self.type == DataSourceType.POSTGRESQL:
            return f"postgresql://{self.username}:{password}@{self.host}:{self.port}/{self.database}"
        elif self.type == DataSourceType.HIVE:
            return f"thrift://{self.host}:{self.port}"
        elif self.type == DataSourceType.MYSQL:
            return f"mysql+pymysql://{self.username}:{password}@{self.host}:{self.port}/{self.database}"
        return ""

    def __repr__(self) -> str:
        return f"<DataSource(id={self.id}, name='{self.name}', type='{self.type}')>"


class DataSourceUsage(Base):
    """
        数据源使用统计模型
    """
    __tablename__ = "datasource_usage"

    id = Column(Integer, primary_key=True, index=True, comment="使用记录ID")
    datasource_id = Column(Integer, ForeignKey("datasources.id"), nullable=False, index=True, comment="数据源ID")
    operation_type = Column(String(50), nullable=False, comment="操作类型: connect, query, metadata")
    duration = Column(Integer, nullable=False, comment="操作耗时(毫秒)")
    success = Column(Boolean, nullable=False, comment="是否成功")
    error_message = Column(Text, nullable=True, comment="错误信息")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")

    # 关系
    datasource = relationship("DataSource", backref="usage_records")

    def __repr__(self) -> str:
        return f"<DataSourceUsage(id={self.id}, datasource_id={self.datasource_id}, operation_type='{self.operation_type}', success={self.success})>"
