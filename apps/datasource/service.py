import logging
import time
import json
import traceback
from typing import Optional, Tuple, Dict, Any, List
from abc import ABC, abstractmethod
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from apps.datasource.models import DataSource, DataSourceType, DataSourceCategory, DataSourceUsage
from apps.datasource.schemas import DataSourceCreate, DataSourceUpdate, ConnectionTestRequest, ConnectionTestResponse
from apps.datasource.hive_config import HiveConfigManager
from apps.datasource.validators import DataSourceValidator
from utils.crypto import CryptoUtil

logger = logging.getLogger(__name__)


class BaseDataSourceHandler(ABC):
    """数据源处理抽象基类"""

    @abstractmethod
    def test_connection(
        self,
        host: str,
        port: int,
        database: Optional[str],
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """测试连接，返回 (success, message, details)"""
        pass

    @abstractmethod
    def get_databases(
        self,
        host: str,
        port: int,
        database: Optional[str],
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        """获取数据库列表"""
        pass

    @abstractmethod
    def get_tables(
        self,
        host: str,
        port: int,
        database: str,
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        """获取表列表"""
        pass

    @abstractmethod
    def get_table_structure(
        self,
        host: str,
        port: int,
        database: str,
        table: str,
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        """获取表结构"""
        pass


class RelationalDataSourceHandler(BaseDataSourceHandler):
    """关系型数据库处理基类"""

    @abstractmethod
    def get_driver_name(self) -> str:
        """获取驱动名称"""
        pass

    @abstractmethod
    def get_default_database(self) -> str:
        """获取默认数据库"""
        pass

    @abstractmethod
    def _connect_and_test(
        self,
        host: str,
        port: int,
        database: str,
        username: Optional[str],
        password: Optional[str]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """连接并测试"""
        pass

    def test_connection(
        self,
        host: str,
        port: int,
        database: Optional[str],
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """测试关系型数据库连接"""
        try:
            target_db = database or self.get_default_database()
            return self._connect_and_test(host, port, target_db, username, password)
        except ImportError:
            return False, f"未安装 {self.get_driver_name()} 驱动", {}
        except Exception as e:
            return False, f"{self.get_driver_name()} 连接失败: {str(e)}", {}


class PostgreSQLHandler(RelationalDataSourceHandler):
    """PostgreSQL 数据源处理类"""

    def get_driver_name(self) -> str:
        return "PostgreSQL"

    def get_default_database(self) -> str:
        return "postgres"

    def _connect_and_test(
        self,
        host: str,
        port: int,
        database: str,
        username: Optional[str],
        password: Optional[str]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        import psycopg2
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=username,
            password=password,
            connect_timeout=10
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        cursor.execute("SELECT current_database()")
        current_db = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        return True, "PostgreSQL 连接成功", {
            "database": current_db,
            "version": version
        }

    def get_databases(
        self,
        host: str,
        port: int,
        database: Optional[str],
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            logger.info(f"开始获取 PostgreSQL 数据库列表: host={host}, port={port}, database={database}")
            import psycopg2
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=database or "postgres",
                user=username,
                password=password,
                connect_timeout=10
            )
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false")
            databases = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            logger.info(f"成功获取 PostgreSQL 数据库列表: {len(databases)} 个数据库")
            return True, "获取数据库列表成功", databases
        except ImportError:
            logger.error("未安装 psycopg2 驱动")
            return False, "未安装 psycopg2 驱动", []
        except Exception as e:
            logger.error(f"获取 PostgreSQL 数据库列表失败: {str(e)}")
            logger.debug(traceback.format_exc())
            return False, f"获取数据库列表失败: {str(e)}", []

    def get_tables(
        self,
        host: str,
        port: int,
        database: str,
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            logger.info(f"开始获取 PostgreSQL 表列表: host={host}, port={port}, database={database}")
            import psycopg2
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=database,
                user=username,
                password=password,
                connect_timeout=10
            )
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            logger.info(f"成功获取 PostgreSQL 表列表: {len(tables)} 个表")
            return True, "获取表列表成功", tables
        except ImportError:
            logger.error("未安装 psycopg2 驱动")
            return False, "未安装 psycopg2 驱动", []
        except Exception as e:
            logger.error(f"获取 PostgreSQL 表列表失败: {str(e)}")
            logger.debug(traceback.format_exc())
            return False, f"获取表列表失败: {str(e)}", []

    def get_table_structure(
        self,
        host: str,
        port: int,
        database: str,
        table: str,
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            logger.info(f"开始获取 PostgreSQL 表结构: host={host}, port={port}, database={database}, table={table}")
            import psycopg2
            conn = psycopg2.connect(
                host=host,
                port=port,
                dbname=database,
                user=username,
                password=password,
                connect_timeout=10
            )
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    column_name, 
                    data_type, 
                    character_maximum_length, 
                    is_nullable, 
                    column_default 
                FROM 
                    information_schema.columns 
                WHERE 
                    table_name = %s
                ORDER BY 
                    ordinal_position
            """, (table,))
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "length": row[2],
                    "nullable": row[3] == "YES",
                    "default": row[4]
                })
            cursor.close()
            conn.close()
            logger.info(f"成功获取 PostgreSQL 表结构: {len(columns)} 个字段")
            return True, "获取表结构成功", columns
        except ImportError:
            logger.error("未安装 psycopg2 驱动")
            return False, "未安装 psycopg2 驱动", []
        except Exception as e:
            logger.error(f"获取 PostgreSQL 表结构失败: {str(e)}")
            logger.debug(traceback.format_exc())
            return False, f"获取表结构失败: {str(e)}", []


class MySQLHandler(RelationalDataSourceHandler):
    """MySQL 数据源处理类"""

    def get_driver_name(self) -> str:
        return "MySQL"

    def get_default_database(self) -> str:
        return "mysql"

    def _connect_and_test(
        self,
        host: str,
        port: int,
        database: str,
        username: Optional[str],
        password: Optional[str]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        import pymysql
        conn = pymysql.connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            connect_timeout=10
        )
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        cursor.execute("SELECT DATABASE()")
        current_db = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        return True, "MySQL 连接成功", {
            "database": current_db,
            "version": version
        }

    def get_databases(
        self,
        host: str,
        port: int,
        database: Optional[str],
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            logger.info(f"开始获取 MySQL 数据库列表: host={host}, port={port}, database={database}")
            import pymysql
            # 获取库列表不应强依赖配置中的 database，先尝试配置库，失败则回退 mysql
            connect_errors = []
            conn = None
            for candidate_db in [database, "mysql", None]:
                try:
                    kwargs = {
                        "host": host,
                        "port": port,
                        "user": username,
                        "password": password,
                        "connect_timeout": 10
                    }
                    if candidate_db:
                        kwargs["database"] = candidate_db
                    conn = pymysql.connect(**kwargs)
                    break
                except Exception as conn_err:
                    connect_errors.append(str(conn_err))
                    conn = None
            if conn is None:
                return False, f"连接 MySQL 失败: {' | '.join(connect_errors)}", []
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            databases = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            logger.info(f"成功获取 MySQL 数据库列表: {len(databases)} 个数据库")
            return True, "获取数据库列表成功", databases
        except ImportError:
            logger.error("未安装 pymysql 驱动")
            return False, "未安装 pymysql 驱动", []
        except Exception as e:
            logger.error(f"获取 MySQL 数据库列表失败: {str(e)}")
            logger.debug(traceback.format_exc())
            return False, f"获取数据库列表失败: {str(e)}", []

    def get_tables(
        self,
        host: str,
        port: int,
        database: str,
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            import pymysql
            conn = pymysql.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password,
                connect_timeout=10
            )
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return True, "获取表列表成功", tables
        except Exception as e:
            return False, f"获取表列表失败: {str(e)}", []

    def get_table_structure(
        self,
        host: str,
        port: int,
        database: str,
        table: str,
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            import pymysql
            conn = pymysql.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password,
                connect_timeout=10
            )
            cursor = conn.cursor()
            cursor.execute(f"DESCRIBE {table}")
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == "YES",
                    "default": row[4]
                })
            cursor.close()
            conn.close()
            return True, "获取表结构成功", columns
        except Exception as e:
            return False, f"获取表结构失败: {str(e)}", []


class OracleHandler(RelationalDataSourceHandler):
    """Oracle 数据源处理类"""

    def get_driver_name(self) -> str:
        return "Oracle"

    def get_default_database(self) -> str:
        return "ORCL"

    def _connect_and_test(
        self,
        host: str,
        port: int,
        database: str,
        username: Optional[str],
        password: Optional[str]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        try:
            import cx_Oracle
            
            # 构建连接字符串
            dsn = cx_Oracle.makedsn(host, port, service_name=database)
            conn = cx_Oracle.connect(
                user=username,
                password=password,
                dsn=dsn,
                connect_timeout=10
            )
            
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM v$version WHERE banner LIKE 'Oracle%'")
            version = cursor.fetchone()[0]
            cursor.execute("SELECT SYS_CONTEXT('USERENV', 'DB_NAME') FROM DUAL")
            current_db = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            return True, "Oracle 连接成功", {
                "database": current_db,
                "version": version
            }
        except ImportError:
            return False, "未安装 cx_Oracle 驱动", {}
        except Exception as e:
            return False, f"Oracle 连接失败: {str(e)}", {}

    def get_databases(
        self,
        host: str,
        port: int,
        database: Optional[str],
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            import cx_Oracle
            dsn = cx_Oracle.makedsn(host, port, service_name=database or "ORCL")
            conn = cx_Oracle.connect(
                user=username,
                password=password,
                dsn=dsn,
                connect_timeout=10
            )
            cursor = conn.cursor()
            # Oracle 中获取表空间或 PDB
            cursor.execute("SELECT name FROM v$tablespace WHERE name NOT LIKE 'SYSTEM' AND name NOT LIKE 'SYSAUX'")
            databases = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return True, "获取数据库列表成功", databases
        except ImportError:
            return False, "未安装 cx_Oracle 驱动", []
        except Exception as e:
            return False, f"获取数据库列表失败: {str(e)}", []

    def get_tables(
        self,
        host: str,
        port: int,
        database: str,
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            import cx_Oracle
            dsn = cx_Oracle.makedsn(host, port, service_name=database)
            conn = cx_Oracle.connect(
                user=username,
                password=password,
                dsn=dsn,
                connect_timeout=10
            )
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM user_tables")
            tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            return True, "获取表列表成功", tables
        except ImportError:
            return False, "未安装 cx_Oracle 驱动", []
        except Exception as e:
            return False, f"获取表列表失败: {str(e)}", []

    def get_table_structure(
        self,
        host: str,
        port: int,
        database: str,
        table: str,
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            import cx_Oracle
            dsn = cx_Oracle.makedsn(host, port, service_name=database)
            conn = cx_Oracle.connect(
                user=username,
                password=password,
                dsn=dsn,
                connect_timeout=10
            )
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    column_name, 
                    data_type, 
                    data_length, 
                    nullable, 
                    data_default 
                FROM 
                    user_tab_columns 
                WHERE 
                    table_name = UPPER(:table)
                ORDER BY 
                    column_id
            """, table=table)
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "length": row[2],
                    "nullable": row[3] == "Y",
                    "default": row[4]
                })
            cursor.close()
            conn.close()
            return True, "获取表结构成功", columns
        except ImportError:
            return False, "未安装 cx_Oracle 驱动", []
        except Exception as e:
            return False, f"获取表结构失败: {str(e)}", []


class DataWarehouseDataSourceHandler(BaseDataSourceHandler):
    """数仓数据源处理基类"""
    pass


class HiveHandler(DataWarehouseDataSourceHandler):
    """Hive 数据源处理类"""

    def _create_spark_session(self):
        """通过项目统一入口创建 SparkSession。"""
        from apps.analyse.config import init_spark_connect_hive
        return init_spark_connect_hive()

    def test_connection(
        self,
        host: str,
        port: int,
        database: Optional[str],
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """测试 Hive 连接（直接使用 init_spark_connect_hive）"""
        try:
            spark = self._create_spark_session()
            databases = [
                (getattr(row, "databaseName", None) or getattr(row, "namespace", None) or row[0])
                for row in spark.sql("show databases").collect()
            ]
            target_db = database or "default"
            current_db = target_db if target_db in databases else (databases[0] if databases else target_db)
            spark.sql(f"use `{current_db}`")
            table_count = len(spark.sql("show tables").collect())
            spark.stop()

            connection_info = HiveConfigManager.get_hive_connection_info(host, port)
            return True, "Hive 连接成功", {
                "database": current_db,
                "databases": databases,
                "tables_count": table_count,
                "metastore_uri": connection_info["metastore_uri"],
                "driver": "spark"
            }
        except Exception as e:
            return False, f"Hive 连接失败: {str(e)}", {}

    def get_databases(
        self,
        host: str,
        port: int,
        database: Optional[str],
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            spark = self._create_spark_session()
            databases = [
                (getattr(row, "databaseName", None) or getattr(row, "namespace", None) or row[0])
                for row in spark.sql("show databases").collect()
            ]
            spark.stop()
            return True, "获取数据库列表成功", databases
        except Exception as e:
            return False, f"获取数据库列表失败: {str(e)}", []

    def get_tables(
        self,
        host: str,
        port: int,
        database: str,
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            spark = self._create_spark_session()
            spark.sql(f"use `{database}`")
            table_rows = spark.sql("show tables").collect()
            tables = [getattr(row, "tableName", None) or row[1] for row in table_rows]
            spark.stop()
            return True, "获取表列表成功", tables
        except Exception as e:
            return False, f"获取表列表失败: {str(e)}", []

    def get_table_structure(
        self,
        host: str,
        port: int,
        database: str,
        table: str,
        username: Optional[str],
        password: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, list]:
        try:
            spark = self._create_spark_session()
            spark.sql(f"use `{database}`")
            desc_rows = spark.sql(f"describe `{table}`").collect()
            columns = []
            for row in desc_rows:
                name = str(row[0]).strip() if row and len(row) > 0 and row[0] is not None else ""
                if not name or name.startswith("#"):
                    continue
                columns.append({
                    "name": name,
                    "type": str(row[1]).strip() if len(row) > 1 and row[1] is not None else "",
                    "nullable": True
                })
            spark.stop()
            return True, "获取表结构成功", columns
        except Exception as e:
            return False, f"获取表结构失败: {str(e)}", []


class DataSourceHandlerFactory:
    """数据源处理器工厂"""

    _handlers: Dict[str, BaseDataSourceHandler] = {
        DataSourceType.POSTGRESQL: PostgreSQLHandler(),
        DataSourceType.MYSQL: MySQLHandler(),
        DataSourceType.ORACLE: OracleHandler(),
        DataSourceType.HIVE: HiveHandler()
    }

    @classmethod
    def get_handler(cls, data_source_type: str) -> Optional[BaseDataSourceHandler]:
        """根据数据源类型获取对应的处理器"""
        if data_source_type is None:
            return None
        normalized = str(data_source_type).strip().lower()
        # 兼容 Enum 字符串表现形式: DataSourceType.HIVE
        if "." in normalized:
            normalized = normalized.split(".")[-1]
        return cls._handlers.get(normalized)


class DataSourceService:
    @staticmethod
    def _safe_decrypt_password(password: Optional[str]) -> Optional[str]:
        """安全解密密码，失败时抛出明确错误，避免使用错误密文连接数据库"""
        if not password:
            return None
        try:
            return CryptoUtil.decrypt(password)
        except Exception as e:
            logger.error(f"密码解密失败: {e}")
            raise ValueError("数据源密码解密失败，请确认 ENCRYPTION_KEY 未变更，并在数据源管理中重新保存密码。")

    @staticmethod
    def _normalize_db_name(name: Optional[str]) -> Optional[str]:
        if name is None:
            return None
        value = str(name).strip()
        return value or None

    """数据源服务"""

    @staticmethod
    def get_by_id(db: Session, data_source_id: int) -> Optional[DataSource]:
        """根据ID获取数据源"""
        return db.query(DataSource).filter(DataSource.id == data_source_id).first()

    @staticmethod
    def get_by_name(db: Session, name: str) -> Optional[DataSource]:
        """根据名称获取数据源"""
        return db.query(DataSource).filter(DataSource.name == name).first()

    @staticmethod
    def list_all(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        type_filter: Optional[str] = None,
        keyword: Optional[str] = None,
        connected: Optional[bool] = None,
    ) -> Tuple[List[DataSource], int]:
        """
        分页列出启用中的数据源；支持按类型、关键字、连接状态过滤。
        返回 (当前页列表, 满足条件的总条数)。
        """
        q = db.query(DataSource).filter(DataSource.is_active == True)

        if type_filter and str(type_filter).strip():
            tf = str(type_filter).strip().lower()
            q = q.filter(func.lower(DataSource.type) == tf)

        if keyword and str(keyword).strip():
            k = f"%{str(keyword).strip().lower()}%"
            q = q.filter(
                or_(
                    func.lower(DataSource.name).like(k),
                    func.lower(DataSource.host).like(k),
                    func.lower(func.coalesce(DataSource.database, "")).like(k),
                )
            )

        if connected is True:
            q = q.filter(DataSource.is_connected.is_(True))
        elif connected is False:
            q = q.filter(
                or_(
                    DataSource.is_connected.is_(False),
                    DataSource.is_connected.is_(None),
                )
            )

        total = q.count()
        items = (
            q.order_by(DataSource.is_default.desc(), DataSource.updated_at.desc())
            .offset(skip)
            .limit(limit)
            .all()
        )
        return items, total

    @staticmethod
    def create(db: Session, data_source_data: DataSourceCreate) -> DataSource:
        """创建数据源"""
        # 将 Pydantic 模型转换为字典（已做前端字段兼容映射）
        data_dict = data_source_data.model_dump()
        
        # 验证数据源配置
        validation_result = DataSourceValidator.validate_all(data_dict)
        if not validation_result["valid"]:
            raise ValueError(validation_result["message"])
        
        # 清理和标准化数据
        sanitized_data = DataSourceValidator.sanitize_data(data_dict)
        
        # 加密密码
        password = CryptoUtil.encrypt(sanitized_data.get("password")) if sanitized_data.get("password") else None
        
        # 创建数据源实例
        data_source = DataSource(
            name=sanitized_data.get("name"),
            type=sanitized_data.get("type"),
            category=sanitized_data.get("category"),
            host=sanitized_data.get("host"),
            port=sanitized_data.get("port"),
            database=sanitized_data.get("database"),
            username=sanitized_data.get("username"),
            password=password,
            extra_config=sanitized_data.get("extra_config"),
            description=sanitized_data.get("description"),
            is_active=sanitized_data.get("is_active", True),
            is_default=bool(sanitized_data.get("is_default", False)),
        )
        
        # 确保分类正确
        if not data_source.category:
            data_source.set_category_from_type()
        
        # 如果用户提供了分类，则使用用户提供的
        if data_source_data.category:
            data_source.category = data_source_data.category
        
        db.add(data_source)
        db.commit()
        db.refresh(data_source)

        # 如果设为默认，确保唯一默认
        if data_source.is_default:
            DataSourceService.set_default(db, data_source.id)

        return data_source

    @staticmethod
    def update(db: Session, data_source_id: int, data_source_data: DataSourceUpdate) -> Optional[DataSource]:
        """更新数据源"""
        data_source = DataSourceService.get_by_id(db, data_source_id)
        if not data_source:
            return None

        update_data = data_source_data.model_dump(exclude_unset=True)
        
        # 如果有更新数据，进行验证
        if update_data:
            # 构建完整的数据源数据进行验证
            current_data = {
                "name": data_source.name,
                "type": data_source.type,
                "host": data_source.host,
                "port": data_source.port,
                "database": data_source.database,
                "username": data_source.username,
                "password": None,  # 密码不参与验证
                "extra_config": data_source.extra_config,
                "description": data_source.description,
                "is_active": data_source.is_active
            }
            
            # 更新数据
            current_data.update(update_data)
            
            # 验证更新后的数据
            validation_result = DataSourceValidator.validate_all(current_data)
            if not validation_result["valid"]:
                raise ValueError(validation_result["message"])
            
            # 清理和标准化数据
            sanitized_data = DataSourceValidator.sanitize_data(current_data)
            update_data = {k: v for k, v in sanitized_data.items() if k in update_data}
        
        # 应用更新
        for key, value in update_data.items():
            if key == 'password' and value:
                value = CryptoUtil.encrypt(value)
            setattr(data_source, key, value)
        
        # 如果更新了类型，重新自动设置分类
        if 'type' in update_data:
            data_source.set_category_from_type()
        
        # 如果用户更新了分类，则使用用户提供的
        if 'category' in update_data and update_data['category']:
            data_source.category = update_data['category']

        db.commit()
        db.refresh(data_source)

        # 如果更新了默认标志，确保唯一默认
        if 'is_default' in update_data and data_source.is_default:
            DataSourceService.set_default(db, data_source.id)

        return data_source

    @staticmethod
    def set_default(db: Session, data_source_id: int) -> Optional[DataSource]:
        """设置默认数据源（确保全局唯一）"""
        data_source = DataSourceService.get_by_id(db, data_source_id)
        if not data_source:
            return None

        # 取消其他默认
        db.query(DataSource).filter(DataSource.id != data_source_id).update({"is_default": False})
        data_source.is_default = True
        db.commit()
        db.refresh(data_source)
        return data_source

    @staticmethod
    def get_default(db: Session) -> Optional[DataSource]:
        """获取默认数据源"""
        return db.query(DataSource).filter(DataSource.is_active == True, DataSource.is_default == True).first()

    @staticmethod
    def delete(db: Session, data_source_id: int) -> bool:
        """删除数据源(软删除)"""
        data_source = DataSourceService.get_by_id(db, data_source_id)
        if not data_source:
            return False

        data_source.is_active = False
        db.commit()
        return True

    @staticmethod
    def test_connection(
        db: Session,
        request: ConnectionTestRequest
    ) -> ConnectionTestResponse:
        """测试数据源连接"""
        start_time = time.time()
        datasource_id = request.id
        
        logger.info(f"开始测试连接: request.id={request.id}, request.type={request.type}")
        
        # 获取连接参数
        if request.id:
            data_source = DataSourceService.get_by_id(db, request.id)
            if not data_source:
                logger.error(f"数据源不存在: id={request.id}")
                if datasource_id:
                    DataSourceService.record_usage(
                        db=db,
                        datasource_id=datasource_id,
                        operation_type="connect",
                        duration=int((time.time() - start_time) * 1000),
                        success=False,
                        error_message="数据源不存在"
                    )
                return ConnectionTestResponse(
                    success=False,
                    message="数据源不存在",
                    latency=time.time() - start_time
                )
            ds_type = data_source.type
            host = data_source.host
            port = data_source.port
            database = data_source.database
            username = data_source.username
            password = None
            if data_source.password:
                try:
                    password = CryptoUtil.decrypt(data_source.password)
                except Exception as e:
                    logger.warning(f"密码解密失败，尝试使用原始密码: {e}")
                    password = data_source.password
            extra_config = data_source.extra_config
            logger.info(f"从数据库获取数据源: type={ds_type}, host={host}, port={port}, database={database}")
        else:
            ds_type = request.type.value if request.type else None
            host = request.host
            port = request.port
            database = request.database
            username = request.username
            password = request.password
            extra_config = request.extra_config
            logger.info(f"从请求获取数据源: type={ds_type}, host={host}, port={port}, database={database}")

        try:
            # 通过工厂获取对应的处理器
            logger.info(f"获取处理器: ds_type={ds_type}")
            handler = DataSourceHandlerFactory.get_handler(ds_type)
            if handler:
                logger.info(f"调用 handler.test_connection: host={host}, port={port}, database={database}")
                success, message, details = handler.test_connection(
                    host, port, database, username, password, extra_config
                )
                logger.info(f"测试结果: success={success}, message={message}")

                # 连接成功后，统一尝试拉取数据库列表，便于前端直接渲染库下拉
                if success:
                    try:
                        db_success, _, db_list = handler.get_databases(
                            host, port, database, username, password, extra_config
                        )
                        if db_success and db_list is not None:
                            if not isinstance(details, dict):
                                details = {}
                            details["databases"] = db_list
                    except Exception as meta_err:
                        # 元数据拉取失败不影响连接测试结果，只记录日志用于排查
                        logger.warning(f"连接成功但获取数据库列表失败: {meta_err}")
            else:
                success = False
                message = f"不支持的数据源类型: {ds_type}"
                details = {}
                logger.error(f"不支持的数据源类型: {ds_type}")

            latency = time.time() - start_time

            # 记录使用统计
            if datasource_id:
                error_message = None if success else message
                DataSourceService.record_usage(
                    db=db,
                    datasource_id=datasource_id,
                    operation_type="connect",
                    duration=int(latency * 1000),
                    success=success,
                    error_message=error_message
                )

            return ConnectionTestResponse(
                success=success,
                message=message,
                latency=latency,
                details=details
            )
        except Exception as e:
            logger.exception(f"测试连接失败: {e}")
            # 记录使用统计
            if datasource_id:
                DataSourceService.record_usage(
                    db=db,
                    datasource_id=datasource_id,
                    operation_type="connect",
                    duration=int((time.time() - start_time) * 1000),
                    success=False,
                    error_message=f"连接测试异常: {str(e)}"
                )
            return ConnectionTestResponse(
                success=False,
                message=f"连接测试异常: {str(e)}",
                latency=time.time() - start_time
            )

    @staticmethod
    def update_connection_status(
        db: Session,
        data_source_id: int,
        is_connected: bool,
        latency_ms: Optional[int] = None
    ) -> Optional[DataSource]:
        """更新数据源连接状态"""
        from datetime import datetime
        data_source = DataSourceService.get_by_id(db, data_source_id)
        if not data_source:
            return None

        data_source.is_connected = is_connected
        if latency_ms is not None:
            data_source.latency = latency_ms
        if is_connected:
            data_source.last_connected_at = datetime.now()

        db.commit()
        db.refresh(data_source)
        return data_source

    @staticmethod
    def check_health(db: Session, data_source_id: int) -> Dict[str, Any]:
        """检查单个数据源健康状态"""
        data_source = DataSourceService.get_by_id(db, data_source_id)
        if not data_source or not data_source.is_active:
            return {
                "data_source_id": data_source_id,
                "name": data_source.name if data_source else None,
                "status": "inactive",
                "message": "数据源不存在或未启用"
            }

        # 构建测试请求
        request = ConnectionTestRequest(
            id=data_source_id
        )

        # 测试连接
        result = DataSourceService.test_connection(db, request)

        # 更新连接状态
        DataSourceService.update_connection_status(db, data_source_id, result.success)

        return {
            "data_source_id": data_source_id,
            "name": data_source.name,
            "type": data_source.type,
            "status": "healthy" if result.success else "unhealthy",
            "message": result.message,
            "latency": result.latency,
            "details": result.details
        }

    @staticmethod
    def check_all_health(db: Session) -> list[Dict[str, Any]]:
        """检查所有启用的数据源健康状态"""
        data_sources = db.query(DataSource).filter(DataSource.is_active == True).all()
        results = []

        for data_source in data_sources:
            result = DataSourceService.check_health(db, data_source.id)
            results.append(result)

        return results

    @staticmethod
    def get_databases(db: Session, data_source_id: int) -> Dict[str, Any]:
        """获取数据源的数据库列表"""
        start_time = time.time()
        data_source = DataSourceService.get_by_id(db, data_source_id)
        if not data_source or not data_source.is_active:
            # 记录使用统计
            DataSourceService.record_usage(
                db=db,
                datasource_id=data_source_id,
                operation_type="metadata",
                duration=int((time.time() - start_time) * 1000),
                success=False,
                error_message="数据源不存在或未启用"
            )
            return {
                "success": False,
                "message": "数据源不存在或未启用",
                "databases": []
            }

        # 解密密码
        try:
            password = DataSourceService._safe_decrypt_password(data_source.password)
        except ValueError as e:
            DataSourceService.record_usage(
                db=db,
                datasource_id=data_source_id,
                operation_type="metadata",
                duration=int((time.time() - start_time) * 1000),
                success=False,
                error_message=str(e)
            )
            return {
                "success": False,
                "message": str(e),
                "databases": []
            }

        # 获取处理器
        ds_type = str(data_source.type).strip().lower()
        handler = DataSourceHandlerFactory.get_handler(ds_type)
        if not handler:
            # 记录使用统计
            DataSourceService.record_usage(
                db=db,
                datasource_id=data_source_id,
                operation_type="metadata",
                duration=int((time.time() - start_time) * 1000),
                success=False,
                error_message=f"不支持的数据源类型: {data_source.type}"
            )
            return {
                "success": False,
                "message": f"不支持的数据源类型: {data_source.type}",
                "databases": []
            }

        # 获取数据库列表
        success, message, databases = handler.get_databases(
            data_source.host,
            data_source.port,
            data_source.database,
            data_source.username,
            password,
            data_source.extra_config
        )

        databases = [d for d in (databases or []) if DataSourceService._normalize_db_name(d)]

        # 记录使用统计
        error_message = None if success else message
        DataSourceService.record_usage(
            db=db,
            datasource_id=data_source_id,
            operation_type="metadata",
            duration=int((time.time() - start_time) * 1000),
            success=success,
            error_message=error_message
        )

        return {
            "success": success,
            "message": message,
            "databases": databases
        }

    @staticmethod
    def get_tables(db: Session, data_source_id: int, database: str) -> Dict[str, Any]:
        """获取指定数据库的表列表"""
        start_time = time.time()
        data_source = DataSourceService.get_by_id(db, data_source_id)
        if not data_source or not data_source.is_active:
            # 记录使用统计
            DataSourceService.record_usage(
                db=db,
                datasource_id=data_source_id,
                operation_type="metadata",
                duration=int((time.time() - start_time) * 1000),
                success=False,
                error_message="数据源不存在或未启用"
            )
            return {
                "success": False,
                "message": "数据源不存在或未启用",
                "tables": []
            }

        # 解密密码
        try:
            password = DataSourceService._safe_decrypt_password(data_source.password)
        except ValueError as e:
            DataSourceService.record_usage(
                db=db,
                datasource_id=data_source_id,
                operation_type="metadata",
                duration=int((time.time() - start_time) * 1000),
                success=False,
                error_message=str(e)
            )
            return {
                "success": False,
                "message": str(e),
                "tables": []
            }

        # 获取处理器
        ds_type = str(data_source.type).strip().lower()
        handler = DataSourceHandlerFactory.get_handler(ds_type)
        if not handler:
            # 记录使用统计
            DataSourceService.record_usage(
                db=db,
                datasource_id=data_source_id,
                operation_type="metadata",
                duration=int((time.time() - start_time) * 1000),
                success=False,
                error_message=f"不支持的数据源类型: {data_source.type}"
            )
            return {
                "success": False,
                "message": f"不支持的数据源类型: {data_source.type}",
                "tables": []
            }

        # 获取表列表
        success, message, tables = handler.get_tables(
            data_source.host,
            data_source.port,
            database,
            data_source.username,
            password,
            data_source.extra_config
        )

        # 记录使用统计
        error_message = None if success else message
        DataSourceService.record_usage(
            db=db,
            datasource_id=data_source_id,
            operation_type="metadata",
            duration=int((time.time() - start_time) * 1000),
            success=success,
            error_message=error_message
        )

        return {
            "success": success,
            "message": message,
            "tables": tables
        }

    @staticmethod
    def get_table_structure(db: Session, data_source_id: int, database: str, table: str) -> Dict[str, Any]:
        """获取指定表的结构"""
        start_time = time.time()
        data_source = DataSourceService.get_by_id(db, data_source_id)
        if not data_source or not data_source.is_active:
            # 记录使用统计
            DataSourceService.record_usage(
                db=db,
                datasource_id=data_source_id,
                operation_type="metadata",
                duration=int((time.time() - start_time) * 1000),
                success=False,
                error_message="数据源不存在或未启用"
            )
            return {
                "success": False,
                "message": "数据源不存在或未启用",
                "columns": []
            }

        # 解密密码
        try:
            password = DataSourceService._safe_decrypt_password(data_source.password)
        except ValueError as e:
            DataSourceService.record_usage(
                db=db,
                datasource_id=data_source_id,
                operation_type="metadata",
                duration=int((time.time() - start_time) * 1000),
                success=False,
                error_message=str(e)
            )
            return {
                "success": False,
                "message": str(e),
                "columns": []
            }

        # 获取处理器
        ds_type = str(data_source.type).strip().lower()
        handler = DataSourceHandlerFactory.get_handler(ds_type)
        if not handler:
            # 记录使用统计
            DataSourceService.record_usage(
                db=db,
                datasource_id=data_source_id,
                operation_type="metadata",
                duration=int((time.time() - start_time) * 1000),
                success=False,
                error_message=f"不支持的数据源类型: {data_source.type}"
            )
            return {
                "success": False,
                "message": f"不支持的数据源类型: {data_source.type}",
                "columns": []
            }

        # 获取表结构
        success, message, columns = handler.get_table_structure(
            data_source.host,
            data_source.port,
            database,
            table,
            data_source.username,
            password,
            data_source.extra_config
        )

        # 记录使用统计
        error_message = None if success else message
        DataSourceService.record_usage(
            db=db,
            datasource_id=data_source_id,
            operation_type="metadata",
            duration=int((time.time() - start_time) * 1000),
            success=success,
            error_message=error_message
        )

        return {
            "success": success,
            "message": message,
            "columns": columns
        }

    @staticmethod
    def record_usage(
        db: Session,
        datasource_id: int,
        operation_type: str,
        duration: int,
        success: bool,
        error_message: Optional[str] = None
    ) -> DataSourceUsage:
        """记录数据源使用情况"""
        usage = DataSourceUsage(
            datasource_id=datasource_id,
            operation_type=operation_type,
            duration=duration,
            success=success,
            error_message=error_message
        )
        db.add(usage)
        db.commit()
        db.refresh(usage)
        return usage

    @staticmethod
    def get_usage_statistics(db: Session, datasource_id: Optional[int] = None, days: int = 7) -> Dict[str, Any]:
        """获取数据源使用统计"""
        from datetime import datetime, timedelta
        start_date = datetime.now() - timedelta(days=days)

        query = db.query(DataSourceUsage).filter(DataSourceUsage.created_at >= start_date)
        if datasource_id:
            query = query.filter(DataSourceUsage.datasource_id == datasource_id)

        usage_records = query.all()

        # 统计数据
        total_operations = len(usage_records)
        successful_operations = sum(1 for record in usage_records if record.success)
        failed_operations = total_operations - successful_operations

        # 按操作类型统计
        operation_stats = {}
        for record in usage_records:
            if record.operation_type not in operation_stats:
                operation_stats[record.operation_type] = {
                    "total": 0,
                    "success": 0,
                    "failure": 0,
                    "total_duration": 0
                }
            operation_stats[record.operation_type]["total"] += 1
            if record.success:
                operation_stats[record.operation_type]["success"] += 1
            else:
                operation_stats[record.operation_type]["failure"] += 1
            operation_stats[record.operation_type]["total_duration"] += record.duration

        # 计算平均耗时
        for op_type, stats in operation_stats.items():
            if stats["total"] > 0:
                stats["average_duration"] = stats["total_duration"] / stats["total"]
            else:
                stats["average_duration"] = 0

        return {
            "total_operations": total_operations,
            "successful_operations": successful_operations,
            "failed_operations": failed_operations,
            "success_rate": successful_operations / total_operations if total_operations > 0 else 0,
            "operation_stats": operation_stats,
            "period": f"最近 {days} 天"
        }

    @staticmethod
    def get_usage_history(db: Session, datasource_id: int, skip: int = 0, limit: int = 50) -> list[Dict[str, Any]]:
        """获取数据源使用历史"""
        usage_records = db.query(DataSourceUsage).filter(
            DataSourceUsage.datasource_id == datasource_id
        ).order_by(DataSourceUsage.created_at.desc()).offset(skip).limit(limit).all()

        return [{
            "id": record.id,
            "operation_type": record.operation_type,
            "duration": record.duration,
            "success": record.success,
            "error_message": record.error_message,
            "created_at": record.created_at
        } for record in usage_records]

    @staticmethod
    def batch_create(db: Session, datasources: list) -> Dict[str, Any]:
        """批量创建数据源"""
        total = len(datasources)
        successful = 0
        failed = 0
        details = []

        for data in datasources:
            try:
                # 检查名称是否已存在
                existing = DataSourceService.get_by_name(db, data.name)
                if existing:
                    failed += 1
                    details.append({
                        "success": False,
                        "name": data.name,
                        "message": "数据源名称已存在"
                    })
                    continue

                # 创建数据源
                datasource = DataSourceService.create(db, data)
                successful += 1
                details.append({
                    "success": True,
                    "id": datasource.id,
                    "name": datasource.name,
                    "message": "创建成功"
                })
            except ValueError as e:
                # 验证错误
                failed += 1
                details.append({
                    "success": False,
                    "name": data.name if hasattr(data, "name") else "未知",
                    "message": f"验证失败: {str(e)}"
                })
            except Exception as e:
                failed += 1
                details.append({
                    "success": False,
                    "name": data.name if hasattr(data, "name") else "未知",
                    "message": f"创建失败: {str(e)}"
                })

        return {
            "success": failed == 0,
            "total": total,
            "successful": successful,
            "failed": failed,
            "details": details
        }

    @staticmethod
    def batch_update(db: Session, updates: list) -> Dict[str, Any]:
        """批量更新数据源"""
        total = len(updates)
        successful = 0
        failed = 0
        details = []

        for update in updates:
            try:
                datasource_id = update.get("id")
                if not datasource_id:
                    failed += 1
                    details.append({
                        "success": False,
                        "id": None,
                        "message": "缺少数据源 ID"
                    })
                    continue

                # 构建更新数据
                update_data = {k: v for k, v in update.items() if k != "id"}
                update_schema = DataSourceUpdate(**update_data)

                # 更新数据源
                datasource = DataSourceService.update(db, datasource_id, update_schema)
                if not datasource:
                    failed += 1
                    details.append({
                        "success": False,
                        "id": datasource_id,
                        "message": "数据源不存在"
                    })
                    continue

                successful += 1
                details.append({
                    "success": True,
                    "id": datasource_id,
                    "message": "更新成功"
                })
            except ValueError as e:
                # 验证错误
                failed += 1
                details.append({
                    "success": False,
                    "id": update.get("id"),
                    "message": f"验证失败: {str(e)}"
                })
            except Exception as e:
                failed += 1
                details.append({
                    "success": False,
                    "id": update.get("id"),
                    "message": f"更新失败: {str(e)}"
                })

        return {
            "success": failed == 0,
            "total": total,
            "successful": successful,
            "failed": failed,
            "details": details
        }

    @staticmethod
    def batch_delete(db: Session, ids: list) -> Dict[str, Any]:
        """批量删除数据源"""
        total = len(ids)
        successful = 0
        failed = 0
        details = []

        for datasource_id in ids:
            try:
                # 删除数据源
                success = DataSourceService.delete(db, datasource_id)
                if success:
                    successful += 1
                    details.append({
                        "success": True,
                        "id": datasource_id,
                        "message": "删除成功"
                    })
                else:
                    failed += 1
                    details.append({
                        "success": False,
                        "id": datasource_id,
                        "message": "数据源不存在"
                    })
            except Exception as e:
                failed += 1
                details.append({
                    "success": False,
                    "id": datasource_id,
                    "message": f"删除失败: {str(e)}"
                })

        return {
            "success": failed == 0,
            "total": total,
            "successful": successful,
            "failed": failed,
            "details": details
        }
