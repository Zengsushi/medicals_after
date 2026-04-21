"""
    数据同步服务
    支持 Hive ↔ MySQL/PostgreSQL（Hive 经 Spark），以及 MySQL/PostgreSQL → Hive。
"""
import logging
import json
from datetime import date, datetime
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text, inspect

from apps.sync.models import (
    SyncTask, SyncLog, SyncTaskStatus, SyncMode
)
from apps.sync.schemas import SyncTaskCreate, SyncTaskUpdate
from apps.datasource.models import DataSource, DataSourceType
from apps.datasource.service import DataSourceService

logger = logging.getLogger(__name__)


class SyncService:
    """数据同步服务"""

    @staticmethod
    def _split_schema_table(table_name: str) -> Tuple[Optional[str], str]:
        """解析 schema.table，无 schema 时返回 (None, table)。"""
        s = str(table_name).strip().strip("`").strip('"')
        if "." in s:
            a, b = s.split(".", 1)
            return a.strip(), b.strip()
        return None, s

    @staticmethod
    def _hive_ddl_type_from_relational(decl_type: str) -> str:
        """将 MySQL/PG 声明式类型粗映射为 Hive 类型（自动建表用）。"""
        t = (decl_type or "").lower()
        if "bigint" in t:
            return "BIGINT"
        if "smallint" in t or "tinyint" in t:
            return "INT"
        if "int" in t and "point" not in t:
            return "INT"
        if "double" in t or "real" in t or "float" in t or "numeric" in t or "decimal" in t:
            return "DOUBLE"
        if "bool" in t:
            return "BOOLEAN"
        if "timestamp" in t or "datetime" in t:
            return "TIMESTAMP"
        if "date" in t and "time" not in t:
            return "DATE"
        return "STRING"

    @staticmethod
    def _ensure_hive_target_table(spark, target_qualified: str, source_columns: List[Dict[str, Any]]):
        """若 Hive 目标表不存在则创建（列类型由关系型源类型粗映射）。"""
        name = str(target_qualified).strip().strip("`")
        parts = name.split(".")
        if len(parts) == 2:
            spark.sql(f"USE `{parts[0]}`")
        exists = False
        try:
            exists = bool(spark.catalog.tableExists(name))
        except Exception:
            exists = False
        if exists:
            return
        col_defs = []
        for c in source_columns:
            cn = str(c["name"]).strip().strip("`").strip('"')
            ht = SyncService._hive_ddl_type_from_relational(str(c.get("type", "")))
            col_defs.append(f"`{cn}` {ht}")
        ddl = f"CREATE TABLE IF NOT EXISTS {name} ({', '.join(col_defs)}) STORED AS PARQUET"
        spark.sql(ddl)

    @staticmethod
    def _source_to_target_type(src_type: str, dialect: str = "mysql") -> str:
        """将源字段类型映射到目标库类型（参考 test/DSD/hive_to_mysql.py）。"""
        t = (src_type or "").lower().strip()
        if dialect == "postgresql":
            if "bigint" in t:
                return "BIGINT"
            if "smallint" in t:
                return "SMALLINT"
            if "tinyint" in t or "boolean" in t:
                return "SMALLINT"
            if "int" in t:
                return "INTEGER"
            if "double" in t or "float" in t:
                return "DOUBLE PRECISION"
            if "decimal" in t:
                return "NUMERIC"
            if "timestamp" in t:
                return "TIMESTAMP"
            if "date" in t:
                return "DATE"
            return "TEXT"

        # 默认按 MySQL 映射
        if "bigint" in t:
            return "BIGINT"
        if "smallint" in t:
            return "SMALLINT"
        if "tinyint" in t:
            return "TINYINT"
        if "int" in t:
            return "INT"
        if "double" in t or "float" in t:
            return "DOUBLE"
        if "decimal" in t:
            return "DECIMAL(20,6)"
        if "timestamp" in t:
            return "DATETIME"
        if "date" in t:
            return "DATE"
        if "boolean" in t:
            return "TINYINT(1)"
        return "TEXT"

    @staticmethod
    def _ensure_target_table(conn, table_name: str, source_columns: List[Dict[str, Any]], dialect: str = "mysql"):
        """确保目标表存在；不存在则创建，存在则补齐缺失列。"""
        inspector = inspect(conn)
        existing_tables = set(inspector.get_table_names())
        src_cols = [(c["name"], c.get("type", "")) for c in source_columns if c.get("name")]
        if not src_cols:
            raise ValueError("源表字段为空，无法创建目标表")

        if table_name not in existing_tables:
            defs = [
                f"`{name}` {SyncService._source_to_target_type(col_type, dialect)}"
                if dialect == "mysql" else f"\"{name}\" {SyncService._source_to_target_type(col_type, dialect)}"
                for name, col_type in src_cols
            ]
            create_sql = (
                f"CREATE TABLE `{table_name}` ({', '.join(defs)})"
                if dialect == "mysql" else
                f"CREATE TABLE \"{table_name}\" ({', '.join(defs)})"
            )
            with conn.begin() as connection:
                connection.execute(text(create_sql))
            return

        existing_cols = {c["name"] for c in inspector.get_columns(table_name)}
        for name, col_type in src_cols:
            if name in existing_cols:
                continue
            alter_sql = (
                f"ALTER TABLE `{table_name}` ADD COLUMN `{name}` {SyncService._source_to_target_type(col_type, dialect)}"
                if dialect == "mysql" else
                f"ALTER TABLE \"{table_name}\" ADD COLUMN \"{name}\" {SyncService._source_to_target_type(col_type, dialect)}"
            )
            with conn.begin() as connection:
                connection.execute(text(alter_sql))

    @staticmethod
    def create_sync_task(db: Session, task_data: SyncTaskCreate) -> SyncTask:
        """创建同步任务"""
        task = SyncTask(**task_data.dict())
        db.add(task)
        db.commit()
        db.refresh(task)
        SyncService.add_log(db, task.id, "INFO", f"同步任务创建成功: {task.name}")
        return task

    @staticmethod
    def update_sync_task(db: Session, task_id: int, task_data: SyncTaskUpdate) -> Optional[SyncTask]:
        """更新同步任务"""
        task = db.query(SyncTask).filter(SyncTask.id == task_id).first()
        if not task:
            return None
        
        update_data = task_data.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(task, key, value)
        
        db.commit()
        db.refresh(task)
        SyncService.add_log(db, task_id, "INFO", f"同步任务更新成功")
        return task

    @staticmethod
    def delete_sync_task(db: Session, task_id: int) -> bool:
        """删除同步任务"""
        task = db.query(SyncTask).filter(SyncTask.id == task_id).first()
        if not task:
            return False
        
        db.delete(task)
        db.commit()
        return True

    @staticmethod
    def get_sync_task(db: Session, task_id: int) -> Optional[SyncTask]:
        """获取同步任务"""
        return db.query(SyncTask).filter(SyncTask.id == task_id).first()

    @staticmethod
    def get_sync_tasks(db: Session, skip: int = 0, limit: int = 100) -> List[SyncTask]:
        """获取同步任务列表"""
        return db.query(SyncTask).order_by(SyncTask.created_at.desc()).offset(skip).limit(limit).all()

    @staticmethod
    def add_log(db: Session, task_id: int, level: str, message: str):
        """添加同步日志"""
        log = SyncLog(task_id=task_id, level=level, message=message)
        db.add(log)
        db.commit()

    @staticmethod
    def get_sync_logs(db: Session, task_id: int, skip: int = 0, limit: int = 100) -> List[SyncLog]:
        """获取同步日志"""
        return db.query(SyncLog).filter(
            SyncLog.task_id == task_id
        ).order_by(SyncLog.created_at.desc()).offset(skip).limit(limit).all()

    @staticmethod
    def get_source_connection(ds: DataSource) -> Tuple[Any, str]:
        """获取源数据源连接"""
        if ds.type == DataSourceType.HIVE:
            return SyncService._get_hive_connection(ds)
        elif ds.type in [DataSourceType.MYSQL, DataSourceType.POSTGRESQL]:
            return SyncService._get_sqlalchemy_connection(ds)
        raise ValueError(f"不支持的源数据源类型: {ds.type}")

    @staticmethod
    def get_target_connection(ds: DataSource) -> Tuple[Any, str]:
        """获取目标数据源连接（Hive 走 Spark，与源端一致；勿用 thrift:// 走 SQLAlchemy）。"""
        if ds.type == DataSourceType.HIVE:
            return SyncService._get_hive_connection(ds)
        if ds.type in [DataSourceType.MYSQL, DataSourceType.POSTGRESQL]:
            return SyncService._get_sqlalchemy_connection(ds)
        raise ValueError(f"不支持的目标数据源类型: {ds.type}")

    @staticmethod
    def _get_hive_connection(ds: DataSource):
        """获取 Hive SparkSession（与 test/DSD/hive_to_mysql.py 保持一致）"""
        try:
            from apps.analyse.config import init_spark_connect_hive
            spark = init_spark_connect_hive()
            if ds.database:
                spark.sql(f"USE {ds.database}")
            return spark, "spark"
        except Exception as e:
            raise RuntimeError(f"Hive Spark 连接失败: {e}")

    @staticmethod
    def _get_sqlalchemy_connection(ds: DataSource):
        """获取 SQLAlchemy 连接"""
        conn_str = ds.get_connection_string()
        if ds.type == DataSourceType.POSTGRESQL:
            try:
                import psycopg2
            except ImportError:
                raise ImportError("请安装 psycopg2: pip install psycopg2-binary")
        elif ds.type == DataSourceType.MYSQL:
            try:
                import pymysql
            except ImportError:
                raise ImportError("请安装 pymysql: pip install pymysql")
        
        from sqlalchemy import create_engine
        engine = create_engine(conn_str)
        return engine, "sqlalchemy"

    @staticmethod
    def get_table_columns(conn, conn_type: str, table_name: str) -> List[Dict[str, Any]]:
        """获取表的列信息"""
        columns = []
        seen_names = set()
        if conn_type == "spark":
            rows = conn.sql(f"DESCRIBE {table_name}").collect()
            for row in rows:
                col_name = str(row[0]).strip() if row and len(row) > 0 and row[0] is not None else ""
                col_type = str(row[1]).strip() if row and len(row) > 1 and row[1] is not None else ""
                # 过滤 Hive DESCRIBE 的分区/注释元信息行，避免把非法列名写入 SQL
                if not col_name or col_name.startswith("#"):
                    continue
                # Hive 常见重复列（字段区+分区字段区）去重
                if col_name in seen_names:
                    continue
                seen_names.add(col_name)
                columns.append({
                    "name": col_name,
                    "type": col_type
                })
        elif conn_type == "sqlalchemy":
            from sqlalchemy import inspect
            inspector = inspect(conn)
            schema, tbl = SyncService._split_schema_table(table_name)
            col_iter = inspector.get_columns(tbl, schema=schema) if schema else inspector.get_columns(tbl)
            for col in col_iter:
                columns.append({
                    "name": col["name"],
                    "type": str(col["type"])
                })
        return columns

    @staticmethod
    def get_table_list(conn, conn_type: str) -> List[str]:
        """获取表列表"""
        tables = []
        if conn_type == "spark":
            rows = conn.sql("SHOW TABLES").collect()
            for row in rows:
                table_name = getattr(row, "tableName", None) or (row[1] if len(row) > 1 else None)
                if table_name:
                    tables.append(table_name)
        elif conn_type == "sqlalchemy":
            from sqlalchemy import inspect
            inspector = inspect(conn)
            tables = inspector.get_table_names()
        return tables

    @staticmethod
    def preview_table_data(db: Session, datasource_id: int, table_name: str, limit: int = 50) -> Dict[str, Any]:
        """预览表数据"""
        ds = db.query(DataSource).filter(DataSource.id == datasource_id).first()
        if not ds:
            raise ValueError("数据源不存在")
        
        conn, conn_type = SyncService.get_source_connection(ds)
        try:
            columns = SyncService.get_table_columns(conn, conn_type, table_name)
            data = []
            
            if conn_type == "spark":
                df = conn.sql(f"SELECT * FROM {table_name} LIMIT {limit}")
                for row in df.collect():
                    data.append(dict(zip([c["name"] for c in columns], list(row))))
            elif conn_type == "sqlalchemy":
                with conn.connect() as sql_conn:
                    result = sql_conn.execute(text(f"SELECT * FROM {table_name} LIMIT {limit}"))
                    for row in result:
                        data.append(dict(row._mapping))
            
            return {
                "columns": columns,
                "data": data
            }
        finally:
            if conn_type == "spark":
                conn.stop()
            elif hasattr(conn, "close"):
                conn.close()

    @staticmethod
    def execute_sync(db: Session, task_id: int):
        """执行同步任务"""
        task = db.query(SyncTask).filter(SyncTask.id == task_id).first()
        if not task:
            raise ValueError("同步任务不存在")
        
        if task.status == SyncTaskStatus.RUNNING:
            raise ValueError("任务正在运行中")
        
        task.status = SyncTaskStatus.RUNNING
        task.started_at = datetime.utcnow()
        task.progress = 0
        db.commit()
        
        SyncService.add_log(db, task_id, "INFO", f"开始执行同步任务: {task.name}")
        
        try:
            SyncService._do_sync(db, task)
            db.refresh(task)
            if task.status == SyncTaskStatus.CANCELLED:
                task.error_message = "任务已取消"
                SyncService.add_log(db, task_id, "INFO", "同步任务已取消")
            else:
                task.status = SyncTaskStatus.SUCCESS
                task.progress = 100
                SyncService.add_log(db, task_id, "INFO", f"同步任务完成，同步行数: {task.row_count}")
        except Exception as e:
            task.status = SyncTaskStatus.FAILED
            task.error_message = str(e)
            SyncService.add_log(db, task_id, "ERROR", f"同步任务失败: {str(e)}")
            logger.exception(f"同步任务失败: {task_id}")
        finally:
            task.completed_at = datetime.utcnow()
            db.commit()

    @staticmethod
    def _do_sync(db: Session, task: SyncTask):
        """执行实际同步逻辑"""
        source_ds = db.query(DataSource).filter(DataSource.id == task.source_id).first()
        target_ds = db.query(DataSource).filter(DataSource.id == task.target_id).first()
        
        if not source_ds or not target_ds:
            raise ValueError("源或目标数据源不存在")
        
        SyncService.add_log(db, task.id, "INFO", f"源数据源: {source_ds.name}, 目标数据源: {target_ds.name}")
        SyncService.add_log(db, task.id, "INFO", f"源表: {task.source_table}, 目标表: {task.target_table}")
        
        source_conn, source_conn_type = SyncService.get_source_connection(source_ds)
        target_conn, target_conn_type = SyncService.get_target_connection(target_ds)

        total_rows = 0

        try:
            # 全量同步统一先清空目标表（不依赖源类型）
            if task.sync_mode == SyncMode.FULL and target_conn_type == "spark":
                try:
                    target_conn.sql(f"TRUNCATE TABLE {task.target_table}")
                    SyncService.add_log(db, task.id, "INFO", f"全量同步已清空 Hive 目标表: {task.target_table}")
                except Exception as e:
                    logger.warning("Hive TRUNCATE 失败（可能为外表或权限）: %s", e)
                    SyncService.add_log(
                        db,
                        task.id,
                        "WARN",
                        f"Hive 目标表未执行 TRUNCATE: {e}。"
                        "若提示 Permission denied，请在启动后端前设置环境变量 HADOOP_USER_NAME 或 SPARK_HDFS_USER"
                        " 为对 warehouse 有写权限的集群用户（如 hadoop），并重启进程。",
                    )
            if task.sync_mode == SyncMode.FULL and target_conn_type == "sqlalchemy":
                dialect = getattr(getattr(target_conn, "dialect", None), "name", "mysql")
                target_table_expr = task.target_table
                if dialect == "mysql":
                    parts = [p.strip().strip("`") for p in str(target_table_expr).split(".") if p.strip()]
                    quoted_table = ".".join([f"`{p}`" for p in parts])
                    truncate_sql = f"TRUNCATE TABLE {quoted_table}"
                else:
                    parts = [p.strip().strip('"') for p in str(target_table_expr).split(".") if p.strip()]
                    quoted_table = ".".join([f"\"{p}\"" for p in parts])
                    truncate_sql = f"TRUNCATE TABLE {quoted_table} RESTART IDENTITY"
                with target_conn.begin() as connection:
                    connection.execute(text(truncate_sql))
                SyncService.add_log(db, task.id, "INFO", f"全量同步已清空目标表: {task.target_table}")

            source_columns = SyncService.get_table_columns(source_conn, source_conn_type, task.source_table)
            SyncService.add_log(db, task.id, "INFO", f"源表列数: {len(source_columns)}")
            
            column_names = [c["name"] for c in source_columns]
            if task.column_mapping:
                column_mapping = task.column_mapping
                source_columns = [c for c in source_columns if c["name"] in column_mapping]
                column_names = [column_mapping[c["name"]] for c in source_columns]
            
            if source_conn_type == "spark":
                # 参考 hive_to_mysql.py：同步前确保目标表存在/结构可用
                if target_conn_type == "sqlalchemy":
                    target_table = task.target_table.split(".")[-1]
                    dialect = getattr(getattr(target_conn, "dialect", None), "name", "mysql")
                    SyncService._ensure_target_table(target_conn, target_table, source_columns, dialect=dialect)
                elif target_conn_type == "spark":
                    SyncService.add_log(
                        db, task.id, "INFO",
                        "目标为 Hive：请确保目标表已存在且列与源兼容（当前不自动建 Hive 目标表）。",
                    )

                query = f"SELECT {', '.join([c['name'] for c in source_columns])} FROM {task.source_table}"
                if task.sync_mode == SyncMode.INCREMENTAL and task.sync_condition:
                    query += f" WHERE {task.sync_condition}"
                df = source_conn.sql(query)
                
                batch_size = max(1, int(task.batch_size or 1000))
                batch = []
                
                for row in df.toLocalIterator():
                    # 任务取消即时生效
                    db.refresh(task)
                    if task.status == SyncTaskStatus.CANCELLED:
                        SyncService.add_log(db, task.id, "INFO", "任务已取消，停止同步")
                        break
                    batch.append(tuple(row))
                    if len(batch) < batch_size:
                        continue
                    
                    SyncService._insert_batch(
                        target_conn, target_conn_type, 
                        task.target_table, column_names, batch
                    )
                    
                    total_rows += len(batch)
                    task.row_count = total_rows
                    task.progress = min(task.progress + 1, 99)
                    db.commit()
                    
                    SyncService.add_log(db, task.id, "INFO", f"已同步 {total_rows} 行")
                    batch = []

                if batch and task.status != SyncTaskStatus.CANCELLED:
                    SyncService._insert_batch(
                        target_conn, target_conn_type,
                        task.target_table, column_names, batch
                    )
                    total_rows += len(batch)
                    task.row_count = total_rows
                    db.commit()

            elif source_conn_type == "sqlalchemy" and target_conn_type == "spark":
                # MySQL / PostgreSQL → Hive：流式读关系型表，批量 insertInto Hive
                src_dialect = getattr(getattr(source_conn, "dialect", None), "name", "mysql")
                SyncService._ensure_hive_target_table(target_conn, task.target_table, source_columns)
                if src_dialect == "postgresql":
                    select_cols = ", ".join(f'"{c["name"]}"' for c in source_columns)
                else:
                    select_cols = ", ".join(f"`{c['name']}`" for c in source_columns)
                base_sql = f"SELECT {select_cols} FROM {task.source_table}"
                if task.sync_mode == SyncMode.INCREMENTAL and task.sync_condition:
                    base_sql += f" WHERE {task.sync_condition}"

                batch_size = max(1, int(task.batch_size or 1000))
                stmt = text(base_sql).execution_options(stream_results=True)
                with source_conn.connect() as sql_conn:
                    result = sql_conn.execute(stmt)
                    while True:
                        db.refresh(task)
                        if task.status == SyncTaskStatus.CANCELLED:
                            SyncService.add_log(db, task.id, "INFO", "任务已取消，停止同步")
                            break
                        chunk = result.fetchmany(batch_size)
                        if not chunk:
                            break
                        batch = [tuple(row) for row in chunk]
                        SyncService._insert_batch(
                            target_conn,
                            target_conn_type,
                            task.target_table,
                            column_names,
                            batch,
                        )
                        total_rows += len(batch)
                        task.row_count = total_rows
                        task.progress = min(task.progress + 1, 99)
                        db.commit()
                        SyncService.add_log(db, task.id, "INFO", f"已同步 {total_rows} 行")

            task.row_count = total_rows

        finally:
            same_spark = (
                source_conn_type == "spark"
                and target_conn_type == "spark"
                and source_conn is target_conn
            )
            if same_spark:
                source_conn.stop()
            else:
                if source_conn_type == "spark":
                    source_conn.stop()
                elif hasattr(source_conn, "dispose"):
                    source_conn.dispose()
                elif hasattr(source_conn, "close"):
                    source_conn.close()

                if target_conn_type == "spark":
                    target_conn.stop()
                elif hasattr(target_conn, "dispose"):
                    target_conn.dispose()
                elif hasattr(target_conn, "close"):
                    target_conn.close()

    @staticmethod
    def _hive_sql_value_literal(val: Any) -> str:
        """将 Python 值格式化为通用 SQL 字面量（无目标类型信息时的回退）。"""
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "TRUE" if val else "FALSE"
        if isinstance(val, int) and not isinstance(val, bool):
            return str(val)
        if isinstance(val, float):
            return repr(val)
        if isinstance(val, Decimal):
            return str(val)
        if isinstance(val, datetime):
            return "'" + val.strftime("%Y-%m-%d %H:%M:%S").replace("'", "''") + "'"
        if isinstance(val, date):
            return "'" + val.strftime("%Y-%m-%d").replace("'", "''") + "'"
        if isinstance(val, (bytes, bytearray)):
            return "'" + val.hex() + "'"
        s = str(val).replace("'", "''")
        return f"'{s}'"

    @staticmethod
    def _hive_sql_value_literal_for_type(val: Any, spark_dt: Any) -> str:
        """
        按 Hive 目标列的 Spark DataType 生成字面量，避免 INSERT VALUES 被解析为与表不兼容的类型
        （例如 MySQL tinyint(1)/INT 写入 BOOLEAN 列）。
        """
        tn = spark_dt.typeName() if spark_dt is not None and hasattr(spark_dt, "typeName") else ""

        if tn == "boolean":
            if val is None:
                return "NULL"
            if isinstance(val, bool):
                return "TRUE" if val else "FALSE"
            if isinstance(val, (int, float)):
                return "TRUE" if int(val) != 0 else "FALSE"
            if isinstance(val, str):
                v = val.strip().lower()
                if v in ("1", "true", "t", "yes", "y", "on"):
                    return "TRUE"
                if v in ("0", "false", "f", "no", "n", "off", ""):
                    return "FALSE"
            return "TRUE" if bool(val) else "FALSE"

        if tn in ("byte", "short", "integer", "long"):
            if val is None:
                return "NULL"
            try:
                return str(int(val))
            except (TypeError, ValueError):
                return str(int(str(val).strip()))

        if tn in ("float", "double"):
            if val is None:
                return "NULL"
            try:
                return repr(float(val))
            except (TypeError, ValueError):
                return repr(float(str(val).strip()))

        if tn == "date":
            if val is None:
                return "NULL"
            if isinstance(val, datetime):
                return f"DATE '{val.strftime('%Y-%m-%d')}'"
            if isinstance(val, date):
                return f"DATE '{val.strftime('%Y-%m-%d')}'"
            s = str(val).strip().replace("'", "''")[:10]
            return f"DATE '{s}'"

        if tn == "timestamp":
            if val is None:
                return "NULL"
            if isinstance(val, datetime):
                ts = val.strftime("%Y-%m-%d %H:%M:%S").replace("'", "''")
                return f"TIMESTAMP '{ts}'"
            s = str(val).strip().replace("'", "''")
            return f"TIMESTAMP '{s}'"

        if tn in ("string", "char", "varchar"):
            if val is None:
                return "NULL"
            s = str(val).replace("'", "''")
            return f"'{s}'"

        if tn == "decimal":
            if val is None:
                return "NULL"
            try:
                d = val if isinstance(val, Decimal) else Decimal(str(val))
                return str(d)
            except Exception:
                return SyncService._hive_sql_value_literal(val)

        if tn == "binary":
            if val is None:
                return "NULL"
            if isinstance(val, (bytes, bytearray)):
                return "X'" + val.hex() + "'"
            return SyncService._hive_sql_value_literal(val)

        return SyncService._hive_sql_value_literal(val)

    @staticmethod
    def _insert_batch_into_hive_sql(
        spark,
        qualified: str,
        column_names: List[str],
        batch: List[tuple],
        spark_dtypes: List[Any],
    ) -> None:
        """
        使用 Spark SQL 的 INSERT ... VALUES 写入 Hive，避免 DataFrameWriter.insertInto
        在部分环境（Windows / Docker Desktop）触发 Python worker 回连失败。
        """
        safe_cols = [str(c).replace("`", "") for c in column_names]
        cols_sql = ", ".join(f"`{c}`" for c in safe_cols)

        # 控制单条 SQL 体积，避免解析/传输过大
        max_rows_per_stmt = 200
        for start in range(0, len(batch), max_rows_per_stmt):
            chunk = batch[start : start + max_rows_per_stmt]
            value_groups = []
            for row in chunk:
                if len(row) != len(column_names):
                    raise ValueError(
                        f"写入 Hive 失败：一行数据长度 {len(row)} 与列数 {len(column_names)} 不一致"
                    )
                cells = [
                    SyncService._hive_sql_value_literal_for_type(row[i], spark_dtypes[i])
                    for i in range(len(column_names))
                ]
                value_groups.append("(" + ", ".join(cells) + ")")
            values_clause = ", ".join(value_groups)
            sql = f"INSERT INTO {qualified} ({cols_sql}) VALUES {values_clause}"
            try:
                spark.sql(sql)
            except Exception as e:
                err = str(e)
                if "AccessControlException" in err or "Permission denied" in err:
                    raise RuntimeError(
                        "HDFS 拒绝写入：当前 Hadoop 用户对 Hive 表在 HDFS 上的目录没有写权限。"
                        "请设置环境变量 HADOOP_USER_NAME 或 SPARK_HDFS_USER 为有写权限的集群用户并重启后端。"
                        f" 详情: {err[:800]}"
                    ) from e
                raise RuntimeError(f"Spark SQL 写入 Hive 失败（表 {qualified}）: {err[:1200]}") from e

    @staticmethod
    def _insert_batch(conn, conn_type: str, table_name: str, column_names: List[str], batch: List[tuple]):
        """批量插入数据"""
        if conn_type == "spark":
            if not batch:
                return
            spark = conn
            qualified = str(table_name).strip().strip("`")
            try:
                target_tbl = spark.table(qualified)
            except Exception as e:
                raise RuntimeError(f"无法解析 Hive 目标表 {qualified}: {e}") from e

            tbl_cols = set(target_tbl.columns)
            missing = [c for c in column_names if c not in tbl_cols]
            if missing:
                raise ValueError(
                    f"写入 Hive 表 {qualified} 失败：目标表缺少列 {missing}，目标列为 {sorted(tbl_cols)}"
                )

            schema_by_name = {f.name: f.dataType for f in target_tbl.schema.fields}
            spark_dtypes = [schema_by_name[c] for c in column_names]
            SyncService._insert_batch_into_hive_sql(
                spark, qualified, column_names, batch, spark_dtypes
            )
            return
        if conn_type == "sqlalchemy":
            dialect = getattr(getattr(conn, "dialect", None), "name", "mysql")
            if dialect == "mysql":
                quoted_table = f"`{table_name.split('.')[-1]}`"
                quoted_cols = ", ".join([f"`{c}`" for c in column_names])
            else:
                quoted_table = f"\"{table_name.split('.')[-1]}\""
                quoted_cols = ", ".join([f"\"{c}\"" for c in column_names])

            placeholders = ", ".join([f":p{i}" for i in range(len(column_names))])
            insert_sql = f"INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})"

            rows = []
            for row in batch:
                rows.append({f"p{i}": val for i, val in enumerate(row)})

            with conn.begin() as connection:
                connection.execute(text(insert_sql), rows)

    @staticmethod
    def test_sync(db: Session, source_id: int, target_id: int, source_table: str, target_table: str, limit: int = 10):
        """测试同步（小批量）"""
        source_ds = db.query(DataSource).filter(DataSource.id == source_id).first()
        target_ds = db.query(DataSource).filter(DataSource.id == target_id).first()
        
        if not source_ds or not target_ds:
            raise ValueError("源或目标数据源不存在")
        
        source_conn, source_conn_type = SyncService.get_source_connection(source_ds)
        target_conn, target_conn_type = SyncService.get_target_connection(target_ds)
        
        try:
            source_columns = SyncService.get_table_columns(source_conn, source_conn_type, source_table)
            column_names = [c["name"] for c in source_columns]
            
            data = []
            if source_conn_type == "spark":
                df = source_conn.sql(f"SELECT * FROM {source_table} LIMIT {limit}")
                for row in df.collect():
                    data.append(dict(zip(column_names, list(row))))
            elif source_conn_type == "sqlalchemy":
                with source_conn.connect() as sql_conn:
                    result = sql_conn.execute(text(f"SELECT * FROM {source_table} LIMIT {limit}"))
                    for row in result:
                        data.append(dict(row._mapping))
            
            return {
                "success": True,
                "source_columns": source_columns,
                "sample_data": data,
                "message": "测试连接成功，数据预览已获取"
            }
        finally:
            same_spark = (
                source_conn_type == "spark"
                and target_conn_type == "spark"
                and source_conn is target_conn
            )
            if same_spark:
                source_conn.stop()
            else:
                if source_conn_type == "spark":
                    source_conn.stop()
                elif hasattr(source_conn, "dispose"):
                    source_conn.dispose()
                elif hasattr(source_conn, "close"):
                    source_conn.close()

                if target_conn_type == "spark":
                    target_conn.stop()
                elif hasattr(target_conn, "dispose"):
                    target_conn.dispose()
                elif hasattr(target_conn, "close"):
                    target_conn.close()
