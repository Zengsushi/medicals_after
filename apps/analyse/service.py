"""
    分析模块 - 服务层
    包含数据分析和统计相关业务逻辑
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_

from apps.user.models import User, UserProfile
from sqlalchemy import create_engine, text
from apps.analyse.config import init_spark_connect_hive
import logging
import json

logger = logging.getLogger(__name__)


class AnalyseService:
    """分析服务类"""

    @staticmethod
    def get_recent_registered_users(
        db: Session,
        days: int = 7,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        获取最近N天注册的真实用户
        
        Args:
            db: 数据库会话
            days: 天数范围，默认7天
            limit: 返回数量限制，默认10个
        
        Returns:
            最近注册用户列表
        """
        # 计算N天前的时间
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # 查询条件：
        # 1. 注册时间在N天内
        # 2. 未删除
        # 3. 按注册时间倒序排列
        users = db.query(User).filter(
            and_(
                User.date_joined >= cutoff_date,
                User.is_deleted == False
            )
        ).order_by(
            User.date_joined.desc()
        ).limit(limit).all()
        
        result = []
        for user in users:
            # 获取主角色
            primary_role = user.get_primary_role()
            
            # 获取真实姓名
            real_name = None
            if user.profile:
                real_name = user.profile.real_name
            
            # 构建响应数据
            user_data = {
                "id": user.id,
                "username": user.username,
                "avatar": user.avatar,
                "real_name": real_name,
                "role": primary_role.code if primary_role else "guest",
                "role_name": primary_role.name if primary_role else "访客",
                "date_joined": user.date_joined.strftime("%Y-%m-%d %H:%M:%S") if user.date_joined else None,
                "is_active": user.is_active
            }
            result.append(user_data)
        
        return result

    @staticmethod
    def execute_sql_and_sync(
        mysql_url: str,
        sql: str,
        target_table: str,
        database: str = "ads_medicals",
        if_exists: str = "replace",
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        执行 Hive SQL 并将结果同步到 MySQL
        
        Args:
            mysql_url: MySQL 连接 URL
            sql: 要执行的 Hive SQL 查询
            target_table: 目标 MySQL 表名
            database: 要使用的 Hive 数据库（默认 ads_medicals）
            if_exists: 表存在时的处理方式（replace/append/fail）
            batch_size: 批量插入的大小，默认 1000
        
        Returns:
            同步结果
        """
        result = {
            "success": True,
            "message": "同步完成",
            "sql": sql,
            "target_table": target_table,
            "database": database,
            "details": {}
        }
        
        spark = None
        mysql_engine = None
        
        try:
            # 初始化 Spark 连接 Hive
            spark = init_spark_connect_hive()
            
            # 先切换到指定数据库
            spark.sql(f"USE {database}")
            logger.info(f"已切换到 Hive 数据库: {database}")
            
            # 执行 SQL 查询
            logger.info(f"执行 Hive SQL: {sql}")
            df = spark.sql(sql)
            
            # 获取查询结果的列信息
            columns = df.columns
            dtypes = df.dtypes
            logger.info(f"查询结果列: {columns}, 类型: {dtypes}")
            
            # 初始化 MySQL 连接
            mysql_engine = create_engine(mysql_url)
            
            # 处理 if_exists 参数
            if if_exists == "replace":
                with mysql_engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
                    conn.commit()
                logger.info(f"已删除旧表: {target_table}")
            
            # 生成并创建 MySQL 表
            if if_exists in ("replace", "fail"):
                create_table_sql = AnalyseService._generate_create_table_sql(
                    target_table, columns, dtypes
                )
                logger.info(f"创建表 SQL: {create_table_sql}")
                
                with mysql_engine.connect() as conn:
                    conn.execute(text(create_table_sql))
                    conn.commit()
                logger.info(f"表 {target_table} 创建成功")
            
            # 收集数据
            total_rows = df.count()
            result["details"]["total_rows"] = total_rows
            logger.info(f"查询到 {total_rows} 条数据")
            
            if total_rows == 0:
                result["message"] = "查询结果为空，无需同步"
                return result
            
            # 转换数据并插入
            inserted_rows = 0
            insert_sql = AnalyseService._generate_insert_sql(target_table, columns)
            
            # 分批处理数据
            for offset in range(0, total_rows, batch_size):
                batch_df = df.offset(offset).limit(batch_size)
                rows = [row.asDict() for row in batch_df.collect()]
                
                if rows:
                    with mysql_engine.connect() as conn:
                        conn.execute(text(insert_sql), rows)
                        conn.commit()
                    inserted_rows += len(rows)
                    logger.info(f"已插入 {inserted_rows}/{total_rows} 条数据")
            
            result["details"]["inserted_rows"] = inserted_rows
            result["message"] = f"成功同步 {inserted_rows} 条数据到 {target_table}"
            
        except Exception as e:
            logger.error(f"执行 SQL 同步失败: {e}")
            result["success"] = False
            result["message"] = f"同步失败: {str(e)}"
            result["details"]["error"] = str(e)
        
        finally:
            # 关闭连接
            if spark:
                try:
                    spark.stop()
                    logger.info("Spark 会话已关闭")
                except Exception as e:
                    logger.warning(f"关闭 Spark 会话失败: {e}")
            
            if mysql_engine:
                mysql_engine.dispose()
                logger.info("MySQL 连接已关闭")
        
        return result

    @staticmethod
    def query_hive(
        sql: str,
        database: str = "ads_medicals",
        limit: int = 1000
    ) -> Dict[str, Any]:
        """
        执行 Hive SQL 查询并返回结果
        
        Args:
            sql: 要执行的 Hive SQL 查询
            database: 要使用的 Hive 数据库
            limit: 返回结果数量限制
        
        Returns:
            查询结果
        """
        result = {
            "success": True,
            "message": "查询完成",
            "data": []
        }
        
        spark = None
        
        try:
            spark = init_spark_connect_hive()
            
            # 切换数据库
            spark.sql(f"USE {database}")
            
            # 执行查询
            logger.info(f"执行查询: {sql}")
            df = spark.sql(sql)
            
            # 获取列信息
            columns = df.columns
            dtypes = df.dtypes
            
            # 限制结果数量
            if limit > 0:
                df = df.limit(limit)
            
            # 收集数据
            data = [row.asDict() for row in df.collect()]
            
            result["columns"] = columns
            result["dtypes"] = dtypes
            result["data"] = data
            result["total"] = len(data)
            result["limit"] = limit
            
        except Exception as e:
            logger.error(f"Hive 查询失败: {e}")
            result["success"] = False
            result["message"] = f"查询失败: {str(e)}"
        
        finally:
            if spark:
                spark.stop()
        
        return result

    @staticmethod
    def get_database_tables(database: str = "ads_medicals") -> List[str]:
        """
        获取指定数据库的表列表
        
        Args:
            database: 数据库名称
        
        Returns:
            表名列表
        """
        spark = None
        try:
            spark = init_spark_connect_hive()
            spark.sql(f"USE {database}")
            tables = spark.catalog.listTables(database)
            return [table.name for table in tables]
        except Exception as e:
            logger.error(f"获取表列表失败: {e}")
            return []
        finally:
            if spark:
                spark.stop()

    @staticmethod
    def get_table_schema(database: str, table: str) -> List[Dict[str, Any]]:
        """
        获取表的结构信息
        
        Args:
            database: 数据库名称
            table: 表名称
        
        Returns:
            表结构信息
        """
        spark = None
        try:
            spark = init_spark_connect_hive()
            spark.sql(f"USE {database}")
            
            # 执行 DESCRIBE TABLE
            schema_df = spark.sql(f"DESCRIBE {table}")
            schema = [row.asDict() for row in schema_df.collect()]
            
            return schema
        except Exception as e:
            logger.error(f"获取表结构失败: {e}")
            return []
        finally:
            if spark:
                spark.stop()

    @staticmethod
    def sync_hive_ads_to_mysql(
        mysql_url: str,
        tables: List[str] = None,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        将 Hive ads 库中的数据同步到 MySQL
        
        Args:
            mysql_url: MySQL 连接 URL
            tables: 要同步的表列表，默认为 None（同步所有表）
            batch_size: 批量插入的大小，默认 1000
        
        Returns:
            同步结果
        """
        result = {
            "success": True,
            "message": "同步完成",
            "details": {}
        }
        
        spark = None
        mysql_engine = None
        
        try:
            # 初始化 Spark 连接 Hive
            spark = init_spark_connect_hive()
            
            # 初始化 MySQL 连接
            mysql_engine = create_engine(mysql_url)
            
            # 获取 ads 库中的表列表
            if tables is None:
                ads_tables = [table.name for table in spark.catalog.listTables("ads")]
            else:
                ads_tables = tables
            
            if not ads_tables:
                result["message"] = "Hive ads 库中没有表"
                return result
            
            for table_name in ads_tables:
                try:
                    # 读取 Hive 表数据
                    hive_table = f"ads.{table_name}"
                    df = spark.sql(f"SELECT * FROM {hive_table}")
                    
                    # 创建 MySQL 表名（以 ads_ 开头）
                    mysql_table = f"ads_{table_name}"
                    
                    # 获取表结构
                    columns = df.columns
                    dtypes = df.dtypes
                    
                    # 生成创建表的 SQL
                    create_table_sql = AnalyseService._generate_create_table_sql(
                        mysql_table, columns, dtypes
                    )
                    
                    # 执行创建表操作
                    with mysql_engine.connect() as conn:
                        conn.execute(text("DROP TABLE IF EXISTS " + mysql_table))
                        conn.execute(text(create_table_sql))
                        conn.commit()
                    
                    # 批量插入数据
                    total_rows = df.count()
                    inserted_rows = 0
                    
                    # 转换数据为字典列表
                    rows = [row.asDict() for row in df.collect()]
                    
                    # 批量插入
                    if rows:
                        with mysql_engine.connect() as conn:
                            # 生成插入 SQL
                            insert_sql = AnalyseService._generate_insert_sql(
                                mysql_table, columns
                            )
                            
                            # 分批插入
                            for i in range(0, len(rows), batch_size):
                                batch = rows[i:i + batch_size]
                                conn.execute(text(insert_sql), batch)
                                conn.commit()
                                inserted_rows += len(batch)
                    
                    # 记录同步结果
                    result["details"][table_name] = {
                        "status": "success",
                        "total_rows": total_rows,
                        "inserted_rows": inserted_rows,
                        "mysql_table": mysql_table
                    }
                    
                except Exception as e:
                    logging.error(f"同步表 {table_name} 失败: {e}")
                    result["success"] = False
                    result["details"][table_name] = {
                        "status": "failed",
                        "error": str(e)
                    }
        
        except Exception as e:
            logging.error(f"同步 Hive ads 到 MySQL 失败: {e}")
            result["success"] = False
            result["message"] = f"同步失败: {str(e)}"
        
        finally:
            # 关闭连接
            if spark:
                spark.stop()
            if mysql_engine:
                mysql_engine.dispose()
        
        return result

    @staticmethod
    def _generate_create_table_sql(table_name: str, columns: List[str], dtypes: List[tuple]) -> str:
        """
        生成创建表的 SQL 语句
        
        Args:
            table_name: 表名
            columns: 列名列表
            dtypes: 列类型列表
        
        Returns:
            创建表的 SQL 语句
        """
        type_map = {
            "string": "VARCHAR(255)",
            "int": "INT",
            "bigint": "BIGINT",
            "double": "DOUBLE",
            "float": "FLOAT",
            "boolean": "BOOLEAN",
            "date": "DATE",
            "timestamp": "DATETIME"
        }
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        
        for col, dtype in zip(columns, dtypes):
            col_name = col
            spark_type = dtype[1]
            
            # 映射 Spark 类型到 MySQL 类型
            mysql_type = type_map.get(spark_type.lower(), "VARCHAR(255)")
            
            # 处理列名中的特殊字符
            col_name = col_name.replace(" ", "_")
            
            create_sql += f"{col_name} {mysql_type}, "
        
        # 移除最后一个逗号和空格
        create_sql = create_sql.rstrip(", ")
        create_sql += ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        
        return create_sql

    @staticmethod
    def _generate_insert_sql(table_name: str, columns: List[str]) -> str:
        """
        生成插入数据的 SQL 语句
        
        Args:
            table_name: 表名
            columns: 列名列表
        
        Returns:
            插入数据的 SQL 语句
        """
        # 处理列名
        processed_columns = [col.replace(" ", "_") for col in columns]
        
        # 生成占位符
        placeholders = [f":{col}" for col in processed_columns]
        
        insert_sql = f"INSERT INTO {table_name} ({', '.join(processed_columns)}) "
        insert_sql += f"VALUES ({', '.join(placeholders)})"
        
        return insert_sql

    @staticmethod
    def get_overview_statistics(
        database: str = "medicals",
        dt: str = None
    ) -> Dict[str, Any]:
        """
        获取 Dashboard 总览页面的核心统计数据

        从 MySQL 数据库查询 ads_overview 表中的所有数据

        Args:
            database: MySQL 数据库名称
            dt: 数据日期 (YYYYMMDD)，默认最新日期
            
        Returns:
            包含 ads_overview 表中所有数据的字典
        """
        result = {
            "success": True,
            "data": [],
            "dt": dt,
            "database": database
        }
        
        try:
            # 从 ads_overview 表查询数据
            from sqlalchemy import create_engine, text
            
            # 构建数据库连接 URL
            # 注意：这里使用了固定的连接参数，实际部署时应该从配置文件中读取
            mysql_url = f"mysql+pymysql://root:123456@localhost:3306/{database}?charset=utf8mb4"
            engine = create_engine(mysql_url)
            
            with engine.connect() as conn:
                # 查询 ads_overview 表
                query = text("SELECT * FROM ads_overview")
                result_set = conn.execute(query)
                rows = result_set.fetchall()
                
                # 处理查询结果
                for row in rows:
                    # 构建行数据
                    row_data = {}
                    for i, col in enumerate(result_set.keys()):
                        row_data[col] = row[i]
                    result["data"].append(row_data)
                
        except Exception as e:
            logger.error(f"从 ads_overview 表获取数据失败: {e}")
            # 出错时返回空数据
            result["data"] = []
        
        return result

    @staticmethod
    def get_dashboard_chart_data(
        chart_type: str,
        limit: int = 20,
        dt: str = None
    ) -> Dict[str, Any]:
        """
        获取 Dashboard 各图表的详细数据

        Args:
            chart_type: 图表类型 (disease/hospital_level/doctor_ranking/consultation_trend/region_distribution/satisfaction)
            limit: 返回条数限制
            dt: 数据日期
            
        Returns:
            图表数据字典
        """
        result = {
            "success": True,
            "chart_type": chart_type,
            "list": [],
            "total": 0
        }
        
        # 尝试从 MySQL 数据库查询数据
        try:
            from sqlalchemy import create_engine, text
            
            # 构建数据库连接 URL
            mysql_url = f"mysql+pymysql://root:123456@localhost:3306/medicals?charset=utf8mb4"
            engine = create_engine(mysql_url)
            
            with engine.connect() as conn:
                # 根据图表类型查询对应的表
                if chart_type == "disease":
                    query = text("SELECT * FROM ads_disease_analysis")
                elif chart_type == "hospital_level":
                    query = text("SELECT * FROM ads_hospital_level_analysis")
                elif chart_type == "doctor_ranking":
                    query = text("SELECT * FROM ads_doctor_ranking ORDER BY recommendation_star DESC")
                elif chart_type == "region_distribution":
                    query = text("SELECT * FROM ads_city_medical_comparison")
                elif chart_type == "consultation_trend":
                    # 数据概览-问诊趋势：按自然日汇总「所有问诊方式」的问诊量（不再按 consultation_method 分行）
                    query = text("""
                        SELECT
                            consultation_date,
                            consultation_method,
                            consultation_count,
                            avg_interactions,
                            male_ratio,
                            female_ratio
                        FROM (
                            SELECT
                                DATE_FORMAT(consultation_date, '%Y-%m-%d') AS consultation_date,
                                '全部' AS consultation_method,
                                SUM(COALESCE(consultation_count, 0)) AS consultation_count,
                                SUM(COALESCE(avg_interactions, 0) * COALESCE(consultation_count, 0))
                                    / NULLIF(SUM(COALESCE(consultation_count, 0)), 0) AS avg_interactions,
                                SUM(COALESCE(male_ratio, 0) * COALESCE(consultation_count, 0))
                                    / NULLIF(SUM(COALESCE(consultation_count, 0)), 0) AS male_ratio,
                                SUM(COALESCE(female_ratio, 0) * COALESCE(consultation_count, 0))
                                    / NULLIF(SUM(COALESCE(consultation_count, 0)), 0) AS female_ratio
                            FROM ads_consultation_trend
                            WHERE consultation_date IS NOT NULL
                              AND COALESCE(consultation_count, 0) > 0
                            GROUP BY DATE_FORMAT(consultation_date, '%Y-%m-%d')
                            ORDER BY consultation_date DESC
                            LIMIT :limit
                        ) AS daily_totals
                        ORDER BY consultation_date ASC
                    """)
                elif chart_type == "hospital_ranking":
                    query = text("SELECT * FROM ads_hospital_ranking ORDER BY avg_recommendation_star DESC")
                elif chart_type == "satisfaction":
                    # 用户要求：满意度旭日图改为使用 ads_disease_analysis 数据渲染
                    # 结构：疾病类目 -> 疾病名称，值为问诊量
                    if dt:
                        query = text("""
                            SELECT
                                COALESCE(NULLIF(disease_category, ''), '未分类') AS disease_category,
                                COALESCE(NULLIF(disease_name, ''), '未知疾病') AS disease_name,
                                SUM(COALESCE(consultation_count, 0)) AS consultation_count,
                                MAX(dt) AS dt
                            FROM ads_disease_analysis
                            WHERE dt = :dt
                            GROUP BY
                                COALESCE(NULLIF(disease_category, ''), '未分类'),
                                COALESCE(NULLIF(disease_name, ''), '未知疾病')
                            ORDER BY consultation_count DESC
                        """)
                    else:
                        query = text("""
                            SELECT
                                COALESCE(NULLIF(disease_category, ''), '未分类') AS disease_category,
                                COALESCE(NULLIF(disease_name, ''), '未知疾病') AS disease_name,
                                SUM(COALESCE(consultation_count, 0)) AS consultation_count,
                                MAX(dt) AS dt
                            FROM ads_disease_analysis
                            WHERE dt = (
                                SELECT MAX(dt) FROM ads_disease_analysis
                            )
                            GROUP BY
                                COALESCE(NULLIF(disease_category, ''), '未分类'),
                                COALESCE(NULLIF(disease_name, ''), '未知疾病')
                            ORDER BY consultation_count DESC
                        """)
                else:
                    return result
                
                # 执行查询
                params: Dict[str, Any] = {}
                if chart_type == "satisfaction" and dt:
                    params["dt"] = dt
                if chart_type == "consultation_trend":
                    params["limit"] = limit
                result_set = conn.execute(query, params)
                rows = result_set.fetchall()
                
                # 处理查询结果
                for row in rows:
                    # 构建行数据
                    row_data = {}
                    for i, col in enumerate(result_set.keys()):
                        row_data[col] = row[i]
                    result["list"].append(row_data)
                
                # 限制返回数量
                if limit > 0:
                    result["list"] = result["list"][:limit]
                
                result["total"] = len(result["list"])
                result["dt"] = dt
                
        except Exception as e:
            logger.error(f"获取 {chart_type} 数据失败: {e}")
            # 出错时返回空数据
            result["list"] = []
            result["total"] = 0
            result["dt"] = dt
        
        return result

    @staticmethod
    def get_total_user_count(db: Session) -> int:
        """
        获取总用户数
        
        Args:
            db: 数据库会话
            
        Returns:
            总用户数
        """
        try:
            from apps.user.models import User
            # 查询未删除的用户总数
            total = db.query(User).filter(User.is_deleted == False).count()
            return total
        except Exception as e:
            logger.error(f"获取总用户数失败: {e}")
            return 0

    @staticmethod
    def get_active_users_stats(db: Session) -> Dict[str, Any]:
        """
        获取今日活跃用户统计数据
        
        统计规则：
        1. 从audit_logs表查询今日登录的用户
        2. 同一个用户只统计一次
        3. 计算与昨日活跃用户数的百分比变化
        
        Args:
            db: 数据库会话
            
        Returns:
            包含今日活跃用户数和百分比变化的字典
        """
        try:
            from datetime import datetime, timedelta
            from sqlalchemy import text
            
            # 获取今日和昨日的日期范围
            today = datetime.now().date()
            yesterday = today - timedelta(days=1)
            
            # 使用原生 SQL 统计活跃用户（兼容当前项目未声明 audit_logs ORM 模型的情况）
            today_query = text("""
                SELECT COUNT(DISTINCT user_id) AS active_count
                FROM audit_logs
                WHERE DATE(created_at) = :target_date
                  AND action = 'login'
            """)
            yesterday_query = text("""
                SELECT COUNT(DISTINCT user_id) AS active_count
                FROM audit_logs
                WHERE DATE(created_at) = :target_date
                  AND action = 'login'
            """)

            today_active = db.execute(today_query, {"target_date": today}).scalar() or 0
            yesterday_active = db.execute(yesterday_query, {"target_date": yesterday}).scalar() or 0
            
            # 计算百分比变化
            if yesterday_active > 0:
                percentage_change = ((today_active - yesterday_active) / yesterday_active) * 100
            else:
                percentage_change = 0 if today_active == 0 else 100
            
            return {
                "today_active": today_active or 0,
                "yesterday_active": yesterday_active or 0,
                "percentage_change": round(percentage_change, 2)
            }
        except Exception as e:
            logger.error(f"获取活跃用户统计失败: {e}")
            return {
                "today_active": 0,
                "yesterday_active": 0,
                "percentage_change": 0
            }
