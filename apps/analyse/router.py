import logging
import os
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from apps.user.models import User
from apps.core.database import get_db
from apps.core import Result
from utils.auth_helpers import require_auth
from apps.analyse.config import init_spark_connect_hive
from apps.analyse.service import AnalyseService
from apps.analyse.legacy_analysis_routes import legacy_router

router = APIRouter(prefix="/api", tags=["analyse"])

_DATE_ANCHOR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _dept_service_swap_doctor_consult_columns() -> bool:
    """
    部分 ETL 将「问诊量」写入 doctor_count、将「医生数」写入 consultation_count。
    为 True 时接口返回的 JSON 仍用 doctor_count / consultation_count 语义，但在 SQL 中交换两列来源。

    环境变量 DEPT_SERVICE_SWAP_DOCTOR_CONSULT：默认 1（交换）；设为 0 / false / no 则按表字段原名语义。
    """
    v = (os.getenv("DEPT_SERVICE_SWAP_DOCTOR_CONSULT") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def _normalize_sql_date_anchor(value) -> Optional[str]:
    """将 MAX(consultation_date) / 查询参数规范为 YYYY-MM-DD，过滤表头等非日期脏值。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.isoformat()
    s = str(value).strip()
    if _DATE_ANCHOR_RE.match(s):
        return s[:10]
    return None


@router.get("/analyse/test", summary="测试 Spark 连接")
async def test_spark_connection(
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    测试 Spark 连接是否正常
    """
    try:
        spark = init_spark_connect_hive()
        result = spark.range(5).collect()
        spark.stop()
        return Result.success(200, "Spark 连接测试成功", {"data": result})
    except Exception as e:
        logging.error(f"Spark 连接测试失败: {e}")
        return Result.error(500, str(e))


@router.get("/analyse/databases", summary="获取 Hive 数据库列表")
async def get_databases(
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取 Hive 中的所有数据库
    """
    try:
        spark = init_spark_connect_hive()
        databases = spark.catalog.listDatabases()
        db_list = [db.name for db in databases]
        spark.stop()
        return Result.success(200, "数据库列表获取成功", {"databases": db_list})
    except Exception as e:
        logging.error(f"获取数据库列表失败: {e}")
        return Result.error(500, str(e))


@router.get("/analyse/tables", summary="获取指定数据库的表列表")
async def get_tables(
        database_name: str = Query("default", description="数据库名称"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取指定数据库中的所有表
    """
    try:
        spark = init_spark_connect_hive()
        tables = spark.catalog.listTables(database_name)
        table_list = [table.name for table in tables]
        spark.stop()
        return Result.success(200, "表列表获取成功", {"database": database_name, "tables": table_list})
    except Exception as e:
        logging.error(f"获取表列表失败: {e}")
        return Result.error(500, str(e))


@router.get("/analyse/query", summary="执行 SQL 查询")
async def execute_query(
        sql: str = Query(..., description="要执行的 SQL 语句"),
        limit: int = Query(100, description="返回结果行数限制", ge=1, le=1000),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    执行 SQL 查询并返回结果
    """
    try:
        spark = init_spark_connect_hive()
        df = spark.sql(sql)
        result = df.limit(limit).collect()
        columns = df.columns
        data = [dict(row.asDict()) for row in result]
        spark.stop()
        return Result.success(200, "查询执行成功", {"columns": columns, "data": data})
    except Exception as e:
        logging.error(f"SQL 查询执行失败: {e}")
        return Result.error(500, str(e))


@router.get("/analyse/recent-users", summary="获取最近注册用户")
async def get_recent_users(
        days: int = Query(7, description="最近N天内注册", ge=1, le=30),
        limit: int = Query(10, description="返回用户数量限制", ge=1, le=100),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取最近N天内注册的真实用户列表
    """
    try:
        recent_users = AnalyseService.get_recent_registered_users(db, days=days, limit=limit)
        return Result.success(200, "最近注册用户获取成功", {"users": recent_users})
    except Exception as e:
        logging.error(f"获取最近注册用户失败: {e}")
        return Result.error(500, str(e))


@router.post("/analyse/sync-hive-ads-to-mysql", summary="将 Hive ads 库数据同步到 MySQL")
async def sync_hive_ads_to_mysql(
        mysql_url: str = Query(..., description="MySQL 连接 URL"),
        tables: List[str] = Query(None, description="要同步的表列表，默认为所有表"),
        batch_size: int = Query(1000, description="批量插入大小", ge=100, le=10000),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    将 Hive ads 库中的数据同步到 MySQL
    会自动创建以 ads_ 开头的表结构
    """
    try:
        sync_result = AnalyseService.sync_hive_ads_to_mysql(
            mysql_url=mysql_url,
            tables=tables,
            batch_size=batch_size
        )

        if sync_result["success"]:
            return Result.success(200, "同步成功", sync_result)
        else:
            return Result.error(500, sync_result["message"], sync_result)
    except Exception as e:
        logging.error(f"同步 Hive ads 到 MySQL 失败: {e}")
        return Result.error(500, str(e))


@router.post("/analyse/sync-sql-to-mysql", summary="执行自定义 SQL 并同步到 MySQL")
async def execute_sql_and_sync_to_mysql(
        mysql_url: str = Query(..., description="MySQL 连接 URL"),
        sql: str = Query(..., description="要执行的 Hive SQL 查询"),
        target_table: str = Query(..., description="目标 MySQL 表名"),
        database: str = Query("ads_medicals", description="Hive 数据库名称"),
        if_exists: str = Query("replace", description="表存在时的处理方式: replace/append/fail"),
        batch_size: int = Query(1000, description="批量插入大小", ge=100, le=10000),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    执行自定义 Hive SQL 查询并将结果同步到 MySQL
    
    示例 SQL:
    ```sql
    SELECT '医院排行榜' AS ads_name, 
           COUNT(*)     AS total_count, 
           MIN(ranking) AS min_rank, 
           MAX(ranking) AS max_rank 
    FROM medicals_ads.ads_hospital_ranking 
    WHERE dt = '20260413'
    ```
    """
    try:
        sync_result = AnalyseService.execute_sql_and_sync(
            mysql_url=mysql_url,
            sql=sql,
            target_table=target_table,
            database=database,
            if_exists=if_exists,
            batch_size=batch_size
        )

        if sync_result["success"]:
            return Result.success(200, sync_result["message"], sync_result)
        else:
            return Result.error(500, sync_result["message"], sync_result)
    except Exception as e:
        logging.error(f"执行 SQL 同步到 MySQL 失败: {e}")
        return Result.error(500, str(e))


@router.get("/analyse/hive-tables", summary="获取 Hive 数据库表列表")
async def get_hive_tables(
        database: str = Query("ads_medicals", description="Hive 数据库名称"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取指定 Hive 数据库的表列表
    """
    try:
        tables = AnalyseService.get_database_tables(database)
        return Result.success(200, "表列表获取成功", {"database": database, "tables": tables})
    except Exception as e:
        logging.error(f"获取表列表失败: {e}")
        return Result.error(500, str(e))


@router.get("/analyse/hive-table-schema", summary="获取 Hive 表结构")
async def get_hive_table_schema(
        database: str = Query("ads_medicals", description="Hive 数据库名称"),
        table: str = Query(..., description="表名称"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取指定 Hive 表的结构信息
    """
    try:
        schema = AnalyseService.get_table_schema(database, table)
        return Result.success(200, "表结构获取成功", {"database": database, "table": table, "schema": schema})
    except Exception as e:
        logging.error(f"获取表结构失败: {e}")
        return Result.error(500, str(e))


@router.get("/analysis/overview", summary="获取 Dashboard 总览统计数据")
async def get_overview_stats(
        dt: str = Query(None, description="数据日期 (YYYYMMDD)，默认最新"),
        db: Session = Depends(get_db)
):
    """
    获取 Dashboard 总览页面的核心统计数据
    
    返回 6 个关键指标:
    - 医院总数
    - 医生总数
    - 问诊总量
    - 疾病种类
    - 城市数量
    - 科室数量
    
    数据来源: MySQL medicals 数据库
    """
    try:
        data = AnalyseService.get_overview_statistics(
            database="medicals",
            dt=dt
        )
        return Result.success(200, "统计数据获取成功", data)
    except Exception as e:
        logging.error(f"获取总览统计数据失败: {e}")
        return Result.error(500, str(e))


@router.get("/analysis/dashboard-chart-data/{chart_type}", summary="获取 Dashboard 图表数据")
async def get_dashboard_chart_data(
        chart_type: str,
        limit: int = Query(20, description="返回条数限制", ge=1, le=500),
        dt: str = Query(None, description="数据日期 (YYYYMMDD)"),
        db: Session = Depends(get_db)
):
    """
    获取 Dashboard 各图表的数据
    
    支持的图表类型:
    - disease: 疾病分布分析数据
    - hospital_level: 医院等级分布数据
    - doctor_ranking: 医生排名数据
    - consultation_trend: 问诊趋势数据
    - region_distribution: 地区分布数据
    """
    try:
        data = AnalyseService.get_dashboard_chart_data(
            chart_type=chart_type,
            limit=limit,
            dt=dt
        )
        return Result.success(200, f"{chart_type} 数据获取成功", data)
    except Exception as e:
        logging.error(f"获取 {chart_type} 数据失败: {e}")
        return Result.error(500, str(e))


def _get_mock_satisfaction_data(limit: int = 10):
    """生成模拟满意度数据（表为空或查询失败时的兜底）"""
    import random
    levels = [
        {"level": "非常满意", "doctors": 1250, "consultations": 680000},
        {"level": "满意", "doctors": 1580, "consultations": 920000},
        {"level": "一般", "doctors": 420, "consultations": 185000},
        {"level": "不满意", "doctors": 85, "consultations": 28000},
        {"level": "非常不满意", "doctors": 25, "consultations": 8500}
    ]
    total = sum(item["doctors"] for item in levels)
    data = []
    for item in levels[:limit]:
        ratio = round(item["doctors"] / total * 100, 1)
        data.append({
            "satisfaction_level": item["level"],
            "doctor_count": item["doctors"],
            "doctor_ratio": ratio,
            "consultation_count": item["consultations"],
            "avg_consultation_price": round(random.uniform(50, 180), 2)
        })
    return data


@router.get("/analysis/satisfaction-analysis", summary="获取满意度分析数据")
async def get_satisfaction_analysis(
        dt: str = Query(None, description="日期分区（YYYYMMDD）"),
        page: int = Query(1, description="页码", ge=1),
        pageSize: int = Query(10, description="每页数量", ge=1, le=100),
        db: Session = Depends(get_db)
):
    """获取满意度分析数据"""
    try:
        query = text("""
            SELECT
                satisfaction_level,
                doctor_count,
                doctor_ratio,
                consultation_count,
                avg_consultation_price
            FROM ads_satisfaction_analysis
            ORDER BY doctor_count DESC
            LIMIT :limit OFFSET :offset
        """)

        rows = db.execute(query, {"limit": pageSize, "offset": (page - 1) * pageSize}).fetchall()
        total = db.execute(text("SELECT COUNT(*) as total FROM ads_satisfaction_analysis")).fetchone()[0]

        data_list = [{
            "satisfaction_level": row[0],
            "doctor_count": int(row[1]) if row[1] else 0,
            "doctor_ratio": float(row[2]) if row[2] else 0,
            "consultation_count": int(row[3]) if row[3] else 0,
            "avg_consultation_price": float(row[4]) if row[4] else 0
        } for row in rows]

        if not data_list:
            data_list = _get_mock_satisfaction_data(pageSize)
            total = len(data_list)

        return Result.success(200, "满意度分析数据获取成功", {"list": data_list, "total": total})
    except Exception as e:
        logging.error(f"获取满意度分析数据失败: {e}")
        data_list = _get_mock_satisfaction_data(pageSize)
        return Result.success(200, "满意度分析数据获取成功（模拟数据）", {"list": data_list, "total": len(data_list)})


@router.get("/analysis/doctor-title-analysis", summary="获取医生职称分析数据")
async def get_doctor_title_analysis(
        dt: str = Query(None, description="日期分区（YYYYMMDD）"),
        page: int = Query(1, description="页码", ge=1),
        pageSize: int = Query(10, description="每页数量", ge=1, le=100),
        db: Session = Depends(get_db)
):
    """获取医生职称分析数据"""
    try:
        query = text("""
            SELECT
                doctor_title,
                doctor_count,
                doctor_ratio,
                consultation_count,
                avg_consultation_price,
                avg_recommendation_star,
                avg_response_rate
            FROM ads_doctor_title_analysis
            ORDER BY doctor_count DESC
            LIMIT :limit OFFSET :offset
        """)

        rows = db.execute(query, {"limit": pageSize, "offset": (page - 1) * pageSize}).fetchall()
        total = db.execute(text("SELECT COUNT(*) as total FROM ads_doctor_title_analysis")).fetchone()[0]

        data_list = [{
            "doctor_title": row[0],
            "doctor_count": int(row[1]) if row[1] else 0,
            "doctor_ratio": float(row[2]) if row[2] else 0,
            "consultation_count": int(row[3]) if row[3] else 0,
            "avg_consultation_price": float(row[4]) if row[4] else 0,
            "avg_recommendation_star": float(row[5]) if row[5] else 0,
            "avg_response_rate": float(row[6]) if row[6] else 0
        } for row in rows]

        return Result.success(200, "医生职称分析数据获取成功", {"list": data_list, "total": total})
    except Exception as e:
        logging.error(f"获取医生职称分析数据失败: {e}")
        return Result.error(500, "医生职称分析数据获取失败")


@router.get("/analysis/doctor-list-by-title", summary="根据职称获取医生列表")
async def get_doctor_list_by_title(
        title: str = Query(..., description="医生职称"),
        page: int = Query(1, description="页码", ge=1),
        pageSize: int = Query(10, description="每页数量", ge=1, le=100),
        db: Session = Depends(get_db)
):
    """根据职称获取医生列表"""
    try:
        query = text("""
            SELECT
                doctor_name,
                doctor_title,
                department,
                hospital_name,
                consultation_count,
                consultation_price,
                recommendation_star
            FROM ads_doctor_ranking
            WHERE doctor_title = :title
            ORDER BY consultation_count DESC
            LIMIT :limit OFFSET :offset
        """)

        rows = db.execute(query, {"title": title, "limit": pageSize, "offset": (page - 1) * pageSize}).fetchall()
        data_list = [{
            "doctor_name": row[0],
            "doctor_title": row[1],
            "department": row[2],
            "hospital_name": row[3],
            "consultation_count": int(row[4]) if row[4] else 0,
            "consultation_price": float(row[5]) if row[5] else 0,
            "recommendation_star": float(row[6]) if row[6] else 0
        } for row in rows]

        return Result.success(200, "医生列表获取成功", {"list": data_list, "total": len(data_list)})
    except Exception as e:
        logging.error(f"获取医生列表失败: {e}")
        return Result.error(500, "医生列表获取失败")


@router.get("/analysis/department-service-analysis", summary="获取科室服务分析数据")
async def get_department_service_analysis(
        dt: str = Query(None, description="日期分区（YYYYMMDD），不传则取表中最新 dt"),
        department: Optional[str] = Query(None, description="科室名称"),
        limit: int = Query(20, description="返回条数限制", ge=1, le=100),
        db: Session = Depends(get_db)
):
    """
    科室服务分析（唯一实现：GET /api/analysis/department-service-analysis）。

    同一分区下若存在多行同一科室，按科室聚合后再排序，避免饼图/柱状图重复科室或口径混乱。

    若库中 doctor_count / consultation_count 与业务含义写反，见 ``_dept_service_swap_doctor_consult_columns``。
    """
    try:
        params: Dict[str, Any] = {"limit": limit}
        swap = _dept_service_swap_doctor_consult_columns()
        # 表字段名 -> 业务语义：未交换时 vol=问诊量=consultation_count；交换后 vol=问诊量=doctor_count
        vol = "doctor_count" if swap else "consultation_count"
        doc = "consultation_count" if swap else "doctor_count"

        resolved_dt = (dt or "").strip() or None
        if not resolved_dt:
            try:
                max_row = db.execute(
                    text("SELECT MAX(dt) FROM ads_department_service_analysis")
                ).fetchone()
                if max_row and max_row[0] is not None:
                    resolved_dt = str(max_row[0]).strip()
            except Exception:
                resolved_dt = None
        dt_filter = ""
        if resolved_dt:
            params["dt"] = resolved_dt
            dt_filter = "AND dt = :dt"

        dept_clause = ""
        if department and str(department).strip():
            params["department"] = str(department).strip()
            dept_clause = "AND department = :department"
        query = text(f"""
            SELECT
                department,
                SUM(COALESCE({doc}, 0)) AS doctor_count,
                SUM(COALESCE(hospital_count, 0)) AS hospital_count,
                SUM(COALESCE({vol}, 0)) AS consultation_count,
                SUM(COALESCE(avg_consultation_price, 0) * COALESCE({vol}, 0))
                    / NULLIF(SUM(COALESCE({vol}, 0)), 0) AS avg_consultation_price,
                SUM(COALESCE(avg_recommendation_star, 0) * COALESCE({vol}, 0))
                    / NULLIF(SUM(COALESCE({vol}, 0)), 0) AS avg_recommendation_star,
                SUM(COALESCE(online_ratio, 0) * COALESCE({vol}, 0))
                    / NULLIF(SUM(COALESCE({vol}, 0)), 0) AS online_ratio,
                MAX(top_hospital) AS top_hospital
            FROM ads_department_service_analysis
            WHERE department IS NOT NULL AND TRIM(department) <> ''
            {dt_filter}
            {dept_clause}
            GROUP BY department
            ORDER BY SUM(COALESCE({vol}, 0)) DESC
            LIMIT :limit
        """)

        rows = db.execute(query, params).fetchall()
        data_list = [{
            "department": row[0],
            "doctor_count": int(row[1]) if row[1] else 0,
            "hospital_count": int(row[2]) if row[2] else 0,
            "consultation_count": int(row[3]) if row[3] else 0,
            "avg_consultation_price": float(row[4]) if row[4] else 0,
            "avg_recommendation_star": float(row[5]) if row[5] else 0,
            "online_ratio": float(row[6]) if row[6] else 0,
            "top_hospital": row[7] or ""
        } for row in rows]

        return Result.success(200, "科室服务分析数据获取成功", {"list": data_list, "total": len(data_list)})
    except Exception as e:
        logging.error(f"获取科室服务分析数据失败: {e}")
        return Result.error(500, "科室服务分析数据获取失败")


@router.get("/analysis/department-satisfaction-analysis", summary="获取科室满意度分析数据")
async def get_department_satisfaction_analysis(
        dt: str = Query(None, description="日期分区（YYYYMMDD），不传则取表中最新 dt"),
        limit: int = Query(20, description="返回条数限制", ge=1, le=100),
        db: Session = Depends(get_db)
):
    """获取科室满意度分析数据（用于大屏科室满意度图表）"""
    try:
        swap = _dept_service_swap_doctor_consult_columns()
        vol = "doctor_count" if swap else "consultation_count"
        params: Dict[str, Any] = {"limit": limit}
        resolved_dt = (dt or "").strip() or None
        if not resolved_dt:
            try:
                max_row = db.execute(
                    text("SELECT MAX(dt) FROM ads_department_service_analysis")
                ).fetchone()
                if max_row and max_row[0] is not None:
                    resolved_dt = str(max_row[0]).strip()
            except Exception:
                resolved_dt = None
        dt_filter = ""
        if resolved_dt:
            params["dt"] = resolved_dt
            dt_filter = "AND dt = :dt"

        # 按科室聚合；问诊量口径与 department-service-analysis 一致（见 _dept_service_swap_doctor_consult_columns）
        query = text(f"""
            SELECT
                department,
                MAX(COALESCE(top_hospital, '')) AS top_hospital,
                SUM(COALESCE(avg_recommendation_star, 0) * COALESCE({vol}, 0))
                    / NULLIF(SUM(COALESCE({vol}, 0)), 0) AS avg_recommendation_star,
                SUM(COALESCE({vol}, 0)) AS consultation_count
            FROM ads_department_service_analysis
            WHERE department IS NOT NULL AND TRIM(department) <> ''
            {dt_filter}
            GROUP BY department
            ORDER BY avg_recommendation_star DESC, consultation_count DESC
            LIMIT :limit
        """)
        rows = db.execute(query, params).fetchall()

        data_list = [{
            "department": row[0],
            "top_hospital": row[1] or "",
            "avg_recommendation_star": float(row[2]) if row[2] is not None else 0.0,
            "consultation_count": int(row[3]) if row[3] is not None else 0
        } for row in rows]

        return Result.success(200, "科室满意度分析数据获取成功", {"list": data_list, "total": len(data_list)})
    except Exception as e:
        logging.error(f"获取科室满意度分析数据失败: {e}")
        return Result.error(500, "科室满意度分析数据获取失败")


@router.get("/analysis/region-medical-resource", summary="获取区域医疗资源数据（ADS）")
async def get_region_medical_resource(
        dt: Optional[str] = Query(None, description="日期分区（YYYYMMDD），不传则取最新分区"),
        city: Optional[str] = Query(None, description="按城市筛选"),
        limit: int = Query(50, description="返回条数上限", ge=1, le=200),
        db: Session = Depends(get_db)
):
    """
    从 ads_region_medical_resource 读取区域医疗资源，按城市汇总医院数、医生数等。
    """
    try:
        params: dict = {"limit": limit}
        if dt:
            dt_clause = "dt = :dt"
            params["dt"] = dt
        else:
            dt_clause = "dt = (SELECT MAX(dt) FROM ads_region_medical_resource r_dt)"

        if city:
            params["city"] = city
            query = text(f"""
                SELECT
                    city,
                    SUM(COALESCE(hospital_count, 0)) AS hospital_count,
                    SUM(COALESCE(doctor_count, 0)) AS doctor_count
                FROM ads_region_medical_resource
                WHERE city = :city AND {dt_clause}
                GROUP BY city
                ORDER BY hospital_count DESC
                LIMIT :limit
            """)
        else:
            query = text(f"""
                SELECT
                    city,
                    SUM(COALESCE(hospital_count, 0)) AS hospital_count,
                    SUM(COALESCE(doctor_count, 0)) AS doctor_count
                FROM ads_region_medical_resource
                WHERE {dt_clause}
                  AND city IS NOT NULL AND TRIM(city) <> ''
                GROUP BY city
                ORDER BY hospital_count DESC
                LIMIT :limit
            """)

        rows = db.execute(query, params).fetchall()
        data_list = [{
            "city": row[0],
            "hospital_count": int(row[1]) if row[1] is not None else 0,
            "doctor_count": int(row[2]) if row[2] is not None else 0,
        } for row in rows]

        return Result.success(
            200,
            "区域医疗资源数据获取成功",
            {"list": data_list, "total": len(data_list)},
        )
    except Exception as e:
        logging.warning(f"按分区查询区域医疗资源失败，尝试不按 dt 聚合: {e}")
        try:
            params2: dict = {"limit": limit}
            if city:
                params2["city"] = city
                query2 = text("""
                    SELECT
                        city,
                        SUM(COALESCE(hospital_count, 0)) AS hospital_count,
                        SUM(COALESCE(doctor_count, 0)) AS doctor_count
                    FROM ads_region_medical_resource
                    WHERE city = :city
                    GROUP BY city
                    ORDER BY hospital_count DESC
                    LIMIT :limit
                """)
            else:
                query2 = text("""
                    SELECT
                        city,
                        SUM(COALESCE(hospital_count, 0)) AS hospital_count,
                        SUM(COALESCE(doctor_count, 0)) AS doctor_count
                    FROM ads_region_medical_resource
                    WHERE city IS NOT NULL AND TRIM(city) <> ''
                    GROUP BY city
                    ORDER BY hospital_count DESC
                    LIMIT :limit
                """)
            rows = db.execute(query2, params2).fetchall()
            data_list = [{
                "city": row[0],
                "hospital_count": int(row[1]) if row[1] is not None else 0,
                "doctor_count": int(row[2]) if row[2] is not None else 0,
            } for row in rows]
            return Result.success(
                200,
                "区域医疗资源数据获取成功",
                {"list": data_list, "total": len(data_list)},
            )
        except Exception as e2:
            logging.error(f"获取区域医疗资源数据失败: {e2}")
            return Result.error(500, "区域医疗资源数据获取失败")


@router.get("/analysis/consultation-trend", summary="获取问诊趋势数据")
async def get_consultation_trend(
        startDate: str = Query(None, description="开始日期 YYYY-MM-DD"),
        endDate: str = Query(None, description="结束日期 YYYY-MM-DD"),
        limit: int = Query(180, description="返回条数限制", ge=1, le=365),
        period: str = Query("day", description="聚合周期: day/week/month"),
        db: Session = Depends(get_db)
):
    """
    获取问诊趋势数据（按周期聚合；每个周期一条记录，为当日/当周/当月「所有问诊方式」的合计）。
    """
    try:
        period = (period or "day").lower()
        if period not in {"day", "week", "month", "quarter"}:
            return Result.error(400, "period 参数仅支持 day/week/month/quarter")

        conditions = ["COALESCE(consultation_count, 0) > 0"]
        params = {"limit": limit}
        if startDate:
            conditions.append("consultation_date >= :startDate")
            params["startDate"] = startDate
        if endDate:
            params["anchorDate"] = _normalize_sql_date_anchor(endDate)
        else:
            max_date_query = text(f"""
                SELECT MAX(consultation_date)
                FROM ads_consultation_trend
                WHERE {' AND '.join(conditions)}
            """)
            max_date_row = db.execute(max_date_query, params).fetchone()
            anchor = _normalize_sql_date_anchor(
                max_date_row[0] if max_date_row else None
            )
            if anchor is None:
                # 脏数据（如把列名「问诊时间」导入成字段值）时，仅对形似日期的行取 MAX
                max_date_clean = text(f"""
                    SELECT MAX(consultation_date)
                    FROM ads_consultation_trend
                    WHERE {' AND '.join(conditions)}
                      AND CAST(consultation_date AS CHAR) REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                """)
                row2 = db.execute(max_date_clean, params).fetchone()
                anchor = _normalize_sql_date_anchor(row2[0] if row2 else None)
            params["anchorDate"] = anchor

        if not params.get("anchorDate"):
            return Result.success(200, "问诊趋势数据获取成功", {"list": [], "total": 0})

        conditions.append("consultation_date <= :anchorDate")
        where_clause = f"WHERE {' AND '.join(conditions)}"

        # GROUP BY 必须与 SELECT 中的周期表达式一致，避免 sql_mode=only_full_group_by 报错
        if period == "week":
            date_expr = (
                "DATE_FORMAT(DATE_SUB(consultation_date, INTERVAL WEEKDAY(consultation_date) DAY), "
                "'%Y-%m-%d')"
            )
            group_by = date_expr
        elif period == "month":
            date_expr = "DATE_FORMAT(consultation_date, '%Y-%m-01')"
            group_by = date_expr
        elif period == "quarter":
            date_expr = "CONCAT(YEAR(consultation_date), '-Q', QUARTER(consultation_date))"
            group_by = date_expr
        else:
            date_expr = "DATE_FORMAT(consultation_date, '%Y-%m-%d')"
            group_by = date_expr

        # 先取「锚点日期」往前的 N 个周期，再按周期汇总所有 consultation_method（不按问诊类型拆分）
        query = text(f"""
            SELECT
                t.period_start,
                t.consultation_method,
                t.consultation_count,
                t.avg_interactions,
                t.male_ratio,
                t.female_ratio
            FROM (
                SELECT
                    {date_expr} AS period_start,
                    '全部' AS consultation_method,
                    SUM(COALESCE(consultation_count, 0)) AS consultation_count,
                    SUM(COALESCE(avg_interactions, 0) * COALESCE(consultation_count, 0))
                        / NULLIF(SUM(COALESCE(consultation_count, 0)), 0) AS avg_interactions,
                    SUM(COALESCE(male_ratio, 0) * COALESCE(consultation_count, 0))
                        / NULLIF(SUM(COALESCE(consultation_count, 0)), 0) AS male_ratio,
                    SUM(COALESCE(female_ratio, 0) * COALESCE(consultation_count, 0))
                        / NULLIF(SUM(COALESCE(consultation_count, 0)), 0) AS female_ratio
                FROM ads_consultation_trend
                {where_clause}
                GROUP BY {group_by}
            ) t
            INNER JOIN (
                SELECT period_start
                FROM (
                    SELECT
                        {date_expr} AS period_start,
                        MAX(consultation_date) AS period_max_date
                    FROM ads_consultation_trend
                    {where_clause}
                    GROUP BY {group_by}
                    ORDER BY period_max_date DESC
                    LIMIT :limit
                ) picked_periods
            ) p ON t.period_start = p.period_start
            ORDER BY t.period_start ASC
        """)

        rows = db.execute(query, params).fetchall()
        data_list = []
        for row in rows:
            period_raw = str(row[0]) if row[0] is not None else ""
            if period == "week":
                period_date = period_raw[:10]
            elif period == "month":
                period_date = period_raw[:7]
            else:
                period_date = period_raw

            data_list.append({
                "period_start": period_date,
                "consultation_date": period_date,
                "consultation_method": row[1],
                "consultation_count": int(row[2]) if row[2] else 0,
                "avg_interactions": float(row[3]) if row[3] else 0,
                "male_ratio": float(row[4]) if row[4] else 0,
                "female_ratio": float(row[5]) if row[5] else 0
            })

        return Result.success(200, "问诊趋势数据获取成功", {"list": data_list, "total": len(data_list)})
    except Exception as e:
        logging.error(f"获取问诊趋势数据失败: {e}")
        return Result.error(500, str(e))


@router.get("/analysis/total-users", summary="获取总用户数")
async def get_total_users(
        db: Session = Depends(get_db)
):
    """
    获取总用户数
    
    从用户表中查询未删除的用户总数
    """
    try:
        total_users = AnalyseService.get_total_user_count(db)
        return Result.success(200, "总用户数获取成功", {
            "total_users": total_users
        })
    except Exception as e:
        logging.error(f"获取总用户数失败: {e}")
        return Result.error(500, str(e))


@router.get("/analysis/active-users", summary="获取今日活跃用户统计")
async def get_active_users(
        db: Session = Depends(get_db)
):
    """
    获取今日活跃用户统计数据
    
    统计规则：
    1. 从audit_logs表查询今日登录的用户
    2. 同一个用户只统计一次
    3. 计算与昨日活跃用户数的百分比变化
    """
    try:
        stats = AnalyseService.get_active_users_stats(db)
        return Result.success(200, "活跃用户统计获取成功", stats)
    except Exception as e:
        logging.error(f"获取活跃用户统计失败: {e}")
        return Result.error(500, str(e))


# 原 analysis_router 中的大屏、排名、管理员统计等路由（与 router 主文件路径不重复）
router.include_router(legacy_router)
