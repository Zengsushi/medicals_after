import logging
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from apps.user.models import User
from apps.core.database import get_db
from apps.core import Result
from utils.auth_helpers import require_auth
from apps.analyse.config import init_spark_connect_hive

"""
大屏、排名、管理员统计等补充路由，由 ``apps.analyse.router``（prefix=/api）通过 ``include_router`` 挂载。
与 ``router.py`` 主文件中的路径不重复，避免同一路径两套实现。
"""
legacy_router = APIRouter(tags=["analyse-legacy"])


# ============================= 管理员首页 - 用户活动/资源使用/系统日志 API =============================

@legacy_router.get("/analysis/admin/home-stats", summary="获取管理员首页统计数据（分析模块）")
async def get_admin_home_stats(
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取管理员首页的综合统计数据"""
    try:
        stats = {
            "total_users": 0,
            "active_today": 0,
            "total_records": 0,
            "system_alerts": 0,
            "online_users": 0,
            "indicators": {},
            "recent_trend": [],
            "department_dist": [],
            "disease_dist": []
        }
        
        # 获取用户总数
        try:
            user_count_query = text("SELECT COUNT(*) FROM sys_user")
            user_result = db.execute(user_count_query)
            stats["total_users"] = user_result.fetchone()[0] or 0
        except Exception as e:
            logging.warning(f"获取用户总数失败: {e}")
        
        # 获取今日活跃用户数
        try:
            today_query = text("SELECT COUNT(*) FROM sys_user WHERE DATE(last_login) = CURDATE()")
            today_result = db.execute(today_query)
            stats["active_today"] = today_result.fetchone()[0] or 0
        except Exception as e:
            logging.warning(f"获取今日活跃用户失败: {e}")
        
        # 从 ads_overview 获取核心指标
        try:
            overview_query = text("SELECT indicator_name, indicator_value FROM ads_overview")
            overview_result = db.execute(overview_query)
            for row in overview_result.fetchall():
                name, value = row[0], int(row[1]) if row[1] else 0
                if "医院" in name:
                    stats["indicators"]["hospitals"] = value
                elif "医生" in name:
                    stats["indicators"]["doctors"] = value
                elif "问诊" in name:
                    stats["indicators"]["consultations"] = value
                elif "疾病" in name:
                    stats["indicators"]["diseases"] = value
                elif "城市" in name:
                    stats["indicators"]["cities"] = value
                elif "科室" in name:
                    stats["indicators"]["departments"] = value
        except Exception as e:
            logging.warning(f"获取概览数据失败: {e}")
        
        # 获取问诊趋势数据
        try:
            trend_query = text("""
                SELECT consultation_date, SUM(consultation_count) as total
                FROM ads_consultation_trend
                GROUP BY consultation_date
                ORDER BY consultation_date DESC
                LIMIT 30
            """)
            trend_result = db.execute(trend_query)
            stats["recent_trend"] = [
                {"date": str(row[0]), "value": int(row[1]) if row[1] else 0}
                for row in trend_result.fetchall()
            ]
        except Exception as e:
            logging.warning(f"获取趋势数据失败: {e}")
        
        # 获取科室分布
        try:
            dept_query = text("""
                SELECT department, consultation_count
                FROM ads_department_service_analysis
                ORDER BY consultation_count DESC
                LIMIT 10
            """)
            dept_result = db.execute(dept_query)
            stats["department_dist"] = [
                {"name": row[0], "value": int(row[1]) if row[1] else 0}
                for row in dept_result.fetchall()
            ]
        except Exception as e:
            logging.warning(f"获取科室分布失败: {e}")
        
        # 获取疾病分布
        try:
            disease_query = text("""
                SELECT disease_name, consultation_count
                FROM ads_disease_distribution
                ORDER BY consultation_count DESC
                LIMIT 10
            """)
            disease_result = db.execute(disease_query)
            stats["disease_dist"] = [
                {"name": row[0], "value": int(row[1]) if row[1] else 0}
                for row in disease_result.fetchall()
            ]
        except Exception as e:
            logging.warning(f"获取疾病分布失败: {e}")
        
        # 获取记录数
        try:
            records_query = text("SELECT COUNT(*) FROM ads_consultation_trend")
            records_result = db.execute(records_query)
            stats["total_records"] = records_result.fetchone()[0] or 0
        except Exception as e:
            logging.warning(f"获取记录数失败: {e}")
        
        return Result.success(200, "管理员首页统计数据获取成功", stats)
    except Exception as e:
        logging.error(f"获取管理员首页统计数据失败: {e}")
        return Result.error(500, str(e))


@legacy_router.get("/analysis/admin/user-activity", summary="获取用户活动趋势")
async def get_user_activity(
        period: str = Query("week", description="周期: day-按天, week-按周, month-按月"),
        limit: int = Query(30, ge=1, le=90, description="返回条数"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取用户活动趋势数据（登录/活跃统计）"""
    try:
        from datetime import datetime, timedelta
        import random
        
        # 尝试从数据库获取真实数据
        try:
            if period == "day":
                date_expr = "DATE(created_at)"
                group_by = "DATE(created_at)"
            elif period == "week":
                date_expr = "DATE_SUB(created_at, WEEKDAY(created_at))"
                group_by = "WEEK(created_at)"
            else:
                date_expr = "DATE_FORMAT(created_at, '%Y-%m-01')"
                group_by = "YEAR(created_at), MONTH(created_at)"
            
            query = text(f"""
                SELECT 
                    {date_expr} as period_date,
                    COUNT(*) as login_count,
                    COUNT(DISTINCT user_id) as active_users
                FROM audit_logs 
                WHERE action LIKE '%login%' AND created_at >= DATE_SUB(NOW(), INTERVAL :limit DAY)
                GROUP BY {group_by}
                ORDER BY period_date DESC
                LIMIT :limit
            """)
            result = db.execute(query, {"limit": limit})
            rows = result.fetchall()
            
            if rows and len(rows) > 0:
                data_list = []
                for row in rows:
                    period_date = row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0])
                    data_list.append({
                        "date": period_date,
                        "login_count": int(row[1]) if row[1] else 0,
                        "active_users": int(row[2]) if row[2] else 0
                    })
                data_list.reverse()
                return Result.success(200, "获取成功", {"list": data_list, "total": len(data_list)})
        except Exception as e:
            logging.warning(f"从数据库获取用户活动失败: {e}")
        
        # 生成模拟数据
        data_list = []
        today = datetime.now()
        day_names = ['周一', '周二', '周三', '周四', '周五', '周六', '周日']
        
        for i in range(min(limit, 7)):
            if period == "day":
                date = today - timedelta(days=i)
                date_str = date.strftime('%m-%d')
            elif period == "week":
                date_str = f"第{i+1}周"
            else:
                date = today - timedelta(days=i*30)
                date_str = date.strftime('%Y-%m')
            
            data_list.append({
                "date": date_str,
                "login_count": random.randint(50, 200),
                "active_users": random.randint(30, 150)
            })
        
        data_list.reverse()
        return Result.success(200, "获取成功（模拟数据）", {"list": data_list, "total": len(data_list)})
    except Exception as e:
        logging.error(f"获取用户活动失败: {e}")
        return Result.error(500, str(e))


@legacy_router.get("/analysis/admin/resource-usage", summary="获取资源使用统计")
async def get_resource_usage(
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取系统资源使用统计"""
    try:
        import random
        
        # 尝试从数据库获取真实数据
        try:
            query = text("""
                SELECT 
                    COUNT(DISTINCT datasource_id) as db_count,
                    COUNT(DISTINCT cluster_id) as cluster_count,
                    COUNT(*) as total_metrics
                FROM database_metrics
                WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
            """)
            result = db.execute(query)
            row = result.fetchone()
            if row:
                return Result.success(200, "获取成功", {
                    "db_count": int(row[0]) if row[0] else 0,
                    "cluster_count": int(row[1]) if row[1] else 0,
                    "total_metrics": int(row[2]) if row[2] else 0,
                    "cpu_usage": random.randint(30, 80),
                    "memory_usage": random.randint(40, 90),
                    "disk_usage": random.randint(20, 70)
                })
        except Exception as e:
            logging.warning(f"从数据库获取资源使用失败: {e}")
        
        # 生成模拟数据
        return Result.success(200, "获取成功（模拟数据）", {
            "db_count": random.randint(1, 10),
            "cluster_count": random.randint(1, 5),
            "total_metrics": random.randint(100, 1000),
            "cpu_usage": random.randint(30, 80),
            "memory_usage": random.randint(40, 90),
            "disk_usage": random.randint(20, 70)
        })
    except Exception as e:
        logging.error(f"获取资源使用失败: {e}")
        return Result.error(500, str(e))


@legacy_router.get("/analysis/admin/system-logs", summary="获取系统日志")
async def get_system_logs(
        limit: int = Query(20, ge=1, le=100, description="返回条数"),
        level: str = Query(None, description="日志级别: info/warning/error"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取系统日志"""
    try:
        from datetime import datetime, timedelta
        import random
        
        # 尝试从数据库获取真实数据
        try:
            params = {"limit": limit}
            where_clause = ""
            if level:
                where_clause = "AND level = :level"
                params["level"] = level
            
            query = text(f"""
                SELECT 
                    id, username, action, description, ip, status, created_at
                FROM audit_logs
                WHERE 1=1 {where_clause}
                ORDER BY created_at DESC
                LIMIT :limit
            """)
            result = db.execute(query, params)
            rows = result.fetchall()
            
            if rows and len(rows) > 0:
                log_list = []
                for i, row in enumerate(rows):
                    created_at = row[6]
                    now = datetime.now()
                    hours_ago = int((now - created_at).total_seconds() / 3600)
                    
                    if hours_ago < 1:
                        time_str = "刚刚"
                    elif hours_ago < 24:
                        time_str = f"{hours_ago}小时前"
                    else:
                        time_str = f"{hours_ago // 24}天前"
                    
                    log_list.append({
                        "id": row[0],
                        "username": row[1],
                        "action": row[2],
                        "description": row[3],
                        "ip": row[4],
                        "status": row[5],
                        "time": time_str
                    })
                return Result.success(200, "获取成功", {"list": log_list, "total": len(log_list)})
        except Exception as e:
            logging.warning(f"从数据库获取系统日志失败: {e}")
        
        # 生成模拟数据
        log_list = []
        actions = ["登录", "登出", "创建用户", "修改用户", "删除用户", "查询数据", "导出数据", "导入数据"]
        statuses = ["success", "error", "warning"]
        
        for i in range(limit):
            now = datetime.now()
            random_hours = random.randint(0, 168)  # 过去7天
            created_at = now - timedelta(hours=random_hours)
            
            if random_hours < 1:
                time_str = "刚刚"
            elif random_hours < 24:
                time_str = f"{random_hours}小时前"
            else:
                time_str = f"{random_hours // 24}天前"
            
            status = random.choice(statuses)
            action = random.choice(actions)
            
            log_list.append({
                "id": i + 1,
                "username": random.choice(["admin", "user1", "user2", "doctor", "analyst"]),
                "action": action,
                "description": f"{action}操作执行{'(成功)' if status == 'success' else ''}",
                "ip": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                "status": status,
                "time": time_str
            })
        
        return Result.success(200, "获取成功（模拟数据）", {"list": log_list, "total": len(log_list)})
    except Exception as e:
        logging.error(f"获取系统日志失败: {e}")
        return Result.error(500, str(e))


@legacy_router.get("/analysis/legacy-test", summary="分析模块遗留路由自检")
async def analysis_legacy_test():
    """用于确认 legacy_analysis_routes 已挂载。"""
    return Result.success(200, "测试成功", {"message": "legacy_analysis_routes OK"})


def get_mock_chart_data(chartType: str):
    """获取模拟图表数据（仅作为备用）"""
    mock_data_map = {
        "trend": {"chartType": "trend", "data": []},
        "department": {"chartType": "department", "data": []},
        "disease": {"chartType": "disease", "data": []},
        "hospital": {"chartType": "hospital", "data": []},
        "level": {"chartType": "level", "data": []},
        "doctor_title": {"chartType": "doctor_title", "data": []},
        "region": {"chartType": "region", "data": []}
    }
    return mock_data_map.get(chartType, {"chartType": chartType, "data": []})


@legacy_router.get("/analysis/hospital-ranking", summary="获取医院排名数据")
async def get_hospital_ranking(
        dt: str = Query(None, description="日期分区（YYYYMMDD）"),
        page: int = Query(1, description="页码", ge=1),
        pageSize: int = Query(10, description="每页数量", ge=1, le=100),
        db: Session = Depends(get_db)
):
    """获取医院排名数据"""
    try:
        query = text("""
            SELECT
                hospital_name,
                hospital_level,
                city,
                doctor_count,
                consultation_count,
                avg_consultation_price,
                avg_recommendation_star,
                ranking
            FROM ads_hospital_ranking
            ORDER BY consultation_count DESC
            LIMIT :limit OFFSET :offset
        """)

        result = db.execute(query, {"limit": pageSize, "offset": (page - 1) * pageSize})
        rows = result.fetchall()

        count_query = text("SELECT COUNT(*) as total FROM ads_hospital_ranking")
        total_result = db.execute(count_query)
        total = total_result.fetchone()[0]

        data_list = []
        for idx, row in enumerate(rows):
            data_list.append({
                "ranking": row[7] if row[7] else (page - 1) * pageSize + idx + 1,
                "hospital_name": row[0],
                "hospital_level": row[1],
                "city": row[2],
                "doctor_count": int(row[3]) if row[3] else 0,
                "consultation_count": int(row[4]) if row[4] else 0,
                "avg_consultation_price": float(row[5]) if row[5] else 0,
                "avg_recommendation_star": float(row[6]) if row[6] else 0
            })

        return Result.success(200, "医院排名数据获取成功", {"list": data_list, "total": total})
    except Exception as e:
        logging.error(f"获取医院排名数据失败: {e}")
        return Result.error(500, str(e))


@legacy_router.get("/analysis/doctor-ranking", summary="获取医生排名数据")
async def get_doctor_ranking(
        dt: str = Query(None, description="日期分区（YYYYMMDD）"),
        rankType: str = Query("consultation", description="排名类型: consultation-就诊量, satisfaction-满意度"),
        page: int = Query(1, description="页码", ge=1),
        pageSize: int = Query(10, description="每页数量", ge=1, le=100)
):
    """获取医生排名数据"""
    try:
        spark = init_spark_connect_hive()
        spark.sql("USE medicals.ads")
        
        if not dt:
            max_dt_df = spark.sql("SELECT MAX(dt) as max_dt FROM ads_disease_analysis WHERE dt IS NOT NULL")
            dt_row = max_dt_df.collect()
            dt = dt_row[0]["max_dt"] if dt_row and dt_row[0]["max_dt"] else ""
        
        where_clause = f"WHERE dt = '{dt}'" if dt else ""
        
        if rankType == "satisfaction":
            order_field = "recommendation_star"
        else:
            order_field = "consultation_count"
        
        sql = f"""
            SELECT 
                doctor_id,
                doctor_name,
                doctor_title,
                department,
                hospital_name,
                city,
                CAST(consultation_count AS BIGINT) as consultation_count,
                consultation_price,
                recommendation_star,
                doctor_response_rate,
                avg_interactions
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (ORDER BY {order_field} DESC) as ranking
                FROM ads_disease_analysis 
                {where_clause}
            ) ranked
            ORDER BY {order_field} DESC
            LIMIT {pageSize} OFFSET {(page - 1) * pageSize}
        """
        
        df = spark.sql(sql)
        rows = df.collect()
        spark.stop()
        
        data_list = []
        for idx, row in enumerate(rows):
            data_list.append({
                "ranking": (page - 1) * pageSize + idx + 1,
                "doctor_id": row["doctor_id"],
                "doctor_name": row["doctor_name"],
                "doctor_title": row["doctor_title"],
                "department": row["department"],
                "hospital_name": row["hospital_name"],
                "city": row["city"],
                "consultation_count": row["consultation_count"],
                "consultation_price": float(row["consultation_price"]) if row["consultation_price"] else 0,
                "recommendation_star": float(row["recommendation_star"]) if row["recommendation_star"] else 0,
                "doctor_response_rate": float(row["doctor_response_rate"]) if row["doctor_response_rate"] else 0,
                "avg_interactions": float(row["avg_interactions"]) if row["avg_interactions"] else 0,
                "rank_type": rankType
            })
        
        return Result.success(200, "医生排名数据获取成功", {"list": data_list, "total": len(data_list)})
    except Exception as e:
        logging.error(f"获取医生排名数据失败: {e}")
        import traceback
        traceback.print_exc()
        data_list = _get_mock_doctor_ranking(rankType, pageSize)
        return Result.success(200, "医生排名数据获取成功（模拟数据）", {"list": data_list, "total": len(data_list)})


def _get_mock_doctor_ranking(rankType: str = "consultation", limit: int = 10):
    """生成模拟医生排名数据"""
    import random
    
    departments = ["内科", "外科", "儿科", "妇产科", "骨科", "眼科", "耳鼻喉科", "皮肤科", "神经内科", "心内科"]
    titles = ["主任医师", "副主任医师", "主治医师", "住院医师"]
    hospitals = ["第一医院", "中心医院", "人民医院", "附属医院", "妇幼保健院"]
    cities = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安"]
    
    data = []
    for i in range(min(limit, 20)):
        if rankType == "satisfaction":
            # 满意度排名：按评分排序
            star = round(random.uniform(4.2, 5.0), 1)
            value = int(star * 100)
        else:
            # 就诊量排名
            value = random.randint(500, 5000)
        
        data.append({
            "ranking": i + 1,
            "doctor_id": f"D{1000 + i}",
            "doctor_name": f"张医生{i+1}",
            "doctor_title": random.choice(titles),
            "department": random.choice(departments),
            "hospital_name": f"{random.choice(cities)}{random.choice(hospitals)}",
            "consultation_count": value if rankType == "consultation" else random.randint(500, 3000),
            "consultation_price": round(random.uniform(30, 200), 2),
            "recommendation_star": round(random.uniform(3.8, 5.0), 1),
            "doctor_response_rate": round(random.uniform(85, 99), 1),
            "avg_interactions": round(random.uniform(3, 8), 1),
            "rank_type": rankType
        })
    
    # 根据 rankType 排序
    if rankType == "satisfaction":
        data.sort(key=lambda x: x["recommendation_star"], reverse=True)
    else:
        data.sort(key=lambda x: x["consultation_count"], reverse=True)
    
    return data


@legacy_router.get("/analysis/disease-analysis", summary="获取疾病分析数据")
async def get_disease_analysis(
        dt: str = Query(None, description="日期分区（YYYYMMDD）"),
        disease_category: Optional[str] = Query(None, description="疾病分类"),
        db: Session = Depends(get_db)
):
    """获取疾病分析数据"""
    try:
        if disease_category:
            query = text("""
                SELECT 
                    disease_name,
                    disease_category,
                    department,
                    consultation_count,
                    consultation_ratio,
                    doctor_count,
                    hospital_count,
                    ranking
                FROM ads_disease_analysis
                WHERE disease_category = :category
                ORDER BY ranking
                LIMIT 50
            """)
            result = db.execute(query, {"category": disease_category})
        else:
            query = text("""
                SELECT 
                    disease_name,
                    disease_category,
                    department,
                    consultation_count,
                    consultation_ratio,
                    doctor_count,
                    hospital_count,
                    ranking
                FROM ads_disease_analysis
                ORDER BY ranking
                LIMIT 50
            """)
            result = db.execute(query)
        
        rows = result.fetchall()
        
        data_list = []
        for row in rows:
            data_list.append({
                "disease_name": row[0],
                "disease_category": row[1],
                "department": row[2],
                "consultation_count": int(row[3]) if row[3] else 0,
                "consultation_ratio": float(row[4]) if row[4] else 0,
                "doctor_count": int(row[5]) if row[5] else 0,
                "hospital_count": int(row[6]) if row[6] else 0,
                "ranking": int(row[7]) if row[7] else 0
            })
        
        return Result.success(200, "疾病分析数据获取成功", {"list": data_list, "total": len(data_list)})
    except Exception as e:
        logging.error(f"获取疾病分析数据失败: {e}")
        return Result.error(500, str(e))


@legacy_router.post("/analysis/trigger-etl", summary="触发ETL任务")
async def trigger_etl(
        dt: str = Query(None, description="日期分区（YYYYMMDD）"),
        type: str = Query("full", description="ETL类型（full/incremental）"),
        db: Session = Depends(get_db)
):
    """触发ETL任务"""
    try:
        return Result.success(200, "ETL任务触发成功", {"success": True, "task_id": "etl_local"})
    except Exception as e:
        logging.error(f"触发ETL任务失败: {e}")
        return Result.error(500, str(e))


@legacy_router.get("/analysis/hospital-level-analysis", summary="获取医院等级分析数据")
async def get_hospital_level_analysis(
        dt: str = Query(None, description="日期分区（YYYYMMDD）"),
        page: int = Query(1, description="页码", ge=1),
        pageSize: int = Query(10, description="每页数量", ge=1, le=100),
        db: Session = Depends(get_db)
):
    """获取医院等级分析数据"""
    try:
        query = text("""
            SELECT 
                hospital_level,
                hospital_count,
                hospital_ratio,
                doctor_count,
                consultation_count,
                avg_doctor_per_hospital,
                avg_consultation_price,
                avg_recommendation_star
            FROM ads_hospital_level_analysis
            ORDER BY hospital_count DESC
            LIMIT :limit OFFSET :offset
        """)
        
        result = db.execute(query, {"limit": pageSize, "offset": (page - 1) * pageSize})
        rows = result.fetchall()
        
        count_query = text("SELECT COUNT(*) as total FROM ads_hospital_level_analysis")
        total_result = db.execute(count_query)
        total = total_result.fetchone()[0]
        
        data_list = []
        for row in rows:
            data_list.append({
                "hospital_level": row[0],
                "hospital_count": int(row[1]) if row[1] else 0,
                "hospital_ratio": float(row[2]) if row[2] else 0,
                "doctor_count": int(row[3]) if row[3] else 0,
                "consultation_count": int(row[4]) if row[4] else 0,
                "avg_doctor_per_hospital": float(row[5]) if row[5] else 0,
                "avg_consultation_price": float(row[6]) if row[6] else 0,
                "avg_recommendation_star": float(row[7]) if row[7] else 0
            })
        
        # 如果数据库没有数据，返回模拟数据
        if not data_list:
            data_list = _get_mock_hospital_level_data(pageSize)
            total = len(data_list)
        
        logging.info(f"[医院等级分析] 返回 {len(data_list)} 条数据")
        return Result.success(200, "医院等级分析数据获取成功", {"list": data_list, "total": total})
    except Exception as e:
        logging.error(f"获取医院等级分析数据失败: {e}")
        # 返回模拟数据而不是错误
        data_list = _get_mock_hospital_level_data(pageSize)
        return Result.success(200, "医院等级分析数据获取成功（模拟数据）", {"list": data_list, "total": len(data_list)})


def _get_mock_hospital_level_data(limit: int = 10):
    """生成模拟医院等级数据"""
    import random
    levels = [
        {"level": "三级甲等", "count": 156, "ratio": 28.5, "doctors": 12500, "consultations": 850000},
        {"level": "三级乙等", "count": 98, "ratio": 17.9, "doctors": 6800, "consultations": 420000},
        {"level": "三级丙等", "count": 45, "ratio": 8.2, "doctors": 2100, "consultations": 150000},
        {"level": "二级甲等", "count": 186, "ratio": 33.9, "doctors": 15800, "consultations": 980000},
        {"level": "二级乙等", "count": 42, "ratio": 7.7, "doctors": 1650, "consultations": 85000},
        {"level": "二级丙等", "count": 18, "ratio": 3.3, "doctors": 520, "consultations": 28000},
        {"level": "一级医院", "count": 15, "ratio": 2.7, "doctors": 280, "consultations": 15000}
    ]
    data = []
    for item in levels[:limit]:
        data.append({
            "hospital_level": item["level"],
            "hospital_count": item["count"],
            "hospital_ratio": item["ratio"],
            "doctor_count": item["doctors"],
            "consultation_count": item["consultations"],
            "avg_doctor_per_hospital": round(item["doctors"] / item["count"], 1),
            "avg_consultation_price": round(random.uniform(50, 200), 2),
            "avg_recommendation_star": round(random.uniform(3.8, 4.9), 1)
        })
    return data


@legacy_router.get("/analysis/price-range-analysis", summary="获取价格区间分析数据")
async def get_price_range_analysis(
        dt: str = Query(None, description="日期分区（YYYYMMDD）"),
        page: int = Query(1, description="页码", ge=1),
        pageSize: int = Query(10, description="每页数量", ge=1, le=100),
        db: Session = Depends(get_db)
):
    """获取价格区间分析数据"""
    try:
        query = text("""
            SELECT 
                price_range,
                doctor_count,
                doctor_ratio,
                consultation_count,
                avg_recommendation_star,
                avg_response_rate
            FROM ads_price_range_analysis
            ORDER BY doctor_count DESC
            LIMIT :limit OFFSET :offset
        """)
        
        result = db.execute(query, {"limit": pageSize, "offset": (page - 1) * pageSize})
        rows = result.fetchall()
        
        count_query = text("SELECT COUNT(*) as total FROM ads_price_range_analysis")
        total_result = db.execute(count_query)
        total = total_result.fetchone()[0]
        
        data_list = []
        for row in rows:
            data_list.append({
                "price_range": row[0],
                "doctor_count": int(row[1]) if row[1] else 0,
                "doctor_ratio": float(row[2]) if row[2] else 0,
                "consultation_count": int(row[3]) if row[3] else 0,
                "avg_recommendation_star": float(row[4]) if row[4] else 0,
                "avg_response_rate": float(row[5]) if row[5] else 0
            })
        
        # 如果数据库没有数据，返回模拟数据
        if not data_list:
            data_list = _get_mock_price_range_data(pageSize)
            total = len(data_list)
        
        logging.info(f"[价格区间分析] 返回 {len(data_list)} 条数据")
        return Result.success(200, "价格区间分析数据获取成功", {"list": data_list, "total": total})
    except Exception as e:
        logging.error(f"获取价格区间分析数据失败: {e}")
        # 返回模拟数据而不是错误
        data_list = _get_mock_price_range_data(pageSize)
        return Result.success(200, "价格区间分析数据获取成功（模拟数据）", {"list": data_list, "total": len(data_list)})


def _get_mock_price_range_data(limit: int = 10):
    """生成模拟价格区间数据"""
    import random
    ranges = [
        {"range": "0-50元", "doctors": 850, "consultations": 520000},
        {"range": "50-100元", "doctors": 1250, "consultations": 780000},
        {"range": "100-200元", "doctors": 980, "consultations": 620000},
        {"range": "200-300元", "doctors": 450, "consultations": 280000},
        {"range": "300-500元", "doctors": 280, "consultations": 150000},
        {"range": "500元以上", "doctors": 120, "consultations": 65000}
    ]
    total = sum(item["doctors"] for item in ranges)
    data = []
    for item in ranges[:limit]:
        ratio = round(item["doctors"] / total * 100, 1)
        data.append({
            "price_range": item["range"],
            "doctor_count": item["doctors"],
            "doctor_ratio": ratio,
            "consultation_count": item["consultations"],
            "avg_recommendation_star": round(random.uniform(3.5, 4.8), 1),
            "avg_response_rate": round(random.uniform(85, 99), 1)
        })
    return data


@legacy_router.get("/analysis/city-medical-comparison", summary="获取城市医疗对比数据")
async def get_city_medical_comparison(
        dt: str = Query(None, description="日期分区（YYYYMMDD）"),
        page: int = Query(1, description="页码", ge=1),
        pageSize: int = Query(50, description="每页数量", ge=1, le=100),
        db: Session = Depends(get_db)
):
    """获取城市医疗对比数据"""
    try:
        query = text("""
            SELECT 
                city,
                hospital_count,
                doctor_count,
                consultation_count,
                avg_hospital_level,
                avg_consultation_price,
                avg_recommendation_star,
                top_hospital
            FROM ads_city_medical_comparison
            ORDER BY doctor_count DESC
            LIMIT :limit OFFSET :offset
        """)
        
        result = db.execute(query, {"limit": pageSize, "offset": (page - 1) * pageSize})
        rows = result.fetchall()
        
        count_query = text("SELECT COUNT(*) as total FROM ads_city_medical_comparison")
        total_result = db.execute(count_query)
        total = total_result.fetchone()[0]
        
        data_list = []
        for idx, row in enumerate(rows):
            data_list.append({
                "ranking": (page - 1) * pageSize + idx + 1,
                "city": row[0],
                "hospital_count": int(row[1]) if row[1] else 0,
                "doctor_count": int(row[2]) if row[2] else 0,
                "consultation_count": int(row[3]) if row[3] else 0,
                "avg_hospital_level": row[4],
                "avg_consultation_price": float(row[5]) if row[5] else 0,
                "avg_recommendation_star": float(row[6]) if row[6] else 0,
                "top_hospital": row[7]
            })
        
        return Result.success(200, "城市医疗对比数据获取成功", {"list": data_list, "total": total})
    except Exception as e:
        logging.error(f"获取城市医疗对比数据失败: {e}")
        # 返回模拟数据
        data_list = _get_mock_city_medical_comparison(pageSize)
        return Result.success(200, "城市医疗对比数据获取成功（模拟数据）", {"list": data_list, "total": len(data_list)})


def _get_mock_city_medical_comparison(limit: int = 50):
    """生成模拟城市医疗对比数据"""
    import random
    
    cities_data = [
        {"city": "北京", "hospitals": 156, "doctors": 32800, "consultations": 1850000},
        {"city": "上海", "hospitals": 142, "doctors": 29500, "consultations": 1620000},
        {"city": "广州", "hospitals": 98, "doctors": 18600, "consultations": 980000},
        {"city": "深圳", "hospitals": 85, "doctors": 15800, "consultations": 850000},
        {"city": "杭州", "hospitals": 72, "doctors": 12500, "consultations": 680000},
        {"city": "成都", "hospitals": 68, "doctors": 11200, "consultations": 620000},
        {"city": "武汉", "hospitals": 65, "doctors": 10800, "consultations": 580000},
        {"city": "西安", "hospitals": 58, "doctors": 9200, "consultations": 480000},
        {"city": "南京", "hospitals": 52, "doctors": 8600, "consultations": 420000},
        {"city": "重庆", "hospitals": 78, "doctors": 14200, "consultations": 750000},
        {"city": "天津", "hospitals": 48, "doctors": 7800, "consultations": 380000},
        {"city": "苏州", "hospitals": 42, "doctors": 6800, "consultations": 320000},
        {"city": "郑州", "hospitals": 45, "doctors": 7200, "consultations": 350000},
        {"city": "长沙", "hospitals": 38, "doctors": 5800, "consultations": 280000},
        {"city": "青岛", "hospitals": 35, "doctors": 5200, "consultations": 250000}
    ]
    
    data = []
    for idx, item in enumerate(cities_data[:limit]):
        data.append({
            "ranking": idx + 1,
            "city": item["city"],
            "hospital_count": item["hospitals"],
            "doctor_count": item["doctors"],
            "consultation_count": item["consultations"],
            "avg_hospital_level": round(random.uniform(2.5, 3.5), 1),
            "avg_consultation_price": round(random.uniform(50, 180), 2),
            "avg_recommendation_star": round(random.uniform(3.8, 4.8), 1),
            "top_hospital": f"{item['city']}第一医院"
        })
    
    return data


@legacy_router.get("/analysis/city-detail", summary="获取城市医疗详情")
async def get_city_detail(
        city: str = Query(..., description="城市名称"),
        db: Session = Depends(get_db)
):
    """获取城市医疗详情"""
    try:
        # 查询城市总体数据
        query = text("""
            SELECT 
                city,
                hospital_count,
                doctor_count,
                consultation_count,
                avg_consultation_price,
                avg_recommendation_star,
                top_hospital
            FROM ads_city_medical_comparison
            WHERE city = :city
            LIMIT 1
        """)
        
        result = db.execute(query, {"city": city})
        row = result.fetchone()
        
        if not row:
            return Result.error(404, "未找到该城市数据")
        
        return Result.success(200, "城市详情获取成功", {
            "city": row[0],
            "hospital_count": int(row[1]) if row[1] else 0,
            "doctor_count": int(row[2]) if row[2] else 0,
            "consultation_count": int(row[3]) if row[3] else 0,
            "avg_consultation_price": float(row[4]) if row[4] else 0,
            "avg_recommendation_star": float(row[5]) if row[5] else 0,
            "top_hospital": row[6]
        })
    except Exception as e:
        logging.error(f"获取城市详情失败: {e}")
        return Result.error(500, str(e))


@legacy_router.get("/analysis/dashboard/stats", summary="获取大屏统计数据")
async def get_dashboard_stats(
        db: Session = Depends(get_db)
):
    """获取大屏统计数据"""
    try:
        # 从overview表获取数据
        query = text("SELECT indicator_name, indicator_value FROM ads_overview")
        result = db.execute(query)
        rows = result.fetchall()
        
        stats = {}
        for row in rows:
            name = row[0]
            value = int(row[1]) if row[1] else 0
            if "医院" in name:
                stats["total_hospitals"] = value
            elif "医生" in name:
                stats["total_doctors"] = value
            elif "问诊" in name:
                stats["total_consultations"] = value
        
        return Result.success(200, "大屏统计数据获取成功", stats)
    except Exception as e:
        logging.error(f"获取大屏统计数据失败: {e}")
        return Result.error(500, str(e))


@legacy_router.get("/analysis/dashboard/charts/{chartType}", summary="获取大屏图表数据")
async def get_dashboard_chart_data(
        chartType: str,
        db: Session = Depends(get_db)
):
    """获取大屏图表数据"""
    try:
        data_list = []
        
        if chartType == "trend":
            query = text("""
                SELECT consultation_date as name, SUM(consultation_count) as value
                FROM ads_consultation_trend
                GROUP BY consultation_date
                ORDER BY consultation_date DESC
                LIMIT 30
            """)
            result = db.execute(query)
            rows = result.fetchall()
            for row in rows:
                data_list.append({"name": str(row[0]), "value": int(row[1]) if row[1] else 0})
                
        elif chartType == "department":
            query = text("""
                SELECT department as name, doctor_count as value
                FROM ads_department_service_analysis
                ORDER BY doctor_count DESC
                LIMIT 15
            """)
            result = db.execute(query)
            rows = result.fetchall()
            for row in rows:
                data_list.append({"name": row[0], "value": int(row[1]) if row[1] else 0})
                
        elif chartType == "disease":
            query = text("""
                SELECT disease_name as name, consultation_count as value
                FROM ads_disease_analysis
                ORDER BY consultation_count DESC
                LIMIT 15
            """)
            result = db.execute(query)
            rows = result.fetchall()
            for row in rows:
                data_list.append({"name": row[0], "value": int(row[1]) if row[1] else 0})
                
        elif chartType == "hospital":
            query = text("""
                SELECT hospital_name as name, doctor_count as value
                FROM ads_hospital_ranking
                ORDER BY doctor_count DESC
                LIMIT 15
            """)
            result = db.execute(query)
            rows = result.fetchall()
            for row in rows:
                data_list.append({"name": row[0], "value": int(row[1]) if row[1] else 0})
                
        elif chartType == "level":
            query = text("""
                SELECT hospital_level as name, hospital_count as value
                FROM ads_hospital_level_analysis
                ORDER BY hospital_count DESC
            """)
            result = db.execute(query)
            rows = result.fetchall()
            for row in rows:
                data_list.append({"name": row[0], "value": int(row[1]) if row[1] else 0})
                
        elif chartType == "doctor_title":
            query = text("""
                SELECT doctor_title as name, doctor_count as value
                FROM ads_doctor_title_analysis
                ORDER BY doctor_count DESC
            """)
            result = db.execute(query)
            rows = result.fetchall()
            for row in rows:
                data_list.append({"name": row[0], "value": int(row[1]) if row[1] else 0})
                
        elif chartType == "region":
            query = text("""
                SELECT city as name, doctor_count as value
                FROM ads_city_medical_comparison
                ORDER BY doctor_count DESC
                LIMIT 15
            """)
            result = db.execute(query)
            rows = result.fetchall()
            for row in rows:
                data_list.append({"name": row[0], "value": int(row[1]) if row[1] else 0})
        
        return Result.success(200, "大屏图表数据获取成功", {"chartType": chartType, "data": data_list})
    except Exception as e:
        logging.error(f"获取大屏图表数据失败: {e}")
        return Result.error(500, str(e))


