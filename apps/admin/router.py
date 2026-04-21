import logging
import os
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from apps.user.models import User
from apps.core.database import get_db
from apps.core import Result
from utils.auth_helpers import require_auth

router = APIRouter(prefix="", tags=["admin"])


@router.get("/api/admin/home-stats", summary="获取管理员首页统计数据")
async def get_admin_home_stats(
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
            "login_activity_trend": [],
            "department_dist": [],
            "disease_dist": []
        }

        # 获取用户总数
        try:
            user_count_query = text("SELECT COUNT(*) FROM users WHERE is_deleted = 0")
            user_result = db.execute(user_count_query)
            stats["total_users"] = user_result.fetchone()[0] or 0
        except Exception as e:
            logging.warning(f"获取用户总数失败: {e}")

        # 今日活跃：优先按审计日志（与服务器日期一致），避免 last_login 时区与 CURDATE() 不一致导致全 0
        try:
            audit_today = text("""
                SELECT COUNT(DISTINCT user_id) FROM audit_logs
                WHERE user_id IS NOT NULL
                  AND DATE(created_at) = CURDATE()
                  AND LOWER(action) LIKE '%login%'
            """)
            row_at = db.execute(audit_today).fetchone()
            stats["active_today"] = int(row_at[0] or 0) if row_at else 0
        except Exception as e:
            logging.warning(f"按审计日志统计今日活跃失败: {e}")
        if stats["active_today"] == 0:
            try:
                today_query = text(
                    "SELECT COUNT(*) FROM users WHERE is_deleted = 0 "
                    "AND last_login IS NOT NULL AND DATE(last_login) = CURDATE()"
                )
                today_result = db.execute(today_query)
                stats["active_today"] = int(today_result.fetchone()[0] or 0)
            except Exception as e:
                logging.warning(f"按 users.last_login 统计今日活跃失败: {e}")

        # 在线用户：有效设备会话；无会话时退化为近 45 分钟内有登录审计的去重用户数
        try:
            sess_q = text("""
                SELECT COUNT(DISTINCT user_id) FROM device_sessions
                WHERE is_active = 1
                  AND (expired_at IS NULL OR expired_at > NOW())
            """)
            row_on = db.execute(sess_q).fetchone()
            stats["online_users"] = int(row_on[0] or 0) if row_on else 0
        except Exception as e:
            logging.warning(f"按 device_sessions 统计在线用户失败: {e}")
        if stats.get("online_users", 0) == 0:
            try:
                recent_q = text("""
                    SELECT COUNT(DISTINCT user_id) FROM audit_logs
                    WHERE user_id IS NOT NULL
                      AND LOWER(action) LIKE '%login%'
                      AND created_at >= DATE_SUB(NOW(), INTERVAL 45 MINUTE)
                """)
                row_r = db.execute(recent_q).fetchone()
                stats["online_users"] = int(row_r[0] or 0) if row_r else 0
            except Exception as e:
                logging.warning(f"按近期登录审计统计在线用户失败: {e}")

        # 管理首页「用户活动」折线/柱状图：近 14 天每日登录去重用户数
        try:
            login_trend_q = text("""
                SELECT DATE(created_at) AS d,
                       COUNT(DISTINCT user_id) AS active_users
                FROM audit_logs
                WHERE user_id IS NOT NULL
                  AND LOWER(action) LIKE '%login%'
                  AND created_at >= DATE_SUB(CURDATE(), INTERVAL 13 DAY)
                GROUP BY DATE(created_at)
                ORDER BY d ASC
            """)
            lt_rows = db.execute(login_trend_q).fetchall()
            stats["login_activity_trend"] = [
                {"date": str(r[0]), "value": int(r[1] or 0)}
                for r in lt_rows
            ]
        except Exception as e:
            logging.warning(f"获取登录活跃趋势失败: {e}")
        
        try:
            overview_row = db.execute(
                text("""
                    SELECT total_hospitals, total_doctors, total_consultations
                    FROM ads_overview
                    ORDER BY created_at DESC
                    LIMIT 1
                """)
            ).fetchone()
            if overview_row:
                stats["indicators"]["hospitals"] = int(overview_row[0] or 0)
                stats["indicators"]["doctors"] = int(overview_row[1] or 0)
                stats["indicators"]["consultations"] = int(overview_row[2] or 0)
        except Exception:
            try:
                overview_result = db.execute(
                    text(
                        "SELECT indicator_name, indicator_value FROM ads_overview LIMIT 500"
                    )
                )
                for row in overview_result.fetchall():
                    name, raw = row[0], row[1]
                    if name is None:
                        continue
                    name_s = str(name)
                    value = int(raw) if raw is not None else 0
                    if "医院" in name_s:
                        stats["indicators"]["hospitals"] = value
                    elif "医生" in name_s:
                        stats["indicators"]["doctors"] = value
                    elif "问诊" in name_s:
                        stats["indicators"]["consultations"] = value
            except Exception as e:
                logging.warning(f"获取概览数据失败: {e}")

        # 问诊趋势（兼容 consultation_date 与 date 两种列名）
        try:
            trend_result = db.execute(
                text("""
                    SELECT consultation_date, SUM(consultation_count) AS total
                    FROM ads_consultation_trend
                    GROUP BY consultation_date
                    ORDER BY consultation_date DESC
                    LIMIT 30
                """)
            )
            stats["recent_trend"] = [
                {"date": str(row[0]), "value": int(row[1]) if row[1] else 0}
                for row in trend_result.fetchall()
            ]
        except Exception:
            try:
                trend_result = db.execute(
                    text("""
                        SELECT `date`, SUM(consultation_count) AS total
                        FROM ads_consultation_trend
                        GROUP BY `date`
                        ORDER BY `date` DESC
                        LIMIT 30
                    """)
                )
                stats["recent_trend"] = [
                    {"date": str(row[0]), "value": int(row[1]) if row[1] else 0}
                    for row in trend_result.fetchall()
                ]
            except Exception as e:
                logging.warning(f"获取趋势数据失败: {e}")

        # 获取科室分布（与 /api/analysis/department-service-analysis 口径一致：最新分区 + 按科室聚合 + 问诊量列语义）
        try:
            _swap = (os.getenv("DEPT_SERVICE_SWAP_DOCTOR_CONSULT") or "1").strip().lower() not in (
                "0",
                "false",
                "no",
                "off",
            )
            _vol = "doctor_count" if _swap else "consultation_count"
            dept_query = text(f"""
                SELECT
                    department,
                    SUM(COALESCE(`{_vol}`, 0)) AS consultation_count
                FROM ads_department_service_analysis
                WHERE department IS NOT NULL AND TRIM(department) <> ''
                  AND dt = (SELECT MAX(dt) FROM ads_department_service_analysis d2)
                GROUP BY department
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
                FROM ads_disease_analysis
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

        # 数据记录数 (从 ads 表估算)
        try:
            table_count_query = text("""
                SELECT SUM(table_rows) 
                FROM information_schema.tables 
                WHERE table_schema = 'medicals' AND table_name LIKE 'ads_%'
            """)
            table_result = db.execute(table_count_query)
            stats["total_records"] = table_result.fetchone()[0] or 0
        except Exception as e:
            logging.warning(f"获取记录数失败: {e}")

        # 系统告警数（近7天失败日志）
        try:
            alert_query = text("""
                SELECT COUNT(*)
                FROM audit_logs
                WHERE status IN ('failed', 'error')
                  AND created_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
            """)
            alert_result = db.execute(alert_query)
            stats["system_alerts"] = alert_result.fetchone()[0] or 0
        except Exception as e:
            logging.warning(f"获取系统告警失败: {e}")

        return Result.success(200, "管理员首页统计数据获取成功", stats)
    except Exception as e:
        logging.error(f"获取管理员首页统计数据失败: {e}")
        return Result.error(500, str(e))


@router.get("/api/admin/user-activity", summary="获取用户活动趋势")
async def get_user_activity(
        period: str = Query("week", description="周期: day-按天, week-按周, month-按月"),
        limit: int = Query(30, ge=1, le=90, description="返回条数"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取用户活动趋势数据（登录/活跃统计）。

    按自然日聚合最近若干天的登录审计。此前 week 模式使用 INTERVAL :limit WEEK，
    在 limit=7 时会被解释为「近 7 周」且与前端「近 7 天」预期不符，易导致无数据。
    """
    try:
        if period == "month":
            span_days = min(int(limit) * 31, 365)
        elif period == "day":
            span_days = min(int(limit), 90)
        else:
            span_days = min(max(int(limit), 1), 90)

        query = text("""
            SELECT
                DATE(created_at) AS period_date,
                COUNT(*) AS login_count,
                COUNT(DISTINCT user_id) AS active_users
            FROM audit_logs
            WHERE LOWER(action) LIKE '%login%'
              AND created_at >= DATE_SUB(CURDATE(), INTERVAL :span DAY)
            GROUP BY DATE(created_at)
            ORDER BY period_date ASC
        """)
        rows = db.execute(query, {"span": max(span_days - 1, 0)}).fetchall()

        data_list = []
        for row in rows:
            period_date = row[0].strftime('%Y-%m-%d') if hasattr(row[0], 'strftime') else str(row[0])
            data_list.append({
                "date": period_date,
                "login_count": int(row[1]) if row[1] else 0,
                "active_users": int(row[2]) if row[2] else 0
            })

        return Result.success(200, "获取成功", {"list": data_list, "total": len(data_list)})
    except Exception as e:
        logging.error(f"获取用户活动失败: {e}")
        return Result.error(500, str(e))


@router.get("/api/admin/resource-usage", summary="获取资源使用统计")
async def get_resource_usage(
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取系统资源使用统计"""
    try:
        # 数据库监控指标（近24h）
        db_metric_query = text("""
            SELECT
                COUNT(DISTINCT datasource_id) AS db_count,
                COUNT(*) AS total_metrics,
                AVG(COALESCE(memory_percent, 0)) AS avg_db_memory_percent
            FROM database_metrics
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
        """)
        db_metric_row = db.execute(db_metric_query).fetchone()
        db_count = int(db_metric_row[0]) if db_metric_row and db_metric_row[0] else 0
        db_metrics_total = int(db_metric_row[1]) if db_metric_row and db_metric_row[1] else 0
        avg_db_memory_percent = float(db_metric_row[2]) if db_metric_row and db_metric_row[2] else 0.0

        # 集群监控指标（近24h）
        cluster_metric_query = text("""
            SELECT
                COUNT(DISTINCT cluster_id) AS cluster_count,
                COUNT(*) AS total_metrics,
                AVG(COALESCE(cpu_usage, 0)) AS avg_cpu_usage,
                AVG(COALESCE(memory_percent, 0)) AS avg_cluster_memory_percent,
                AVG(COALESCE(hdfs_percent, 0)) AS avg_hdfs_percent
            FROM cluster_metrics
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY)
        """)
        cluster_metric_row = db.execute(cluster_metric_query).fetchone()
        cluster_count = int(cluster_metric_row[0]) if cluster_metric_row and cluster_metric_row[0] else 0
        cluster_metrics_total = int(cluster_metric_row[1]) if cluster_metric_row and cluster_metric_row[1] else 0
        avg_cpu_usage = float(cluster_metric_row[2]) if cluster_metric_row and cluster_metric_row[2] else 0.0
        avg_cluster_memory_percent = float(cluster_metric_row[3]) if cluster_metric_row and cluster_metric_row[
            3] else 0.0
        avg_hdfs_percent = float(cluster_metric_row[4]) if cluster_metric_row and cluster_metric_row[4] else 0.0

        # 用户统计
        user_query = text("""
            SELECT 
                COUNT(*) AS total_users,
                SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_users,
                SUM(CASE WHEN last_login >= DATE_SUB(NOW(), INTERVAL 7 DAY) THEN 1 ELSE 0 END) AS recent_logins
            FROM users
            WHERE is_deleted = 0
        """)
        user_row = db.execute(user_query).fetchone()
        total_users = int(user_row[0]) if user_row and user_row[0] else 0
        active_users = int(user_row[1]) if user_row and user_row[1] else 0
        recent_logins = int(user_row[2]) if user_row and user_row[2] else 0

        total_metrics = db_metrics_total + cluster_metrics_total
        memory_usage = avg_cluster_memory_percent if avg_cluster_memory_percent > 0 else avg_db_memory_percent

        data = {
            "cpu": {
                "name": "CPU使用率",
                "value": round(avg_cpu_usage, 1),
                "max": 100
            },
            "memory": {
                "name": "内存使用率",
                "value": round(memory_usage, 1),
                "max": 100
            },
            "disk": {
                "name": "磁盘使用率",
                "value": round(avg_hdfs_percent, 1),
                "max": 100
            },
            "network": {
                "name": "网络带宽",
                "value": 0,
                "max": 100
            },
            "database": {
                "name": "数据库连接",
                "value": db_count,
                "max": 100
            },
            "activeUsers": {
                "name": "活跃用户",
                "value": active_users,
                "total": total_users
            },
            "recentLogins": {
                "name": "近7天登录",
                "value": recent_logins
            },
            "totalMetrics": {
                "name": "监控指标数",
                "value": total_metrics
            }
        }

        return Result.success(200, "获取成功", data)
    except Exception as e:
        logging.error(f"获取资源使用失败: {e}")
        return Result.error(500, str(e))


@router.get("/api/admin/system-logs", summary="获取系统日志")
async def get_system_logs(
        limit: int = Query(20, ge=1, le=100, description="返回条数"),
        level: str = Query(None, description="日志级别: info/warning/error"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取系统日志"""
    try:
        from datetime import datetime

        conditions = ["action IS NOT NULL"]
        params = {"limit": limit}

        if level:
            conditions.append("status = :level")
            params["level"] = level

        where_clause = " AND ".join(conditions)

        query = text(f"""
            SELECT 
                id,
                username,
                action,
                description,
                ip_address,
                status,
                created_at
            FROM audit_logs
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit
        """)
        rows = db.execute(query, params).fetchall()

        log_list = []
        for row in rows:
            created_at = row[6]
            if hasattr(created_at, 'strftime'):
                time_diff = datetime.now() - created_at
                if time_diff.days > 0:
                    time_str = f"{time_diff.days}天前"
                elif time_diff.seconds // 3600 > 0:
                    time_str = f"{time_diff.seconds // 3600}小时前"
                else:
                    time_str = f"{max(1, time_diff.seconds // 60)}分钟前"
            else:
                time_str = "最近"

            log_list.append({
                "id": row[0],
                "username": row[1] or "系统",
                "action": row[2],
                "description": row[3] or row[2],
                "ip": row[4] or "未知",
                "status": row[5] or "info",
                "time": time_str
            })

        return Result.success(200, "获取成功", {"list": log_list, "total": len(log_list)})
    except Exception as e:
        logging.error(f"获取系统日志失败: {e}")
        return Result.error(500, str(e))
