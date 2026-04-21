import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from apps.core.database import get_db
from apps.core import Result

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

logger = logging.getLogger(__name__)


@router.get("/realtime", summary="获取实时数据")
async def get_realtime_data(
    limit: int = Query(24, description="返回数据点数量"),
    db: Session = Depends(get_db)
):
    """获取实时动态数据"""
    try:
        query = """
            SELECT
                DATE_FORMAT(consultation_date, '%%Y-%%m-%%d') AS period_label,
                SUM(consultation_count) AS total_count
            FROM ads_consultation_trend
            GROUP BY consultation_date
            ORDER BY consultation_date DESC
            LIMIT :limit
        """
        rows = db.execute(text(query), {"limit": limit}).fetchall()

        # 反转后按时间升序返回，便于前端渲染
        ordered_rows = list(reversed(rows))
        data_list = [
            {"hour": row[0], "value": int(row[1]) if row[1] else 0}
            for row in ordered_rows
        ]

        return Result.success(
            200,
            "实时数据获取成功",
            {"list": data_list}
        )
    except Exception as e:
        logger.error(f"获取实时数据失败: {e}")
        return Result.error(500, str(e))
