"""
监控模块 - API路由
"""
import logging
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from apps.user.models import User
from apps.core.database import get_db
from apps.core import Result
from utils.auth_helpers import require_auth
from apps.monitor.service import (
    DatabaseMonitorService,
    ClusterMonitorService,
    MonitorService
)
from apps.monitor.schemas import (
    DatabaseMetricResponse,
    ClusterMetricResponse,
    MonitorOverviewResponse
)


router = APIRouter(prefix="/api", tags=["monitor"])
logger = logging.getLogger(__name__)


# ============================= 数据库监控 API =============================

@router.get("/monitor/database/{datasource_id}/latest", summary="获取数据库最新监控指标")
async def get_database_latest_metric(
    datasource_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取指定数据源的最新监控指标
    """
    try:
        metric = DatabaseMonitorService.get_latest_metric(db, datasource_id)
        if not metric:
            metric = DatabaseMonitorService.collect_metric(db, datasource_id)
        return Result.success(200, "获取成功", DatabaseMetricResponse.model_validate(metric))
    except Exception as e:
        logger.exception(f"获取数据库监控指标失败: {e}")
        return Result.error(500, str(e))


@router.post("/monitor/database/{datasource_id}/collect", summary="采集数据库监控指标")
async def collect_database_metric(
    datasource_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    手动采集指定数据源的监控指标
    """
    try:
        metric = DatabaseMonitorService.collect_metric(db, datasource_id)
        return Result.success(200, "采集成功", DatabaseMetricResponse.model_validate(metric))
    except Exception as e:
        logger.exception(f"采集数据库监控指标失败: {e}")
        return Result.error(500, str(e))


@router.get("/monitor/database/{datasource_id}/history", summary="获取数据库监控历史")
async def get_database_metrics_history(
    datasource_id: int,
    limit: int = Query(100, ge=1, le=1000, description="返回数量"),
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取指定数据源的监控历史数据
    """
    try:
        metrics = DatabaseMonitorService.get_metrics_history(db, datasource_id, limit)
        return Result.success(200, "获取成功", {
            "metrics": [DatabaseMetricResponse.model_validate(m) for m in metrics],
            "total": len(metrics)
        })
    except Exception as e:
        logger.exception(f"获取数据库监控历史失败: {e}")
        return Result.error(500, str(e))


# ============================= 集群监控 API =============================

@router.get("/monitor/cluster/{cluster_id}/latest", summary="获取集群最新监控指标")
async def get_cluster_latest_metric(
    cluster_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取指定集群的最新监控指标
    """
    try:
        metric = ClusterMonitorService.get_latest_metric(db, cluster_id)
        if not metric:
            metric = ClusterMonitorService.collect_metric(db, cluster_id)
        return Result.success(200, "获取成功", ClusterMetricResponse.model_validate(metric))
    except Exception as e:
        logger.exception(f"获取集群监控指标失败: {e}")
        return Result.error(500, str(e))


@router.post("/monitor/cluster/{cluster_id}/collect", summary="采集集群监控指标")
async def collect_cluster_metric(
    cluster_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    手动采集指定集群的监控指标
    """
    try:
        metric = ClusterMonitorService.collect_metric(db, cluster_id)
        return Result.success(200, "采集成功", ClusterMetricResponse.model_validate(metric))
    except Exception as e:
        logger.exception(f"采集集群监控指标失败: {e}")
        return Result.error(500, str(e))


@router.get("/monitor/cluster/{cluster_id}/history", summary="获取集群监控历史")
async def get_cluster_metrics_history(
    cluster_id: int,
    limit: int = Query(100, ge=1, le=1000, description="返回数量"),
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取指定集群的监控历史数据
    """
    try:
        metrics = ClusterMonitorService.get_metrics_history(db, cluster_id, limit)
        return Result.success(200, "获取成功", {
            "metrics": [ClusterMetricResponse.model_validate(m) for m in metrics],
            "total": len(metrics)
        })
    except Exception as e:
        logger.exception(f"获取集群监控历史失败: {e}")
        return Result.error(500, str(e))


# ============================= 监控概览 API =============================

@router.get("/monitor/overview", summary="获取监控概览")
async def get_monitor_overview(
    datasource_id: Optional[int] = Query(None, description="数据源ID"),
    cluster_id: Optional[int] = Query(None, description="集群ID"),
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取监控概览（数据库和集群的最新监控数据）
    """
    try:
        overview = MonitorService.get_overview(db, datasource_id, cluster_id)
        return Result.success(200, "获取成功", overview)
    except Exception as e:
        logger.exception(f"获取监控概览失败: {e}")
        return Result.error(500, str(e))
