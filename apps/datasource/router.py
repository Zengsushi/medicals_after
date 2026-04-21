import logging
from typing import Optional
from fastapi import APIRouter, Depends, Query, Request, HTTPException
from sqlalchemy.orm import Session

from apps.user.models import User
from apps.core.database import get_db
from apps.core import Result
from utils.auth_helpers import require_auth
from apps.datasource.models import DataSource, DataSourceType, DataSourceCategory
from apps.datasource.schemas import (
    DataSourceCreate,
    DataSourceUpdate,
    DataSourceResponse,
    ConnectionTestRequest,
    ConnectionTestResponse,
    DataSourceTypeResponse,
    DataSourceCategoryResponse,
    DataSourceBatchCreate,
    DataSourceBatchUpdate,
    DataSourceBatchDelete,
    BatchOperationResponse
)
from apps.datasource.service import DataSourceService

router = APIRouter(prefix="/api", tags=["datasources"])
logger = logging.getLogger(__name__)


@router.get("/datasources/categories", summary="获取数据源分类列表")
async def get_datasource_categories(
        current_user: User = Depends(require_auth())
):
    """
    获取所有可用的数据源分类
    """
    try:
        categories = DataSourceCategory.get_all_categories()
        return Result.success(200, "获取成功", {
            "categories": categories
        })
    except Exception as e:
        logger.exception(f"获取数据源分类失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/types", summary="获取数据源类型列表")
async def get_datasource_types(
        category: Optional[str] = Query(None, description="数据源分类"),
        current_user: User = Depends(require_auth())
):
    """
    获取所有可用的数据源类型，或按分类获取
    """
    try:
        if category:
            types = DataSourceCategory.get_types_by_category(category)
        else:
            types = DataSourceType.get_all_types()
        return Result.success(200, "获取成功", {
            "types": types
        })
    except Exception as e:
        logger.exception(f"获取数据源类型失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources", summary="获取数据源列表")
async def list_datasources(
        # 前端分页兼容：page/pageSize
        page: Optional[int] = Query(None, ge=1, description="页码(兼容前端)"),
        pageSize: Optional[int] = Query(None, ge=1, le=1000, description="每页数量(兼容前端)"),
        skip: int = Query(0, ge=0, description="跳过数量"),
        limit: int = Query(100, ge=1, le=1000, description="返回数量"),
        source_type: Optional[str] = Query(
            None,
            alias="type",
            description="数据源类型: mysql/postgresql/hive/oracle/sqlserver",
        ),
        keyword: Optional[str] = Query(None, description="名称/主机/库名模糊搜索"),
        status: Optional[str] = Query(None, description="连接状态: online / offline"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取所有数据源列表
    """
    try:
        if page and pageSize:
            skip = (page - 1) * pageSize
            limit = pageSize

        connected = None
        if status:
            s = status.strip().lower()
            if s == "online":
                connected = True
            elif s == "offline":
                connected = False

        datasources, total = DataSourceService.list_all(
            db,
            skip=skip,
            limit=limit,
            type_filter=source_type,
            keyword=keyword,
            connected=connected,
        )
        return Result.success(200, "获取成功", {
            "datasources": [DataSourceResponse.model_validate(ds) for ds in datasources],
            "total": total,
        })
    except Exception as e:
        logger.exception(f"获取数据源列表失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/stats", summary="获取数据源统计")
async def get_datasource_stats(
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取数据源统计数据（用于卡片展示）
    """
    try:
        datasources, _total_all = DataSourceService.list_all(db, skip=0, limit=1000)

        total = len(datasources)
        online = sum(1 for ds in datasources if ds.is_connected == 1)
        offline = total - online
        default_count = sum(1 for ds in datasources if ds.is_default == 1)

        # 计算平均延迟（如果有）
        latencies = [ds.latency for ds in datasources if ds.latency]
        avg_latency = sum(latencies) / len(latencies) if latencies else None

        return Result.success(200, "获取成功", {
            "total": total,
            "online": online,
            "offline": offline,
            "default": default_count,
            "avgLatency": avg_latency
        })
    except Exception as e:
        logger.exception(f"获取数据源统计失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/{data_source_id:int}", summary="获取数据源详情")
async def get_datasource_detail(
        data_source_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取数据源详情
    """
    try:
        datasource = DataSourceService.get_by_id(db, data_source_id)
        if not datasource:
            return Result.error(404, "数据源不存在")
        return Result.success(200, "获取成功", DataSourceResponse.model_validate(datasource))
    except Exception as e:
        logger.exception(f"获取数据源详情失败: {e}")
        return Result.error(500, str(e))


@router.post("/datasources", summary="创建数据源")
async def create_datasource(
        data: DataSourceCreate,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    创建新的数据源
    """
    try:
        existing = DataSourceService.get_by_name(db, data.name)
        if existing:
            return Result.error(400, "数据源名称已存在")

        datasource = DataSourceService.create(db, data)
        return Result.success(200, "创建成功", DataSourceResponse.model_validate(datasource))
    except Exception as e:
        logger.exception(f"创建数据源失败: {e}")
        return Result.error(500, str(e))


@router.patch("/datasources/{data_source_id:int}", summary="更新数据源")
async def update_datasource(
        data_source_id: int,
        data: DataSourceUpdate,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    更新数据源
    """
    try:
        datasource = DataSourceService.update(db, data_source_id, data)
        if not datasource:
            return Result.error(404, "数据源不存在")
        return Result.success(200, "更新成功", DataSourceResponse.model_validate(datasource))
    except Exception as e:
        logger.exception(f"更新数据源失败: {e}")
        return Result.error(500, str(e))


@router.delete("/datasources/{data_source_id:int}", summary="删除数据源")
async def delete_datasource(
        data_source_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    删除数据源(软删除)
    """
    try:
        success = DataSourceService.delete(db, data_source_id)
        if not success:
            return Result.error(404, "数据源不存在")
        return Result.success(200, "删除成功")
    except Exception as e:
        logger.exception(f"删除数据源失败: {e}")
        return Result.error(500, str(e))


@router.post("/datasources/test-connection", summary="测试数据源连接")
async def test_connection(
        request: ConnectionTestRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    测试数据源连接
    """
    try:
        result = DataSourceService.test_connection(db, request)

        if request.id and result.success:
            latency_ms = int(result.latency * 1000) if result.latency else None
            DataSourceService.update_connection_status(db, request.id, True, latency_ms=latency_ms)

        return Result.success(200, "测试完成", {
            "success": result.success,
            "message": result.message,
            "latency": result.latency,
            "details": result.details,
            "databases": (result.details or {}).get("databases", [])
        })
    except Exception as e:
        logger.exception(f"测试连接失败: {e}")
        return Result.error(500, str(e))


@router.post("/datasources/hive/databases", summary="直接获取 Hive 数据库列表")
async def get_hive_databases(
        request: ConnectionTestRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    直接通过 Spark 执行 show databases
    """
    try:
        from apps.analyse.config import init_spark_connect_hive
        spark = init_spark_connect_hive()
        df = spark.sql("show databases")
        # 处理 DataFrame 返回，转换为可序列化结构
        databases = [
            (getattr(row, "databaseName", None) or getattr(row, "namespace", None) or row[0])
            for row in df.collect()
        ]
        spark.stop()
        return Result.success(200, "获取成功", {"databases": databases})
    except Exception as e:
        logger.exception(f"获取 Hive 数据库列表失败: {e}")
        return Result.error(500, str(e))


@router.patch("/datasources/{data_source_id:int}/set-default", summary="设置默认数据源")
async def set_default_datasource(
        data_source_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    try:
        ds = DataSourceService.set_default(db, data_source_id)
        if not ds:
            return Result.error(404, "数据源不存在")
        return Result.success(200, "设置成功", DataSourceResponse.model_validate(ds))
    except Exception as e:
        logger.exception(f"设置默认数据源失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/default", summary="获取默认数据源")
async def get_default_datasource(
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    try:
        ds = DataSourceService.get_default(db)
        if not ds:
            return Result.success(200, "暂无默认数据源", data=None)
        return Result.success(200, "获取成功", DataSourceResponse.model_validate(ds))
    except Exception as e:
        logger.exception(f"获取默认数据源失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/{data_source_id:int}/health", summary="检查单个数据源健康状态")
async def check_datasource_health(
        data_source_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    检查单个数据源的健康状态
    """
    try:
        result = DataSourceService.check_health(db, data_source_id)
        return Result.success(200, "检查完成", result)
    except Exception as e:
        logger.exception(f"检查健康状态失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/health", summary="检查所有数据源健康状态")
async def check_all_datasources_health(
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    检查所有启用的数据源健康状态
    """
    try:
        results = DataSourceService.check_all_health(db)
        return Result.success(200, "检查完成", {
            "results": results,
            "total": len(results),
            "healthy_count": sum(1 for r in results if r.get("status") == "healthy"),
            "unhealthy_count": sum(1 for r in results if r.get("status") == "unhealthy"),
            "inactive_count": sum(1 for r in results if r.get("status") == "inactive")
        })
    except Exception as e:
        logger.exception(f"检查所有数据源健康状态失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/{data_source_id:int}/databases", summary="获取数据源的数据库列表")
async def get_datasource_databases(
        data_source_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取数据源的数据库列表
    """
    try:
        datasource = DataSourceService.get_by_id(db, data_source_id)
        if not datasource:
            return Result.error(404, "数据源不存在")

        # Hive 统一走简化链路：init_spark_connect_hive + show databases
        if str(datasource.type).strip().lower() == "hive":
            from apps.analyse.config import init_spark_connect_hive
            spark = init_spark_connect_hive()
            df = spark.sql("show databases")
            databases = [
                (getattr(row, "databaseName", None) or getattr(row, "namespace", None) or row[0])
                for row in df.collect()
            ]
            spark.stop()
            return Result.success(200, "获取成功", {
                "success": True,
                "message": "获取数据库列表成功",
                "databases": databases
            })

        result = DataSourceService.get_databases(db, data_source_id)
        if not result.get("success", False):
            return Result.error(400, result.get("message", "获取数据库列表失败"), data=result)
        return Result.success(200, "获取成功", result)
    except Exception as e:
        logger.exception(f"获取数据库列表失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/{data_source_id:int}/tables", summary="获取指定数据库的表列表")
async def get_datasource_tables(
        data_source_id: int,
        database: str = Query(..., description="数据库名称"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取指定数据库的表列表
    """
    try:
        result = DataSourceService.get_tables(db, data_source_id, database)
        if not result.get("success", False):
            return Result.error(400, result.get("message", "获取表列表失败"), data=result)
        return Result.success(200, "获取成功", result)
    except Exception as e:
        logger.exception(f"获取表列表失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/{data_source_id:int}/table-structure", summary="获取指定表的结构")
async def get_table_structure(
        data_source_id: int,
        database: str = Query(..., description="数据库名称"),
        table: str = Query(..., description="表名称"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取指定表的结构
    """
    try:
        result = DataSourceService.get_table_structure(db, data_source_id, database, table)
        if not result.get("success", False):
            return Result.error(400, result.get("message", "获取表结构失败"), data=result)
        return Result.success(200, "获取成功", result)
    except Exception as e:
        logger.exception(f"获取表结构失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/{data_source_id:int}/usage-statistics", summary="获取数据源使用统计")
async def get_datasource_usage_statistics(
        data_source_id: int,
        days: int = Query(7, ge=1, le=30, description="统计天数"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取数据源使用统计
    """
    try:
        result = DataSourceService.get_usage_statistics(db, data_source_id, days)
        return Result.success(200, "获取成功", result)
    except Exception as e:
        logger.exception(f"获取使用统计失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/usage-statistics", summary="获取所有数据源使用统计")
async def get_all_datasources_usage_statistics(
        days: int = Query(7, ge=1, le=30, description="统计天数"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取所有数据源使用统计
    """
    try:
        result = DataSourceService.get_usage_statistics(db, days=days)
        return Result.success(200, "获取成功", result)
    except Exception as e:
        logger.exception(f"获取使用统计失败: {e}")
        return Result.error(500, str(e))


@router.get("/datasources/{data_source_id:int}/usage-history", summary="获取数据源使用历史")
async def get_datasource_usage_history(
        data_source_id: int,
        skip: int = Query(0, ge=0, description="跳过数量"),
        limit: int = Query(50, ge=1, le=100, description="返回数量"),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    获取数据源使用历史
    """
    try:
        result = DataSourceService.get_usage_history(db, data_source_id, skip, limit)
        return Result.success(200, "获取成功", {
            "history": result,
            "total": len(result)
        })
    except Exception as e:
        logger.exception(f"获取使用历史失败: {e}")
        return Result.error(500, str(e))


@router.post("/datasources/batch/create", summary="批量创建数据源")
async def batch_create_datasources(
        request: DataSourceBatchCreate,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    批量创建数据源
    """
    try:
        result = DataSourceService.batch_create(db, request.datasources)
        return Result.success(200, "批量创建完成", result)
    except Exception as e:
        logger.exception(f"批量创建数据源失败: {e}")
        return Result.error(500, str(e))


@router.post("/datasources/batch/update", summary="批量更新数据源")
async def batch_update_datasources(
        request: DataSourceBatchUpdate,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    批量更新数据源
    """
    try:
        result = DataSourceService.batch_update(db, request.updates)
        return Result.success(200, "批量更新完成", result)
    except Exception as e:
        logger.exception(f"批量更新数据源失败: {e}")
        return Result.error(500, str(e))


@router.post("/datasources/batch/delete", summary="批量删除数据源")
async def batch_delete_datasources(
        request: DataSourceBatchDelete,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    批量删除数据源
    """
    try:
        result = DataSourceService.batch_delete(db, request.ids)
        return Result.success(200, "批量删除完成", result)
    except Exception as e:
        logger.exception(f"批量删除数据源失败: {e}")
        return Result.error(500, str(e))
