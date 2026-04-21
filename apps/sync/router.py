"""
    数据同步模块路由
    支持从 Hive 到其他数据源的同步
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, BackgroundTasks
from sqlalchemy.orm import Session

from apps.core.database import get_db, SessionLocal
from apps.core import Result
from apps.user.models import User
from utils.auth_helpers import require_auth

from apps.sync.models import SyncTask, SyncTaskStatus
from apps.sync.schemas import (
    SyncTaskCreate, SyncTaskUpdate, SyncTaskResponse,
    SyncLogResponse, SyncTestRequest, SyncExecuteRequest,
    SyncTablePreviewRequest,
    SyncPreviewRequest,
    SyncExecuteNowRequest
)
from apps.sync.service import SyncService
from apps.datasource.service import DataSourceService

router = APIRouter(prefix="/api/sync", tags=["sync"])
logger = logging.getLogger(__name__)


def run_sync_task_background(task_id: int):
    """
    后台任务使用独立数据库会话，避免复用请求生命周期中的 Session 导致连接失效。
    """
    db = SessionLocal()
    try:
        SyncService.execute_sync(db, task_id)
    except Exception:
        logger.exception("后台执行同步任务失败")
    finally:
        db.close()


@router.get("/tasks", summary="获取同步任务列表")
async def get_sync_tasks(
        page: Optional[int] = Query(None, ge=1, description="页码(兼容前端)"),
        pageSize: Optional[int] = Query(None, ge=1, le=500, description="每页数量(兼容前端)"),
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=500),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取同步任务列表"""
    if page and pageSize:
        skip = (page - 1) * pageSize
        limit = pageSize
    tasks = SyncService.get_sync_tasks(db, skip=skip, limit=limit)
    total = db.query(SyncTask).count()
    return Result.success(200, "获取成功", data={
        "list": [SyncTaskResponse.model_validate(task) for task in tasks],
        "total": total,
        "page": page or (skip // limit + 1),
        "pageSize": limit
    })


@router.get("/tasks/{task_id}", summary="获取同步任务详情")
async def get_sync_task(
        task_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取同步任务详情"""
    task = SyncService.get_sync_task(db, task_id)
    if not task:
        return Result.error(404, "同步任务不存在")
    return Result.success(200, "获取成功", data=SyncTaskResponse.model_validate(task))


@router.post("/tasks", summary="创建同步任务")
async def create_sync_task(
        task_data: SyncTaskCreate,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """创建同步任务"""
    try:
        task = SyncService.create_sync_task(db, task_data)
        return Result.success(200, "创建成功", data=SyncTaskResponse.model_validate(task))
    except Exception as e:
        logger.exception("创建同步任务失败")
        return Result.error(500, str(e))


@router.patch("/tasks/{task_id}", summary="更新同步任务")
async def update_sync_task(
        task_id: int,
        task_data: SyncTaskUpdate,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """更新同步任务"""
    task = SyncService.update_sync_task(db, task_id, task_data)
    if not task:
        return Result.error(404, "同步任务不存在")
    return Result.success(200, "更新成功", data=SyncTaskResponse.model_validate(task))


@router.delete("/tasks/{task_id}", summary="删除同步任务")
async def delete_sync_task(
        task_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """删除同步任务"""
    success = SyncService.delete_sync_task(db, task_id)
    if not success:
        return Result.error(404, "同步任务不存在")
    return Result.success(200, "删除成功")


@router.get("/tasks/{task_id}/logs", summary="获取同步任务日志")
async def get_sync_logs(
        task_id: int,
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=500),
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """获取同步任务日志"""
    logs = SyncService.get_sync_logs(db, task_id, skip=skip, limit=limit)
    return Result.success(200, "获取成功", data=[
        SyncLogResponse.model_validate(log) for log in logs
    ])


@router.post("/tasks/test", summary="测试同步连接")
async def test_sync(
        test_data: SyncTestRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """测试同步连接，预览数据"""
    try:
        result = SyncService.test_sync(
            db,
            test_data.source_id,
            test_data.target_id,
            test_data.source_table,
            test_data.target_table,
            test_data.limit
        )
        return Result.success(200, "测试成功", data=result)
    except Exception as e:
        logger.exception("测试同步失败")
        return Result.error(500, str(e))


@router.post("/tasks/{task_id}/execute", summary="执行同步任务")
async def execute_sync_task(
        task_id: int,
        background_tasks: BackgroundTasks,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """执行同步任务（异步执行）"""
    task = db.query(SyncTask).filter(SyncTask.id == task_id).first()
    if not task:
        return Result.error(404, "同步任务不存在")

    if task.status == "running":
        return Result.error(400, "任务正在运行中")

    background_tasks.add_task(run_sync_task_background, task_id)

    return Result.success(200, "任务已启动，请稍后查看状态")


@router.post("/preview", summary="预览同步数据(前端页面用)")
async def preview_sync(
        body: SyncPreviewRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    预览同步数据：从源表取 sample，返回列信息和样例数据
    """
    try:
        source_table = f"{body.source_database}.{body.source_table}"
        target_table = f"{body.target_database}.{body.target_table}"
        result = SyncService.test_sync(
            db=db,
            source_id=body.source_id,
            target_id=body.target_id,
            source_table=source_table,
            target_table=target_table,
            limit=body.limit,
        )
        return Result.success(200, "预览成功", data=result)
    except Exception as e:
        logger.exception("预览同步失败")
        return Result.error(500, str(e))


@router.post("/execute", summary="立即执行同步(前端页面用)")
async def execute_sync_now(
        body: SyncExecuteNowRequest,
        background_tasks: BackgroundTasks,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """
    立即创建一个同步任务并异步执行，返回 task_id
    """
    try:
        source_table = f"{body.source_database}.{body.source_table}"
        target_table = f"{body.target_database}.{body.target_table}"
        name = f"{body.source_database}.{body.source_table} -> {body.target_database}.{body.target_table}"

        # 复用同配置任务，避免每次采集都新建任务或被误认为“执行后删除”
        task = db.query(SyncTask).filter(
            SyncTask.source_id == body.source_id,
            SyncTask.target_id == body.target_id,
            SyncTask.source_table == source_table,
            SyncTask.target_table == target_table
        ).order_by(SyncTask.id.desc()).first()

        if task and task.status == SyncTaskStatus.RUNNING:
            return Result.error(400, "该同步任务正在运行中，请稍后再试")

        if task:
            task.name = name
            task.sync_mode = body.sync_mode
            task.column_mapping = body.column_mapping
            task.batch_size = 1000
            task.status = SyncTaskStatus.PENDING
            task.progress = 0
            task.row_count = 0
            task.error_message = None
            task.started_at = None
            task.completed_at = None
            db.commit()
            db.refresh(task)
            SyncService.add_log(db, task.id, "INFO", "复用已有任务并重新执行")
        else:
            task = SyncService.create_sync_task(db, SyncTaskCreate(
                name=name,
                source_id=body.source_id,
                target_id=body.target_id,
                source_table=source_table,
                target_table=target_table,
                sync_mode=body.sync_mode,
                column_mapping=body.column_mapping,
                batch_size=1000,
            ))

        background_tasks.add_task(run_sync_task_background, task.id)
        return Result.success(200, "任务已创建并开始执行", data={"task_id": task.id})
    except Exception as e:
        logger.exception("立即执行同步失败")
        return Result.error(500, str(e))


@router.post("/tasks/{task_id}/cancel", summary="取消同步任务(前端页面用)")
async def cancel_sync_task(
        task_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    try:
        task = SyncService.get_sync_task(db, task_id)
        if not task:
            return Result.error(404, "同步任务不存在")
        if task.status == "running":
            task.status = "cancelled"
            db.commit()
            SyncService.add_log(db, task_id, "INFO", "任务已标记为取消")
        return Result.success(200, "取消成功", data={"task_id": task_id})
    except Exception as e:
        logger.exception("取消同步任务失败")
        return Result.error(500, str(e))


@router.post("/tasks/{task_id}/retry", summary="重试同步任务(前端页面用)")
async def retry_sync_task(
        task_id: int,
        background_tasks: BackgroundTasks,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    try:
        task = SyncService.get_sync_task(db, task_id)
        if not task:
            return Result.error(404, "同步任务不存在")
        task.status = "pending"
        task.progress = 0
        task.error_message = None
        db.commit()
        SyncService.add_log(db, task_id, "INFO", "任务已重试（重新开始执行）")
        background_tasks.add_task(run_sync_task_background, task_id)
        return Result.success(200, "重试已启动", data={"task_id": task_id})
    except Exception as e:
        logger.exception("重试同步任务失败")
        return Result.error(500, str(e))


@router.get("/sources/{data_source_id}/databases", summary="获取数据源数据库列表(兼容 syncAPI)")
async def sync_get_databases(
        data_source_id: int,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    try:
        result = DataSourceService.get_databases(db, data_source_id)
        if not result.get("success", False):
            return Result.error(400, result.get("message", "获取数据库列表失败"), data=result)
        return Result.success(200, "获取成功", data=result)
    except Exception as e:
        logger.exception("获取数据库列表失败")
        return Result.error(500, str(e))


@router.get("/sources/{data_source_id}/databases/{database}/tables", summary="获取数据库表列表(兼容 syncAPI)")
async def sync_get_tables(
        data_source_id: int,
        database: str,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    try:
        result = DataSourceService.get_tables(db, data_source_id, database)
        if not result.get("success", False):
            return Result.error(400, result.get("message", "获取表列表失败"), data=result)
        return Result.success(200, "获取成功", data=result)
    except Exception as e:
        logger.exception("获取表列表失败")
        return Result.error(500, str(e))


@router.get("/sources/{data_source_id}/databases/{database}/tables/{table}/columns",
            summary="获取表列信息(兼容 syncAPI)")
async def sync_get_columns(
        data_source_id: int,
        database: str,
        table: str,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    try:
        result = DataSourceService.get_table_structure(db, data_source_id, database, table)
        if not result.get("success", False):
            return Result.error(400, result.get("message", "获取表结构失败"), data=result)
        return Result.success(200, "获取成功", data=result)
    except Exception as e:
        logger.exception("获取表结构失败")
        return Result.error(500, str(e))


@router.post("/table/preview", summary="预览表数据")
async def preview_table(
        request: SyncTablePreviewRequest,
        current_user: User = Depends(require_auth()),
        db: Session = Depends(get_db)
):
    """预览表数据"""
    try:
        result = SyncService.preview_table_data(
            db,
            request.datasource_id,
            request.table_name,
            request.limit
        )
        return Result.success(200, "获取成功", data=result)
    except Exception as e:
        logger.exception("预览表数据失败")
        return Result.error(500, str(e))
