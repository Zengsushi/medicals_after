"""
    数据采集模块 Router
"""
import logging
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from apps.user.models import User
from apps.collection.models import (
    CollectionTaskStatus
)
from apps.collection.schemas import (
    CollectionSourceCreate,
    CollectionSourceUpdate,
    CollectionSourceResponse,
    CollectionTaskCreate,
    CollectionTaskUpdate,
    CollectionTaskResponse,
    CollectionExecutionResponse,
    CollectionLogResponse,
    CollectionTaskExecuteRequest,
    CollectionTaskStatusResponse
)
from apps.collection.service import (
    CollectionSourceService,
    CollectionTaskService,
    CollectionExecutionService,
    CollectionLogService
)
from apps.core.database import get_db
from apps.core import Result
from utils.auth_helpers import require_auth, get_client_info

router = APIRouter(prefix="/api", tags=["collection"])

logger = logging.getLogger(__name__)


# ========== 采集源接口 ==========

@router.get("/collection/sources", summary="获取采集源列表")
async def get_sources(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    source_type: Optional[str] = Query(None, description="采集源类型"),
    is_active: Optional[bool] = Query(None, description="是否启用"),
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """获取采集源列表"""
    skip = (page - 1) * page_size
    sources = CollectionSourceService.get_sources(
        db, skip=skip, limit=page_size,
        source_type=source_type, is_active=is_active
    )
    return Result.success(
        200,
        "采集源列表获取成功",
        {"sources": [CollectionSourceResponse.model_validate(s) for s in sources]}
    )


@router.get("/collection/sources/{source_id}", summary="获取采集源详情")
async def get_source(
    source_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """获取采集源详情"""
    source = CollectionSourceService.get_source(db, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="采集源不存在")
    return Result.success(
        200,
        "采集源详情获取成功",
        {"source": CollectionSourceResponse.model_validate(source)}
    )


@router.post("/collection/sources", summary="创建采集源")
async def create_source(
    source_data: CollectionSourceCreate,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """创建采集源"""
    try:
        source = CollectionSourceService.create_source(db, source_data)
        return Result.success(
            201,
            "采集源创建成功",
            {"source": CollectionSourceResponse.model_validate(source)}
        )
    except Exception as e:
        logger.error(f"创建采集源失败: {e}")
        return Result.error(500, str(e))


@router.put("/collection/sources/{source_id}", summary="更新采集源")
async def update_source(
    source_id: int,
    source_data: CollectionSourceUpdate,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """更新采集源"""
    source = CollectionSourceService.update_source(db, source_id, source_data)
    if not source:
        raise HTTPException(status_code=404, detail="采集源不存在")
    return Result.success(
        200,
        "采集源更新成功",
        {"source": CollectionSourceResponse.model_validate(source)}
    )


@router.delete("/collection/sources/{source_id}", summary="删除采集源")
async def delete_source(
    source_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """删除采集源"""
    success = CollectionSourceService.delete_source(db, source_id)
    if not success:
        raise HTTPException(status_code=404, detail="采集源不存在")
    return Result.success(200, "采集源删除成功", {})


@router.post("/collection/sources/{source_id}/toggle", summary="切换采集源状态")
async def toggle_source_status(
    source_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """切换采集源启用/停用状态"""
    source = CollectionSourceService.toggle_source_status(db, source_id)
    if not source:
        raise HTTPException(status_code=404, detail="采集源不存在")
    return Result.success(
        200,
        "采集源状态切换成功",
        {"source": CollectionSourceResponse.model_validate(source)}
    )


# ========== 采集任务接口 ==========

@router.get("/collection/tasks", summary="获取采集任务列表")
async def get_tasks(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    task_type: Optional[str] = Query(None, description="任务类型"),
    status: Optional[str] = Query(None, description="任务状态"),
    source_id: Optional[int] = Query(None, description="采集源ID"),
    is_active: Optional[bool] = Query(None, description="是否启用"),
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """获取采集任务列表"""
    skip = (page - 1) * page_size
    tasks = CollectionTaskService.get_tasks(
        db, skip=skip, limit=page_size,
        task_type=task_type, status=status,
        source_id=source_id, is_active=is_active
    )
    return Result.success(
        200,
        "采集任务列表获取成功",
        {"tasks": [CollectionTaskResponse.model_validate(t) for t in tasks]}
    )


@router.get("/collection/tasks/{task_id}", summary="获取采集任务详情")
async def get_task(
    task_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """获取采集任务详情"""
    task = CollectionTaskService.get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="采集任务不存在")
    return Result.success(
        200,
        "采集任务详情获取成功",
        {"task": CollectionTaskResponse.model_validate(task)}
    )


@router.post("/collection/tasks", summary="创建采集任务")
async def create_task(
    task_data: CollectionTaskCreate,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """创建采集任务"""
    try:
        task = CollectionTaskService.create_task(db, task_data, created_by=current_user.id)
        return Result.success(
            201,
            "采集任务创建成功",
            {"task": CollectionTaskResponse.model_validate(task)}
        )
    except Exception as e:
        logger.error(f"创建采集任务失败: {e}")
        return Result.error(500, str(e))


@router.put("/collection/tasks/{task_id}", summary="更新采集任务")
async def update_task(
    task_id: int,
    task_data: CollectionTaskUpdate,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """更新采集任务"""
    task = CollectionTaskService.update_task(db, task_id, task_data)
    if not task:
        raise HTTPException(status_code=404, detail="采集任务不存在")
    return Result.success(
        200,
        "采集任务更新成功",
        {"task": CollectionTaskResponse.model_validate(task)}
    )


@router.delete("/collection/tasks/{task_id}", summary="删除采集任务")
async def delete_task(
    task_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """删除采集任务"""
    success = CollectionTaskService.delete_task(db, task_id)
    if not success:
        raise HTTPException(status_code=404, detail="采集任务不存在")
    return Result.success(200, "采集任务删除成功", {})


@router.post("/collection/tasks/{task_id}/toggle", summary="切换采集任务状态")
async def toggle_task_status(
    task_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """切换采集任务启用/停用状态"""
    task = CollectionTaskService.toggle_task_status(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="采集任务不存在")
    return Result.success(
        200,
        "采集任务状态切换成功",
        {"task": CollectionTaskResponse.model_validate(task)}
    )


@router.post("/collection/tasks/{task_id}/execute", summary="手动执行采集任务")
async def execute_task(
    task_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """手动执行采集任务"""
    client_info = get_client_info()
    try:
        execution = CollectionTaskService.execute_task(
            db, task_id, triggered_by=current_user.id
        )
        if not execution:
            task = CollectionTaskService.get_task(db, task_id)
            if not task:
                raise HTTPException(status_code=404, detail="采集任务不存在")
            return Result.error(400, "任务已在运行中")
        
        return Result.success(
            200,
            "采集任务已开始执行",
            {"execution": CollectionExecutionResponse.model_validate(execution)}
        )
    except Exception as e:
        logger.error(f"执行采集任务失败: {e}")
        return Result.error(500, str(e))


@router.post("/collection/tasks/{task_id}/cancel", summary="取消正在运行的采集任务")
async def cancel_task(
    task_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """取消正在运行的采集任务"""
    task = CollectionTaskService.cancel_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="采集任务不存在或没有在运行")
    return Result.success(
        200,
        "采集任务已取消",
        {"task": CollectionTaskResponse.model_validate(task)}
    )


@router.get("/collection/tasks/{task_id}/status", summary="获取采集任务状态")
async def get_task_status(
    task_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """获取采集任务状态"""
    task = CollectionTaskService.get_task(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="采集任务不存在")
    
    # 获取最近的执行记录
    executions = CollectionExecutionService.get_executions(db, task_id=task_id, limit=1)
    current_execution = executions[0] if executions else None
    
    return Result.success(
        200,
        "任务状态获取成功",
        {
            "task_id": task.id,
            "status": task.status,
            "current_execution": CollectionExecutionResponse.model_validate(current_execution) if current_execution else None,
            "message": f"任务状态: {task.status}"
        }
    )


# ========== 采集执行记录接口 ==========

@router.get("/collection/executions", summary="获取采集执行记录列表")
async def get_executions(
    task_id: Optional[int] = Query(None, description="任务ID"),
    status: Optional[str] = Query(None, description="执行状态"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """获取采集执行记录列表"""
    skip = (page - 1) * page_size
    executions = CollectionExecutionService.get_executions(
        db, task_id=task_id, status=status,
        skip=skip, limit=page_size
    )
    return Result.success(
        200,
        "采集执行记录获取成功",
        {"executions": [CollectionExecutionResponse.model_validate(e) for e in executions]}
    )


@router.get("/collection/executions/{execution_id}", summary="获取采集执行详情")
async def get_execution(
    execution_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """获取采集执行详情"""
    execution = CollectionExecutionService.get_execution(db, execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="采集执行记录不存在")
    return Result.success(
        200,
        "采集执行详情获取成功",
        {"execution": CollectionExecutionResponse.model_validate(execution)}
    )


# ========== 采集日志接口 ==========

@router.get("/collection/logs", summary="获取采集日志列表")
async def get_logs(
    execution_id: Optional[int] = Query(None, description="执行ID"),
    level: Optional[str] = Query(None, description="日志级别"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(50, ge=1, le=100, description="每页数量"),
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """获取采集日志列表"""
    skip = (page - 1) * page_size
    logs = CollectionLogService.get_logs(
        db, execution_id=execution_id, level=level,
        skip=skip, limit=page_size
    )
    return Result.success(
        200,
        "采集日志获取成功",
        {"logs": [CollectionLogResponse.model_validate(l) for l in logs]}
    )


@router.get("/collection/statistics", summary="获取采集任务统计数据")
async def get_collection_statistics(
    db: Session = Depends(get_db)
):
    """获取采集任务统计数据"""
    try:
        statistics = CollectionTaskService.get_task_statistics(db)
        return Result.success(
            200,
            "采集任务统计数据获取成功",
            statistics
        )
    except Exception as e:
        logger.error(f"获取采集任务统计数据失败: {e}")
        return Result.error(500, str(e))
