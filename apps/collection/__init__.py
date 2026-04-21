"""
    数据采集模块
    支持：采集源管理、采集任务、执行记录、日志等
"""

from apps.collection.models import (
    CollectionSource,
    CollectionTask,
    CollectionExecution,
    CollectionLog,
    CollectionTaskStatus,
    CollectionTaskType,
    CollectionSourceType
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
from apps.collection.router import router

__all__ = [
    "CollectionSource",
    "CollectionTask",
    "CollectionExecution",
    "CollectionLog",
    "CollectionTaskStatus",
    "CollectionTaskType",
    "CollectionSourceType",
    "CollectionSourceCreate",
    "CollectionSourceUpdate",
    "CollectionSourceResponse",
    "CollectionTaskCreate",
    "CollectionTaskUpdate",
    "CollectionTaskResponse",
    "CollectionExecutionResponse",
    "CollectionLogResponse",
    "CollectionTaskExecuteRequest",
    "CollectionTaskStatusResponse",
    "CollectionSourceService",
    "CollectionTaskService",
    "CollectionExecutionService",
    "CollectionLogService",
    "router"
]
