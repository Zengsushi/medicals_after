"""
    数据采集模块 Service
"""
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import desc, and_

from apps.collection.models import (
    CollectionSource,
    CollectionTask,
    CollectionExecution,
    CollectionLog,
    CollectionTaskStatus,
    CollectionTaskType
)
from apps.collection.schemas import (
    CollectionSourceCreate,
    CollectionSourceUpdate,
    CollectionTaskCreate,
    CollectionTaskUpdate
)
from apps.datasource.models import DataSource
from apps.user.models import User

logger = logging.getLogger(__name__)


class CollectionSourceService:
    """采集源服务"""

    @staticmethod
    def get_source(db: Session, source_id: int) -> Optional[CollectionSource]:
        """获取单个采集源"""
        return db.query(CollectionSource).filter(
            CollectionSource.id == source_id
        ).first()

    @staticmethod
    def get_sources(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        source_type: Optional[str] = None,
        is_active: Optional[bool] = None
    ) -> List[CollectionSource]:
        """获取采集源列表"""
        query = db.query(CollectionSource)
        
        if source_type:
            query = query.filter(CollectionSource.type == source_type)
        
        if is_active is not None:
            query = query.filter(CollectionSource.is_active == is_active)
        
        return query.order_by(desc(CollectionSource.created_at)).offset(skip).limit(limit).all()

    @staticmethod
    def create_source(
        db: Session,
        source_data: CollectionSourceCreate
    ) -> CollectionSource:
        """创建采集源"""
        source = CollectionSource(**source_data.model_dump())
        db.add(source)
        db.commit()
        db.refresh(source)
        logger.info(f"创建采集源: {source.name} (ID: {source.id})")
        return source

    @staticmethod
    def update_source(
        db: Session,
        source_id: int,
        source_data: CollectionSourceUpdate
    ) -> Optional[CollectionSource]:
        """更新采集源"""
        source = CollectionSourceService.get_source(db, source_id)
        if not source:
            return None
        
        update_data = source_data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(source, key, value)
        
        db.commit()
        db.refresh(source)
        logger.info(f"更新采集源: {source.name} (ID: {source.id})")
        return source

    @staticmethod
    def delete_source(db: Session, source_id: int) -> bool:
        """删除采集源"""
        source = CollectionSourceService.get_source(db, source_id)
        if not source:
            return False
        
        db.delete(source)
        db.commit()
        logger.info(f"删除采集源: {source.name} (ID: {source.id})")
        return True

    @staticmethod
    def toggle_source_status(db: Session, source_id: int) -> Optional[CollectionSource]:
        """切换采集源状态"""
        source = CollectionSourceService.get_source(db, source_id)
        if not source:
            return None
        
        source.is_active = not source.is_active
        db.commit()
        db.refresh(source)
        return source


class CollectionTaskService:
    """采集任务服务"""

    @staticmethod
    def get_task(db: Session, task_id: int) -> Optional[CollectionTask]:
        """获取单个采集任务"""
        return db.query(CollectionTask).options(
            selectinload(CollectionTask.source),
            selectinload(CollectionTask.target_datasource),
            selectinload(CollectionTask.creator)
        ).filter(CollectionTask.id == task_id).first()

    @staticmethod
    def get_tasks(
        db: Session,
        skip: int = 0,
        limit: int = 100,
        task_type: Optional[str] = None,
        status: Optional[str] = None,
        source_id: Optional[int] = None,
        is_active: Optional[bool] = None
    ) -> List[CollectionTask]:
        """获取采集任务列表"""
        query = db.query(CollectionTask).options(
            selectinload(CollectionTask.source)
        )
        
        if task_type:
            query = query.filter(CollectionTask.type == task_type)
        
        if status:
            query = query.filter(CollectionTask.status == status)
        
        if source_id:
            query = query.filter(CollectionTask.source_id == source_id)
        
        if is_active is not None:
            query = query.filter(CollectionTask.is_active == is_active)
        
        return query.order_by(desc(CollectionTask.created_at)).offset(skip).limit(limit).all()

    @staticmethod
    def create_task(
        db: Session,
        task_data: CollectionTaskCreate,
        created_by: Optional[int] = None
    ) -> CollectionTask:
        """创建采集任务"""
        task_dict = task_data.model_dump()
        task_dict["created_by"] = created_by
        task = CollectionTask(**task_dict)
        db.add(task)
        db.commit()
        db.refresh(task)
        logger.info(f"创建采集任务: {task.name} (ID: {task.id})")
        return task

    @staticmethod
    def update_task(
        db: Session,
        task_id: int,
        task_data: CollectionTaskUpdate
    ) -> Optional[CollectionTask]:
        """更新采集任务"""
        task = CollectionTaskService.get_task(db, task_id)
        if not task:
            return None
        
        update_data = task_data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(task, key, value)
        
        db.commit()
        db.refresh(task)
        logger.info(f"更新采集任务: {task.name} (ID: {task.id})")
        return task

    @staticmethod
    def delete_task(db: Session, task_id: int) -> bool:
        """删除采集任务"""
        task = CollectionTaskService.get_task(db, task_id)
        if not task:
            return False
        
        db.delete(task)
        db.commit()
        logger.info(f"删除采集任务: {task.name} (ID: {task.id})")
        return True

    @staticmethod
    def toggle_task_status(db: Session, task_id: int) -> Optional[CollectionTask]:
        """切换采集任务状态"""
        task = CollectionTaskService.get_task(db, task_id)
        if not task:
            return None
        
        task.is_active = not task.is_active
        db.commit()
        db.refresh(task)
        return task

    @staticmethod
    def execute_task(
        db: Session,
        task_id: int,
        triggered_by: Optional[int] = None
    ) -> Optional[CollectionExecution]:
        """执行采集任务"""
        task = CollectionTaskService.get_task(db, task_id)
        if not task:
            return None
        
        if task.status == CollectionTaskStatus.RUNNING:
            logger.warning(f"任务 {task.id} 已经在运行中")
            return None
        
        # 创建执行记录
        execution = CollectionExecution(
            task_id=task_id,
            status=CollectionTaskStatus.RUNNING,
            start_time=datetime.utcnow(),
            triggered_by=triggered_by,
            config_snapshot={
                "query": task.query,
                "batch_size": task.batch_size,
                "incremental_field": task.incremental_field,
                "last_sync_value": task.last_sync_value
            }
        )
        db.add(execution)
        
        # 更新任务状态
        task.status = CollectionTaskStatus.RUNNING
        task.last_run_at = datetime.utcnow()
        
        db.commit()
        db.refresh(execution)
        db.refresh(task)
        
        logger.info(f"开始执行采集任务: {task.name} (ID: {task.id}, Execution ID: {execution.id})")
        
        # 这里可以调用实际的采集执行逻辑
        # 为了演示，我们模拟执行完成
        try:
            CollectionTaskService._simulate_execution(db, task, execution)
        except Exception as e:
            CollectionTaskService._handle_execution_error(db, task, execution, str(e))
        
        return execution

    @staticmethod
    def _simulate_execution(
        db: Session,
        task: CollectionTask,
        execution: CollectionExecution
    ):
        """模拟采集执行（实际项目中这里会调用真实的采集逻辑）"""
        import time
        
        # 模拟执行时间
        time.sleep(2)
        
        # 模拟采集记录
        total_records = 1234
        success_records = 1230
        failed_records = 4
        
        # 更新执行记录
        execution.status = CollectionTaskStatus.COMPLETED
        execution.end_time = datetime.utcnow()
        execution.duration = int((execution.end_time - execution.start_time).total_seconds())
        execution.total_records = total_records
        execution.success_records = success_records
        execution.failed_records = failed_records
        
        # 更新任务
        task.status = CollectionTaskStatus.COMPLETED
        task.total_records += total_records
        task.success_records += success_records
        task.failed_records += failed_records
        task.duration = execution.duration
        
        # 添加成功日志
        CollectionTaskService._add_log(
            db, execution.id, "INFO",
            f"采集任务完成: {task.name}, 成功 {success_records} 条, 失败 {failed_records} 条",
            success_records
        )
        
        db.commit()
        logger.info(f"采集任务完成: {task.name} (ID: {task.id})")

    @staticmethod
    def _handle_execution_error(
        db: Session,
        task: CollectionTask,
        execution: CollectionExecution,
        error_message: str
    ):
        """处理执行错误"""
        import traceback
        
        execution.status = CollectionTaskStatus.FAILED
        execution.end_time = datetime.utcnow()
        execution.duration = int((execution.end_time - execution.start_time).total_seconds())
        execution.error_message = error_message
        execution.error_stacktrace = traceback.format_exc()
        
        task.status = CollectionTaskStatus.FAILED
        task.error_message = error_message
        
        # 添加错误日志
        CollectionTaskService._add_log(
            db, execution.id, "ERROR",
            f"采集任务失败: {task.name}, 错误: {error_message}",
            0
        )
        
        db.commit()
        logger.error(f"采集任务失败: {task.name} (ID: {task.id}), 错误: {error_message}")

    @staticmethod
    def _add_log(
        db: Session,
        execution_id: int,
        level: str,
        message: str,
        record_count: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """添加采集日志"""
        log = CollectionLog(
            execution_id=execution_id,
            level=level,
            message=message,
            record_count=record_count,
            details=details
        )
        db.add(log)

    @staticmethod
    def cancel_task(db: Session, task_id: int) -> Optional[CollectionTask]:
        """取消正在运行的任务"""
        task = CollectionTaskService.get_task(db, task_id)
        if not task or task.status != CollectionTaskStatus.RUNNING:
            return None
        
        task.status = CollectionTaskStatus.CANCELLED
        db.commit()
        db.refresh(task)
        logger.info(f"取消采集任务: {task.name} (ID: {task.id})")
        return task

    @staticmethod
    def get_task_statistics(db: Session) -> Dict[str, Any]:
        """获取采集任务统计数据"""
        total_tasks = db.query(CollectionTask).count()
        running_tasks = db.query(CollectionTask).filter(CollectionTask.status == CollectionTaskStatus.RUNNING).count()
        success_tasks = db.query(CollectionTask).filter(CollectionTask.status == CollectionTaskStatus.COMPLETED).count()
        failed_tasks = db.query(CollectionTask).filter(CollectionTask.status == CollectionTaskStatus.FAILED).count()
        
        # 计算成功率
        success_rate = 0
        if total_tasks > 0:
            success_rate = round((success_tasks / total_tasks) * 100, 2)
        
        # 计算平均执行时间
        import sqlalchemy as sa
        avg_duration_query = db.query(sa.func.avg(CollectionTask.duration)).filter(CollectionTask.duration > 0).scalar()
        avg_duration = round(avg_duration_query or 0, 2)
        
        # 最近7天的任务执行趋势
        from datetime import datetime, timedelta
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        daily_stats = []
        
        for i in range(7):
            date = seven_days_ago + timedelta(days=i)
            start_of_day = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = date.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            day_executions = db.query(CollectionExecution).filter(
                and_(
                    CollectionExecution.start_time >= start_of_day,
                    CollectionExecution.start_time <= end_of_day
                )
            ).count()
            
            daily_stats.append({
                "date": start_of_day.strftime("%m-%d"),
                "count": day_executions
            })
        
        return {
            "total_tasks": total_tasks,
            "running_tasks": running_tasks,
            "success_tasks": success_tasks,
            "failed_tasks": failed_tasks,
            "success_rate": success_rate,
            "avg_duration": avg_duration,
            "daily_stats": daily_stats
        }


class CollectionExecutionService:
    """采集执行服务"""

    @staticmethod
    def get_execution(db: Session, execution_id: int) -> Optional[CollectionExecution]:
        """获取单个执行记录"""
        return db.query(CollectionExecution).options(
            selectinload(CollectionExecution.task),
            selectinload(CollectionExecution.trigger_user)
        ).filter(CollectionExecution.id == execution_id).first()

    @staticmethod
    def get_executions(
        db: Session,
        task_id: Optional[int] = None,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[CollectionExecution]:
        """获取执行记录列表"""
        query = db.query(CollectionExecution)
        
        if task_id:
            query = query.filter(CollectionExecution.task_id == task_id)
        
        if status:
            query = query.filter(CollectionExecution.status == status)
        
        return query.order_by(desc(CollectionExecution.created_at)).offset(skip).limit(limit).all()


class CollectionLogService:
    """采集日志服务"""

    @staticmethod
    def get_logs(
        db: Session,
        execution_id: Optional[int] = None,
        level: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[CollectionLog]:
        """获取采集日志列表"""
        query = db.query(CollectionLog)
        
        if execution_id:
            query = query.filter(CollectionLog.execution_id == execution_id)
        
        if level:
            query = query.filter(CollectionLog.level == level)
        
        return query.order_by(desc(CollectionLog.created_at)).offset(skip).limit(limit).all()
