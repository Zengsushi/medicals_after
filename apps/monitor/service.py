"""
监控模块 - 业务逻辑服务
"""
import logging
import time
import random
from typing import Optional, Tuple, Dict, Any
from sqlalchemy.orm import Session
from datetime import datetime

from apps.monitor.models import DatabaseMetric, ClusterMetric
from apps.monitor.schemas import (
    DatabaseMetricResponse,
    ClusterMetricResponse,
    MonitorOverviewResponse
)
from apps.datasource.models import DataSource
from apps.cluster.models import Cluster


logger = logging.getLogger(__name__)


class DatabaseMonitorService:
    """数据库监控服务"""

    @staticmethod
    def get_latest_metric(
        db: Session,
        datasource_id: int
    ) -> Optional[DatabaseMetric]:
        """获取最新的数据库监控指标"""
        return db.query(DatabaseMetric).filter(
            DatabaseMetric.datasource_id == datasource_id
        ).order_by(DatabaseMetric.timestamp.desc()).first()

    @staticmethod
    def collect_metric(
        db: Session,
        datasource_id: int
    ) -> DatabaseMetric:
        """采集数据库监控指标"""
        datasource = db.query(DataSource).filter(
            DataSource.id == datasource_id
        ).first()
        
        if not datasource:
            raise ValueError("数据源不存在")
        
        try:
            # 模拟监控数据采集
            # 实际项目中，这里应该连接数据库并获取真实的监控数据
            
            # 模拟内存使用
            memory_total = 4096.0  # 4GB
            memory_used = random.uniform(1024.0, 3072.0)
            memory_percent = (memory_used / memory_total) * 100
            
            # 模拟访问次数
            query_count = random.randint(100, 10000)
            connection_count = random.randint(5, 50)
            max_connections = 100
            
            # 创建监控记录
            metric = DatabaseMetric(
                datasource_id=datasource_id,
                timestamp=datetime.now(),
                memory_used=memory_used,
                memory_total=memory_total,
                memory_percent=memory_percent,
                query_count=query_count,
                connection_count=connection_count,
                max_connections=max_connections,
                extra_info={
                    "datasource_name": datasource.name,
                    "db_type": datasource.db_type
                }
            )
            
            db.add(metric)
            db.commit()
            db.refresh(metric)
            
            return metric
            
        except Exception as e:
            logger.exception(f"采集数据库监控指标失败: {e}")
            db.rollback()
            raise

    @staticmethod
    def get_metrics_history(
        db: Session,
        datasource_id: int,
        limit: int = 100
    ) -> list[DatabaseMetric]:
        """获取历史监控指标"""
        return db.query(DatabaseMetric).filter(
            DatabaseMetric.datasource_id == datasource_id
        ).order_by(DatabaseMetric.timestamp.desc()).limit(limit).all()


class ClusterMonitorService:
    """集群监控服务"""

    @staticmethod
    def get_latest_metric(
        db: Session,
        cluster_id: int
    ) -> Optional[ClusterMetric]:
        """获取最新的集群监控指标"""
        return db.query(ClusterMetric).filter(
            ClusterMetric.cluster_id == cluster_id
        ).order_by(ClusterMetric.timestamp.desc()).first()

    @staticmethod
    def collect_metric(
        db: Session,
        cluster_id: int
    ) -> ClusterMetric:
        """采集集群监控指标"""
        cluster = db.query(Cluster).filter(
            Cluster.id == cluster_id
        ).first()
        
        if not cluster:
            raise ValueError("集群不存在")
        
        try:
            # 模拟监控数据采集
            # 实际项目中，这里应该连接Hadoop集群并获取真实的监控数据
            
            # 模拟内存使用
            memory_total = 32768.0  # 32GB
            memory_used = random.uniform(8192.0, 24576.0)
            memory_percent = (memory_used / memory_total) * 100
            
            # 模拟CPU使用
            cpu_usage = random.uniform(20.0, 80.0)
            
            # 模拟HDFS存储使用
            hdfs_total = 1024.0  # 1TB
            hdfs_used = random.uniform(200.0, 800.0)
            hdfs_percent = (hdfs_used / hdfs_total) * 100
            
            # 模拟节点和任务
            active_nodes = random.randint(3, 10)
            dead_nodes = random.randint(0, 2)
            active_tasks = random.randint(0, 50)
            completed_tasks = random.randint(100, 1000)
            
            # 创建监控记录
            metric = ClusterMetric(
                cluster_id=cluster_id,
                timestamp=datetime.now(),
                memory_used=memory_used,
                memory_total=memory_total,
                memory_percent=memory_percent,
                cpu_usage=cpu_usage,
                hdfs_used=hdfs_used,
                hdfs_total=hdfs_total,
                hdfs_percent=hdfs_percent,
                active_nodes=active_nodes,
                dead_nodes=dead_nodes,
                active_tasks=active_tasks,
                completed_tasks=completed_tasks,
                extra_info={
                    "cluster_name": cluster.name,
                    "cluster_type": cluster.type
                }
            )
            
            db.add(metric)
            db.commit()
            db.refresh(metric)
            
            return metric
            
        except Exception as e:
            logger.exception(f"采集集群监控指标失败: {e}")
            db.rollback()
            raise

    @staticmethod
    def get_metrics_history(
        db: Session,
        cluster_id: int,
        limit: int = 100
    ) -> list[ClusterMetric]:
        """获取历史监控指标"""
        return db.query(ClusterMetric).filter(
            ClusterMetric.cluster_id == cluster_id
        ).order_by(ClusterMetric.timestamp.desc()).limit(limit).all()


class MonitorService:
    """监控概览服务"""

    @staticmethod
    def get_overview(
        db: Session,
        datasource_id: Optional[int] = None,
        cluster_id: Optional[int] = None
    ) -> MonitorOverviewResponse:
        """获取监控概览"""
        database_metric = None
        cluster_metric = None
        
        if datasource_id:
            database_metric = DatabaseMonitorService.get_latest_metric(db, datasource_id)
            if not database_metric:
                database_metric = DatabaseMonitorService.collect_metric(db, datasource_id)
        
        if cluster_id:
            cluster_metric = ClusterMonitorService.get_latest_metric(db, cluster_id)
            if not cluster_metric:
                cluster_metric = ClusterMonitorService.collect_metric(db, cluster_id)
        
        return MonitorOverviewResponse(
            database_metrics=DatabaseMetricResponse.model_validate(database_metric) if database_metric else None,
            cluster_metrics=ClusterMetricResponse.model_validate(cluster_metric) if cluster_metric else None,
            timestamp=datetime.now()
        )
