import logging
from typing import Optional
from fastapi import APIRouter, Depends, Query, Request, HTTPException
from sqlalchemy.orm import Session

from apps.user.models import User
from apps.core.database import get_db
from apps.core import Result
from utils.auth_helpers import require_auth
from apps.cluster.models import Cluster
from apps.cluster.schemas import (
    ClusterCreate,
    ClusterUpdate,
    ClusterResponse,
    ClusterTestRequest,
    ClusterTestResponse,
    ClusterMetricsResponse,
    HDFSOperationRequest,
    HDFSDirectoryCreateRequest,
    HDFSUploadRequest,
    HDFSOperationResponse,
    HDFSListResponse
)
from apps.cluster.service import ClusterService, HDFSService


router = APIRouter(prefix="/api", tags=["clusters"])
logger = logging.getLogger(__name__)


# ============================= 集群管理 API =============================

@router.get("/clusters", summary="获取集群列表")
async def list_clusters(
    skip: int = Query(0, ge=0, description="跳过数量"),
    limit: int = Query(100, ge=1, le=1000, description="返回数量"),
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取所有集群列表
    """
    try:
        clusters = ClusterService.list_all(db, skip=skip, limit=limit)
        return Result.success(200, "获取成功", {
            "clusters": [ClusterResponse.model_validate(c) for c in clusters],
            "total": len(clusters)
        })
    except Exception as e:
        logger.exception(f"获取集群列表失败: {e}")
        return Result.error(500, str(e))


@router.get("/clusters/{cluster_id}", summary="获取集群详情")
async def get_cluster_detail(
    cluster_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取集群详情
    """
    try:
        cluster = ClusterService.get_by_id(db, cluster_id)
        if not cluster:
            return Result.error(404, "集群不存在")
        return Result.success(200, "获取成功", ClusterResponse.model_validate(cluster))
    except Exception as e:
        logger.exception(f"获取集群详情失败: {e}")
        return Result.error(500, str(e))


@router.post("/clusters", summary="创建集群")
async def create_cluster(
    data: ClusterCreate,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    创建新的集群
    """
    try:
        existing = ClusterService.get_by_name(db, data.name)
        if existing:
            return Result.error(400, "集群名称已存在")

        cluster = ClusterService.create(db, data)
        return Result.success(200, "创建成功", ClusterResponse.model_validate(cluster))
    except Exception as e:
        logger.exception(f"创建集群失败: {e}")
        return Result.error(500, str(e))


@router.patch("/clusters/{cluster_id}", summary="更新集群")
async def update_cluster(
    cluster_id: int,
    data: ClusterUpdate,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    更新集群
    """
    try:
        cluster = ClusterService.update(db, cluster_id, data)
        if not cluster:
            return Result.error(404, "集群不存在")
        return Result.success(200, "更新成功", ClusterResponse.model_validate(cluster))
    except Exception as e:
        logger.exception(f"更新集群失败: {e}")
        return Result.error(500, str(e))


@router.delete("/clusters/{cluster_id}", summary="删除集群")
async def delete_cluster(
    cluster_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    删除集群(软删除)
    """
    try:
        success = ClusterService.delete(db, cluster_id)
        if not success:
            return Result.error(404, "集群不存在")
        return Result.success(200, "删除成功")
    except Exception as e:
        logger.exception(f"删除集群失败: {e}")
        return Result.error(500, str(e))


@router.post("/clusters/test-connection", summary="测试集群连接")
async def test_cluster_connection(
    request: ClusterTestRequest,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    测试集群连接
    """
    try:
        result = ClusterService.test_connection(db, request)

        if request.id and result.success:
            ClusterService.update_connection_status(db, request.id, True)

        return Result.success(200, "测试完成", {
            "success": result.success,
            "message": result.message,
            "latency": result.latency,
            "details": result.details
        })
    except Exception as e:
        logger.exception(f"测试连接失败: {e}")
        return Result.error(500, str(e))


@router.get("/clusters/{cluster_id}/metrics", summary="获取集群性能监控")
async def get_cluster_metrics(
    cluster_id: int,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    获取集群性能监控数据
    """
    try:
        result = ClusterService.get_metrics(db, cluster_id)
        if result.success:
            return Result.success(200, result.message, {"metrics": result.metrics})
        return Result.error(500, result.message)
    except Exception as e:
        logger.exception(f"获取集群监控数据失败: {e}")
        return Result.error(500, str(e))


# ============================= HDFS 操作 API =============================

@router.get("/clusters/{cluster_id}/hdfs/list", summary="列出 HDFS 文件")
async def list_hdfs_files(
    cluster_id: int,
    path: str = Query("/", description="HDFS 路径"),
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    列出 HDFS 指定路径下的文件和目录
    """
    try:
        result = HDFSService.list_files(db, cluster_id, path)
        if result.success:
            return Result.success(200, result.message, {"files": result.files})
        return Result.error(500, result.message)
    except Exception as e:
        logger.exception(f"列出 HDFS 文件失败: {e}")
        return Result.error(500, str(e))


@router.post("/clusters/{cluster_id}/hdfs/directory", summary="创建 HDFS 目录")
async def create_hdfs_directory(
    cluster_id: int,
    request: HDFSDirectoryCreateRequest,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    在 HDFS 上创建目录
    """
    try:
        result = HDFSService.create_directory(
            db, cluster_id, request.path, request.permission
        )
        if result.success:
            return Result.success(200, result.message, result.data)
        return Result.error(500, result.message)
    except Exception as e:
        logger.exception(f"创建 HDFS 目录失败: {e}")
        return Result.error(500, str(e))


@router.post("/clusters/{cluster_id}/hdfs/upload", summary="上传文件到 HDFS")
async def upload_to_hdfs(
    cluster_id: int,
    request: HDFSUploadRequest,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    上传文件到 HDFS
    """
    try:
        result = HDFSService.upload_file(
            db, cluster_id, request.path, request.content, request.local_path
        )
        if result.success:
            return Result.success(200, result.message, result.data)
        return Result.error(500, result.message)
    except Exception as e:
        logger.exception(f"上传 HDFS 文件失败: {e}")
        return Result.error(500, str(e))


@router.delete("/clusters/{cluster_id}/hdfs", summary="删除 HDFS 文件/目录")
async def delete_hdfs_file(
    cluster_id: int,
    request: HDFSOperationRequest,
    current_user: User = Depends(require_auth()),
    db: Session = Depends(get_db)
):
    """
    删除 HDFS 上的文件或目录
    """
    try:
        result = HDFSService.delete_file(db, cluster_id, request.path)
        if result.success:
            return Result.success(200, result.message, result.data)
        return Result.error(500, result.message)
    except Exception as e:
        logger.exception(f"删除 HDFS 文件失败: {e}")
        return Result.error(500, str(e))
