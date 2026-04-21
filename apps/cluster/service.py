import logging
import time
import json
import socket
from typing import Optional, Tuple, Dict, Any
from sqlalchemy.orm import Session

from apps.cluster.models import Cluster
from apps.cluster.schemas import (
    ClusterCreate,
    ClusterUpdate,
    ClusterTestRequest,
    ClusterTestResponse,
    ClusterMetricsResponse,
    HDFSOperationResponse,
    HDFSListResponse
)


logger = logging.getLogger(__name__)


class ClusterService:
    """集群服务"""

    @staticmethod
    def get_by_id(db: Session, cluster_id: int) -> Optional[Cluster]:
        """根据ID获取集群"""
        return db.query(Cluster).filter(Cluster.id == cluster_id).first()

    @staticmethod
    def get_by_name(db: Session, name: str) -> Optional[Cluster]:
        """根据名称获取集群"""
        return db.query(Cluster).filter(Cluster.name == name).first()

    @staticmethod
    def list_all(db: Session, skip: int = 0, limit: int = 100) -> list[Cluster]:
        """获取所有集群列表"""
        return db.query(Cluster).filter(Cluster.is_active == True).offset(skip).limit(limit).all()

    @staticmethod
    def create(db: Session, cluster_data: ClusterCreate) -> Cluster:
        """创建集群"""
        cluster = Cluster(
            name=cluster_data.name,
            type=cluster_data.type,
            master_host=cluster_data.master_host,
            master_port=cluster_data.master_port,
            web_ui_url=cluster_data.web_ui_url,
            hdfs_host=cluster_data.hdfs_host,
            hdfs_port=cluster_data.hdfs_port,
            extra_config=cluster_data.extra_config,
            description=cluster_data.description,
            is_active=cluster_data.is_active
        )
        db.add(cluster)
        db.commit()
        db.refresh(cluster)
        return cluster

    @staticmethod
    def update(db: Session, cluster_id: int, cluster_data: ClusterUpdate) -> Optional[Cluster]:
        """更新集群"""
        cluster = ClusterService.get_by_id(db, cluster_id)
        if not cluster:
            return None

        update_data = cluster_data.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(cluster, key, value)

        db.commit()
        db.refresh(cluster)
        return cluster

    @staticmethod
    def delete(db: Session, cluster_id: int) -> bool:
        """删除集群(软删除)"""
        cluster = ClusterService.get_by_id(db, cluster_id)
        if not cluster:
            return False

        cluster.is_active = False
        db.commit()
        return True

    @staticmethod
    def test_connection(
        db: Session,
        request: ClusterTestRequest
    ) -> ClusterTestResponse:
        """测试集群连接"""
        start_time = time.time()

        # 获取连接参数
        if request.id:
            cluster = ClusterService.get_by_id(db, request.id)
            if not cluster:
                return ClusterTestResponse(
                    success=False,
                    message="集群不存在",
                    latency=time.time() - start_time
                )
            cluster_type = cluster.type
            master_host = cluster.master_host
            master_port = cluster.master_port
            web_ui_url = cluster.web_ui_url
            extra_config = cluster.extra_config
        else:
            cluster_type = request.type
            master_host = request.master_host
            master_port = request.master_port
            web_ui_url = request.web_ui_url
            extra_config = request.extra_config

        try:
            success, message, details = ClusterService._test_cluster_connectivity(
                cluster_type, master_host, master_port, web_ui_url, extra_config
            )

            latency = time.time() - start_time

            return ClusterTestResponse(
                success=success,
                message=message,
                latency=latency,
                details=details
            )
        except Exception as e:
            logger.exception(f"测试集群连接失败: {e}")
            return ClusterTestResponse(
                success=False,
                message=f"连接测试异常: {str(e)}",
                latency=time.time() - start_time
            )

    @staticmethod
    def _test_cluster_connectivity(
        cluster_type: str,
        master_host: str,
        master_port: Optional[int],
        web_ui_url: Optional[str],
        extra_config: Optional[str]
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """测试集群连接性"""
        details = {}
        try:
            # 测试 TCP 连接
            if master_port:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10)
                result = sock.connect_ex((master_host, master_port))
                sock.close()

                if result != 0:
                    return False, f"无法连接到 {master_host}:{master_port}", details

                details["tcp_connection"] = "success"

            # 测试 Spark 集群
            if cluster_type == "spark":
                if master_port:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(10)
                    result = sock.connect_ex((master_host, master_port))
                    sock.close()
                    if result == 0:
                        details["spark_master"] = "reachable"
                        return True, "Spark 集群连接成功", details
                return False, "Spark 主节点连接失败", details

            # 测试 Hadoop 集群
            elif cluster_type == "hadoop":
                if master_port:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(10)
                    result = sock.connect_ex((master_host, master_port))
                    sock.close()
                    if result == 0:
                        details["hadoop_master"] = "reachable"
                        return True, "Hadoop 集群连接成功", details
                return False, "Hadoop 主节点连接失败", details

            return True, "集群连接测试成功", details

        except Exception as e:
            return False, f"连接测试失败: {str(e)}", details

    @staticmethod
    def get_metrics(
        db: Session,
        cluster_id: int
    ) -> ClusterMetricsResponse:
        """获取集群性能监控"""
        cluster = ClusterService.get_by_id(db, cluster_id)
        if not cluster:
            return ClusterMetricsResponse(
                success=False,
                message="集群不存在"
            )

        try:
            metrics = {
                "cluster_id": cluster_id,
                "cluster_name": cluster.name,
                "cluster_type": cluster.type,
                "timestamp": int(time.time()),
                "cpu_usage": None,
                "memory_usage": None,
                "disk_usage": None,
                "network_traffic": None,
                "active_workers": None,
                "active_tasks": None
            }

            return ClusterMetricsResponse(
                success=True,
                message="获取监控数据成功",
                metrics=metrics
            )
        except Exception as e:
            logger.exception(f"获取集群监控数据失败: {e}")
            return ClusterMetricsResponse(
                success=False,
                message=f"获取监控数据失败: {str(e)}"
            )

    @staticmethod
    def update_connection_status(
        db: Session,
        cluster_id: int,
        is_connected: bool
    ) -> Optional[Cluster]:
        """更新集群连接状态"""
        from datetime import datetime
        cluster = ClusterService.get_by_id(db, cluster_id)
        if not cluster:
            return None

        cluster.is_connected = is_connected
        if is_connected:
            cluster.last_connected_at = datetime.now()

        db.commit()
        db.refresh(cluster)
        return cluster


class HDFSService:
    """HDFS 操作服务"""

    @staticmethod
    def _get_hdfs_client(cluster: Cluster):
        """获取 HDFS 客户端"""
        try:
            from hdfs import InsecureClient
            host = cluster.hdfs_host or cluster.master_host
            port = cluster.hdfs_port or 9000
            url = f"http://{host}:{port}"
            client = InsecureClient(url, user='hadoop')
            return client
        except ImportError:
            raise Exception("未安装 hdfs 库，请先安装: pip install hdfs")

    @staticmethod
    def list_files(
        db: Session,
        cluster_id: int,
        path: str
    ) -> HDFSListResponse:
        """列出 HDFS 文件"""
        cluster = ClusterService.get_by_id(db, cluster_id)
        if not cluster:
            return HDFSListResponse(success=False, message="集群不存在")

        try:
            client = HDFSService._get_hdfs_client(cluster)
            files = []
            if client.status(path, strict=False):
                contents = client.list(path, status=True)
                for fname, fstatus in contents:
                    files.append({
                        "name": fname,
                        "path": f"{path}/{fname}",
                        "type": "directory" if fstatus['type'] == 'DIRECTORY' else "file",
                        "size": fstatus['length'],
                        "modification_time": fstatus['modificationTime'],
                        "permission": fstatus['permission']
                    })
            return HDFSListResponse(success=True, message="获取成功", files=files)
        except Exception as e:
            logger.exception(f"列出 HDFS 文件失败: {e}")
            return HDFSListResponse(success=False, message=f"列出文件失败: {str(e)}")

    @staticmethod
    def create_directory(
        db: Session,
        cluster_id: int,
        path: str,
        permission: str = "755"
    ) -> HDFSOperationResponse:
        """创建 HDFS 目录"""
        cluster = ClusterService.get_by_id(db, cluster_id)
        if not cluster:
            return HDFSOperationResponse(success=False, message="集群不存在")

        try:
            client = HDFSService._get_hdfs_client(cluster)
            client.makedirs(path)
            if permission:
                client.chmod(path, permission)
            return HDFSOperationResponse(
                success=True,
                message="目录创建成功",
                data={"path": path}
            )
        except Exception as e:
            logger.exception(f"创建 HDFS 目录失败: {e}")
            return HDFSOperationResponse(success=False, message=f"创建目录失败: {str(e)}")

    @staticmethod
    def upload_file(
        db: Session,
        cluster_id: int,
        hdfs_path: str,
        content: Optional[str] = None,
        local_path: Optional[str] = None
    ) -> HDFSOperationResponse:
        """上传文件到 HDFS"""
        cluster = ClusterService.get_by_id(db, cluster_id)
        if not cluster:
            return HDFSOperationResponse(success=False, message="集群不存在")

        try:
            client = HDFSService._get_hdfs_client(cluster)
            if local_path:
                with open(local_path, 'rb') as local_f:
                    with client.write(hdfs_path) as hdfs_f:
                        hdfs_f.write(local_f.read())
            elif content:
                with client.write(hdfs_path) as hdfs_f:
                    hdfs_f.write(content.encode('utf-8'))
            else:
                return HDFSOperationResponse(success=False, message="需要提供 local_path 或 content")

            return HDFSOperationResponse(
                success=True,
                message="文件上传成功",
                data={"path": hdfs_path}
            )
        except Exception as e:
            logger.exception(f"上传 HDFS 文件失败: {e}")
            return HDFSOperationResponse(success=False, message=f"上传文件失败: {str(e)}")

    @staticmethod
    def delete_file(
        db: Session,
        cluster_id: int,
        path: str
    ) -> HDFSOperationResponse:
        """删除 HDFS 文件/目录"""
        cluster = ClusterService.get_by_id(db, cluster_id)
        if not cluster:
            return HDFSOperationResponse(success=False, message="集群不存在")

        try:
            client = HDFSService._get_hdfs_client(cluster)
            client.delete(path, recursive=True)
            return HDFSOperationResponse(
                success=True,
                message="删除成功",
                data={"path": path}
            )
        except Exception as e:
            logger.exception(f"删除 HDFS 文件失败: {e}")
            return HDFSOperationResponse(success=False, message=f"删除失败: {str(e)}")
