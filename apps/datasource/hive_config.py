"""
Hive 数据源配置管理模块
提供 Hive 连接的配置管理、环境变量设置等功能
"""
import os
import json
import warnings
from typing import Optional, Dict, Any


class HiveConfigManager:
    """Hive 配置管理器"""
    
    @staticmethod
    def get_default_java_home() -> str:
        """获取默认的 JAVA_HOME"""
        return os.getenv("JAVA_HOME", r"E:\java_version\jdk17.0.11")
    
    @staticmethod
    def get_default_hadoop_home() -> str:
        """获取默认的 HADOOP_HOME"""
        default_path = os.path.join(
            os.path.dirname(__file__), 
            "..", "..", "config", "hadoop-3.3.4"
        )
        return os.getenv("HADOOP_HOME", default_path)
    
    @staticmethod
    def setup_environment() -> Dict[str, str]:
        """设置 Hive 所需的环境变量"""
        java_home = HiveConfigManager.get_default_java_home()
        hadoop_home = HiveConfigManager.get_default_hadoop_home()
        
        env_vars = {}
        
        if java_home:
            os.environ["JAVA_HOME"] = java_home
            env_vars["JAVA_HOME"] = java_home
        
        if hadoop_home:
            os.environ["HADOOP_HOME"] = hadoop_home
            env_vars["HADOOP_HOME"] = hadoop_home
        
        # 禁用警告
        warnings.filterwarnings("ignore")
        
        return env_vars
    
    @staticmethod
    def build_spark_config(host: str, port: int, extra_config: Optional[str] = None) -> Dict[str, str]:
        """构建 Spark 配置"""
        config = {}
        
        # 如果提供了 host 和 port，使用远程 Hive Metastore
        if host and port:
            config["hive.metastore.uris"] = f"thrift://{host}:{port}"
        else:
            # 使用本地 Derby 数据库作为 Metastore
            metastore_path = os.path.join(os.path.dirname(__file__), "..", "..", "metastore_db")
            config["javax.jdo.option.ConnectionURL"] = f"jdbc:derby:;databaseName={metastore_path};create=true"
            config["hive.metastore.warehouse.dir"] = os.path.join(os.path.dirname(__file__), "..", "..", "warehouse")
        
        # 解析额外配置
        if extra_config:
            try:
                extra_config_dict = json.loads(extra_config)
                config.update(extra_config_dict)
            except json.JSONDecodeError:
                pass
        
        return config
    
    @staticmethod
    def validate_hive_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """验证 Hive 配置"""
        required_keys = ["host", "port"]
        missing_keys = [key for key in required_keys if key not in config]
        
        if missing_keys:
            return {
                "valid": False,
                "message": f"缺少必要配置: {', '.join(missing_keys)}",
                "missing_keys": missing_keys
            }
        
        # 验证端口范围
        if not isinstance(config.get("port"), int) or config["port"] <= 0 or config["port"] > 65535:
            return {
                "valid": False,
                "message": "端口必须是有效的整数（1-65535）"
            }
        
        return {
            "valid": True,
            "message": "配置验证通过"
        }
    
    @staticmethod
    def get_hive_connection_info(host: str, port: int) -> Dict[str, str]:
        """获取 Hive 连接信息"""
        return {
            "metastore_uri": f"thrift://{host}:{port}",
            "host": host,
            "port": port
        }
