"""
数据源验证模块
提供数据源配置的验证功能
"""
from typing import Dict, Any, Optional, List
from apps.datasource.models import DataSourceType, DataSourceCategory
from apps.datasource.hive_config import HiveConfigManager


class DataSourceValidator:
    """数据源验证器"""
    
    @staticmethod
    def validate_required_fields(data: Dict[str, Any]) -> Dict[str, Any]:
        """验证必填字段"""
        required_fields = ["name", "type", "host", "port"]
        missing_fields = [field for field in required_fields if field not in data or not data[field]]
        
        if missing_fields:
            return {
                "valid": False,
                "message": f"缺少必填字段: {', '.join(missing_fields)}",
                "missing_fields": missing_fields
            }
        
        return {
            "valid": True,
            "message": "必填字段验证通过"
        }
    
    @staticmethod
    def validate_data_source_type(data_type: str) -> Dict[str, Any]:
        """验证数据源类型"""
        valid_types = [item.value for item in DataSourceType]
        if data_type not in valid_types:
            return {
                "valid": False,
                "message": f"无效的数据源类型: {data_type}，支持的类型: {', '.join(valid_types)}"
            }
        
        return {
            "valid": True,
            "message": "数据源类型验证通过"
        }
    
    @staticmethod
    def validate_port(port: int) -> Dict[str, Any]:
        """验证端口号"""
        if not isinstance(port, int) or port <= 0 or port > 65535:
            return {
                "valid": False,
                "message": "端口号必须是 1-65535 之间的整数"
            }
        
        return {
            "valid": True,
            "message": "端口号验证通过"
        }
    
    @staticmethod
    def validate_hive_config(data: Dict[str, Any]) -> Dict[str, Any]:
        """验证 Hive 数据源配置"""
        if data.get("type") == DataSourceType.HIVE:
            return HiveConfigManager.validate_hive_config(data)
        return {
            "valid": True,
            "message": "非 Hive 数据源，跳过 Hive 配置验证"
        }
    
    @staticmethod
    def validate_all(data: Dict[str, Any]) -> Dict[str, Any]:
        """执行所有验证"""
        # 验证必填字段
        required_result = DataSourceValidator.validate_required_fields(data)
        if not required_result["valid"]:
            return required_result
        
        # 验证数据源类型
        type_result = DataSourceValidator.validate_data_source_type(data.get("type"))
        if not type_result["valid"]:
            return type_result
        
        # 验证端口号
        port_result = DataSourceValidator.validate_port(data.get("port"))
        if not port_result["valid"]:
            return port_result
        
        # 验证 Hive 配置（如果是 Hive 数据源）
        hive_result = DataSourceValidator.validate_hive_config(data)
        if not hive_result["valid"]:
            return hive_result
        
        return {
            "valid": True,
            "message": "所有验证通过"
        }
    
    @staticmethod
    def sanitize_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """清理和标准化数据"""
        sanitized = {}
        
        # 基本字段
        for field in ["name", "type", "host", "port", "database", "username", "password", "extra_config", "description", "is_active"]:
            if field in data:
                sanitized[field] = data[field]
        
        # 标准化端口号
        if "port" in sanitized:
            sanitized["port"] = int(sanitized["port"])
        
        # 标准化布尔值
        if "is_active" in sanitized:
            sanitized["is_active"] = bool(sanitized["is_active"])
        
        # 自动设置分类
        if "type" in sanitized:
            sanitized["category"] = DataSourceType.get_category(sanitized["type"])
        
        return sanitized
