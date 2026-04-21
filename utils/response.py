"""
统一响应格式帮助类（增强版）

与前端 errorFactory.js 完全对齐的响应格式：
{
    "success": true/false,
    "code": 200 | 401 | 403 | 500,
    "message": "操作成功 / 需要登录 / 权限不足 / 服务器错误",
    "data": {...} | null,
    "error": {
        "type": "auth|permission|validation|business|system",
        "code": "error_code",
        ...details
    } | null
}

使用示例:
    from utils.response import ResponseHelper, success, error, paginated
    
    # 成功响应
    return ResponseHelper.success(data={"users": []})
    
    # 错误响应
    return ResponseHelper.error(401, "未授权", error={"type": "auth"})
    
    # 使用便捷函数
    return success({"id": 1})
    return error.unauthorized()
    return paginated(items, total, page, page_size)
"""

import logging
import traceback
from typing import Any, Optional, Dict, List

from .exceptions import (
    AppException,
    HTTPStatus,
    ErrorCategory,
    HTTP_STATUS_MESSAGES
)

logger = logging.getLogger(__name__)


class ResponseHelper:
    """统一响应格式帮助类"""

    @staticmethod
    def success(
        data: Any = None,
        message: str = "操作成功",
        code: int = 200,
        extra: Optional[Dict] = None
    ) -> Dict:
        """
        返回成功响应
        
        Args:
            data: 响应数据
            message: 成功消息
            code: 状态码
            extra: 额外字段
        
        Returns:
            Dict: 统一格式的成功响应
        """
        logger.info(f"[SUCCESS] {message}")
        
        response = {
            "success": True,
            "code": code,
            "message": message,
            "data": data,
            "error": None
        }
        
        if extra:
            response.update(extra)
        
        return response

    @staticmethod
    def created(data: Any = None, message: str = "创建成功") -> Dict:
        """返回创建成功响应 (201)"""
        return ResponseHelper.success(
            data=data,
            message=message,
            code=201
        )

    @staticmethod
    def error(
        status_code: int = 500,
        message: str = "程序内部异常",
        category: str = ErrorCategory.SYSTEM,
        code: Optional[str] = None,
        data: Any = None,
        error_detail: Optional[Dict] = None,
        include_trace: bool = False
    ) -> Dict:
        """
        返回错误响应
        
        Args:
            status_code: HTTP 状态码
            message: 用户可见的错误消息
            category: 错误分类 (network/auth/permission/validation/business/system)
            code: 错误代码
            data: 响应数据（通常为 null）
            error_detail: 详细错误信息
            include_trace: 是否包含堆栈跟踪
        
        Returns:
            Dict: 统一格式的错误响应
        """
        logger.error(f"[{category.upper()}] {status_code}: {message}")
        
        error_obj = {
            "type": category,
            "code": code or f"error_{status_code}"
        }
        
        if error_detail:
            if include_trace and not error_obj.get('traceback'):
                error_obj["traceback"] = traceback.format_exc()
            error_obj.update(error_detail)
        elif include_trace:
            error_obj["traceback"] = traceback.format_exc()

        return {
            "success": False,
            "code": status_code,
            "message: message,
            "data": data,
            "error": error_obj
        }

    # ==================== 快捷方法 ====================

    @staticmethod
    def bad_request(message: str = "请求参数错误", errors: List[Dict] = None) -> Dict:
        """400 - 请求参数错误"""
        detail = {"fields": errors} if errors else {}
        return ResponseHelper.error(
            status_code=400,
            message=message,
            category=ErrorCategory.VALIDATION,
            code="bad_request",
            error_detail=detail
        )

    @staticmethod
    def unauthorized(message: str = "身份认证已过期") -> Dict:
        """401 - 未认证"""
        return ResponseHelper.error(
            status_code=401,
            message=message,
            category=ErrorCategory.AUTH,
            code="unauthorized"
        )

    @staticmethod
    def forbidden(message: str = "没有权限访问") -> Dict:
        """403 - 无权限"""
        return ResponseHelper.error(
            status_code=403,
            message=message,
            category=ErrorCategory.PERMISSION,
            code="forbidden"
        )

    @staticmethod
    def not_found(message: str = "资源不存在") -> Dict:
        """404 - 资源不存在"""
        return ResponseHelper.error(
            status_code=404,
            message=message,
            category=ErrorCategory.BUSINESS,
            code="not_found"
        )

    @staticmethod
    def validation_error(message: str = "数据验证失败", errors: Dict = None) -> Dict:
        """422 - 数据验证错误"""
        detail = {"fields": errors} if errors else {}
        return ResponseHelper.error(
            status_code=422,
            message=message,
            category=ErrorCategory.VALIDATION,
            code="validation_error",
            error_detail=detail
        )

    @staticmethod
    def conflict(message: str = "资源冲突") -> Dict:
        """409 - 资源冲突"""
        return ResponseHelper.error(
            status_code=409,
            message=message,
            category=ErrorCategory.BUSINESS,
            code="conflict"
        )

    @staticmethod
    def server_error(message: str = "服务器内部错误") -> Dict:
        """500 - 服务器内部错误"""
        import os
        is_debug = os.getenv('DEBUG', '').lower() in ('1', 'true', 'yes')
        
        return ResponseHelper.error(
            status_code=500,
            message=message,
            category=ErrorCategory.SYSTEM,
            code="internal_error",
            include_trace=is_debug
        )

    @staticmethod
    def network_error(message: str = "网络连接失败") -> Dict:
        """网络错误"""
        return ResponseHelper.error(
            status_code=503,
            message=message,
            category=ErrorCategory.NETWORK,
            code="network_error"
        )

    @staticmethod
    def rate_limit(message: str = "请求过于频繁") -> Dict:
        """429 - 频率限制"""
        return ResponseHelper.error(
            status_code=429,
            message=message,
            category=ErrorCategory.SYSTEM,
            code="rate_limit"
        )

    @staticmethod
    def from_exception(exc: AppException, include_trace: bool = False) -> Dict:
        """从 AppException 创建响应"""
        return ResponseHelper.error(
            status_code=exc.status_code,
            message=exc.user_message,
            category=exc.category,
            code=exc.code,
            error_detail=exc.details or {},
            include_trace=include_trace
        )

    @staticmethod
    def paginated(
        items: list,
        total: int,
        page: int,
        page_size: int,
        message: str = "查询成功"
    ) -> Dict:
        """
        分页响应
        
        Args:
            items: 当前页数据列表
            total: 总记录数
            page: 当前页码
            page_size: 每页大小
            message: 消息
        
        Returns:
            Dict: 分页格式的响应
        """
        total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0
        
        logger.info(f"[PAGINATED] page={page}, size={page_size}, total={total}")
        
        return {
            "success": True,
            "code": 200,
            "message": message,
            "data": {
                "items": items,
                "pagination": {
                    "total": total,
                    "page": page,
                    "page_size": page_size,
                    "total_pages": total_pages,
                    "has_next": page < total_pages,
                    "has_prev": page > 1
                }
            },
            "error": None
        }


# ==================== 便捷函数导出 ====================

def success(data=None, message="操作成功", code=200):
    """创建成功响应的便捷函数"""
    return ResponseHelper.success(data=data, message=message, code=code)

def error(status_code=500, message="程序内部异常", **kwargs):
    """创建错误响应的便捷函数"""
    return ResponseHelper.error(status_code=status_code, message=message, **kwargs)

def paginated(items, total, page, page_size, message="查询成功"):
    """创建分页响应的便捷函数"""
    return ResponseHelper.paginated(items, total, page, page_size, message)


# ==================== 兼容旧接口 ====================

class Result:
    """兼容旧接口的 Result 类"""

    @staticmethod
    def unauth(msg: str = "身份认证已过期") -> Dict:
        return ResponseHelper.unauthorized(msg)

    @staticmethod
    def success(code: int = 200, msg: str = "操作成功", data: Any = None) -> Dict:
        return ResponseHelper.success(data=data, message=msg, code=code)

    @staticmethod
    def error(code: int = 500, msg: str = "程序内部异常", data: Any = None, error: Any = None) -> Dict:
        return ResponseHelper.error(
            status_code=code,
            message=msg,
            data=data,
            error_detail={"raw": error} if error else None
        )
