"""
全局异常处理中间件

功能：
1. 捕获所有未处理的异常
2. 统一响应格式（与前端 errorFactory.js 对应）
3. 开发/生产环境差异化输出
4. 自动日志记录
5. 支持自定义异常和 Python 内置异常

使用方式：
    from utils.exception_handler import register_exception_handlers, exception_handler
    
    app = FastAPI()
    register_exception_handlers(app)
    
    # 或者在路由中使用装饰器
    @app.get("/test")
    @exception_handler()
    async def test():
        raise ValueError("测试错误")
"""

import traceback
import logging
import sys
import json
from typing import Any, Optional, Dict, Callable
from functools import wraps
from datetime import datetime

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from fastapi.exceptions import HTTPException as FastAPIHTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException

from .exceptions import (
    AppException,
    HTTPStatus,
    ErrorCategory,
    AuthenticationError,
    PermissionDeniedError,
    ValidationError,
    BusinessError,
    SystemError,
    get_exception_mapping,
    classify_exception,
    HTTP_STATUS_MESSAGES
)

logger = logging.getLogger(__name__)


class ExceptionHandlerConfig:
    """异常处理器配置"""
    
    DEBUG = False  # 是否为调试模式（从环境变量或配置读取）
    
    # 敏感信息过滤字段（生产环境不返回）
    SENSITIVE_FIELDS = [
        'password',
        'token',
        'secret',
        'api_key',
        'credential'
    ]
    
    # 需要记录详细日志的异常类型
    LOG_DETAIL_EXCEPTIONS = [
        SystemError,
        Exception  # 记录所有未知异常
    ]
    
    @classmethod
    def is_debug(cls) -> bool:
        """判断是否为调试模式"""
        import os
        if cls.DEBUG:
            return True
        return os.getenv('DEBUG', '').lower() in ('1', 'true', 'yes')
    
    @classmethod
    def should_include_trace(cls, exception: Exception) -> bool:
        """是否应该包含堆栈跟踪"""
        return cls.is_debug() or any(
            isinstance(exception, exc_type)
            for exc_type in cls.LOG_DETAIL_EXCEPTIONS
        )
    
    @classmethod
    def sanitize_data(cls, data: Dict) -> Dict:
        """过滤敏感数据"""
        if not cls.is_debug():
            for field in cls.SENSITIVE_FIELDS:
                if field in data:
                    data[field] = '***FILTERED***'
        return data


def create_error_response(
    status_code: int,
    message: str,
    category: str = ErrorCategory.SYSTEM,
    code: Optional[str] = None,
    details: Optional[Dict] = None,
    include_trace: bool = False,
    original_exception: Optional[Exception] = None
) -> Dict:
    """
    创建统一格式的错误响应
    
    Args:
        status_code: HTTP 状态码
        message: 用户可见的错误消息
        category: 错误分类
        code: 错误代码
        details: 详细错误信息
        include_trace: 是否包含堆栈跟踪
        original_exception: 原始异常对象
    
    Returns:
        Dict: 统一格式的错误响应
    """
    error_obj = {
        "type": category,
        "code": code or f"http_{status_code}",
    }
    
    if details:
        error_obj["details"] = ExceptionHandlerConfig.sanitize_data(details)
    
    if include_trace and original_exception:
        error_obj["traceback"] = traceback.format_exc()
        error_obj["exception_type"] = type(original_exception).__name__
    
    response = {
        "success": False,
        "code": status_code,
        "message": message,
        "data": None,
        "error": error_obj
    }
    
    return response


def log_exception(exception: Exception, request: Optional[Request] = None):
    """记录异常日志"""
    request_info = ""
    if request:
        request_info = f" | {request.method} {request.url.path}"
    
    if isinstance(exception, AppException):
        logger.warning(
            f"[{exception.category.upper()}]{request_info} "
            f"{type(exception).__name__}: {exception.message}"
        )
        if exception.details:
            logger.debug(f"Exception details: {exception.details}")
    else:
        logger.error(
            f"[UNHANDLED]{request_info} "
            f"{type(exception).__name__}: {str(exception)}",
            exc_info=True
        )


async def handle_app_exception(request: Request, exc: AppException) -> JSONResponse:
    """处理应用自定义异常"""
    log_exception(exc, request)
    
    include_trace = ExceptionHandlerConfig.should_include_trace(exc)
    
    response_data = create_error_response(
        status_code=exc.status_code,
        message=exc.user_message,
        category=exc.category,
        code=exc.code,
        details=exc.details,
        include_trace=include_trace,
        original_exception=exc
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=response_data
    )


async def handle_fastapi_http_exception(request: Request, exc: FastAPIHTTPException) -> JSONResponse:
    """处理 FastAPI/Starlette HTTP 异常"""
    log_exception(exc, request)
    
    # 映射到我们的分类
    category_map = {
        401: ErrorCategory.AUTH,
        403: ErrorCategory.PERMISSION,
        404: ErrorCategory.BUSINESS,
        405: ErrorCategory.VALIDATION,
        422: ErrorCategory.VALIDATION,
        429: ErrorCategory.SYSTEM,
    }
    category = category_map.get(exc.status_code, ErrorCategory.SYSTEM)
    
    message = exc.detail or HTTP_STATUS_MESSAGES.get(exc.status_code, '请求失败')
    
    include_trace = ExceptionHandlerConfig.should_include_trace(exc)
    
    response_data = create_error_response(
        status_code=exc.status_code,
        message=message,
        category=category,
        code=f"http_{exc.status_code}",
        include_trace=include_trace,
        original_exception=exc
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=response_data
    )


async def handle_unhandled_exception(request: Request, exc: Exception) -> JSONResponse:
    """处理未捕获的通用异常"""
    log_exception(exc, request)
    
    # 开发环境显示详细信息，生产环境显示友好消息
    if ExceptionHandlerConfig.is_debug():
        message = f"{type(exc).__name__}: {str(exc)}"
        details = {
            "exception": type(exc).__name__,
            "args": [str(arg) for arg in exc.args],
            "traceback": traceback.format_exc()
        }
    else:
        message = "服务器内部错误，请稍后重试"
        details = None
    
    response_data = create_error_response(
        status_code=500,
        message=message,
        category=ErrorCategory.SYSTEM,
        code="internal_error",
        details=details,
        include_trace=True,
        original_exception=exc
    )
    
    return JSONResponse(
        status_code=500,
        content=response_data
    )


def register_exception_handlers(app):
    """
    注册全局异常处理器到 FastAPI 应用
    
    Args:
        app: FastAPI 实例
    """
    # 处理应用自定义异常
    @app.exception_handler(AppException)
    async def app_exception_handler(request: Request, exc: AppException):
        return await handle_app_exception(request, exc)
    
    # 处理 FastAPI HTTP 异常
    @app.exception_handler(FastAPIHTTPException)
    async def fastapi_exception_handler(request: Request, exc: FastAPIHTTPException):
        return await handle_fastapi_http_exception(request, exc)
    
    # 处理 Starlette HTTP 异常
    @app.exception_handler(StarletteHTTPException)
    async def starlette_exception_handler(request: Request, exc: StarletteHTTPException):
        return await handle_fastapi_http_exception(request, exc)
    
    # 处理值错误（通常来自参数验证）
    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError):
        log_exception(exc, request)
        
        response_data = create_error_response(
            status_code=422,
            message=str(exc) or "参数值无效",
            category=ErrorCategory.VALIDATION,
            code="value_error",
            include_trace=ExceptionHandlerConfig.is_debug(),
            original_exception=exc
        )
        
        return JSONResponse(status_code=422, content=response_data)
    
    # 处理类型错误
    @app.exception_handler(TypeError)
    async def type_error_handler(request: Request, exc: TypeError):
        log_exception(exc, request)
        
        response_data = create_error_response(
            status_code=400,
            message="参数类型错误",
            category=ErrorCategory.VALIDATION,
            code="type_error",
            include_trace=ExceptionHandlerConfig.is_debug(),
            original_exception=exc
        )
        
        return JSONResponse(status_code=400, content=response_data)
    
    # 处理键错误
    @app.exception_handler(KeyError)
    async def key_error_handler(request: Request, exc: KeyError):
        log_exception(exc, request)
        
        response_data = create_error_response(
            status_code=400,
            message=f"缺少必要参数: {exc.args[0] if exc.args else 'unknown'}",
            category=ErrorCategory.VALIDATION,
            code="missing_field",
            include_trace=False,
            original_exception=exc
        )
        
        return JSONResponse(status_code=400, content=response_data)
    
    # 最终兜底：处理所有其他未捕获的异常
    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        return await handle_unhandled_exception(request, exc)
    
    logger.info("✓ 全局异常处理器已注册")


# ==================== 装饰器版本 ====================

def exception_handler(
    reraise: bool = False,
    default_message: str = "操作失败",
    log_errors: bool = True
):
    """
    异常处理装饰器
    
    用于在路由函数级别统一处理异常
    
    使用示例:
        @app.get("/users")
        @exception_handler()
        async def get_users():
            users = await db.fetch_all()
            return {"data": users}
    
    Args:
        reraise: 是否重新抛出异常（用于调试）
        default_message: 默认错误消息
        log_errors: 是否记录错误日志
    """
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except AppException as e:
                if log_errors:
                    log_exception(e, kwargs.get('request'))
                
                if reraise:
                    raise
                
                response_data = create_error_response(
                    status_code=e.status_code,
                    message=e.user_message,
                    category=e.category,
                    code=e.code,
                    details=e.details,
                    include_trace=ExceptionHandlerConfig.should_include_trace(e),
                    original_exception=e
                )
                
                return JSONResponse(status_code=e.status_code, content=response_data)
            
            except (FastAPIHTTPException, StarletteHTTPException):
                raise
            
            except Exception as e:
                if log_errors:
                    log_exception(e, kwargs.get('request'))
                
                if reraise:
                    raise
                
                if ExceptionHandlerConfig.is_debug():
                    message = f"{type(e).__name__}: {str(e)}"
                else:
                    message = default_message
                
                response_data = create_error_response(
                    status_code=500,
                    message=message,
                    category=ErrorCategory.SYSTEM,
                    code="internal_error",
                    include_trace=ExceptionHandlerConfig.is_debug(),
                    original_exception=e
                )
                
                return JSONResponse(status_code=500, content=response_data)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except AppException as e:
                if log_errors:
                    log_exception(e, kwargs.get('request'))
                
                if reraise:
                    raise
                
                response_data = create_error_response(
                    status_code=e.status_code,
                    message=e.user_message,
                    category=e.category,
                    code=e.code,
                    details=e.details,
                    include_trace=ExceptionHandlerConfig.should_include_trace(e),
                    original_exception=e
                )
                
                return JSONResponse(status_code=e.status_code, content=response_data)
            
            except (FastAPIHTTPException, StarletteHTTPException):
                raise
            
            except Exception as e:
                if log_errors:
                    log_exception(e, kwargs.get('request'))
                
                if reraise:
                    raise
                
                if ExceptionHandlerConfig.is_debug():
                    message = f"{type(e).__name__}: {str(e)}"
                else:
                    message = default_message
                
                response_data = create_error_response(
                    status_code=500,
                    message=message,
                    category=ErrorCategory.SYSTEM,
                    code="internal_error",
                    include_trace=ExceptionHandlerConfig.is_debug(),
                    original_exception=e
                )
                
                return JSONResponse(status_code=500, content=response_data)
        
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# ==================== 辅助函数 ====================

def success_response(data: Any = None, message: str = "操作成功", code: int = 200) -> Dict:
    """创建成功响应"""
    return {
        "success": True,
        "code": code,
        "message": message,
        "data": data,
        "error": None
    }


def paginated_response(
    items: list,
    total: int,
    page: int,
    page_size: int,
    message: str = "查询成功"
) -> Dict:
    """创建分页响应"""
    return {
        "success": True,
        "code": 200,
        "message": message,
        "data": {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if page_size > 0 else 0
        },
        "error": None
    }


# 导出便捷别名
register_handlers = register_exception_handlers
handle_error = exception_handler
