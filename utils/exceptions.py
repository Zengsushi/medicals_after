"""
统一异常处理模块

提供自定义异常类和 HTTP 状态码映射
支持开发/生产环境差异化输出
"""

import traceback
import logging
from typing import Any, Optional, Dict, List
from enum import IntEnum

logger = logging.getLogger(__name__)


class HTTPStatus(IntEnum):
    """HTTP 状态码枚举"""
    
    # 2xx 成功
    OK = 200
    CREATED = 201
    ACCEPTED = 202
    NO_CONTENT = 204
    
    # 4xx 客户端错误
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    CONFLICT = 409
    UNPROCESSABLE_ENTITY = 422
    TOO_MANY_REQUESTS = 429
    
    # 5xx 服务器错误
    INTERNAL_SERVER_ERROR = 501
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503


class ErrorCategory:
    """错误分类（与前端 errorFactory.js 对应）"""
    NETWORK = 'network'
    AUTH = 'auth'
    PERMISSION = 'permission'
    VALIDATION = 'validation'
    BUSINESS = 'business'
    SYSTEM = 'system'


class AppException(Exception):
    """
    应用基础异常类
    
    所有业务异常应继承此类，确保统一的错误响应格式
    """
    
    status_code: int = HTTPStatus.INTERNAL_SERVER_ERROR
    category: str = ErrorCategory.SYSTEM
    message: str = "服务器内部错误"
    user_message: Optional[str] = None
    code: Optional[str] = None
    details: Optional[Dict] = None
    
    def __init__(
        self,
        message: str = None,
        user_message: str = None,
        code: str = None,
        details: Dict = None,
        **kwargs
    ):
        super().__init__(message or self.message)
        
        if message:
            self.message = message
        
        self.user_message = user_message or self.user_message or self.message
        self.code = code or self.code
        self.details = details or {}
        
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def to_dict(self, include_trace: bool = False) -> Dict:
        """转换为响应字典"""
        result = {
            "success": False,
            "code": self.status_code,
            "message": self.user_message,
            "data": None,
            "error": {
                "type": self.category,
                "code": self.code,
                **self.details
            }
        }
        
        if include_trace:
            result["error"]["traceback"] = traceback.format_exc()
            result["error"]["exception"] = type(self).__name__
        
        return result


# ==================== 认证相关异常 ====================

class AuthenticationError(AppException):
    """认证失败异常"""
    status_code = HTTPStatus.UNAUTHORIZED
    category = ErrorCategory.AUTH
    message = "身份认证已过期，请重新登录"


class TokenExpiredError(AuthenticationError):
    """Token 过期"""
    code = "token_expired"
    message = "登录令牌已过期，请重新登录"


class InvalidTokenError(AuthenticationError):
    """无效 Token"""
    code = "invalid_token"
    message = "无效的认证令牌"


class MissingTokenError(AuthenticationError):
    """缺少 Token"""
    code = "missing_token"
    message = "缺少认证令牌"


class SessionExpiredError(AuthenticationError):
    """会话过期"""
    code = "session_expired"
    message = "会话已失效，请重新登录"


# ==================== 权限相关异常 ====================

class PermissionDeniedError(AppException):
    """权限不足异常"""
    status_code = HTTPStatus.FORBIDDEN
    category = ErrorCategory.PERMISSION
    message = "没有权限执行此操作"
    code = "permission_denied"


class RoleRequiredError(PermissionDeniedError):
    """需要更高角色权限"""
    code = "role_required"
    message = "需要更高的访问权限"


class ResourceAccessDeniedError(PermissionDeniedError):
    """资源访问被拒绝"""
    code = "resource_denied"
    message = "没有权限访问该资源"


# ==================== 验证相关异常 ====================

class ValidationError(AppException):
    """数据验证异常"""
    status_code = HTTPStatus.UNPROCESSABLE_ENTITY
    category = ErrorCategory.VALIDATION
    message = "数据验证失败"
    code = "validation_error"


class RequiredFieldError(ValidationError):
    """必填字段缺失"""
    code = "field_required"
    message = "请填写必填项"


class FieldFormatError(ValidationError):
    """字段格式错误"""
    code = "field_format"
    message = "数据格式不正确"


class FieldLengthError(ValidationError):
    """字段长度超限"""
    code = "field_length"
    message = "数据长度超出限制"


class UniqueConstraintError(ValidationError):
    """唯一约束冲突"""
    code = "unique_constraint"
    message = "数据已存在，不能重复"


class InvalidParameterError(AppException):
    """无效参数"""
    status_code = HTTPStatus.BAD_REQUEST
    category = ErrorCategory.VALIDATION
    message = "请求参数错误"
    code = "bad_request"


# ==================== 业务逻辑异常 ====================

class BusinessError(AppException):
    """业务逻辑异常基类"""
    status_code = HTTPStatus.BAD_REQUEST
    category = ErrorCategory.BUSINESS
    message = "操作失败"
    code = "business_error"


class ResourceNotFoundError(BusinessError):
    """资源不存在"""
    status_code = HTTPStatus.NOT_FOUND
    code = "not_found"
    message = "资源不存在或已被删除"


class ResourceConflictError(BusinessError):
    """资源冲突"""
    status_code = HTTPStatus.CONFLICT
    code = "conflict"
    message = "操作冲突，请刷新后重试"


class OperationFailedError(BusinessError):
    """操作失败"""
    code = "operation_failed"
    message = "操作失败，请稍后重试"


class CreateFailedError(OperationFailedError):
    """创建失败"""
    code = "create_failed"
    message = "创建失败，请重试"


class UpdateFailedError(OperationFailedError):
    """更新失败"""
    code = "update_failed"
    message = "更新失败，请重试"


class DeleteFailedError(OperationFailedError):
    """删除失败"""
    code = "delete_failed"
    message = "删除失败，可能有关联数据"


# ==================== 系统级异常 ====================

class SystemError(AppException):
    """系统内部错误"""
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    category = ErrorCategory.SYSTEM
    message = "服务器内部错误"
    code = "internal_error"


class DatabaseError(SystemError):
    """数据库错误"""
    code = "database_error"
    message = "数据库操作失败"


class ExternalServiceError(SystemError):
    """外部服务调用失败"""
    code = "external_service_error"
    message = "外部服务调用失败"


class RateLimitError(AppException):
    """请求频率限制"""
    status_code = HTTPStatus.TOO_MANY_REQUESTS
    category = ErrorCategory.SYSTEM
    message = "请求过于频繁，请稍后重试"
    code = "rate_limit"


# ==================== 异常映射表 ====================

EXCEPTION_MAPPING = {
    AuthenticationError: {"status": 401, "category": ErrorCategory.AUTH},
    PermissionDeniedError: {"status": 403, "category": ErrorCategory.PERMISSION},
    ValidationError: {"status": 422, "category": ErrorCategory.VALIDATION},
    BusinessError: {"status": 400, "category": ErrorCategory.BUSINESS},
    SystemError: {"status": 500, "category": ErrorCategory.SYSTEM},
}

HTTP_STATUS_MESSAGES = {
    200: "操作成功",
    201: "创建成功",
    204: "操作成功",
    400: "请求参数错误",
    401: "身份认证已过期",
    403: "没有权限访问",
    404: "资源不存在",
    405: "请求方法不允许",
    409: "资源冲突",
    422: "数据验证失败",
    429: "请求过于频繁",
    500: "服务器内部错误",
    502: "网关错误",
    503: "服务暂时不可用",
}


def get_exception_mapping(exception: Exception) -> Dict:
    """获取异常的 HTTP 映射配置"""
    for exc_class, mapping in EXCEPTION_MAPPING.items():
        if isinstance(exception, exc_class):
            return mapping
    return {"status": 500, "category": ErrorCategory.SYSTEM}


def classify_exception(exception: Exception) -> str:
    """分类异常类型"""
    mapping = get_exception_mapping(exception)
    return mapping["category"]
