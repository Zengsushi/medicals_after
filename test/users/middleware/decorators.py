"""
    权限检查装饰器（增强版）
    提供细粒度的功能级权限控制 + 审计日志
"""
import functools
import logging
from datetime import datetime
from typing import Callable, List, Optional, Union, Any
from fastapi import HTTPException, Request
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN

from apps.user.models import User

logger = logging.getLogger(__name__)

_audit_log_buffer = []
AUDIT_BUFFER_MAX = 1000


class PermissionDeniedError(Exception):
    """权限拒绝异常"""
    def __init__(self, message: str, required_permissions: List[str] = None, allowed_permissions: List[str] = None):
        self.message = message
        self.required_permissions = required_permissions or []
        self.allowed_permissions = allowed_permissions or []
        super().__init__(self.message)


class AuditLogEntry:
    """审计日志条目"""

    def __init__(
        self,
        user_id: int = None,
        username: str = None,
        action: str = '',
        resource: str = '',
        permission_code: str = '',
        result: str = 'denied',
        ip_address: str = '',
        user_agent: str = '',
        details: dict = None
    ):
        self.user_id = user_id
        self.username = username
        self.action = action
        self.resource = resource
        self.permission_code = permission_code
        self.result = result  # 'denied' | 'granted'
        self.ip_address = ip_address
        self.user_agent = user_agent
        self.details = details or {}
        self.timestamp = datetime.now()

    def to_dict(self) -> dict:
        return {
            'user_id': self.user_id,
            'username': self.username,
            'action': self.action,
            'resource': self.resource,
            'permission_code': self.permission_code,
            'result': self.result,
            'ip_address': self.ip_address,
            'user_agent': self.user_agent[:200] if self.user_agent else '',
            'details': self.details,
            'timestamp': self.timestamp.isoformat()
        }


def record_audit(entry: AuditLogEntry):
    """记录审计日志到缓冲区"""
    _audit_log_buffer.append(entry)
    if len(_audit_log_buffer) > AUDIT_BUFFER_MAX:
        _audit_log_buffer.pop(0)


def get_audit_buffer() -> List[dict]:
    """获取审计日志缓冲区"""
    return [e.to_dict() for e in _audit_log_buffer]


def clear_audit_buffer():
    """清空审计日志缓冲区"""
    _audit_log_buffer.clear()


def extract_client_info(request: Request = None) -> dict:
    """从请求中提取客户端信息"""
    if not request:
        return {'ip_address': '', 'user_agent': ''}

    forwarded = request.headers.get('x-forwarded-for')
    if forwarded:
        ip_address = forwarded.split(',')[0].strip()
    else:
        ip_address = request.client.host if request.client else ''

    return {
        'ip_address': ip_address,
        'user_agent': request.headers.get('user-agent', '')
    }


def check_permission(*permission_codes: str):
    """
    权限检查装饰器 - 细粒度权限控制（增强版，含审计日志）

    用法:
        @check_permission('users:view')
        @check_permission('users:view', 'users:edit')

    特性:
        - 自动记录越权尝试到审计日志
        - 支持从 Request 对象提取客户端信息
        - 返回详细的 403 错误信息
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            current_user: User = kwargs.get('current_user')
            request: Request = kwargs.get('request')

            if not current_user:
                for arg in args:
                    if isinstance(arg, User):
                        current_user = arg
                        break
                    if isinstance(arg, Request):
                        request = arg

            if not current_user:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail="需要登录后才能访问"
                )

            required_perms = set(permission_codes)
            user_perms = set(current_user.get_permissions())

            has_permission = bool(required_perms.intersection(user_perms))
            client_info = extract_client_info(request)

            if not has_permission:
                missing_perms = required_perms - user_perms

                audit_entry = AuditLogEntry(
                    user_id=current_user.id,
                    username=current_user.username,
                    action=func.__name__,
                    resource=getattr(func, '__qualname__', ''),
                    permission_code=', '.join(permission_codes),
                    result='denied',
                    ip_address=client_info['ip_address'],
                    user_agent=client_info['user_agent'],
                    details={
                        'missing_permissions': list(missing_perms),
                        'user_permissions': list(user_perms),
                        'endpoint': func.__qualname__
                    }
                )
                record_audit(audit_entry)

                logger.warning(
                    f"[PERMISSION_DENIED] 用户 {current_user.username}(id={current_user.id}) "
                    f"尝试访问需要权限 {required_perms} 的资源，"
                    f"缺少权限 {missing_perms}, "
                    f"IP={client_info['ip_address']}"
                )

                raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN,
                    detail={
                        "code": "PERMISSION_DENIED",
                        "message": f"缺少必要权限: {', '.join(missing_perms)}",
                        "required": list(required_perms),
                        "allowed": list(user_perms),
                        "audit_id": len(_audit_log_buffer)
                    }
                )

            if logger.isEnabledFor(logging.DEBUG):
                audit_entry = AuditLogEntry(
                    user_id=current_user.id,
                    username=current_user.username,
                    action=func.__name__,
                    permission_code=', '.join(permission_codes),
                    result='granted',
                    ip_address=client_info['ip_address']
                )
                record_audit(audit_entry)

            return await func(*args, **kwargs)
        return wrapper
    return decorator


def check_permissions_all(*permission_codes: str):
    """
    权限检查装饰器 - 需要拥有所有指定权限
    用法: @check_permissions_all('users:view', 'users:edit')
    """
    return check_permission(*permission_codes)


def check_permissions_any(*permission_codes: str):
    """
    权限检查装饰器 - 拥有任一指定权限即可
    用法: @check_permissions_any('users:delete', 'admins:manage')
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            current_user: User = kwargs.get('current_user')
            request: Request = kwargs.get('request')

            if not current_user:
                for arg in args:
                    if isinstance(arg, User):
                        current_user = arg
                        break
                    if isinstance(arg, Request):
                        request = arg

            if not current_user:
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail="需要登录后才能访问"
                )

            required_perms = set(permission_codes)
            user_perms = set(current_user.get_permissions())

            has_any_permission = bool(required_perms.intersection(user_perms))

            if not has_any_permission:
                client_info = extract_client_info(request)

                audit_entry = AuditLogEntry(
                    user_id=current_user.id,
                    username=current_user.username,
                    action=func.__name__,
                    permission_code=f'any({", ".join(permission_codes)})',
                    result='denied',
                    ip_address=client_info['ip_address'],
                    details={
                        'required_any_of': list(required_perms),
                        'user_permissions': list(user_perms)
                    }
                )
                record_audit(audit_entry)

                logger.warning(
                    f"[PERMISSION_DENIED] 用户 {current_user.username} "
                    f"缺少所有必要权限 {required_perms}"
                )

                raise HTTPException(
                    status_code=HTTP_403_FORBIDDEN,
                    detail={
                        "code": "PERMISSION_DENIED",
                        "message": f"需要以下任一权限: {', '.join(required_perms)}",
                        "required": list(required_perms),
                        "allowed": list(user_perms)
                    }
                )

            return await func(*args, **kwargs)
        return wrapper
    return decorator


def check_resource_permission(resource: str, action: str):
    """
    资源操作权限检查装饰器
    用法: @check_resource_permission('users', 'delete')

    生成权限代码: users:delete
    """
    permission_code = f"{resource}:{action}"
    return check_permission(permission_code)


def check_resource_any_action(resource: str, *actions: str):
    """
    资源任意操作权限检查
    用法: @check_resource_any_action('users', 'view', 'edit', 'delete')
    """
    permission_codes = [f"{resource}:{action}" for action in actions]
    return check_permissions_any(*permission_codes)


class PermissionChecker:
    """细粒度权限检查工具类"""

    @staticmethod
    def can_view(user: User, resource: str) -> bool:
        return user.has_permission(f"{resource}:view")

    @staticmethod
    def can_create(user: User, resource: str) -> bool:
        return user.has_permission(f"{resource}:create")

    @staticmethod
    def can_update(user: User, resource: str) -> bool:
        return user.has_permission(f"{resource}:update")

    @staticmethod
    def can_delete(user: User, resource: str) -> bool:
        return user.has_permission(f"{resource}:delete")

    @staticmethod
    def can_export(user: User, resource: str) -> bool:
        return user.has_permission(f"{resource}:export")

    @staticmethod
    def can_import(user: User, resource: str) -> bool:
        return user.has_permission(f"{resource}:import")

    @staticmethod
    def get_available_actions(user: User, resource: str) -> List[str]:
        actions = []
        action_map = {
            'view': PermissionChecker.can_view,
            'create': PermissionChecker.can_create,
            'update': PermissionChecker.can_update,
            'delete': PermissionChecker.can_delete,
            'export': PermissionChecker.can_export,
            'import': PermissionChecker.can_import,
        }

        for action, check_func in action_map.items():
            if check_func(user, resource):
                actions.append(action)

        return actions

    @staticmethod
    def filter_by_permission(user: User, resource: str, items: List[dict]) -> List[dict]:
        if user.has_permission(f"{resource}:view"):
            return items
        return []

    @staticmethod
    def build_permission_matrix(user: User, resources: List[str]) -> dict:
        matrix = {}
        for resource in resources:
            matrix[resource] = {
                'view': PermissionChecker.can_view(user, resource),
                'create': PermissionChecker.can_create(user, resource),
                'update': PermissionChecker.can_update(user, resource),
                'delete': PermissionChecker.can_delete(user, resource),
                'export': PermissionChecker.can_export(user, resource),
                'import': PermissionChecker.can_import(user, resource),
            }
        return matrix


PERMISSION_CATEGORIES = {
    "用户管理": ["users:view", "users:create", "users:update", "users:delete", "users:export", "users:import"],
    "角色管理": ["role:view", "role:create", "role:update", "role:delete", "role:export", "role:import"],
    "权限管理": ["permission:view", "permission:create", "permission:update", "permission:delete", "permission:export", "permission:import"],
    "菜单管理": ["menu:view", "menu:create", "menu:update", "menu:delete", "menu:export", "menu:import"],
    "字典管理": ["dict:view", "dict:create", "dict:update", "dict:delete", "dict:export", "dict:import"],
    "数据源管理": ["source:view", "source:create", "source:update", "source:delete", "source:export", "source:import"],
    "可视化": ["visual:view", "visual:large", "visual:export"],
    "系统管理": ["admin:view", "admin:manage"],
}


def get_all_permissions() -> List[dict]:
    """获取所有定义的权限列表"""
    permissions = []
    for category, perms in PERMISSION_CATEGORIES.items():
        for perm in perms:
            module, action = perm.split(':')
            permissions.append({
                "code": perm,
                "module": module,
                "action": action,
                "category": category
            })
    return permissions