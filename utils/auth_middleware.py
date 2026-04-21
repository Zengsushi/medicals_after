"""
统一认证中间件 (Unified Authentication Middleware)

提供统一的身份验证、权限检查、速率限制功能
消除重复代码，增强安全性
"""

import time
import hashlib
import logging
from functools import wraps
from typing import Optional, Callable, Any, List
from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from utils.redisbase import RedisBase
from apps.user.models import User
from apps.user.rbac_models import Role

logger = logging.getLogger(__name__)

security = HTTPBearer(auto_error=False)


class RateLimiter:
    """
    基于 Redis 的滑动窗口速率限制器
    
    用于防止暴力破解、DDoS 攻击等
    
    Example:
        limiter = RateLimiter(redis_client)
        
        # 登录接口限制：每分钟 5 次
        @limiter.limit("login:{ip}", max_requests=5, window=60)
        async def login(request: Request):
            ...
        
        # API 接口限制：每秒 100 次
        @limiter.limit("api:{user_id}", max_requests=100, window=1)
        async def api_endpoint(user_id: int):
            ...
    """
    
    def __init__(self, redis_client=None):
        self.redis = redis_client or RedisBase._client
    
    def limit(
        self,
        key_pattern: str,
        max_requests: int = 100,
        window: int = 60,
        error_message: str = "请求过于频繁，请稍后再试"
    ):
        """
        速率限制装饰器
        
        Args:
            key_pattern: 限制键模式，支持 {ip}, {user_id}, {endpoint} 等变量
            max_requests: 时间窗口内最大请求数
            window: 时间窗口（秒）
            error_message: 被限制时返回的错误信息
        """
        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                request = None
                
                # 从参数中提取 Request 对象
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break
                for val in kwargs.values():
                    if isinstance(val, Request):
                        request = val
                        break
                
                # 生成限制键
                key = key_pattern.format(
                    ip=self._get_client_ip(request) if request else "unknown",
                    user_id=getattr(request.state, 'user_id', 'anonymous') if request else "anonymous",
                    endpoint=func.__name__
                )
                
                # 检查是否超限
                current_count = self._increment_and_check(key, max_requests, window)
                
                if current_count > max_requests:
                    logger.warning(f"Rate limit exceeded: {key} ({current_count}/{max_requests})")
                    raise HTTPException(
                        status_code=429,
                        detail=error_message,
                        headers={
                            "X-RateLimit-Limit": str(max_requests),
                            "X-RateLimit-Remaining": "0",
                            "X-RateLimit-Reset": str(int(time.time()) + window),
                            "Retry-After": str(window)
                        }
                    )
                
                # 设置响应头（在成功后设置）
                result = await func(*args, **kwargs)
                
                return result
            
            return wrapper
        
        return decorator
    
    def _get_client_ip(self, request: Request) -> str:
        """获取客户端 IP"""
        x_forwarded_for = request.headers.get("x-forwarded-for", "")
        if x_forwarded_for:
            return x_forwarded_for.split(",")[0].strip()
        
        x_real_ip = request.headers.get("x-real-ip", "")
        if x_real_ip:
            return x_real_ip
        
        return request.client.host if request.client else "127.0.0.1"
    
    def _increment_and_check(self, key: str, limit: int, window: int) -> int:
        """
        滑动窗口算法实现速率限制
        
        使用 Redis Sorted Set 实现精确的滑动窗口：
        - 每次请求添加当前时间戳到 sorted set
        - 清除窗口外的旧记录
        - 返回当前窗口内的请求数
        """
        now = time.time()
        window_start = now - window
        
        pipe = self.redis.pipeline()
        
        # 移除窗口外的记录
        pipe.zremrangebyscore(key, 0, window_start)
        
        # 添加当前请求
        pipe.zadd(key, {str(now): now})
        
        # 设置过期时间（自动清理）
        pipe.expire(key, window + 1)
        
        # 统计当前窗口内请求数
        pipe.zcard(key)
        
        results = pipe.execute()
        count = results[-1]  # zcard 的结果
        
        return count


# 全局速率限制器实例
rate_limiter = RateLimiter()


class SecurityMiddleware:
    """
    安全中间件集合
    
    提供常见的安全检查和防护功能
    """
    
    # 敏感操作审计日志
    SENSITIVE_OPERATIONS = {
        'login': '用户登录',
        'logout': '用户登出',
        'password_change': '密码修改',
        'password_reset': '密码重置',
        'token_refresh': '令牌刷新',
        'user_create': '创建用户',
        'user_delete': '删除用户',
        'role_assign': '角色分配'
    }
    
    @staticmethod
    def check_password_strength(password: str) -> tuple[bool, str]:
        """
        检查密码强度
        
        Returns:
            (is_valid, error_message)
        """
        if len(password) < 8:
            return False, "密码长度至少为8位"
        
        if len(password) > 128:
            return False, "密码长度不能超过128位"
        
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)
        has_special = any(not c.isalnum() for c in password)
        
        conditions_met = sum([has_upper, has_lower, has_digit, has_special])
        
        if conditions_met < 3:
            return False, "密码必须包含大写字母、小写字母、数字、特殊字符中的至少3种"
        
        # 检查常见弱密码
        common_passwords = [
            '12345678', 'password', 'qwerty123', 'admin123',
            'letmein', 'welcome1', 'monkey123', 'abc12345'
        ]
        
        if password.lower() in [p.lower() for p in common_passwords]:
            return False, "不能使用常见的弱密码"
        
        return True, ""
    
    @staticmethod
    def validate_file_upload(file: Any, allowed_types: List[str], max_size_mb: int = 5) -> tuple[bool, str]:
        """
        验证上传文件的安全性
        
        Args:
            file: UploadFile 对象
            allowed_types: 允许的 MIME 类型列表
            max_size_mb: 最大文件大小（MB）
            
        Returns:
            (is_valid, error_message)
        """
        if not file.filename:
            return False, "文件名不能为空"
        
        # 检查文件扩展名
        dangerous_extensions = ['.exe', '.bat', '.cmd', '.ps1', '.sh', '.php', '.jsp', '.asp']
        file_ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
        
        if f'.{file_ext}' in dangerous_extensions:
            return False, "不允许上传可执行文件"
        
        # 检查 MIME 类型
        if file.content_type and file.content_type not in allowed_types:
            return False, f"不支持的文件类型: {file.content_type}"
        
        # 检查文件大小
        file.file.seek(0, 2)  # 移动到文件末尾
        file_size = file.file.tell()
        file.file.seek(0)      # 重置到开头
        
        if file_size > max_size_mb * 1024 * 1024:
            return False, f"文件大小超过限制 ({max_size_mb}MB)"
        
        return True, ""
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """
        清理文件名，防止路径穿越攻击
        
        Args:
            filename: 原始文件名
            
        Returns:
            安全的文件名
        """
        import os
        
        # 移除路径分隔符
        safe_name = os.path.basename(filename)
        
        # 移除危险字符
        dangerous_chars = ['..', '/', '\\', '\x00', '|', ';', '&', '$', '`']
        for char in dangerous_chars:
            safe_name = safe_name.replace(char, '_')
        
        # 限制长度
        if len(safe_name) > 255:
            name, ext = os.path.splitext(safe_name)
            safe_name = name[:255-len(ext)] + ext
        
        return safe_name


def get_auth_dependency(require_permissions: Optional[List[str]] = None):
    """
    创建统一认证依赖注入函数（与 utils.auth_helpers.verify_token_and_get_user 对齐）。

    Args:
        require_permissions: 需要的权限列表（可选）

    Usage:
        @router.get("/protected")
        async def endpoint(user: User = Depends(get_auth_dependency())):
            return {"user": user.username}
    """
    from utils.auth_helpers import verify_token_and_get_user

    async def dependency(user: User = Depends(verify_token_and_get_user)) -> User:
        if require_permissions:
            user_permissions = set(user.get_permissions())

            if 'admins:manage' in user_permissions or 'admins:view' in user_permissions:
                return user

            missing = [p for p in require_permissions if p not in user_permissions]
            if missing:
                raise HTTPException(
                    status_code=403,
                    detail={
                        "code": "PERMISSION_DENIED",
                        "message": f"缺少必要权限: {', '.join(missing)}",
                        "required": require_permissions,
                        "current": list(user_permissions)
                    }
                )

        return user

    return dependency


async def extract_request_metadata(request: Request) -> dict:
    """
    提取请求元数据（IP、User-Agent、设备指纹等）
    
    统一的数据提取逻辑，避免在各接口中重复编写
    """
    client_ip = _extract_ip(request)
    user_agent = request.headers.get("user-agent", "")
    
    metadata = {
        "ip_address": client_ip,
        "user_agent": user_agent,
        "method": request.method,
        "url": str(request.url.path),
        "content_length": request.headers.get("content-length"),
        "accept_language": request.headers.get("accept-language", ""),
        "referer": request.headers.get("referer", ""),
        "x_forwarded_for": request.headers.get("x-forwarded-for"),
        "timestamp": time.time()
    }
    
    # 存储到 request.state 以便后续使用
    request.state.metadata = metadata
    request.state.client_ip = client_ip
    
    return metadata


def _extract_ip(request: Request) -> str:
    """提取客户端真实 IP"""
    x_forwarded_for = request.headers.get("x-forwarded-for", "")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
    
    x_real_ip = request.headers.get("x-real-ip", "")
    if x_real_ip:
        return x_real_ip
    
    return request.client.host if request.client else "127.0.0.1"


def log_api_call(operation: str):
    """
    API 调用日志装饰器
    
    自动记录接口调用信息，包括：
    - 请求参数（脱敏）
    - 响应状态
    - 执行时间
    - 错误信息
    
    Usage:
        @log_api_call("user_login")
        async def login(request: Request, ...):
            ...
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            request = None
            
            # 提取 request 对象
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            for val in kwargs.values():
                if isinstance(val, Request):
                    request = val
                    break
            
            try:
                result = await func(*args, **kwargs)
                
                duration = time.time() - start_time
                
                if duration > 1.0:  # 超过 1s 的慢请求
                    logger.warning(
                        f"[SLOW API] {operation} took {duration:.2f}s "
                        f"- IP: {_extract_ip(request) if request else 'unknown'}"
                    )
                else:
                    logger.debug(
                        f"[API] {operation} completed in {duration:.3f}s"
                    )
                
                return result
                
            except HTTPException as e:
                duration = time.time() - start_time
                logger.error(
                    f"[API ERROR] {operation} failed after {duration:.3f}s "
                    f"- Status: {e.status_code} - Detail: {e.detail}"
                )
                raise
                
            except Exception as e:
                duration = time.time() - start_time
                logger.exception(
                    f"[API EXCEPTION] {operation} raised {type(e).__name__} after {duration:.3f}s"
                )
                raise
        
        return wrapper
    
    return decorator


class LoginAttemptTracker:
    """
    登录尝试跟踪器
    
    防止暴力破解：
    - 记录失败尝试次数
    - 超过阈值后锁定账户或增加延迟
    - 成功登录后重置计数
    """
    
    MAX_ATTEMPTS = 5          # 最大失败次数
    LOCKOUT_DURATION = 900     # 锁定时长（15分钟）
    INCREMENTAL_DELAY = True   # 是否启用递增延迟
    
    def __init__(self):
        self.redis = RedisBase._client
    
    def record_failed_attempt(self, identifier: str, ip: str) -> dict:
        """
        记录一次失败的登录尝试
        
        Returns:
            {
                "attempts": 当前失败次数,
                "locked": 是否被锁定,
                "lockout_remaining": 剩余锁定时间(秒),
                "next_retry_delay": 下次重试延迟(秒)
            }
        """
        key = f"login_attempts:{identifier}"
        lock_key = f"login_lockout:{identifier}"
        
        # 检查是否已被锁定
        ttl = self.redis.ttl(lock_key)
        if ttl > 0:
            return {
                "attempts": self.MAX_ATTEMPTS,
                "locked": True,
                "lockout_remaining": ttl,
                "next_retry_delay": ttl
            }
        
        # 增加失败计数
        attempts = self.redis.incr(key)
        
        if attempts == 1:
            # 第一次失败，设置过期时间（5分钟窗口）
            self.redis.expire(key, 300)
        
        # 计算下次重试延迟（指数退避）
        if self.INCREMENTAL_DELAY:
            delay = min(2 ** attempts, 60)  # 最大 60 秒延迟
        else:
            delay = 0
        
        # 检查是否需要锁定
        locked = attempts >= self.MAX_ATTEMPTS
        
        if locked:
            # 设置锁定
            self.redis.setex(lock_key, "", self.LOCKOUT_DURATION)
            self.redis.delete(key)
            
            logger.warning(
                f"Account locked due to too many failed attempts: {identifier} "
                f"- IP: {ip} - Attempts: {attempts}"
            )
        
        return {
            "attempts": attempts,
            "locked": locked,
            "lockout_remaining": self.LOCKOUT_DURATION if locked else 0,
            "next_retry_delay": delay
        }
    
    def record_successful_login(self, identifier: str):
        """
        记录成功的登录，重置失败计数
        """
        key = f"login_attempts:{identifier}"
        lock_key = f"login_lockout:{identifier}"
        
        self.redis.delete(key)
        self.redis.delete(lock_key)
        
        logger.info(f"Successful login recorded for: {identifier}")
    
    def is_locked(self, identifier: str) -> bool:
        """检查账户是否被锁定"""
        lock_key = f"login_lockout:{identifier}"
        return self.redis.exists(lock_key) > 0


# 全局实例
login_tracker = LoginAttemptTracker()


__all__ = [
    'RateLimiter',
    'rate_limiter',
    'SecurityMiddleware',
    'get_auth_dependency',
    'extract_request_metadata',
    'log_api_call',
    'LoginAttemptTracker',
    'login_tracker'
]
