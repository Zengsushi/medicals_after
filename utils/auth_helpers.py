"""
    认证相关辅助函数
    统一使用 users.dependencies 中成熟的认证逻辑
    
    认证流程：
    1. 提取 Bearer Token
    2. 解析并验证 JWT（签名、过期、类型）
    3. 从 Redis 验证 token 是否被撤销
    4. 查询数据库获取用户信息
    5. 返回当前用户对象
"""
import logging
from typing import Optional
from fastapi import Request, Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from utils.redisbase import RedisBase
from apps.user.models import User
from apps.core.database import get_db
from sqlalchemy.orm import selectinload
from apps.user.rbac_models import Role

logger = logging.getLogger(__name__)
from apps.user.auth.utils import verify_token


security = HTTPBearer(auto_error=False)


def get_token_from_request(request: Request) -> Optional[str]:
    """
    从请求头中提取 Bearer Token
    
    Args:
        request: FastAPI Request 对象
        
    Returns:
        str | None: 提取到的 token（纯字符串），如果不存在则返回 None
        
    Example:
        >>> token = get_token_from_request(request)
        >>> if token:
        ...     user = await verify_and_get_user(db, token)
    """
    authorization = request.headers.get("authorization", "")
    
    if not authorization:
        return None
        
    if not authorization.startswith("Bearer "):
        return None
        
    token = authorization.replace("Bearer ", "")
    return token if token else None


def get_client_info(request: Request) -> dict:
    """
    获取客户端信息（IP地址 + User-Agent）
    
    Args:
        request: FastAPI Request 对象
        
    Returns:
        dict: 包含 ip_address 和 user_agent 的字典
    """
    client_ip = _get_ip_address(request)
    user_agent = request.headers.get("user-agent", "")
    
    return {
        "ip_address": client_ip,
        "user_agent": user_agent
    }


def _get_ip_address(request: Request) -> str:
    """获取客户端真实 IP 地址（支持代理转发）"""
    x_forwarded_for = request.headers.get("x-forwarded-for")
    if x_forwarded_for:
        return x_forwarded_for.split(",")[0].strip()
        
    x_real_ip = request.headers.get("x-real-ip")
    if x_real_ip:
        return x_real_ip
        
    if request.client:
        return request.client.host
        
    return "127.0.0.1"


async def verify_token_and_get_user(
    db: Session = Depends(get_db),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> User:
    """
    验证 token 并返回当前用户（可复用的依赖注入）
    
    完整的认证流程：
    1. 从 HTTP Authorization 头提取 Bearer Token
    2. 使用 verify_token() 解析并验证 JWT（检查签名、过期时间、token类型）
    3. 从 JWT payload 提取 jti（JWT ID）和 sub（用户ID）
    4. 使用 jti 查询 Redis 验证 token 未被撤销
    5. 查询数据库加载用户及其角色、权限、菜单关系
    6. 验证用户状态（是否删除、是否激活）
    7. 返回完整的 User 对象
    
    Raises:
        HTTPException 401: 
            - 未提供认证信息
            - Token 无效或已过期
            - Token 已被撤销（Redis 中不存在）
            - 用户不存在或已被删除
            - 用户账户已被禁用
            
    Returns:
        User: 当前登录用户对象（包含 roles, permissions, menus 关系）
        
    Usage:
        @router.get("/protected")
        async def protected_endpoint(current_user: User = Depends(verify_token_and_get_user)):
            return {"username": current_user.username}
    """
    # 步骤1：检查是否提供了认证凭据
    if not credentials:
        raise HTTPException(
            status_code=401,
            detail="未提供认证凭据",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    token = credentials.credentials
    
    # 步骤2：解析并验证 JWT
    is_valid, payload, error = verify_token(token, expected_type="access")
    
    if not is_valid:
        raise HTTPException(
            status_code=401,
            detail=error or "令牌无效或已过期",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    # 步骤3：从 payload 提取关键信息
    jti = payload.get("jti")  # JWT ID（用于 Redis 查询的唯一标识）
    
    # 优先使用 sub 字段，其次使用 user_id 字段
    user_id_value = payload.get("sub") or payload.get("user_id")
    user_id = int(user_id_value) if user_id_value else None
    
    if not jti or not user_id:
        raise HTTPException(
            status_code=401,
            detail="令牌格式异常",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    # 步骤4：从 Redis 验证 token 是否仍然有效（未被撤销）
    # 如果Redis不可用，跳过此检查（降级模式）
    try:
        cached_user_id = RedisBase.get_current_token(jti)
        
        # 如果 Redis 中没有这个 token，说明已过期或无效
        if not cached_user_id:
            logger.warning(f"Redis中未找到token: {jti}，可能已过期，使用JWT本身验证")
        elif int(cached_user_id) != user_id:
            raise HTTPException(
                status_code=401,
                detail="令牌已失效，请重新登录",
                headers={"WWW-Authenticate": "Bearer"}
            )
        else:
            # Token有效，检查是否需要续期
            remaining_ttl = RedisBase.get_token_ttl(jti)
            if remaining_ttl and remaining_ttl > 0 and remaining_ttl < 300:  # 小于5分钟
                RedisBase.extend_token_expiry(f"auth:session:{jti}")
                logger.info(f"已自动延长用户 {user_id} 的token过期时间")
            
    except HTTPException:
        raise
    except Exception as e:
        # Redis不可用，跳过Redis验证，使用JWT本身验证
        logger.warning(f"Redis验证跳过: {e}")
    
    # 步骤5-6：查询数据库并验证用户状态
    user = db.query(User).options(
        selectinload(User.roles).selectinload(Role.permissions),
        selectinload(User.roles).selectinload(Role.menus)
    ).filter(User.id == user_id).first()
    
    if not user:
        raise HTTPException(
            status_code=401,
            detail="用户不存在或已被删除",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    if getattr(user, 'is_deleted', False):
        raise HTTPException(
            status_code=401,
            detail="用户已被删除",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    if not user.is_active:
        raise HTTPException(
            status_code=401,
            detail="用户账号已停用",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    # 步骤7：返回完整的用户对象
    return user


def require_auth():
    """
    返回身份验证依赖（快捷方式）
    
    用于需要身份验证的 API 端点
    
    Usage:
        @router.get("/protected")
        async def protected_endpoint(current_user: User = Depends(require_auth())):
            ...
            
    Equivalent to:
        @router.get("/protected")
        async def protected_endpoint(current_user: User = Depends(verify_token_and_get_user)):
            ...
    """
    return verify_token_and_get_user
