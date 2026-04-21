import os
import time
import uuid

import redis
import dotenv
import hashlib
from fastapi import HTTPException

from utils.result import Result

# 加载环境变量
dotenv.load_dotenv()


class TokenAnalysisError(Exception):
    pass


class TokenAnalysisIsNone(Exception):
    pass


class TokenClearError(Exception):
    pass


class RedisBase:
    _client = redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
        decode_responses=True,
        socket_connect_timeout=float(os.getenv("REDIS_CONNECT_TIMEOUT", "1.0")),
        socket_timeout=float(os.getenv("REDIS_SOCKET_TIMEOUT", "2.0")),
        health_check_interval=int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30")),
    )

    expire_time = int(os.getenv("TOKEN_EXPIRE_SECONDS", 3600))

    @staticmethod
    def _gen_token(user: 'User') -> str:
        raw = f"{user.id}:{uuid.uuid4()}:{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()

    @classmethod
    def _set(cls, key, value, expire=None):
        if expire:
            cls._client.setex(key, expire, value)
        else:
            cls._client.set(key, value, ex=cls.expire_time)

    @classmethod
    def set_with_expiry(cls, key: str, value: any, expire: int):
        """设置带过期时间的键值对"""
        cls._client.setex(key, expire, value)

    @classmethod
    def delete_token(cls, user_id: int):
        token = cls._client.get(f"user_token:{user_id}")
        if token:
            cls._client.delete(f"token:{token}")
            cls._client.delete(f"user_token:{user_id}")

    @classmethod
    def delete_session(cls, jti: str):
        """删除会话密钥"""
        cls._client.delete(f"auth:session:{jti}")

    @classmethod
    def create_token(cls, user: 'User') -> str:
        """
        单用户单token认证模型
        """

        token = cls._gen_token(user)

        user_key = f"user_token:{user.id}"
        token_key = f"token:{token}"
        old_token = cls._client.get(user_key)

        if old_token:
            print(old_token)
            cls._client.delete(f"token:{old_token}")

        # 双向绑定 身份认证
        cls._set(user_key, token)
        cls._set(token_key, user.id)

        return token

    @classmethod
    def get_current_token(cls, jti: str):
        """
        获取当前用户ID（通过 JWT 的 jti 查询）
        
        支持两种 Redis Key 格式以兼容新旧系统：
        1. 新格式: auth:session:{jti} (JWT 系统使用)
        2. 旧格式: token:{jti} (旧系统使用)
        
        Args:
            jti: JWT ID (从 JWT payload 中提取)
            
        Returns:
            int: 用户ID
            
        Raises:
            HTTPException 401: Token 无效或已过期
            HTTPException 500: 认证异常
        """
        try:
            # 优先查询新格式的 key (JWT 系统)
            user_id = cls._client.get(f"auth:session:{jti}")
            
            # 如果新格式没找到，尝试旧格式 (向后兼容)
            if not user_id:
                user_id = cls._client.get(f"token:{jti}")
                
            if not user_id:
                raise HTTPException(status_code=401, detail="令牌已失效，请重新登录")
                
            return int(user_id)
            
        except HTTPException as e:
            raise
            
        except Exception as e:
            import logging
            logging.error(f"Redis 查询异常: {e}")
            raise HTTPException(status_code=500, detail="认证Token解析异常")

    @classmethod
    def ping(cls) -> bool:
        try:
            return bool(cls._client.ping())
        except Exception:
            return False

    @classmethod
    def get_token_ttl(cls, jti: str) -> int:
        """
        获取token的剩余过期时间（秒）
        
        Args:
            jti: JWT ID
            
        Returns:
            int: 剩余过期时间（秒），如果键不存在则返回0
        """
        try:
            # 优先查询新格式的 key
            ttl = cls._client.ttl(f"auth:session:{jti}")
            
            # 如果新格式没找到，尝试旧格式
            if ttl <= 0:
                ttl = cls._client.ttl(f"token:{jti}")
                
            return ttl
        except Exception:
            return 0

    @classmethod
    def extend_token_expiry(cls, key: str, expire: int = None):
        """
        延长token的过期时间
        
        Args:
            key: Redis键名
            expire: 过期时间（秒），如果不提供则使用默认过期时间
        """
        try:
            if expire:
                cls._client.expire(key, expire)
            else:
                cls._client.expire(key, cls.expire_time)
        except Exception:
            pass
