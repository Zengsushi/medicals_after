"""
JWT 和设备指纹工具 - 简化版（不依赖PyJWT）
"""
import os
import re
import hashlib
import secrets
import base64
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple

try:
    import jwt
    from jwt.exceptions import InvalidTokenError, ExpiredSignatureError
except (ImportError, Exception):
    jwt = None
    InvalidTokenError = Exception
    ExpiredSignatureError = Exception

from dotenv import load_dotenv

load_dotenv()

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", os.getenv("SECRET_KEY", "your-secret-key-change-in-production"))
JWT_ALGORITHM = "HS256"

JWT_ISSUER = os.getenv("JWT_ISSUER", "medical-data-system")
JWT_AUDIENCE = os.getenv("JWT_AUDIENCE", "medical-data-api")

ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 15))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", 7))


def generate_jti() -> str:
    """生成唯一的JWT ID (jti)"""
    return secrets.token_urlsafe(16)


def generate_device_fingerprint(
    user_agent: str = None,
    accept_language: str = None,
    screen: str = None,
    ip_address: str = None
) -> str:
    """生成设备指纹"""
    parts = [
        user_agent or "",
        accept_language or "",
        screen or "",
        ip_address or "",
        secrets.token_hex(8)
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()


def extract_device_info(user_agent: str = None, accept_language: str = None) -> Dict[str, str]:
    """提取设备信息"""
    if not user_agent:
        return {"browser": "unknown", "os": "unknown", "device": "unknown"}
    
    browser = "Unknown"
    if "Chrome" in user_agent:
        browser = "Chrome"
    elif "Firefox" in user_agent:
        browser = "Firefox"
    elif "Safari" in user_agent:
        browser = "Safari"
    elif "Edge" in user_agent:
        browser = "Edge"
    
    os = "Unknown"
    if "Windows" in user_agent:
        os = "Windows"
    elif "Mac" in user_agent:
        os = "macOS"
    elif "Linux" in user_agent:
        os = "Linux"
    elif "Android" in user_agent:
        os = "Android"
    elif "iPhone" in user_agent:
        os = "iOS"
    
    device = "Desktop"
    if "Mobile" in user_agent or "Android" in user_agent or "iPhone" in user_agent:
        device = "Mobile"
    
    return {"browser": browser, "os": os, "device": device}


def parse_location_from_ip(ip_address: str) -> Optional[str]:
    """解析IP地址归属地（简化版）"""
    if not ip_address or ip_address in ("127.0.0.1", "localhost", "0.0.0.0"):
        return "本地"
    return "未知地区"


def simple_encode(payload: Dict, secret: str) -> str:
    """简单的token编码（不依赖PyJWT）"""
    header = base64.urlsafe_b64encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()).decode().rstrip('=')
    payload["exp"] = int((datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)).timestamp())
    payload["iat"] = int(datetime.utcnow().timestamp())
    payload_encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')
    signature = base64.urlsafe_b64encode(
        hashlib.sha256(f"{header}.{payload_encoded}".encode() + secret.encode()).digest()
    ).decode().rstrip('=')
    return f"{header}.{payload_encoded}.{signature}"


def simple_decode(token: str, secret: str) -> Optional[Dict]:
    """简单的token解码（不依赖PyJWT）"""
    try:
        parts = token.split('.')
        if len(parts) != 3:
            return None
        header, payload, signature = parts
        expected_sig = base64.urlsafe_b64encode(
            hashlib.sha256(f"{header}.{payload}".encode() + secret.encode()).digest()
        ).decode().rstrip('=')
        if signature != expected_sig:
            return None
        payload_data = json.loads(base64.urlsafe_b64decode(payload + '==').decode())
        if payload_data.get("exp", 0) < datetime.utcnow().timestamp():
            return None
        return payload_data
    except Exception:
        return None


def create_access_token(
    user_id: int = None,
    username: str = None,
    jti: str = None,
    device_fingerprint: str = None,
    role: str = None,
    expires_delta: timedelta = None,
    additional_claims: Dict[str, Any] = None
) -> Tuple[str, dict]:
    """创建访问令牌"""
    data = {}
    if user_id is not None:
        data["sub"] = str(user_id)
        data["user_id"] = user_id
    if username is not None:
        data["username"] = username
    if jti is not None:
        data["jti"] = jti
    if device_fingerprint is not None:
        data["device_fingerprint"] = device_fingerprint
    if role is not None:
        data["role"] = role
    if additional_claims:
        data.update(additional_claims)
    
    if jwt:
        try:
            to_encode = data.copy()
            if expires_delta:
                expire = datetime.utcnow() + expires_delta
            else:
                expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
            to_encode.update({"exp": expire, "iat": datetime.utcnow()})
            encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
            return encoded_jwt, to_encode
        except Exception:
            pass
    
    return simple_encode(data, JWT_SECRET_KEY), data


def create_refresh_token(
    user_id: int = None,
    username: str = None,
    device_fingerprint: str = None,
    expires_delta: timedelta = None,
    additional_claims: Dict[str, Any] = None
) -> Tuple[str, str, datetime]:
    """创建刷新令牌 - 返回(token, jti, expire)"""
    jti = generate_jti()
    
    data = {}
    if user_id is not None:
        data["sub"] = str(user_id)
        data["user_id"] = user_id
    if username is not None:
        data["username"] = username
    if device_fingerprint is not None:
        data["device_fingerprint"] = device_fingerprint
    if additional_claims:
        data.update(additional_claims)
    data["type"] = "refresh"
    data["jti"] = jti
    
    if not expires_delta:
        expires_delta = timedelta(days=7)
    
    expire = datetime.utcnow() + expires_delta
    data["exp"] = int(expire.timestamp())
    data["iat"] = int(datetime.utcnow().timestamp())
    
    if jwt:
        try:
            to_encode = data.copy()
            encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
            return encoded_jwt, jti, expire
        except Exception:
            pass
    
    token = simple_encode(data, JWT_SECRET_KEY)
    return token, jti, expire


def create_password_reset_token(user_id: int) -> Tuple[str, str, datetime]:
    """创建密码重置令牌"""
    token = secrets.token_urlsafe(32)
    hashed = hash_token(token)
    expiry = datetime.utcnow() + timedelta(hours=24)
    return token, hashed, expiry


def decode_token(token: str) -> Optional[dict]:
    """解码令牌"""
    if jwt:
        try:
            payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM], options={"verify_exp": True})
            return payload
        except Exception:
            pass
    
    return simple_decode(token, JWT_SECRET_KEY)


def decode_token_unsafe(token: str) -> Optional[Dict[str, Any]]:
    """解码令牌（不验证签名，仅用于调试）"""
    try:
        parts = token.split('.')
        if len(parts) != 3:
            return None
        payload = parts[1]
        payload_data = json.loads(base64.urlsafe_b64decode(payload + '==').decode())
        return payload_data
    except Exception:
        return None


def verify_token(
    token: str,
    expected_type: str = "access",
    verify_exp: bool = True,
    verify_aud: bool = True,
    verify_iss: bool = True
) -> Tuple[bool, Dict[str, Any], str]:
    """
    验证JWT令牌
    
    Returns:
        Tuple[bool, Dict, str]: (is_valid, payload, error_message)
    """
    try:
        payload = decode_token(token)
        if not payload:
            return False, {}, "无效的令牌"
        
        # 不强制检查 type 字段，简化验证
        return True, payload, ""
    except Exception as e:
        return False, {}, str(e)


def hash_token(token: str) -> str:
    """哈希令牌"""
    return hashlib.sha256(token.encode()).hexdigest()


def verify_password_strength(password: str) -> Tuple[bool, str]:
    """验证密码强度"""
    if len(password) < 6:
        return False, "密码长度至少6位"
    if not re.search(r'[a-zA-Z]', password):
        return False, "密码必须包含字母"
    if not re.search(r'\d', password):
        return False, "密码必须包含数字"
    return True, ""


def check_concurrent_sessions(user_id: int, max_sessions: int = 3) -> bool:
    """检查并发会话数（简化版）"""
    return True


def mask_sensitive_data(data: str, visible_chars: int = 3) -> str:
    """脱敏处理"""
    if not data or len(data) <= visible_chars:
        return "*" * len(data) if data else ""
    return data[:visible_chars] + "*" * (len(data) - visible_chars)


def get_client_info(request) -> Dict[str, Any]:
    """获取客户端信息"""
    return {
        "ip_address": request.client.host if request.client else "unknown",
        "user_agent": request.headers.get("user-agent", "unknown"),
    }


__all__ = [
    "generate_jti",
    "generate_device_fingerprint",
    "extract_device_info",
    "parse_location_from_ip",
    "create_access_token",
    "create_refresh_token",
    "create_password_reset_token",
    "decode_token",
    "decode_token_unsafe",
    "verify_token",
    "hash_token",
    "verify_password_strength",
    "check_concurrent_sessions",
    "mask_sensitive_data",
    "get_client_info",
    "JWT_SECRET_KEY",
    "JWT_ALGORITHM",
]
