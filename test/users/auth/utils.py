"""
    JWT 和设备指纹工具
    遵循 RFC 8725 JWT 最佳实践
"""
import os
import re
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple
import jwt
from jwt.exceptions import InvalidTokenError, ExpiredSignatureError
from dotenv import load_dotenv

load_dotenv()

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", os.getenv("SECRET_KEY", "your-secret-key-change-in-production"))
JWT_ALGORITHM = "HS256"

JWT_ISSUER = os.getenv("JWT_ISSUER", "medical-data-system")
JWT_AUDIENCE = os.getenv("JWT_AUDIENCE", "medical-data-api")

ACCESS_TOKEN_EXPIRE_MINUTES = 15
REFRESH_TOKEN_EXPIRE_DAYS = 7
PASSWORD_RESET_TOKEN_EXPIRE_HOURS = 1

BCRYPT_ROUNDS = 12

WEAK_PASSWORDS = [
    "123456", "password", "12345678", "qwerty", "123456789",
    "12345", "1234", "111111", "1234567", "abc123",
    "password1", "admin", "letmein", "welcome", "monkey",
    "1234567890", "password123", "admin123", "root", "toor",
]


def generate_jti() -> str:
    """生成唯一的JWT ID (jti) - RFC 8725 推荐"""
    return secrets.token_urlsafe(16)


def generate_device_fingerprint(
    user_agent: str = None,
    accept_language: str = None,
    screen: str = None,
    ip_address: str = None
) -> str:
    """
    生成设备指纹
    通过组合多个浏览器特征生成唯一标识
    """
    parts = [
        user_agent or "",
        accept_language or "",
        screen or "",
        ip_address or "",
        secrets.token_hex(8)
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def extract_device_info(user_agent: str = None, accept_language: str = None) -> Dict[str, str]:
    """
    从 User-Agent 提取设备信息
    """
    info = {
        "device_type": "unknown",
        "browser": "unknown",
        "os": "unknown"
    }

    if not user_agent:
        return info

    ua = user_agent.lower()

    if "mobile" in ua or "android" in ua or "iphone" in ua:
        info["device_type"] = "mobile"
    elif "tablet" in ua or "ipad" in ua:
        info["device_type"] = "tablet"
    else:
        info["device_type"] = "desktop"

    browser_patterns = [
        (r"edge/(\d+)", "Edge"),
        (r"chrome/(\d+)", "Chrome"),
        (r"firefox/(\d+)", "Firefox"),
        (r"safari/(\d+)", "Safari"),
        (r"opera/(\d+)", "Opera"),
        (r"msie (\d+)", "IE"),
    ]
    for pattern, name in browser_patterns:
        if re.search(pattern, ua):
            info["browser"] = name
            break

    os_patterns = [
        (r"windows nt 10", "Windows 10"),
        (r"windows nt 6\.3", "Windows 8.1"),
        (r"windows", "Windows"),
        (r"mac os x", "macOS"),
        (r"iphone|ipad", "iOS"),
        (r"android", "Android"),
        (r"linux", "Linux"),
    ]
    for pattern, name in os_patterns:
        if re.search(pattern, ua):
            info["os"] = name
            break

    return info


def parse_location_from_ip(ip_address: str) -> Optional[str]:
    """
    从IP地址解析地理位置 (简化版)
    实际生产中应使用 IP 地理位置库或 API
    """
    if not ip_address or ip_address in ["127.0.0.1", "localhost", "::1"]:
        return "本地"

    if ip_address.startswith("192.168.") or ip_address.startswith("10."):
        return "内网"

    return f"IP: {ip_address}"


def create_access_token(
    user_id: int,
    username: str,
    jti: str = None,
    device_fingerprint: str = None,
    role: str = None,
    expires_delta: timedelta = None,
    additional_claims: Dict[str, Any] = None
) -> Tuple[str, datetime]:
    """
    创建访问令牌 (Access Token)
    遵循 RFC 8725 最佳实践:
    - iss (issuer): 令牌发行者
    - aud (audience): 令牌受众
    - jti (JWT ID): 唯一标识符，用于令牌撤销
    - exp (expiration time): 过期时间
    - iat (issued at): 发行时间
    - sub (subject): 主题/用户ID
    """
    if not jti:
        jti = generate_jti()

    if not expires_delta:
        expires_delta = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    now = datetime.utcnow()
    expire = now + expires_delta

    payload = {
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "sub": str(user_id),
        "username": username,
        "jti": jti,
        "type": "access",
        "role": role,
        "device_fingerprint": device_fingerprint,
        "exp": expire,
        "iat": now,
    }

    if additional_claims:
        payload.update(additional_claims)

    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return token, expire


def create_refresh_token(
    user_id: int,
    username: str,
    device_fingerprint: str = None,
    expires_delta: timedelta = None,
    additional_claims: Dict[str, Any] = None
) -> Tuple[str, str, datetime]:
    """
    创建刷新令牌 (Refresh Token)
    遵循 RFC 8725 最佳实践
    """
    jti = generate_jti()

    if not expires_delta:
        expires_delta = timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    now = datetime.utcnow()
    expire = now + expires_delta

    payload = {
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "sub": str(user_id),
        "username": username,
        "jti": jti,
        "type": "refresh",
        "device_fingerprint": device_fingerprint,
        "exp": expire,
        "iat": now,
    }

    if additional_claims:
        payload.update(additional_claims)

    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return token, jti, expire


def create_password_reset_token(user_id: int) -> Tuple[str, str, datetime]:
    """
    创建密码重置令牌
    时效1小时，单次使用
    遵循 RFC 8725 最佳实践
    """
    jti = generate_jti()
    now = datetime.utcnow()
    expire = now + timedelta(hours=PASSWORD_RESET_TOKEN_EXPIRE_HOURS)

    payload = {
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "sub": str(user_id),
        "jti": jti,
        "type": "password_reset",
        "exp": expire,
        "iat": now,
    }

    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return token, jti, expire


def verify_token(
    token: str,
    expected_type: str = "access",
    verify_exp: bool = True,
    verify_aud: bool = True,
    verify_iss: bool = True
) -> Tuple[bool, Dict[str, Any], str]:
    """
    验证JWT令牌
    遵循 RFC 8725 最佳实践验证:
    - typ (token type)
    - iss (issuer)
    - aud (audience)
    - exp (expiration)
    - jti (JWT ID)
    """
    try:
        options = {
            "verify_exp": verify_exp,
            "verify_signature": True,
            "require": ["exp", "iat", "sub"]
        }

        if verify_iss:
            options["verify_iss"] = True
        else:
            options["verify_iss"] = False

        if verify_aud:
            options["verify_aud"] = True
        else:
            options["verify_aud"] = False

        payload = jwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM],
            audience=JWT_AUDIENCE if verify_aud else None,
            issuer=JWT_ISSUER if verify_iss else None,
            options=options
        )

        token_type = payload.get("type")
        if token_type != expected_type:
            return False, {}, f"令牌类型不匹配: 期望 {expected_type}，得到 {token_type}"

        return True, payload, ""

    except ExpiredSignatureError:
        return False, {}, "令牌已过期"
    except InvalidTokenError as e:
        return False, {}, f"令牌无效: {str(e)}"


def decode_token_unsafe(token: str) -> Optional[Dict[str, Any]]:
    """
    不验证直接解码token (用于日志等)
    警告: 仅用于调试目的
    """
    try:
        return jwt.decode(token, options={"verify_signature": False})
    except Exception:
        return None


def hash_token(token: str) -> str:
    """对token进行哈希存储"""
    return hashlib.sha256(token.encode()).hexdigest()


def verify_password_strength(password: str) -> Tuple[bool, str]:
    """
    验证密码强度
    返回: (is_valid, error_message)
    """
    if len(password) < 8:
        return False, "密码长度至少8个字符"

    if not re.search(r"[a-z]", password):
        return False, "密码必须包含小写字母"

    if not re.search(r"[A-Z]", password):
        return False, "密码必须包含大写字母"

    if not re.search(r"\d", password):
        return False, "密码必须包含数字"

    if password.lower() in WEAK_PASSWORDS or password.lower() in [p + "1" for p in WEAK_PASSWORDS]:
        return False, "密码太弱，请使用更复杂的密码"

    return True, ""


def check_concurrent_sessions(user_id: int, max_sessions: int = 3) -> bool:
    """
    检查用户并发会话数是否超限
    应由调用者查询 Redis/数据库实现
    """
    return True