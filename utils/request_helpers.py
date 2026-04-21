"""
    请求相关辅助函数
    封装 IP 地址、User-Agent 等请求信息的提取逻辑
"""
from fastapi import Request
from typing import Dict, Any


def get_client_ip(request: Request) -> str:
    """
    获取客户端 IP 地址
    
    Args:
        request: FastAPI Request 对象
        
    Returns:
        str: 客户端 IP 地址
    """
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
        
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip
        
    if request.client:
        return request.client.host
        
    return "127.0.0.1"


def get_user_agent(request: Request) -> str:
    """
    获取客户端 User-Agent
    
    Args:
        request: FastAPI Request 对象
        
    Returns:
        str: User-Agent 字符串
    """
    return request.headers.get("user-agent", "")


def extract_request_metadata(request: Request) -> Dict[str, Any]:
    """
    提取完整的请求元数据
    
    包含：IP地址、User-Agent、设备指纹等
    
    Args:
        request: FastAPI Request 对象
        
    Returns:
        dict: 请求元数据字典
    """
    from apps.user.auth.utils import generate_device_fingerprint
    
    client_info = {
        "ip_address": get_client_ip(request),
        "user_agent": get_user_agent(request),
        "device_fingerprint": generate_device_fingerprint(
            user_agent=get_user_agent(request),
            ip_address=get_client_ip(request)
        ),
        "method": request.method,
        "url": str(request.url)
    }
    
    return client_info
