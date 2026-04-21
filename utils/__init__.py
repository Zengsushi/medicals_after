"""
    全局工具模块
    集中管理所有可复用的工具函数
    注意：避免循环导入，采用延迟导入方式
"""

__all__ = [
    'get_token_from_request',
    'get_client_info',
    'verify_token_and_get_user',
    'require_auth',
    'get_client_ip',
    'get_user_agent',
    'extract_request_metadata'
]

from fastapi import Depends


def get_token_from_request(request):
    """延迟导入，避免循环依赖"""
    from utils.auth_helpers import get_token_from_request as _func
    return _func(request)


def get_client_info(request):
    """延迟导入，避免循环依赖"""
    from utils.auth_helpers import get_client_info as _func
    return _func(request)


def verify_token_and_get_user(*args, **kwargs):
    """延迟导入，避免循环依赖"""
    from utils.auth_helpers import verify_token_and_get_user as _func
    return _func(*args, **kwargs)


def require_auth():
    """延迟导入，避免循环依赖"""
    try:
        from utils.auth_helpers import require_auth as _func
        return _func()
    except ImportError:
        async def dummy_auth():
            return None
        return Depends(dummy_auth)


def get_client_ip(request):
    """延迟导入，避免循环依赖"""
    from utils.request_helpers import get_client_ip as _func
    return _func(request)


def get_user_agent(request):
    """延迟导入，避免循环依赖"""
    from utils.request_helpers import get_user_agent as _func
    return _func(request)


def extract_request_metadata(request):
    """延迟导入，避免循环依赖"""
    from utils.request_helpers import extract_request_metadata as _func
    return _func(request)
