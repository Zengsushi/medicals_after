"""
    用户应用模块
    使用延迟导入避免循环依赖
"""


def _get_user_router():
    """延迟导入用户路由器"""
    from apps.user.router import router as user_router
    return user_router


def __getattr__(name):
    """支持属性访问时的延迟导入"""
    if name == 'user_router':
        return _get_user_router()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ['user_router']
