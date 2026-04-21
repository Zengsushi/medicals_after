"""
    菜单应用模块
    使用延迟导入避免循环依赖
"""


def _get_menu_router():
    """延迟导入菜单路由器"""
    from apps.menu.router import router as menu_router
    return menu_router


def __getattr__(name):
    """支持属性访问时的延迟导入"""
    if name == 'menu_router':
        return _get_menu_router()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ['menu_router']
