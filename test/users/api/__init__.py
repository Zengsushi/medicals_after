"""
    users.api 模块入口
    使用延迟导入避免循环依赖
"""


def __getattr__(name):
    """支持属性访问时的延迟导入"""
    if name == 'menu_router':
        from users.api.menu import router as menu_router
        return menu_router
    elif name == 'user_router':
        # 直接从模块文件导入（绕过包的循环）
        import importlib
        api_module = importlib.import_module('users.api')
        return api_module.user_router
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ['user_router', 'menu_router']
