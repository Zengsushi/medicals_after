"""
根目录兼容入口：历史代码与 test 目录可能使用 ``from redisbase import RedisBase``。
实现已迁移至 ``utils.redisbase``，请在新代码中直接引用后者。
"""

from utils.redisbase import RedisBase, TokenAnalysisError, TokenAnalysisIsNone, TokenClearError

__all__ = ['RedisBase', 'TokenAnalysisError', 'TokenAnalysisIsNone', 'TokenClearError']
