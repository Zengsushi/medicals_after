"""
统一响应格式帮助类

目标格式:
{
    "success": true/false,
    "code": 200 | 401 | 403 | 500,
    "message": "操作成功 / 需要登录 / 权限不足 / 服务器错误",
    "data": {...} | null,
    "error": {...} | null
}
"""

import logging
import traceback
from typing import Any, Optional, Dict


class ResponseHelper:
    """统一响应格式帮助类"""

    @staticmethod
    def success(data: Any = None, message: str = "操作成功", code: int = 200) -> Dict:
        """
        返回成功响应

        Args:
            data: 响应数据
            message: 成功消息
            code: HTTP 状态码

        Returns:
            Dict: 统一格式的成功响应
        """
        logging.info(message)
        return {
            "success": True,
            "code": code,
            "message": message,
            "data": data,
            "error": None
        }

    @staticmethod
    def error(
        code: int = 500,
        message: str = "程序内部异常",
        data: Any = None,
        error: Optional[Dict] = None,
        include_trace: bool = False
    ) -> Dict:
        """
        返回错误响应

        Args:
            code: HTTP 状态码
            message: 错误消息
            data: 错误数据
            error: 详细错误信息 (stack trace 等)
            include_trace: 是否包含堆栈跟踪

        Returns:
            Dict: 统一格式的错误响应
        """
        logging.error(message)

        error_detail = error
        if include_trace and error is None:
            error_detail = {
                "stack": traceback.format_exc()
            }

        return {
            "success": False,
            "code": code,
            "message": message,
            "data": data,
            "error": error_detail
        }

    @staticmethod
    def validation_error(message: str = "数据验证失败", errors: Any = None) -> Dict:
        """返回数据验证错误"""
        return ResponseHelper.error(
            code=422,
            message=message,
            data=errors,
            error={"type": "validation_error"}
        )

    @staticmethod
    def unauthorized(message: str = "身份认证已过期") -> Dict:
        """返回未授权错误"""
        return ResponseHelper.error(
            code=401,
            message=message,
            error={"type": "unauthorized"}
        )

    @staticmethod
    def forbidden(message: str = "没有权限访问") -> Dict:
        """返回禁止访问错误"""
        return ResponseHelper.error(
            code=403,
            message=message,
            error={"type": "forbidden"}
        )

    @staticmethod
    def not_found(message: str = "资源不存在") -> Dict:
        """返回资源不存在错误"""
        return ResponseHelper.error(
            code=404,
            message=message,
            error={"type": "not_found"}
        )

    @staticmethod
    def bad_request(message: str = "请求参数错误") -> Dict:
        """返回请求参数错误"""
        return ResponseHelper.error(
            code=400,
            message=message,
            error={"type": "bad_request"}
        )


class Result:
    """兼容旧接口的 Result 类"""

    @staticmethod
    def unauth(msg: str = "身份认证已过期") -> Dict:
        return ResponseHelper.unauthorized(msg)

    @staticmethod
    def success(code: int = 200, msg: str = "操作成功", data: Any = None) -> Dict:
        return ResponseHelper.success(data=data, message=msg, code=code)

    @staticmethod
    def error(code: int = 500, msg: str = "程序内部异常", data: Any = None, error: Any = None) -> Dict:
        return ResponseHelper.error(code=code, message=msg, data=data, error=error)
