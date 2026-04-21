import logging


class Result:
    """
        统一响应格式类
    """

    @staticmethod
    def unauth(msg="身份认证已过期"):
        return {
            "code": 401,
            'message': msg,
            'data': []
        }

    @staticmethod
    def success(code: int = 200, msg: str = "操作成功", data=None):
        logging.info(msg)
        return {
            "code": code,
            "message": msg,
            "data": data or []
        }

    @staticmethod
    def error(code: int = 500, msg: str = "程序内部异常", data=None, error=None):
        logging.error(msg)

        return {
            "code": code,
            "message": msg,
            "data": data or [],
            "error": error or ""
        }
