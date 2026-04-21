import logging
from fastapi import APIRouter
from apps.core import Result

router = APIRouter(prefix="/api/test", tags=["test"])


@router.get("/ping", summary="测试API")
async def test_api():
    """测试API是否正常工作"""
    return Result.success(200, "测试成功", {"message": "Hello, World!"})