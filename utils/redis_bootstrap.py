import os
import subprocess
import time
from pathlib import Path
from typing import Optional, Callable, Any, Dict


def _default_redis_server_exe() -> str:
    # 用户要求：后端需要 redis 环境在 .path 中启动
    # 允许通过环境变量覆盖，便于不同机器/路径部署
    workspace_root = Path(os.getenv("WORKSPACE_ROOT", "B:\\"))
    return str(
        workspace_root
        / ".path"
        / "Redis-7.4.2-Windows-x64-msys2-with-Service"
        / "redis-server.exe"
    )


def ensure_redis_running(
    ping_fn: Callable[[], bool],
    redis_server_exe: Optional[str] = None,
    startup_timeout_s: Optional[float] = None,
) -> Dict[str, Any]:
    """
    确保 Redis 可用：优先 ping；若不可用则按给定路径启动 redis-server.exe 后重试。

    Args:
        ping_fn: 一个无参函数，返回 bool（例如 RedisBase.ping）
        redis_server_exe: redis-server.exe 路径（可为空，使用默认 .path 路径）
        startup_timeout_s: 启动等待超时（秒）

    Returns:
        dict: { started: bool, healthy: bool, exe: str, message: str }
    """
    exe = redis_server_exe or os.getenv("REDIS_SERVER_EXE") or _default_redis_server_exe()
    timeout = float(startup_timeout_s or os.getenv("REDIS_STARTUP_TIMEOUT_S", "5"))

    try:
        if ping_fn():
            return {"started": False, "healthy": True, "exe": exe, "message": "redis already healthy"}
    except Exception:
        # ping 失败直接走启动流程
        pass

    exe_path = Path(exe)
    if not exe_path.exists():
        return {
            "started": False,
            "healthy": False,
            "exe": exe,
            "message": f"redis-server.exe not found: {exe}",
        }

    # 启动 redis-server（不阻塞，不弹窗）
    try:
        subprocess.Popen(
            [str(exe_path)],
            cwd=str(exe_path.parent),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as e:
        return {"started": False, "healthy": False, "exe": exe, "message": f"failed to start redis: {e}"}

    # 等待 Redis 就绪
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if ping_fn():
                return {"started": True, "healthy": True, "exe": exe, "message": "redis started and healthy"}
        except Exception:
            pass
        time.sleep(0.2)

    return {"started": True, "healthy": False, "exe": exe, "message": "redis started but not healthy in time"}

