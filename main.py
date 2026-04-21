import os
import uvicorn
import logging
import dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from apps.core.database import db_init, SessionLocal
from apps.user.rbac_models import Role
from apps.user.rbac_seed import run_seed
from utils.redisbase import RedisBase
from utils.redis_bootstrap import ensure_redis_running

# 路由（新模块体系）
from apps.user.router import router as user_router
from apps.menu.router import router as menu_router
from apps.admin.router import router as admin_router
from apps.dashboard.router import router as dashboard_router
from apps.collection.router import router as collection_router
from apps.analyse.router import router as analyse_router
from apps.datasource.router import router as datasource_router
from apps.sync.router import router as sync_router


@asynccontextmanager
async def startup(app: FastAPI):
    try:
        dotenv.load_dotenv()

        # Redis 启动/健康检查（按用户要求从 .path 启动 redis-server.exe）
        redis_state = ensure_redis_running(RedisBase.ping)
        logging.info(f"Redis bootstrap: {redis_state}")

        logging.warning("检测到数据模型被修改,即将对模型进行初始化")
        db_init()

        # 首次部署无角色时跑完整 RBAC 种子（不自动改已有库的 role_menu）
        db = SessionLocal()
        try:
            if db.query(Role).count() == 0:
                run_seed(db)
        except Exception as e:
            logging.warning("RBAC 初始化失败（可稍后手动执行 rbac_seed）: %s", e)
        finally:
            db.close()

        logging.info("数据库初始化成功")
        yield
    except Exception as e:
        logging.warning("数据库初始化异常", e)
        yield


app = FastAPI(
    title="Medical Bs API",
    description="医疗数据分析系统",
    version="0.0.1",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=startup,
)

# 静态文件目录 挂载
app.mount("/static", StaticFiles(directory="static"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  # 请求方式
    allow_headers=["*"],  # 请求头
)

# 模块路由注册
app.include_router(user_router)
app.include_router(menu_router)
app.include_router(admin_router)
app.include_router(dashboard_router)
app.include_router(collection_router)
app.include_router(analyse_router)
app.include_router(datasource_router)
app.include_router(sync_router)


@app.get("/")
async def root():
    logging.info("服务启动成功!")
    return {"message": "Hello World"}


if __name__ == '__main__':
    uvicorn.run("main:app",
                host=os.getenv("SERVER_IP", "127.0.0.1"),
                port=int(os.getenv("SERVER_PORT", 8000)),
                reload=True)
