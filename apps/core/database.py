import os
import tqdm
import logging
import dotenv
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

dotenv.load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:123456@localhost:3306/medicals")

engine = create_engine(DATABASE_URL)

# 创建 数据库会话类
SessionLocal = sessionmaker(
    autocommit=False,  # 是否自动提交
    autoflush=False,  # 是否自动刷新
    bind=engine  # 数据库连接引擎
)

Base = declarative_base()


def db_init():
    """
        数据库初始化 [ 自动创建不存在的表 ]
    :return:
    """
    logging.info("数据库初始化中...")

    # 导入所有模型模块，确保它们被注册到 Base.metadata
    from apps.user import models as user_models
    from apps.user import rbac_models
    from apps.user import security_models
    from apps.menu import models as menu_models
    from apps.datasource import models as datasource_models
    from apps.cluster import models as cluster_models
    from apps.sync import models as sync_models
    from apps.monitor import models as monitor_models
    from apps.collection import models as collection_models

    inspects = inspect(engine)
    existing_table = inspects.get_table_names()

    tables = Base.metadata.sorted_tables
    for table in tqdm.tqdm(tables):
        if table.name not in existing_table:
            table.create(bind=engine)

    # 轻量级“自动补列”（不依赖 alembic），保证前后端闭环字段可用
    try:
        from sqlalchemy import text

        def _has_column(table_name: str, column_name: str) -> bool:
            try:
                cols = [c["name"] for c in inspects.get_columns(table_name)]
                return column_name in cols
            except Exception:
                return False

        # datasources: is_default, latency
        if "datasources" in existing_table:
            with engine.begin() as conn:
                if not _has_column("datasources", "is_default"):
                    conn.execute(text("ALTER TABLE datasources ADD COLUMN is_default TINYINT(1) NOT NULL DEFAULT 0"))
                if not _has_column("datasources", "latency"):
                    conn.execute(text("ALTER TABLE datasources ADD COLUMN latency INT NULL"))
    except Exception as e:
        logging.warning(f"自动补列失败（可忽略，建议用 alembic 管理迁移）: {e}")


def get_db():
    """
        获取数据库会话 ,
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
