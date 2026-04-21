import os
import tqdm
import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "mysql+pymysql://root:123456@localhost:3306/medicals"

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

    inspects = inspect(engine)
    existing_table = inspects.get_table_names()

    tables = Base.metadata.sorted_tables
    for table in tqdm.tqdm(tables):
        if table.name not in existing_table:
            table.create(bind=engine)


def get_db():
    """
        获取数据库会话 ,
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
