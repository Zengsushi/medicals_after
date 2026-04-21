"""
    数据库迁移脚本
    用于为 RBAC 模型添加缺失的列
"""

from database import engine
from sqlalchemy import text


def column_exists(conn, table, column):
    """检查列是否存在"""
    result = conn.execute(text(f"SHOW COLUMNS FROM `{table}` LIKE '{column}'"))
    return result.fetchone() is not None


def upgrade():
    """添加缺失的列到 roles 表"""
    with engine.connect() as conn:
        if not column_exists(conn, 'roles', 'description'):
            conn.execute(text("ALTER TABLE `roles` ADD COLUMN `description` VARCHAR(500) NULL AFTER `name`"))
            print("✅ 添加 roles.description")

        if not column_exists(conn, 'roles', 'is_active'):
            conn.execute(text("ALTER TABLE `roles` ADD COLUMN `is_active` TINYINT(1) DEFAULT 1 AFTER `description`"))
            print("✅ 添加 roles.is_active")

        if not column_exists(conn, 'roles', 'is_system'):
            conn.execute(text("ALTER TABLE `roles` ADD COLUMN `is_system` TINYINT(1) DEFAULT 0 AFTER `is_active`"))
            print("✅ 添加 roles.is_system")

        if not column_exists(conn, 'roles', 'created_at'):
            conn.execute(text("ALTER TABLE `roles` ADD COLUMN `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP AFTER `is_system`"))
            print("✅ 添加 roles.created_at")

        if not column_exists(conn, 'roles', 'updated_at'):
            conn.execute(text("ALTER TABLE `roles` ADD COLUMN `updated_at` DATETIME NULL ON UPDATE CURRENT_TIMESTAMP AFTER `created_at`"))
            print("✅ 添加 roles.updated_at")

        conn.commit()

    print("\n迁移完成!")


if __name__ == "__main__":
    upgrade()
