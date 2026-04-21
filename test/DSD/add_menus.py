#!/usr/bin/env python3
"""
添加菜单到数据库
"""
import pymysql

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "medicals"
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456"


def add_menus():
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cursor = conn.cursor()

    cursor.execute("SELECT id, name, path FROM menus WHERE name = '数据源管理'")
    parent_menu = cursor.fetchone()

    if not parent_menu:
        print("❌ 未找到 '数据源管理' 菜单，请先确保该菜单已存在")
        cursor.close()
        conn.close()
        return

    parent_id = parent_menu[0]
    print(f"✅ 找到父菜单: {parent_menu[1]} (ID: {parent_id})")

    cursor.execute("SELECT id, name FROM menus WHERE parent_id = %s", (parent_id,))
    existing_children = cursor.fetchall()
    print(f"当前子菜单: {existing_children}")

    new_menus = [
        {
            "name": "Hive源管理",
            "path": "/database/manage",
            "component": "views/database/SourceManage.vue",
            "icon": "DatabaseOutlined",
            "order": 1,
            "is_folder": False
        },
        {
            "name": "MySQL源管理",
            "path": "/database/mysql",
            "component": "views/database/SourceManage.vue",
            "icon": "MySQLOutlined",
            "order": 2,
            "is_folder": False
        },
        {
            "name": "元数据管理",
            "path": "/database/metadata",
            "component": "views/database/SourceManage.vue",
            "icon": "ApartmentOutlined",
            "order": 3,
            "is_folder": False
        }
    ]

    for menu in new_menus:
        cursor.execute(
            "SELECT id FROM menus WHERE path = %s",
            (menu["path"],)
        )
        existing = cursor.fetchone()

        if existing:
            print(f"⚠️  菜单已存在: {menu['name']} (ID: {existing[0]})")
            continue

        sql = """
            INSERT INTO menus (name, path, component, icon, order, is_folder, is_active, position, parent_id, parent_path, permission_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            menu["name"],
            menu["path"],
            menu["component"],
            menu["icon"],
            menu["order"],
            menu["is_folder"],
            True,
            1,
            parent_id,
            "/database",
            "sources:view"
        )

        cursor.execute(sql, values)
        print(f"✅ 添加菜单成功: {menu['name']}")

    conn.commit()

    cursor.execute("SELECT id, name, path, parent_id FROM menus WHERE parent_id = %s ORDER BY `order`", (parent_id,))
    children = cursor.fetchall()
    print(f"\n📋 数据源管理子菜单:")
    for child in children:
        print(f"   - {child[1]} (ID: {child[0]}, Path: {child[2]})")

    cursor.close()
    conn.close()
    print("\n✅ 操作完成!")


if __name__ == "__main__":
    add_menus()
