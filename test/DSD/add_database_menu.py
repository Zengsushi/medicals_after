#!/usr/bin/env python3
"""
添加数据源管理菜单到元数据管理下
"""
import pymysql

conn = pymysql.connect(
    host="localhost",
    port=3306,
    user="root",
    password="123456",
    database="medicals"
)
cursor = conn.cursor()

# 检查是否已存在
cursor.execute("SELECT id FROM menus WHERE path = '/database/sources'")
if cursor.fetchone():
    print('⚠️ 数据源管理菜单已存在')
else:
    sql = """
    INSERT INTO menus (name, path, component, icon, `order`, is_folder, is_active, position, parent_id, parent_path, permission_code)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql, (
        '数据源管理',
        '/database/sources',
        'views/database/SourceManage.vue',
        'DatabaseOutlined',
        1,
        0,
        1,
        1,
        7,  # parent_id = 元数据管理ID
        '/database',
        'sources:view'
    ))
    print('✅ 添加数据源管理成功')

conn.commit()

# 显示元数据管理下的所有子菜单
cursor.execute("SELECT id, name, path, parent_id FROM menus WHERE parent_id = 7 ORDER BY `order`")
print("\n📋 元数据管理子菜单:")
for row in cursor.fetchall():
    print(f"   - {row[1]} (ID: {row[0]}, Path: {row[2]})")

cursor.close()
conn.close()
