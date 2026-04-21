#!/usr/bin/env python3
"""
清空 MySQL ADS 表脚本
用于在没有 Hive 连接时先清空 MySQL 数据
"""
import pymysql

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "medicals"
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456"


def main():
    print("=" * 60)
    print("🗑️ 清空 MySQL ADS 表")
    print("=" * 60)

    # 连接 MySQL
    print("\n🔌 连接 MySQL...")
    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    cursor = conn.cursor()

    # 获取 MySQL 中所有 ads_ 开头的表
    print("\n📋 查询 MySQL 中 ADS 表...")
    cursor.execute("SHOW TABLES LIKE 'ads_%'")
    ads_tables = [row[0] for row in cursor.fetchall()]
    print(f"✅ 找到 {len(ads_tables)} 个 ADS 表: {ads_tables}")

    # 清空所有 ADS 表
    if ads_tables:
        print("\n🗑️ 清空 MySQL ADS 表...")
        for table in ads_tables:
            try:
                cursor.execute(f"TRUNCATE TABLE `{table}`")
                print(f"   ✅ 清空表: {table}")
            except Exception as e:
                print(f"   ❌ 清空表失败 {table}: {e}")

        conn.commit()
        print("\n✅ 所有 ADS 表已清空!")
    else:
        print("\n⚠️ 没有找到 ADS 表")

    cursor.close()
    conn.close()

    print("\n" + "=" * 60)
    print("📌 下一步：")
    print("   启动后端服务后，调用以下 API 进行数据同步：")
    print("   POST /api/analyse/sync-hive-ads-to-mysql")
    print("   参数: mysql_url=mysql+pymysql://root:123456@localhost:3306/medicals")
    print("=" * 60)


if __name__ == "__main__":
    main()
