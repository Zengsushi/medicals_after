#!/usr/bin/env python3
"""
清空 MySQL ADS 表并从 Hive 同步数据
使用 pyhive 连接 Hive
"""
import pymysql

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "medicals"
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456"

HIVE_HOST = "master"
HIVE_PORT = 10000
HIVE_DATABASE = "medical_ads"


def get_hive_connection():
    """获取 Hive 连接"""
    try:
        from pyhive import hive
        conn = hive.connect(
            host=HIVE_HOST,
            port=HIVE_PORT,
            username="hadoop"
        )
        return conn
    except ImportError:
        print("❌ 请安装 pyhive: pip install pyhive[hive]")
        return None
    except Exception as e:
        print(f"❌ Hive 连接失败: {e}")
        return None


def get_ads_tables_from_hive(cursor):
    """从 Hive 获取 ADS 表列表"""
    print(f"\n📋 正在查询 Hive 数据库 '{HIVE_DATABASE}' 中的表...")
    try:
        cursor.execute(f"USE {HIVE_DATABASE}")
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"✅ 找到 {len(tables)} 个表: {tables}")
        return tables
    except Exception as e:
        print(f"❌ 查询 Hive 表失败: {e}")
        return []


def get_table_columns(cursor, table_name):
    """获取 Hive 表的列信息"""
    try:
        cursor.execute(f"DESCRIBE {HIVE_DATABASE}.{table_name}")
        columns = []
        for row in cursor.fetchall():
            col_name = row[0]
            if col_name and not col_name.startswith('#'):
                columns.append(col_name)
        return columns
    except Exception as e:
        print(f"   ⚠️ 获取列信息失败: {e}")
        return []


def get_mysql_type(hive_type):
    """Hive 类型转 MySQL 类型"""
    hive_type = hive_type.lower() if hive_type else "string"

    if "bigint" in hive_type:
        return "BIGINT"
    elif "int" in hive_type:
        return "INT"
    elif "double" in hive_type or "float" in hive_type:
        return "DOUBLE"
    elif "decimal" in hive_type:
        return "DECIMAL(20,6)"
    elif "boolean" in hive_type:
        return "TINYINT(1)"
    elif "timestamp" in hive_type:
        return "DATETIME"
    else:
        return "VARCHAR(500)"


def create_mysql_table(cursor, table_name, columns):
    """创建 MySQL 表"""
    mysql_table = f"ads_{table_name}"

    col_defs = []
    for col in columns:
        col_defs.append(f"`{col}` VARCHAR(500)")

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{mysql_table}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {", ".join(col_defs)}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    try:
        cursor.execute(f"DROP TABLE IF EXISTS `{mysql_table}`")
        cursor.execute(create_sql)
        print(f"   ✅ 创建表: {mysql_table}")
    except Exception as e:
        print(f"   ❌ 创建表失败: {e}")


def sync_table_data(hive_cursor, mysql_cursor, table_name):
    """同步表数据"""
    mysql_table = f"ads_{table_name}"

    try:
        print(f"   📥 正在查询: {table_name}")
        hive_cursor.execute(f"SELECT * FROM {HIVE_DATABASE}.{table_name}")
        rows = hive_cursor.fetchall()

        if not rows:
            print(f"   ⚠️ 表 {table_name} 无数据")
            return

        print(f"   📊 共 {len(rows)} 条数据")

        mysql_cursor.execute(f"TRUNCATE TABLE `{mysql_table}`")

        for row in rows:
            values = []
            for val in row:
                if val is None:
                    values.append(None)
                else:
                    values.append(str(val)[:500])

            placeholders = ', '.join(['%s'] * len(values))
            sql = f"INSERT INTO `{mysql_table}` VALUES (NULL, {placeholders})"

            try:
                mysql_cursor.execute(sql, values)
            except Exception as e:
                print(f"   ⚠️ 插入部分数据失败: {e}")
                break

        print(f"   ✅ 同步完成")

    except Exception as e:
        print(f"   ❌ 同步失败: {e}")


def main():
    print("=" * 60)
    print("🚀 开始同步 Hive 数据到 MySQL")
    print("=" * 60)

    # 连接 MySQL
    print("\n🔌 连接 MySQL...")
    mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    mysql_cursor = mysql_conn.cursor()

    # 获取 MySQL 中所有 ads_ 开头的表
    print("\n📋 查询 MySQL 中 ADS 表...")
    mysql_cursor.execute("SHOW TABLES LIKE 'ads_%'")
    ads_tables = [row[0] for row in mysql_cursor.fetchall()]
    print(f"✅ MySQL 中找到 {len(ads_tables)} 个 ADS 表: {ads_tables}")

    # 清空所有 ADS 表
    if ads_tables:
        print("\n🗑️ 清空 MySQL ADS 表...")
        for table in ads_tables:
            try:
                mysql_cursor.execute(f"TRUNCATE TABLE `{table}`")
                print(f"   ✅ 清空表: {table}")
            except Exception as e:
                print(f"   ❌ 清空表失败 {table}: {e}")
        mysql_conn.commit()

    # 连接 Hive
    print("\n🔌 连接 Hive...")
    hive_conn = get_hive_connection()
    if not hive_conn:
        print("❌ 无法连接 Hive，尝试使用其他方式...")
        mysql_cursor.close()
        mysql_conn.close()
        return

    hive_cursor = hive_conn.cursor()

    # 获取 Hive 表
    hive_tables = get_ads_tables_from_hive(hive_cursor)

    if not hive_tables:
        print("\n⚠️ Hive 中没有找到表，尝试其他数据库...")

        for db_name in ["ads_medicals", "default"]:
            try:
                print(f"\n📋 尝试数据库: {db_name}")
                hive_cursor.execute(f"USE {db_name}")
                hive_cursor.execute("SHOW TABLES")
                hive_tables = [row[0] for row in hive_cursor.fetchall()]
                if hive_tables:
                    print(f"✅ 在 {db_name} 中找到表: {hive_tables}")
                    break
            except:
                continue

    if hive_tables:
        print(f"\n🔄 开始同步 {len(hive_tables)} 个表...")

        for table in hive_tables:
            columns = get_table_columns(hive_cursor, table)
            if columns:
                create_mysql_table(mysql_cursor, table, columns)
                sync_table_data(hive_cursor, mysql_cursor, table)
                mysql_conn.commit()
            else:
                print(f"   ⚠️ 表 {table} 无法获取列信息，跳过")

        # 验证结果
        print("\n📊 同步结果验证:")
        mysql_cursor.execute("SHOW TABLES LIKE 'ads_%'")
        result_tables = [row[0] for row in mysql_cursor.fetchall()]
        for table in result_tables:
            try:
                mysql_cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
                count = mysql_cursor.fetchone()[0]
                print(f"   - {table}: {count} 条记录")
            except:
                print(f"   - {table}: 查询失败")
    else:
        print("\n❌ 未找到可同步的 Hive 表")

    # 清理
    print("\n🔌 关闭连接...")
    hive_cursor.close()
    hive_conn.close()
    mysql_cursor.close()
    mysql_conn.close()

    print("\n" + "=" * 60)
    print("✅ 同步完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
