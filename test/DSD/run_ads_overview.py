#!/usr/bin/env python3
"""
执行 ads_overview Hive SQL 并同步数据到 MySQL
"""
import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
sys.path.append(project_root)

import pymysql

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "medicals"
MYSQL_USER = "root"
MYSQL_PASSWORD = "123456"

DT = "20260413"

ADS_OVERVIEW_SQL = f"""
INSERT OVERWRITE TABLE medicals_ads.ads_overview PARTITION (dt = '{DT}')
SELECT '医院总数' AS indicator_name, COUNT(DISTINCT hospital_id) AS indicator_value, CURRENT_TIMESTAMP AS etl_time
FROM medicals_dws.dws_hospital_service
WHERE dt = '{DT}'
UNION ALL
SELECT '医生总数' AS indicator_name, COUNT(DISTINCT doctor_id) AS indicator_value, CURRENT_TIMESTAMP AS etl_time
FROM medicals_dws.dws_doctor_service
WHERE dt = '{DT}'
UNION ALL
SELECT '问诊总量' AS indicator_name, SUM(consultation_count) AS indicator_value, CURRENT_TIMESTAMP AS etl_time
FROM medicals_dws.dws_consultation_service
WHERE dt = '{DT}'
UNION ALL
SELECT '疾病种类' AS indicator_name, COUNT(DISTINCT disease_name) AS indicator_value, CURRENT_TIMESTAMP AS etl_time
FROM medicals_dws.dws_disease_service
WHERE dt = '{DT}'
UNION ALL
SELECT '城市数量' AS indicator_name, COUNT(DISTINCT city) AS indicator_value, CURRENT_TIMESTAMP AS etl_time
FROM medicals_dws.dws_region_service
WHERE dt = '{DT}'
UNION ALL
SELECT '科室数量' AS indicator_name, COUNT(DISTINCT department) AS indicator_value, CURRENT_TIMESTAMP AS etl_time
FROM medicals_dws.dws_department_service
WHERE dt = '{DT}'
"""


def main():
    print("=" * 60)
    print("🚀 步骤1: 执行 Hive SQL 写入 ads_overview")
    print("=" * 60)

    try:
        from apps.analyse.config import init_spark_connect_hive
        spark = init_spark_connect_hive()
        print("✅ Spark 会话创建成功")
    except Exception as e:
        print(f"❌ Spark 会话创建失败: {e}")
        return

    print(f"\n📋 执行 SQL (dt={DT})...")
    try:
        spark.sql(ADS_OVERVIEW_SQL)
        print("✅ Hive SQL 执行成功")
    except Exception as e:
        print(f"❌ Hive SQL 执行失败: {e}")
        spark.stop()
        return

    print("\n📋 验证 Hive 数据...")
    try:
        df = spark.sql(f"SELECT * FROM medicals_ads.ads_overview WHERE dt = '{DT}'")
        row_count = df.count()
        print(f"✅ ads_overview 共 {row_count} 条数据:")
        df.show(truncate=False)
    except Exception as e:
        print(f"❌ 验证失败: {e}")
        spark.stop()
        return

    print("\n" + "=" * 60)
    print("🚀 步骤2: 同步 ads_overview 到 MySQL")
    print("=" * 60)

    print("\n🔌 连接 MySQL...")
    mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )
    mysql_cursor = mysql_conn.cursor()

    mysql_cursor.execute("TRUNCATE TABLE `ads_overview`")
    print("✅ 清空 MySQL ads_overview 表")

    columns = df.columns
    print(f"📋 列: {columns}")

    col_str = ", ".join([f"`{c}`" for c in columns])
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO `ads_overview` ({col_str}) VALUES ({placeholders})"

    batch = []
    for row in df.collect():
        values = []
        for val in row:
            if val is None:
                values.append(None)
            else:
                values.append(str(val))
        batch.append(tuple(values))

    try:
        mysql_cursor.executemany(insert_sql, batch)
        mysql_conn.commit()
        print(f"✅ 写入 MySQL 成功: {len(batch)} 条记录")
    except Exception as e:
        print(f"❌ 写入 MySQL 失败: {e}")
        mysql_conn.rollback()

    mysql_cursor.execute("SELECT COUNT(*) FROM `ads_overview`")
    count = mysql_cursor.fetchone()[0]
    print(f"📊 MySQL ads_overview: {count} 条记录")

    mysql_cursor.close()
    mysql_conn.close()
    spark.stop()

    print("\n" + "=" * 60)
    print("✅ 全部完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
