import pymysql

# 连接 MySQL 数据库
conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    password='123456',
    database='medicals'
)

cursor = conn.cursor()

# 获取所有 ads_ 开头的表
cursor.execute('SHOW TABLES LIKE \'ads_%\'')
tables = cursor.fetchall()

# 检查每个表的数据量
for table in tables:
    table_name = table[0]
    cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
    count = cursor.fetchone()[0]
    print(f'{table_name}: {count} rows')

# 关闭连接
cursor.close()
conn.close()
