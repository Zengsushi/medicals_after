"""
初始化 Hive 配置
确保 Hive 连接所需的环境和配置目录存在
"""
import os
import logging
from apps.datasource.hive_config import HiveConfigManager

logger = logging.getLogger(__name__)


def initialize_hive_config():
    """初始化 Hive 配置"""
    try:
        # 检查 Java 环境
        java_home = HiveConfigManager.get_default_java_home()
        if not os.path.exists(java_home):
            logger.warning(f"JAVA_HOME 目录不存在: {java_home}")
            logger.warning("请确保已安装 Java 1.8 或更高版本")
        else:
            logger.info(f"找到 JAVA_HOME: {java_home}")

        # 检查 Hadoop 配置目录
        hadoop_home = HiveConfigManager.get_default_hadoop_home()
        if not os.path.exists(hadoop_home):
            logger.warning(f"HADOOP_HOME 目录不存在: {hadoop_home}")
            logger.info("正在创建 Hadoop 配置目录...")

            # 创建目录
            os.makedirs(hadoop_home, exist_ok=True)

            # 创建必要的子目录
            bin_dir = os.path.join(hadoop_home, "bin")
            lib_dir = os.path.join(hadoop_home, "lib")
            etc_dir = os.path.join(hadoop_home, "etc", "hadoop")

            os.makedirs(bin_dir, exist_ok=True)
            os.makedirs(lib_dir, exist_ok=True)
            os.makedirs(etc_dir, exist_ok=True)

            # 创建占位文件
            with open(os.path.join(etc_dir, "core-site.xml"), "w") as f:
                f.write('''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
''')

            with open(os.path.join(etc_dir, "hdfs-site.xml"), "w") as f:
                f.write('''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
''')

            logger.info(f"已创建 Hadoop 配置目录: {hadoop_home}")
        else:
            logger.info(f"找到 HADOOP_HOME: {hadoop_home}")

        # 测试环境变量设置
        env_vars = HiveConfigManager.setup_environment()
        logger.info(f"设置的环境变量: {env_vars}")

        logger.info("Hive 配置初始化完成")
        return True

    except Exception as e:
        logger.error(f"初始化 Hive 配置失败: {e}")
        return False


if __name__ == "__main__":
    # 配置日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    initialize_hive_config()
