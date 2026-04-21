import os
import sys
import warnings

try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    SPARK_AVAILABLE = False

os.environ["JAVA_HOME"] = r"E:\java_version\jdk17.0.11"
os.environ["HADOOP_HOME"] = "B:\\3_after_end\\medicalBs\\config\\hadoop-3.3.4"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "\\bin;" + os.environ["PATH"]
warnings.filterwarnings("ignore")


def init_spark_connect_hive():
    """
    spark
    """
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed. Please install it with: pip install pyspark")

    hdfs_user = (os.getenv("SPARK_HDFS_USER") or os.getenv("HADOOP_USER_NAME") or "").strip()
    if hdfs_user:
        os.environ["HADOOP_USER_NAME"] = hdfs_user

    metastore = os.getenv("HIVE_METASTORE_URIS", "thrift://master:9083")
    default_fs = os.getenv("HADOOP_DEFAULT_FS", "hdfs://node1:8020")
    master = os.getenv("SPARK_MASTER")
    if not master:
        master = "local[4]" if sys.platform == "win32" else "local[*]"

    if sys.platform == "win32":
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    driver_host = os.getenv("SPARK_DRIVER_HOST", "127.0.0.1")
    bind_addr = os.getenv("SPARK_BIND_ADDRESS", "127.0.0.1")
    py_timeout = os.getenv("SPARK_PYTHON_WORKER_TIMEOUT", "120")

    builder = (
        SparkSession.builder.appName("medical-bs-hive")
        .master(master)
        .enableHiveSupport()
        # Spark 3 会忽略无前缀的 hive.metastore.uris；需放在 Hadoop 配置命名空间下
        .config("spark.hadoop.hive.metastore.uris", metastore)
        .config("spark.hadoop.fs.defaultFS", default_fs)
        .config("spark.driver.host", driver_host)
        .config("spark.driver.bindAddress", bind_addr)
        .config("spark.python.worker.timeout", py_timeout)
        .config("spark.python.worker.reuse", "true")
    )
    if hdfs_user:
        builder = builder.config("spark.hadoop.user.name", hdfs_user)

    return builder.getOrCreate()
