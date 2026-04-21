import os
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
    if not SPARK_AVAILABLE:
        raise RuntimeError("PySpark is not installed. Please install it with: pip install pyspark")

    spark = (SparkSession.builder.appName("test")
             # .master("spark://bm:7077")
             # .config("spark.sql.adaptive.enabled", "true")
             .master("local[*]")
             .enableHiveSupport()
             .config("hive.metastore.uris", "thrift://master:9083")
             .config("spark.hadoop.fs.defaultFS", "hdfs://node1:8020") \
             .getOrCreate())
    return spark


if __name__ == '__main__':
    spark = init_spark_connect_hive()
    spark.sql("show databases")
