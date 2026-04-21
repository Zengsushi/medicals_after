from apps.analyse.config import init_spark_connect_hive

if __name__ == '__main__':
    spark = init_spark_connect_hive()
    spark.sql("use medicals_ads")

    ads_hospital_ranking_df = spark.sql("""
        select * from ads_hospital_ranking; 
    """).show()
