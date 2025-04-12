import findspark
findspark.init()
from pyspark.sql import SparkSession

def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("SparkSalesETL")\
        .config("spark.driver.extraClassPath", f"C:\mysql-jar\mysql-connector-j-9.2.0") \
        .getOrCreate()
    return spark