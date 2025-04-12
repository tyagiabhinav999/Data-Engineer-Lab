import findspark
findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName('test') \
        .master('local[*]') \
        .config("spark.driver.extraClassPath", "<path to mysql-connector-java-5.1.49-bin.jar>") \
        .getOrCreate()