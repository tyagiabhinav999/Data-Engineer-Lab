import findspark
findspark.init()
from pyspark.sql import SparkSession
from pathlib import Path

def spark_session():
    # Path to the jar directory
    jar_dir = Path(r"C:\mysql-jar")

    # Find the first .jar file in the folder
    jar_files = list(jar_dir.glob("*.jar"))

    if not jar_files:
        raise FileNotFoundError(f"No .jar files found in {jar_dir}")

    jar_path = str(jar_files[0])  # Convert to string for Spark config

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("SparkSalesETL") \
        .config("spark.jars", jar_path) \
        .getOrCreate()

    return spark
