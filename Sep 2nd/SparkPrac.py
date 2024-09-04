# Import PySpark and initialize a Spark session
from pyspark.sql import SparkSession
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PySpark Notebook Example") \
    .getOrCreate()
# Verify the Spark session is working spark
spark
