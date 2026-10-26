import os
import sys

os.environ['JAVA_HOME'] = r'C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot'
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + r'\bin;' + os.environ['JAVA_HOME'] + r'\bin;' + os.environ.get('PATH', '')

print("Importing PySpark...", flush=True)
from pyspark.sql import SparkSession

print("Building SparkSession...", flush=True)
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("TestApp")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

print(f"Spark Version: {spark.version}", flush=True)
df = spark.range(0, 10).toDF("id")
print(f"Count: {df.count()}", flush=True)
print("SUCCESS!", flush=True)
