import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("First Spark Application").getOrCreate()

data = [
    {"userId": 1, "paymentAmount": 100.0, "date": "2025-01-01"},
    {"userId": 2, "paymentAmount": 150.5, "date": "2025-01-02"},
    {"userId": 3, "paymentAmount": 200.75, "date": "2025-01-03"},
    {"userId": 2, "paymentAmount": 50.25, "date": "2025-01-04"},
    {"userId": 1, "paymentAmount": 80.0, "date": "2025-01-05"}
]

df = spark.createDataFrame(data)

df.show()