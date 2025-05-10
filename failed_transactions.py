from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FilterFailed").getOrCreate()

df = spark.read.option("header", True).csv("gs://moghith-bucket/cleaned/")

failed_txns = df.filter(col("Status") == "Failed")

failed_txns.coalesce(1).write.option("header", True).mode("overwrite").csv("gs://moghith-bucket/failed_txns/")
