from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CleanMerge").getOrCreate()

df = spark.read.option("header", True).csv("gs://moghith-bucket/injection/*.csv")

df_clean = df.dropna(how="any").dropDuplicates()

df_clean.coalesce(1).write.option("header", True).mode("overwrite").csv("gs://moghith-bucket/cleaned/")
