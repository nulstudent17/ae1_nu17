import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp, date_format, count

# Args: input_csv output_prefix
#   input_csv: gs://imdb-bucket-ae1/nasa-processed/nasa_solar_flares_2023.csv
#   output_prefix: gs://imdb-bucket-ae1/agg/nasa
input_csv = sys.argv[1]
output_prefix = sys.argv[2]

spark = (SparkSession.builder
         .appName("AE1-ENG-NASA-Batch")
         .config("spark.sql.shuffle.partitions", "32")
         .getOrCreate())

df = spark.read.option("header", True).csv(input_csv)

# Normalize to seconds and parse
df = df.withColumn("begin_norm", regexp_replace(col("beginTime"), r"Z$", ":00Z"))
df = df.withColumn("begin_ts", to_timestamp(col("begin_norm"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# Daily counts by classType
daily = (df.select("classType", "begin_ts")
           .filter(col("begin_ts").isNotNull())
           .withColumn("day", date_format(col("begin_ts"), "yyyy-MM-dd"))
           .groupBy("classType", "day")
           .agg(count("*").alias("flare_count")))

# Monthly totals for trend line
monthly = (df.select("classType", "begin_ts")
             .filter(col("begin_ts").isNotNull())
             .withColumn("month", date_format(col("begin_ts"), "yyyy-MM"))
             .groupBy("classType", "month")
             .agg(count("*").alias("flare_count")))

daily.write.mode("overwrite").parquet(f"{output_prefix}/daily_by_class/")
monthly.write.mode("overwrite").parquet(f"{output_prefix}/monthly_by_class/")

spark.stop()
