import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, avg, count, round as sround

# Args: input_prefix output_prefix
# e.g., input_prefix = gs://imdb-bucket-ae1/imdb-processed
#       output_prefix = gs://imdb-bucket-ae1/agg/imdb
input_prefix = sys.argv[1]
output_prefix = sys.argv[2]

spark = (SparkSession.builder
         .appName("AE1-ENG-IMDB-Batch")
         .config("spark.sql.shuffle.partitions", "64")
         .getOrCreate())

b = spark.read.option("header", True).csv(f"{input_prefix}/title_basics_cleaned.csv")
r = spark.read.option("header", True).csv(f"{input_prefix}/title_ratings_cleaned.csv")

# types
b = (b.withColumn("start_year", col("start_year").cast("int"))
       .withColumn("runtime_minutes", col("runtime_minutes").cast("int")))
r = (r.withColumn("average_rating", col("average_rating").cast("double"))
       .withColumn("num_votes", col("num_votes").cast("int")))

br = (b.join(r, "title_id", "inner")
        .filter(col("start_year").isNotNull()))

# genre x year
gy = (br.withColumn("genre", explode(split(col("genres"), ",")))
         .groupBy("genre", "start_year")
         .agg(count("*").alias("title_count"),
              sround(avg("average_rating"), 2).alias("avg_rating"),
              sround(avg("num_votes"), 0).alias("avg_votes")))

# top titles (for dashboard table)
top = (br.filter(col("num_votes") >= 1000)
          .select("title_id", "title", "start_year", "average_rating", "num_votes", "genres")
          .orderBy(col("average_rating").desc(), col("num_votes").desc()))

gy.write.mode("overwrite").parquet(f"{output_prefix}/genre_year/")
top.write.mode("overwrite").parquet(f"{output_prefix}/top_titles/")

spark.stop()
