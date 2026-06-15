from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, sum as _sum, count, avg, min as _min, max as _max

spark = SparkSession.builder.appName("process-csv-task2").getOrCreate()

# 1. Read credit_applications.csv from S3
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("s3a://petr-bondarev-module4-task1/input/credit_applications.csv")

print(f"Total rows: {df.count()}")
print(f"Columns: {df.columns}")

# 2. Add date features (column is 'event_time')
df = df.withColumn("year", year(col("event_time"))) \
       .withColumn("month", month(col("event_time"))) \
       .withColumn("day", dayofmonth(col("event_time")))

# 3. Aggregated statistics by day and channel
daily_stats = df.groupBy("year", "month", "day", "channel") \
    .agg(
        count("*").alias("total_applications"),
        _sum("approved_amount").alias("total_approved_amount"),
        avg("credit_score").alias("avg_credit_score"),
        _min("credit_score").alias("min_credit_score"),
        _max("credit_score").alias("max_credit_score")
    )

# 4. Risk analytics
risk_analytics = df.groupBy("risk_level").agg(
    count("*").alias("total_applications"),
    _sum("approved_amount").alias("total_approved_amount"),
    avg("credit_score").alias("avg_credit_score")
)

# 5. Channel + decision_status statistics
channel_stats = df.groupBy("channel", "decision_status").agg(
    count("*").alias("total_applications"),
    _sum("approved_amount").alias("total_approved_amount")
)

# 6. Write ALL as uncompressed CSV (≥50 MB)
output_base = "s3a://petr-bondarev-module4-task1/processed/task2/"

df.write.mode("overwrite").option("header", "true").option("compression", "none") \
    .csv(f"{output_base}credit_applications_processed")

daily_stats.write.mode("overwrite").option("header", "true").option("compression", "none") \
    .csv(f"{output_base}daily_stats")

risk_analytics.write.mode("overwrite").option("header", "true").option("compression", "none") \
    .csv(f"{output_base}risk_analytics")

channel_stats.write.mode("overwrite").option("header", "true").option("compression", "none") \
    .csv(f"{output_base}channel_stats")

# Also save as Parquet for demonstration
df.write.mode("overwrite").parquet(f"{output_base}credit_applications_parquet")

print("Task 2 processing complete!")