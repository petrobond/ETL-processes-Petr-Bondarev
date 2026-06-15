from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, BooleanType,
)

spark = SparkSession.builder.appName("kafka-full-pipeline").getOrCreate()

KAFKA_SERVERS = "rc1b-gt0itb0u4p8ba5rs.mdb.yandexcloud.net:9091"
TOPIC = "dataproc-kafka-topic"
BUCKET = "petr-bondarev-module4-task1"

# ---- Schema для credit_applications.csv ----
CREDIT_SCHEMA = StructType([
    StructField("application_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("region_code", StringType(), True),
    StructField("product_type", StringType(), True),
    StructField("requested_amount", IntegerType(), True),
    StructField("term_months", IntegerType(), True),
    StructField("credit_score", IntegerType(), True),
    StructField("risk_level", StringType(), True),
    StructField("decision_status", StringType(), True),
    StructField("approved_amount", IntegerType(), True),
    StructField("channel", StringType(), True),
    StructField("employee_review_flag", StringType(), True),
    StructField("processing_time_sec", IntegerType(), True),
])

# ============================================================
# Шаг 1: Читаем CSV и отправляем в Kafka (≥50 MB)
# ============================================================
print("=" * 60)
print("STEP 1: Sending credit_applications.csv to Kafka...")
print("=" * 60)

df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv(f"s3a://{BUCKET}/input/credit_applications.csv")

count = df.count()
print(f"Total rows to send: {count}")

# Convert to JSON messages
df_kafka = df.select(to_json(struct([col(c).alias(c) for c in df.columns])).alias("value"))

df_kafka.write.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("topic", TOPIC) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            "username=user1 password=password1;") \
    .save()

print(f"Sent {count} messages to Kafka topic '{TOPIC}'")

# ============================================================
# Шаг 2: Читаем ВСЕ данные из Kafka и РАСКЛАДЫВАЕМ JSON → плоская таблица
# ============================================================
print("=" * 60)
print("STEP 2: Reading ALL messages from Kafka + flattening JSON...")
print("=" * 60)

df_batch = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            "username=user1 password=password1;") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

batch_count = df_batch.count()
print(f"Messages read (batch): {batch_count}")

# ---- FLATTEN: разбираем JSON-строку в колонки ----
flat_df = df_batch \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), CREDIT_SCHEMA).alias("data")) \
    .select("data.*") \
    .filter(col("application_id").isNotNull())

flat_count = flat_df.count()
print(f"Flattened rows: {flat_count}")

# Сохраняем в Parquet (сжатый, колоночный — для DataLens)
flat_df.write.mode("overwrite") \
    .parquet(f"s3a://{BUCKET}/kafka-read-batch-output/parquet/")

# Сохраняем в CSV с заголовками (для демонстрации «плоского вида»)
flat_df.coalesce(1).write.mode("overwrite") \
    .option("header", "true").option("compression", "none") \
    .csv(f"s3a://{BUCKET}/kafka-read-batch-output/csv/")

print(f"Batch output written: {flat_count} rows (Parquet + CSV)")

# ============================================================
# Шаг 3: Читаем из Kafka (streaming) + flatten JSON → Parquet
# ============================================================
print("=" * 60)
print("STEP 3: Reading from Kafka (streaming) + flattening...")
print("=" * 60)

df_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            "username=user1 password=password1;") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "500000") \
    .load()

# Flatten stream
stream_flat = df_stream \
    .selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), CREDIT_SCHEMA).alias("data")) \
    .select("data.*") \
    .filter(col("application_id").isNotNull())

query = stream_flat.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", f"s3a://{BUCKET}/kafka-read-stream-output/parquet/") \
    .option("checkpointLocation", f"s3a://{BUCKET}/kafka-read-stream-output/_checkpoint/") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination(60)  # Wait 60 seconds for streaming

print("Step 3 complete! Stream data written to kafka-read-stream-output/parquet/")
print("=" * 60)
print("KAFKA FULL PIPELINE COMPLETE!")
print(f"Messages sent to Kafka:     {count}")
print(f"Flattened rows (batch):     {flat_count}")
print("Output (flat table):         kafka-read-batch-output/parquet/")
print("Output (flat table):         kafka-read-batch-output/csv/")
print("Output (stream table):       kafka-read-stream-output/parquet/")
print("=" * 60)