from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


KAFKA_SERVER = "kafka:29092"
TOPIC = "ai-events"

spark = SparkSession.builder \
    .appName("RealtimeBigDataPipeline") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "24") \
    .config("spark.default.parallelism", "24") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
    .config("spark.sql.files.maxPartitionBytes", "64MB") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint",
            f"s3.{AWS_REGION}.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


schema = StructType([
    StructField("timestamp", DoubleType()),
    StructField("source", StringType()),
    StructField("region", StringType()),
    StructField("model", StringType()),
    StructField("latency_ms", IntegerType()),
    StructField("tokens", IntegerType()),
    StructField("gpu_utilization", IntegerType()),
    StructField("batch_size", IntegerType())
])


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 80000) \
    .load()


parsed_df = kafka_df \
    .selectExpr(
        "CAST(value AS STRING)",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp"
    ) \
    .select(
        from_json(col("value"), schema).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp")
    ) \
    .select("data.*", "topic", "partition", "offset", "kafka_timestamp")


bronze_df = parsed_df \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .repartition(24)


bronze_query = bronze_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation",
            f"s3a://{S3_BUCKET}/checkpoints/bronze") \
    .partitionBy("topic") \
    .trigger(processingTime="10 seconds") \
    .start(f"s3a://{S3_BUCKET}/bronze")


silver_df = bronze_df \
    .withWatermark("event_time", "2 minutes") \
    .withColumn(
        "token_efficiency",
        when(col("latency_ms") > 0,
             col("tokens") / col("latency_ms")).otherwise(0)
    ) \
    .withColumn(
        "gpu_efficiency",
        when(col("gpu_utilization") > 0,
             col("tokens") / col("gpu_utilization")).otherwise(0)
    ) \
    .withColumn(
        "latency_bucket",
        when(col("latency_ms") < 100, "fast")
        .when(col("latency_ms") < 300, "medium")
        .otherwise("slow")
    ) \
    .repartition(24)


silver_query = silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation",
            f"s3a://{S3_BUCKET}/checkpoints/silver") \
    .partitionBy("model") \
    .trigger(processingTime="10 seconds") \
    .start(f"s3a://{S3_BUCKET}/silver")


gold_df = silver_df \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("model"),
        col("topic")
    ) \
    .agg(
        count("*").alias("requests"),
        avg("latency_ms").alias("avg_latency"),
        avg("token_efficiency").alias("avg_token_efficiency"),
        avg("gpu_utilization").alias("avg_gpu_utilization"),
        sum("tokens").alias("tokens_processed")
    )


gold_final = gold_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("topic"),
    col("model"),
    col("requests"),
    col("avg_latency"),
    col("avg_token_efficiency"),
    col("avg_gpu_utilization"),
    col("tokens_processed")
).repartition(24)


gold_query = gold_final.writeStream \
    .format("delta") \
    .outputMode("append") \
    .partitionBy("topic") \
    .option("checkpointLocation",
            f"s3a://{S3_BUCKET}/checkpoints/gold") \
    .trigger(processingTime="20 seconds") \
    .start(f"s3a://{S3_BUCKET}/gold")

spark.streams.awaitAnyTermination()