import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

load_dotenv()

AWS_ID = os.getenv("AWS_ACCESS_KEY")
AWS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


builder = (
    SparkSession.builder
    .appName("AI-Social-Intelligence")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")



sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.access.key", AWS_ID)
hadoop_conf.set("fs.s3a.secret.key", AWS_KEY)
hadoop_conf.set("fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

hadoop_conf.set("fs.s3a.path.style.access", "true")

# Performance tuning
hadoop_conf.set("fs.s3a.connection.timeout", "60000")
hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
hadoop_conf.set("fs.s3a.paging.maximum", "5000")
hadoop_conf.set("fs.s3a.threads.max", "20")



kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "ai_social_trends")
    .option("startingOffsets", "latest")
    .load()
)

kafka_df.createOrReplaceTempView("raw_kafka_stream")

processed_df = spark.sql("""
    SELECT
        CAST(value AS STRING) AS json_payload,
        CURRENT_TIMESTAMP() AS processed_at,

        CASE
            WHEN LOWER(CAST(value AS STRING)) LIKE '%amazing%'
              OR LOWER(CAST(value AS STRING)) LIKE '%breakthrough%'
            THEN 1.0
            ELSE 0.5
        END AS sentiment_score

    FROM raw_kafka_stream
""")

query = (
    processed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"s3a://{S3_BUCKET}/checkpoints/ai_trends/"
    )
    .trigger(processingTime="60 seconds")
    .start(
        f"s3a://{S3_BUCKET}/bronze/ai_social_stream/"
    )
)

print("Streaming pipeline started...")

query.awaitTermination()