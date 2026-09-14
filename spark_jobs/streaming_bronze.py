import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
)

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "transactions_stream"
MINIO_BRONZE_PATH = "s3a://fraud-lake/bronze/transactions"
CHECKPOINT_PATH = "s3a://fraud-lake/_checkpoints/streaming_bronze"

CLICKHOUSE_JDBC_URL = "jdbc:clickhouse://clickhouse:8123/default"
CLICKHOUSE_TABLE = "realtime_transactions"

schema = StructType([
    StructField("step", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("name_orig", StringType()),
    StructField("oldbalance_orig", DoubleType()),
    StructField("newbalance_orig", DoubleType()),
    StructField("name_dest", StringType()),
    StructField("oldbalance_dest", DoubleType()),
    StructField("newbalance_dest", DoubleType()),
    StructField("is_fraud", BooleanType()),
    StructField("event_time", DoubleType()),
])


def build_spark():
    return (
        SparkSession.builder
        .appName("streaming_bronze")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin")
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def write_to_clickhouse(batch_df, batch_id):
    """foreachBatch: tulis micro-batch ke ClickHouse lewat JDBC."""
    if batch_df.rdd.isEmpty():
        return
    (
        batch_df.write
        .format("jdbc")
        .option("url", CLICKHOUSE_JDBC_URL)
        .option("dbtable", CLICKHOUSE_TABLE)
        .option("user", "default")
        .option("password", os.environ["CLICKHOUSE_PASSWORD"])
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode("append")
        .save()
    )
    print(f"[streaming] batch {batch_id}: {batch_df.count()} baris ditulis ke ClickHouse")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", current_timestamp())
    )

    bronze_query = (
        parsed.writeStream
        .format("parquet")
        .option("path", MINIO_BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    clickhouse_query = (
        parsed.writeStream
        .foreachBatch(write_to_clickhouse)
        .option("checkpointLocation", CHECKPOINT_PATH + "_ch")
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
