import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

JDBC_URL = "jdbc:postgresql://postgres:5432/fraud_db"
JDBC_PROPS = {"user": "fraud_user", "password": os.environ["POSTGRES_PASSWORD"], "driver": "org.postgresql.Driver"}
MONGO_URI = "mongodb://mongo:27017/fraud_db.device_sessions"

BRONZE = "s3a://fraud-lake/bronze"
SILVER = "s3a://fraud-lake/silver"
GOLD = "s3a://fraud-lake/gold"


def build_spark():
    return (
        SparkSession.builder
        .appName("batch_bronze_silver_gold")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin")
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .getOrCreate()
    )


def extract_to_bronze(spark):
    transactions = spark.read.jdbc(JDBC_URL, "transactions", properties=JDBC_PROPS)
    users = spark.read.jdbc(JDBC_URL, "users", properties=JDBC_PROPS)
    merchants = spark.read.jdbc(JDBC_URL, "merchants", properties=JDBC_PROPS)
    sessions = spark.read.format("mongodb").load()

    transactions.write.mode("overwrite").parquet(f"{BRONZE}/transactions_batch")
    users.write.mode("overwrite").parquet(f"{BRONZE}/users")
    merchants.write.mode("overwrite").parquet(f"{BRONZE}/merchants")
    sessions.write.mode("overwrite").parquet(f"{BRONZE}/device_sessions")

    print("[bronze] extract selesai.")
    return transactions, users, merchants, sessions


def bronze_to_silver(spark, transactions, users, merchants):
    tx_clean = (
        transactions
        .dropDuplicates(["step", "name_orig", "name_dest", "amount"])
        .filter(F.col("amount") > 0)
    )

    silver = (
        tx_clean
        .join(users, tx_clean.name_orig == users.external_id, "left")
        .withColumnRenamed("name", "user_name")
        .withColumnRenamed("account_balance", "user_account_balance")
        .join(merchants, tx_clean.name_dest == merchants.external_id, "left")
        .withColumnRenamed("name", "merchant_name")
        .withColumnRenamed("category", "merchant_category")
        .withColumnRenamed("risk_score", "merchant_risk_score")
        .select(
            "step", "type", "amount", "name_orig", "user_name",
            "user_account_balance", "name_dest", "merchant_name",
            "merchant_category", "merchant_risk_score", "is_fraud",
        )
    )

    silver.write.mode("overwrite").partitionBy("type").parquet(f"{SILVER}/transactions_enriched")
    print(f"[silver] {silver.count()} baris ditulis.")
    return silver


def silver_to_gold(silver):
    fraud_rate_daily = (
        silver
        .withColumn("day", (F.col("step") / 24).cast("int"))
        .groupBy("day")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.col("is_fraud").cast("int")).alias("fraud_transactions"),
        )
        .withColumn("fraud_rate", F.col("fraud_transactions") / F.col("total_transactions"))
    )
    fraud_rate_daily.write.mode("overwrite").parquet(f"{GOLD}/fraud_rate_daily")

    high_risk_merchants = (
        silver.groupBy("name_dest", "merchant_name", "merchant_category")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
            F.avg("merchant_risk_score").alias("avg_risk_score"),
        )
        .orderBy(F.desc("fraud_count"))
    )
    high_risk_merchants.write.mode("overwrite").parquet(f"{GOLD}/high_risk_merchants")

    window_spec = Window.partitionBy("name_orig", "step")
    velocity = (
        silver
        .withColumn("tx_count_in_step", F.count("*").over(window_spec))
        .select("name_orig", "step", "tx_count_in_step")
        .dropDuplicates(["name_orig", "step"])
    )
    velocity.write.mode("overwrite").parquet(f"{GOLD}/transaction_velocity_by_user")

    print("[gold] fraud_rate_daily, high_risk_merchants, transaction_velocity_by_user ditulis.")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    transactions, users, merchants, _sessions = extract_to_bronze(spark)
    silver = bronze_to_silver(spark, transactions, users, merchants)
    silver_to_gold(silver)

    spark.stop()


if __name__ == "__main__":
    main()
