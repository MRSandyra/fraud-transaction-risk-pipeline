#!/bin/bash

set -e

clickhouse client --user "${CLICKHOUSE_USER:-default}" --password "$CLICKHOUSE_PASSWORD" --multiquery <<SQL
-- Target table for Spark Structured Streaming writes (near real-time)
CREATE TABLE IF NOT EXISTS default.realtime_transactions
(
    step             Int32,
    type             String,
    amount           Float64,
    name_orig        String,
    oldbalance_orig  Float64,
    newbalance_orig  Float64,
    name_dest        String,
    oldbalance_dest  Float64,
    newbalance_dest  Float64,
    is_fraud         UInt8,
    event_time       Float64,
    ingested_at      DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (step, name_orig);

-- External tables reading the GOLD layer straight from MinIO (S3-compatible),
-- no separate loader needed -- these are the sources for the dbt staging models.
--
-- Columns are spelled out so CREATE works on a fresh stack, before the batch
-- job has written any gold files: without them ClickHouse infers the schema
-- by reading the Parquet at CREATE time and fails on an empty bucket.
-- Types match what Spark's silver_to_gold() actually writes. Keep them
-- Nullable: Spark writes optional Parquet columns, and a non-Nullable column
-- turns NULL into '' / 0, which silently breaks "merchant_name is not null"
-- in dim_high_risk_merchants.
CREATE TABLE IF NOT EXISTS default.gold_fraud_rate_daily
(
    day                 Nullable(Int32),
    total_transactions  Nullable(Int64),
    fraud_transactions  Nullable(Int64),
    fraud_rate          Nullable(Float64)
)
ENGINE = S3(
    'http://minio:9000/fraud-lake/gold/fraud_rate_daily/*.parquet',
    'minio_admin', '${MINIO_ROOT_PASSWORD}', 'Parquet'
);

CREATE TABLE IF NOT EXISTS default.gold_high_risk_merchants
(
    name_dest           Nullable(String),
    merchant_name       Nullable(String),
    merchant_category   Nullable(String),
    total_transactions  Nullable(Int64),
    fraud_count         Nullable(Int64),
    avg_risk_score      Nullable(Decimal(7, 6))   -- avg() over Postgres NUMERIC(3,2)
)
ENGINE = S3(
    'http://minio:9000/fraud-lake/gold/high_risk_merchants/*.parquet',
    'minio_admin', '${MINIO_ROOT_PASSWORD}', 'Parquet'
);

CREATE TABLE IF NOT EXISTS default.gold_transaction_velocity_by_user
(
    name_orig           Nullable(String),
    step                Nullable(Int32),
    tx_count_in_step    Nullable(Int64)
)
ENGINE = S3(
    'http://minio:9000/fraud-lake/gold/transaction_velocity_by_user/*.parquet',
    'minio_admin', '${MINIO_ROOT_PASSWORD}', 'Parquet'
);
SQL
