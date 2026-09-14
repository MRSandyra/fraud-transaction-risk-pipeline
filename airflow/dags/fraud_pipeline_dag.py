from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fraud_pipeline_dag",
    description="Batch ETL/ELT + dbt transform untuk fraud monitoring pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud", "medallion", "dbt", "pyspark"],
) as dag:

    run_batch_etl = BashOperator(
        task_id="run_batch_etl_pyspark",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--packages org.postgresql:postgresql:42.7.3,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 "
            "/opt/spark/spark_jobs/batch_bronze_silver_gold.py"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir . --target docker",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir . --target docker",
    )

    run_batch_etl >> dbt_run >> dbt_test