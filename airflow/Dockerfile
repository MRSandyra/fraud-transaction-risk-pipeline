FROM apache/airflow:2.9.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir dbt-clickhouse==1.7.1
