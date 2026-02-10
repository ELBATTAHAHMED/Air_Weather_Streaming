FROM apache/airflow:2.8.1-python3.11

USER root

ARG SPARK_VERSION=3.5.8
ARG HADOOP_PROFILE=3
ARG AIRFLOW_VERSION=2.8.1
ARG AIRFLOW_PYTHON_VERSION=3.11
ARG AIRFLOW_CONSTRAINTS_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON_VERSION}.txt

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      curl \
      ca-certificates \
      default-jre-headless \
      docker.io \
      powershell; \
    curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz" -o /tmp/spark.tgz; \
    tar -xzf /tmp/spark.tgz -C /opt; \
    ln -s "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}" /opt/spark; \
    ln -sf /opt/spark/bin/spark-submit /usr/local/bin/spark-submit; \
    rm -f /tmp/spark.tgz; \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="/home/airflow/.local/lib/python3.11/site-packages"

USER airflow

RUN pip install --no-cache-dir \
    --constraint "${AIRFLOW_CONSTRAINTS_URL}" \
    kafka-python \
    psycopg2-binary \
    requests
