FROM apache/airflow:2.10.5

USER root
ENV DEBIAN_FRONTEND=noninteractive

# Minimal apt deps, OpenJDK, cleanup
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget ca-certificates curl gnupg tar gzip unzip \
        openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

# Spark version (adjust if needed)
ARG SPARK_VERSION=3.5.4
ARG SPARK_PKG=spark-${SPARK_VERSION}-bin-hadoop3
ARG SPARK_URL="https://downloads.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_PKG}.tgz"

# Download and extract Spark
RUN mkdir -p /opt/spark && \
    wget -qO /tmp/spark.tgz "${SPARK_URL}" && \
    tar -xzf /tmp/spark.tgz -C /opt/spark && \
    rm /tmp/spark.tgz && \
    mv /opt/spark/${SPARK_PKG} /opt/spark/spark

# Download JDBC drivers and place into Spark jars
RUN curl -fsSL -o /tmp/postgresql.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar && \
    curl -fsSL -o /tmp/clickhouse.jar https://github.com/ClickHouse/clickhouse-java/releases/download/v0.6.4/clickhouse-jdbc-0.6.4-all.jar && \
    mkdir -p /opt/spark/spark/jars && \
    mv /tmp/postgresql.jar /opt/spark/spark/jars/ && \
    mv /tmp/clickhouse.jar /opt/spark/spark/jars/ && \
    chown -R airflow:root /opt/spark/spark

# Environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.8-src.zip

# Switch back to non-root user
USER airflow

# Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Notes:
# - sshd and Oracle JDK removed (use sidecar/container for remote shell; use OpenJDK).
# - If you need a different Spark version or additional jars, adjust ARGs above.