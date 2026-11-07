FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
ENV JAVA_HOME=/opt/java PYTHONPATH=/work/src PYTHONUNBUFFERED=1 \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark PYSPARK_PYTHON=python3
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# download the Spark-Kafka connector once, at build time
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3').getOrCreate().stop()"
WORKDIR /work
