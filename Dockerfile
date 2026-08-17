FROM apache/airflow:3.3.0-python3.10

USER root

# Устанавливаем OpenJDK 17 и procps (необходим для работы Spark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jdk-headless \
       procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Задаем переменную окружения JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Устанавливаем PySpark
RUN pip install --no-cache-dir "apache-airflow[apache.spark]" pyspark==3.5.0