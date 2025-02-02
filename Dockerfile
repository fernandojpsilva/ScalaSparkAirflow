FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
RUN if [ "$(uname -m)" = "arm64" ]; then \
      export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64; \
    else \
      export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64; \
    fi

USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark apache-airflow-providers-openlineage==1.12.1 pyspark