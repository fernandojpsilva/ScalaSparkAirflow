FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark apache-airflow-providers-openlineage==1.12.1 pyspark