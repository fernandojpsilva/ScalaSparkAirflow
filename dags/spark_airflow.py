import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "spark_airflow_testing",
    default_args = {
        "owner": "Fernando Silva",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

check_spark = BashOperator(
    task_id='check_spark',
    bash_command='''
    spark_status=$(curl -s spark-master:8080 2>&1 | grep "Spark Master")
    if [ -z "$spark_status" ]; then
        echo "Spark is not ready"
        exit 1
    else
        echo "Spark is ready"
    fi
    ''',
    dag=dag,
)

check_kafka = BashOperator(
    task_id='check_kafka',
    bash_command='''
    kafka_status=$(curl -s kafka:9092 -v 2>&1 | grep "Connected to")
    if [ -z "$kafka_status" ]; then
        echo "Kafka is not ready"
        exit 1
    else
        echo "Kafka is ready"
    fi
    ''',
    dag=dag,
)

submit_spark_job = SparkSubmitOperator(
    task_id='submit_spark_job',
    application='/opt/airflow/jobs/python/web_data_processor.py',
    conn_id='spark-conn',
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0',
    verbose=True,
    driver_memory='1g',
    executor_memory='1g',
    num_executors=1,
    executor_cores=1,
    dag=dag,
)

start_producer = BashOperator(
    task_id='start_web_data_producer',
    bash_command='python /opt/airflow/jobs/web_data_producer.py &',
    dag=dag,
)

stop_producer = BashOperator(
    task_id='stop_web_data_producer',
    bash_command='''
    pid=$(ps aux | grep web_data_producer.py | grep -v grep | awk '{print $2}')
    if [ ! -z "$pid" ]; then
        echo "Stopping producer with PID $pid"
        kill $pid
    else
        echo "Producer not running"
    fi
    ''',
    dag=dag,
)

producer_job = SparkSubmitOperator(
    task_id="producer_job",
    conn_id="spark-conn",
    application="jobs/python/kafka_producer.py",
    dag=dag
)

scala_job = SparkSubmitOperator(
    task_id="scala_job",
    conn_id="spark-conn",
    application="jobs/scala/target/scala-2.12/word-count_2.12-0.1.jar",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> [check_kafka, check_spark] >> start_producer >> submit_spark_job >> stop_producer >> end