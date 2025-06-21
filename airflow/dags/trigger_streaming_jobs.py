from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='run_kafka_and_spark_streaming',
    default_args=default_args,
    description='Run Kafka producer and Spark streaming concurrently for 60 seconds',
    schedule_interval=None,
    catchup=False
)

# Task 1: Kafka producer (Airflow container)
run_kafka_producer = BashOperator(
    task_id='run_kafka_producer',
    bash_command='python /app/producer.py',
    dag=dag
)

# Task 2: Spark job (in Spark container)
run_spark_stream = BashOperator(
    task_id='run_spark_streaming',
    bash_command='docker exec spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /app/spark_stream.py',
    dag=dag
)

# Dummy wait task that waits for both to finish
wait_for_both = EmptyOperator(
    task_id='wait_for_completion',
    trigger_rule='all_done',
    dag=dag
)

# Run in parallel, wait after both
[run_kafka_producer, run_spark_stream] >> wait_for_both
