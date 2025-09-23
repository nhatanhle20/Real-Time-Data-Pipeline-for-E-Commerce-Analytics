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
    dag_id='daily_data_simulation',
    default_args=default_args,
    description='Run Kafka producers and Spark streaming concurrently for 60 seconds',
    schedule_interval='@daily',
    catchup=False
)

# Task 1: Transaction Kafka producer
run_transaction_producer = BashOperator(
    task_id='run_transaction_producer',
    bash_command='python /app/transaction_producer.py',
    dag=dag
)

# Task 2: Clickstream Kafka producer
run_clickstream_producer = BashOperator(
    task_id='run_clickstream_producer',
    bash_command='python /app/clickstream_producer.py',
    dag=dag
)

# Task 3: Inventory Kafka producer
run_inventory_producer = BashOperator(
    task_id='run_inventory_producer',
    bash_command='python /app/inventory_update_producer.py',
    dag=dag
)

# Task 4: Spark job (in Spark container)
run_spark_stream = BashOperator(
    task_id='run_spark_streaming',
    bash_command='docker exec spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /app/spark_stream.py',
    dag=dag
)

# Dummy wait task that waits for all tasks to finish
wait_for_all = EmptyOperator(
    task_id='wait_for_completion',
    trigger_rule='all_done',
    dag=dag
)

# Run all producers and Spark job in parallel
[run_transaction_producer, run_clickstream_producer, run_inventory_producer, run_spark_stream] >> wait_for_all
