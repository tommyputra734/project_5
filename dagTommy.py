import airflow
from datetime import timedelta
from airflow import DAG 
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5)
}

dag_spark = DAG(
    dag_id = "dag_Project5_Tommy",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    dagrun_timeout=timedelta(minutes=60),
    description='Analyzing Trip using Spark Jon in Airflow',
    start_date= days_ago(1)
)

start = DummyOperator(task_id='start', dag=dag_spark)

spark_project5 = SparkSubmitOperator(
    application="/home/dev/airflow/spark-code/project_5_Tommy.py"
    conn_id="spark-standalone",
    task_id="spark_project5",
    dag=dag_spark
)

end = DummyOperator(task_id = 'end', dag=dag_spark)

start >> spark_project5 >> end