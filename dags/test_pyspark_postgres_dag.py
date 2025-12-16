from pyspark.sql import SparkSession
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import findspark
findspark.init()

# constants for connection
POSTGRES_HOST_NAME = 'host.docker.internal'
POSTGRES_PORT = 5432
POSTGRES_DBNAME = 'postgres_test'
POSTGRES_USERNAME = 'airflow'
POSTGRES_PASSWORD = 'airflow'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['testmail@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


# Функция для выполнения PySpark задачи
def run_pyspark_postgres_operaions():
    # Создаем SparkSession
    # the Spark session should be instantiated as follows
    spark = SparkSession \
        .builder \
        .appName("Python Spark PostgreSQL basic example") \
        .master("local") \
        .getOrCreate()

    # connect to our table and show data
    df = spark.read.format("jdbc"). \
        options(url=f'jdbc:postgresql://{POSTGRES_HOST_NAME}:{POSTGRES_PORT}/{POSTGRES_DBNAME}',  # jdbc:postgresql://<host>:<port>/<database>
                dbtable='pet',
                user=POSTGRES_USERNAME,
                password=POSTGRES_PASSWORD,
                driver='org.postgresql.Driver').load()
    df.show()

    # Закрываем SparkSession
    spark.stop()


with DAG(
    dag_id='test_pyspark_postgres_dag',
    start_date=datetime(2024, 12, 1),
    default_args=default_args,
    schedule="@once",
    catchup=False,
) as dag:
    run_pyspark_postgres_operaions = PythonOperator(
        task_id="run_pyspark_job",
        python_callable=run_pyspark_postgres_operaions
    )

    run_pyspark_postgres_operaions
