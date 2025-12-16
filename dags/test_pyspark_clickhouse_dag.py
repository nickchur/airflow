from pyspark.sql import SparkSession
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import findspark
findspark.init()

# constants for connection
CLICKHOUSE_HOST_NAME = 'host.docker.internal'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_DBNAME = 'default'
CLICKHOUSE_USERNAME = 'clickhouse_user'
CLICKHOUSE_PASSWORD = 'clickhouse_password'
TABLE_NAME = 'test_table'

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
def run_clickhouse_basic_operaions():
    # Создаем SparkSession
    # the Spark session should be instantiated as follows
    spark = SparkSession \
        .builder \
        .appName("Python Spark ClickHouse basic example") \
        .master("local") \
        .getOrCreate()

   # JDBC URL for connection to ClickHouse
    jdbc_url = f"jdbc:clickhouse://{CLICKHOUSE_HOST_NAME}:{
        CLICKHOUSE_PORT}/{CLICKHOUSE_DBNAME}"
    jdbc_properties = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": CLICKHOUSE_USERNAME,
        "password": CLICKHOUSE_PASSWORD
    }

    # Creating test DataFrame
    data = [(1, 'John Doe'), (2, 'Jane Doe')]
    columns = ["id", "name"]
    df = spark.createDataFrame(data, columns)

    # Saving DataFrame to ClickHouse
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", 'super') \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("user", CLICKHOUSE_USERNAME) \
        .option("password", CLICKHOUSE_PASSWORD) \
        .option("createTableOptions", "ENGINE=MergeTree() ORDER BY id SETTINGS index_granularity=8192") \
        .mode("overwrite") \
        .save()

    # Reading DataFrame from ClickHouse
    spark.read.jdbc(url=jdbc_url, table="super",
                    properties=jdbc_properties).show()

    # Закрываем SparkSession
    spark.stop()


with DAG(
    dag_id='test_pyspark_clickhouse_dag',
    start_date=datetime(2024, 12, 1),
    default_args=default_args,
    schedule="@once",
    catchup=False,
) as dag:
    run_clickhouse_basic_operaions = PythonOperator(
        task_id="run_pyspark_job",
        python_callable=run_clickhouse_basic_operaions
    )

    run_clickhouse_basic_operaions
