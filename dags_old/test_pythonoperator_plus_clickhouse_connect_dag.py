from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import clickhouse_connect

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


def create_new_table_via_pythonoperator(host, port, database, username, password):
    client = clickhouse_connect.get_client(
        host=host, port=port, database=database, username=username, password=password)
    client.command(
        'CREATE TABLE IF NOT EXISTS new_table (key UInt32, value String, metric Float64) ENGINE MergeTree ORDER BY key')
    row1 = [1000, 'String Value 1000', 5.233]
    row2 = [2000, 'String Value 2000', -107.04]
    data = [row1, row2]
    client.insert('new_table', data, column_names=['key', 'value', 'metric'])
    return None


with DAG(
    dag_id='test_pythonoperator_plus_clickhouse_connect_dag',
    start_date=datetime(2024, 12, 1),
    default_args=default_args,
    schedule="@once",
    catchup=False,
) as dag:
    create_new_table = PythonOperator(
        task_id="create_new_clickhouse_table_via_python_operator_dag",
        python_callable=create_new_table_via_pythonoperator,
        op_kwargs={'host': 'host.docker.internal', 'port': 8123,
                   'database': 'default',
                   'username': 'clickhouse_user',
                   'password': 'clickhouse_password'}
    )

    create_new_table
