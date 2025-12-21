from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(12),
    'email': ['testmail@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


def get_name(ti):
    ti.xcom_push(key='name', value='John')


def say_hello(ti):
    name = ti.xcom_pull(task_ids='get_name', key='name')
    print(f'Hi! My name is {name}')


with DAG(
        dag_id="test_bash_and_python_operator_dag",
        start_date=datetime(2024, 12, 1),
        default_args=default_args,
        schedule_interval="@hourly",
        catchup=True) as dag:

    task_1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello!'
    )
    task_2 = BashOperator(
        task_id='second_task',
        bash_command='echo biba i boba!'
    )
    task_3 = BashOperator(
        task_id='third_task',
        bash_command='echo lizun!'
    )
    task_4 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task_5 = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello
    )

    task_1 >> task_2
    task_3 >> [task_4, task_5]
