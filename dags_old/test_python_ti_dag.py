from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['testmail@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


def say_hello(ti):
    name = ti.xcom_pull(task_ids='get_name', key='name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"My name is {name} and I'm {age} years old!")


def get_age(ti):
    ti.xcom_push(key='age', value=29)


def get_name(ti):
    ti.xcom_push(key='name', value='Alex')


with DAG(
        dag_id="test_python_ti_dag",
        start_date=datetime(2024, 12, 1),
        default_args=default_args,
        schedule_interval="7 * * * *") as dag:

    task_1 = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello
    )

    task_2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task_3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task_2, task_3] >> task_1
