from airflow.decorators import dag, task
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


@dag(dag_id='test_with_taskflow_dag',
     start_date=datetime(2024, 12, 1),
     default_args=default_args,
     schedule_interval="10 * * * *")
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {'first_name': 'Tony',
                'second_name': 'Hawk'}

    @task()
    def get_age():
        return 20

    @task()
    def greet(first_name, second_name, age):
        print(f"Hi! My name is {first_name} {
              second_name} and I'm {age} years old!")

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'],
          second_name=name_dict['second_name'], age=age)


greet_dag = hello_world_etl()
