from pyspark.sql.functions import when, col, lit, split, filter, explode, array, array_contains, count, min, max, mean, expr, udf, from_json, to_json
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, MapType, ArrayType, BooleanType
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import findspark
findspark.init()


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
def run_pyspark_basic_operaions():
    # Создаем SparkSession
    spark = SparkSession.builder \
        .appName("Simple PySpark Job") \
        .getOrCreate()

    # создаем простые данные и датафреймы
    data = [(1, 'Alex', 'Morgan', 'Chelsea, England', 2000, {'gender': 'F', 'age': 23}, 1),
            (2, 'Jake', 'Jonas', 'Barcelona, Spain',
             10000, {'gender': 'M', 'age': 35}, 3),
            (3, 'John', 'Fisk', 'CSKA, Russia', 1000, {'gender': 'M', 'age': 30}, 5)]

    data_1 = [(4, 'Lionel', 'Messi', 'France, PSG', 50000, {'gender': 'M', 'age': 34}, 2),
              (5, 'Michael', 'Duglas', 'New York Galaxy, USA',
               400, {'gender': 'M', 'age': 61}, 3),
              (6, 'Jane', 'One', 'Milan, Italy',
               3000, {'gender': 'F', 'age': 21}, 1),
              (7, 'Jane', 'One', 'Milan, Italy', 3000, {'gender': 'F', 'age': 21}, 2)]

    data_2 = [(1, 'IT'),
              (2, 'HR'),
              (3, 'Development'),
              (4, 'Analyitcs')]

    schema = StructType([StructField(name='id', dataType=IntegerType()),
                        StructField(name='first_name', dataType=StringType()),
                        StructField(name='second_name', dataType=StringType()),
                        StructField(name='team', dataType=StringType()),
                        StructField(name='salary', dataType=IntegerType()),
                        StructField(name='gender_and_age', dataType=MapType(
                            StringType(), StringType())),
                        StructField(name='dep_id', dataType=IntegerType())
                         ])

    schema_1 = StructType([StructField(name='id', dataType=IntegerType()),
                           StructField(name='dep_name', dataType=StringType())
                           ])

    df = spark.createDataFrame(data=data, schema=schema)
    df_1 = spark.createDataFrame(data=data_1, schema=schema)
    df_2 = spark.createDataFrame(data=data_2, schema=schema_1)

    # изменения данных и печать структуры таблиц
    df = df.select(df.id,
                   df.first_name,
                   split('team', ',')[0].alias('footbal_club'),
                   split('team', ',')[1].alias('footbal_country'),
                   df.salary,
                   df.gender_and_age['gender'].alias('gender'),
                   df.gender_and_age['age'].alias('age'),
                   df.dep_id.alias('dep_id'))
    df.printSchema()

    df_1 = df_1.select(df_1.id,
                       df_1.first_name,
                       split('team', ',')[0].alias('footbal_club'),
                       split('team', ',')[1].alias('footbal_country'),
                       df_1.salary,
                       df_1.gender_and_age['gender'].alias('gender'),
                       df_1.gender_and_age['age'].alias('age'),
                       df_1.dep_id.alias('dep_id'),
                       when(df_1.gender_and_age['age'] > 30, True).otherwise(False).alias('is_high_age').cast(BooleanType()))
    df_1.printSchema()

    df_2.show()
    df_2.printSchema()

    # проверка сохранения и загрузки файлов (проверка проблемы с winutils и Hadoop)
    df.coalesce(1).write.format('csv').mode(
        'overwrite').options(header=True).save('test')
    df_1.coalesce(1).write.format('csv').mode(
        'overwrite').options(header=True).save('test1')

    df = spark.read.format('csv').options(header=True).load('test')
    df_1 = spark.read.format('csv').options(header=True).load('test1')

    # поиск
    df.filter((df.age > 30) | (df.footbal_club.like('%a%'))).show()

    # удаление дубликатов
    df.dropDuplicates(['gender']).show()

    # сортировка
    df.orderBy(df.gender.asc(), df.first_name.desc()).show()
    df_1.sort(['gender', 'first_name'], ascending=[True, False]).show()

    # объединение таблиц в длину
    df.unionByName(
        other=df_1, allowMissingColumns=True).dropDuplicates().show()

    # группировка
    df.groupBy('gender').agg(count('footbal_country').alias('count_od_football_countries'),
                             min('age').alias('minimum_age')).show()

    # тестирование join (объединение таблиц по полям в ширину)
    df.join(other=df_2, on=df.dep_id == df_2.id, how='leftanti').show()

    # функции fillna and pivot
    df_3 = df.groupby('footbal_country').pivot('first_name').count().fillna(0)
    df_3.show()

    # функция unpivot
    df_3.select(df_3.footbal_country, expr(
        "stack(3, 'Alex', Alex, 'Jake', Jake, 'John', John) as (first_name, count)")).collect()[0]

    # sample
    df.sample(fraction=0.5, seed=123).show()

    # udf
    SalaryAge = udf(lambda x, y: x + y)
    df.select(SalaryAge(df.salary.cast(IntegerType()),
              df.age.cast(IntegerType()))).show()

    # rdd, map
    rdd = spark.sparkContext.parallelize(data)
    rdd.collect()
    rdd.map(lambda x: (x[0] + x[4],)).toDF().show()

    # partitionBy
    df.coalesce(1).write.format('csv').mode('overwrite').partitionBy(
        'gender').options(header=True).save('test2')
    df_4 = spark.read.format('csv').options(header=True).load('test2/gender=F')
    df_4.show()


with DAG(
    dag_id='test_pyspark_basic_operations_dag',
    start_date=datetime(2024, 12, 1),
    default_args=default_args,
    schedule="@once",
    catchup=False,
) as dag:
    run_pyspark_basic_operaions = PythonOperator(
        task_id="run_pyspark_job",
        python_callable=run_pyspark_basic_operaions
    )

    run_pyspark_basic_operaions
