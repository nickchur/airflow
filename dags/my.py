# import pendulum
import json
from random import randint
from datetime import timedelta, datetime

from airflow import DAG, Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# from hrp_operators import HrpS3ToClickhouseTableOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import BranchSQLOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowFailException

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models.xcom_arg import XComArg

from my_conf import EXCHANGE_DATASET, ON_CLUSTER
from my_conf import default_args, SQL_INS, SQL_INIT

import logging


ch_bd = default_args.get('clickhouse_bd', 'default')
ch_id = default_args.get('clickhouse_conn_id', 'ch_local')
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def do_chk_meta(**context):
    """
    Проверка метаданных _GP_EXCHANGE.
    Возвращает chk_meta (список строк JSON). Если проверка не проходит — выбрасывает AirflowFailException.
    В лог пишется человекочитаемое сообщение вместо вызова несуществующего dag_run.log_event.
    """
    dag_run = context['dag_run']
    if dag_run: logging.info("custom_business_event: %s", {"info": "some details", "run_id": dag_run.run_id})
    # dag_run.log_event(event="custom_business_event", extra='{"info": "some details"}')

    ch_hook = ClickHouseHook(clickhouse_conn_id=ch_id, database=ch_bd)
    ti = context.get('ti') or context.get('task_instance')

    # Выполняем SQL-проверку: агрегируем wf_id и сравниваем ожидаемые количества
    chk_meta= ch_hook.execute(f"""
        select toJSONString(map(
            'is_ok', (a.wf_cnt = b.bd_wf)::bool::text
            , 'wf_id', a.wf_id::text
            , 'wf_key', a.wf_key::text
            , 'wf_cnt', a.wf_cnt::text
            , 'chk_cnt', b.bd_wf::text
            , 'chk_row', b.bd_row::text
            ))
        from (
            select wf_id, max(a.wf_key) wf_key, sum(JSONExtract(wf_data,'cnt','Int32')) wf_cnt
            from gp_ue_exchange a
            where wf_name = '_gp_exchange'
                and JSONExtract(a.wf_data,'wf_name','String') = '_gp_exchange'
            group by 1
        ) a
        left join (
            select wf_id, count(distinct wf_name) bd_wf, count(*) bd_row
            from gp_ue_exchange
            -- where wf_name <> '_gp_exchange'
            GROUP by 1
        ) b on a.wf_id = b.wf_id
        order by a.wf_id desc, a.wf_key desc
    """)

    # Проверка результата: если хоть одна строка содержит is_ok == false — фейлим таск
    if not chk_meta or [row for row in chk_meta if json.loads(row[0]).get('is_ok') == 'false']:
        logging.error("Data check failed in chk_meta task: %s", chk_meta)
        raise AirflowFailException("Data check failed in chk_meta task")
    return chk_meta

def choose_branchs(**context):
    """
    Собирает список доступных wf_name'ов из таблицы gp_ue_exchange и
    возвращает список task_id'ов для BranchPythonOperator.
    Также пушит результат в XCom под ключом 'tables' для отладки.
    """
    ch_hook = ClickHouseHook(clickhouse_conn_id=ch_id, database=ch_bd)
    ti = context.get('ti') or context.get('task_instance')

    # Получаем метрики по каждой wf_name (кол-во, min/max id и ключи и т.п.)
    tables= ch_hook.execute(f"""
        select wf_name, toJSONString(map(
            'row_cnt', toString(row_cnt)
            , 'min_id', toString(min_id)
            , 'max_id', toString(max_id)
            , 'cnt_id', toString(cnt_id)
            , 'min_key', toString(min_key)
            , 'max_key', toString(max_key)
            , 'cnt_key', toString(cnt_key)
            ))
        from (
            select wf_name , count(*) row_cnt
                , min(wf_id) min_id, max(wf_id) max_id, count(distinct wf_id) cnt_id
                , min(wf_key) min_key, max(wf_key) max_key, count(distinct wf_key) cnt_key
            from gp_ue_exchange
            group by 1
        ) a
        order by wf_name asc
    """)
    ti.xcom_push(key='tables', value=tables)  # полезно для отладки и тестирования

    # downstream_task_ids — набор доступных дочерних тасков, фильтруем по ним
    downstream = context['task'].downstream_task_ids
    # игнорируем контрольную запись '_gp_exchange'
    branches = ["process_" + row[0] for row in tables if ("process_" + row[0]) in downstream and row[0] != '_gp_exchange']
    return branches if branches else 'process__empty'

def process_any(fields, create='', drop_source=False, clear_trg=None, wf_key='wf_key', chk_in=True, chk_out=True, **context):
    """
    Универсальная функция обработки для process_<wf_name> тасков.
    Аргументы:
        fields      - SQL-фрагмент с полями для выборки
        create      - SQL DDL для создания целевой таблицы (если нужно)
        drop_source - флаг удаления партиции-источника после загрузки
        clear_trg   - опции очистки целевой таблицы: 'drop'|'truncate'|'delete'|'optimize'|None
        chk_in      - включить проверку входящих данных перед загрузкой
        chk_out     - включить проверку выходных данных после загрузки
    Поведение:
        - ставит записи в целевую таблицу gp_<table>
        - логирует факты загрузки в gp__gp_exchange (контроль)
        - поддерживает разные стратегии очистки целевой таблицы
    """
    # ClickHouse hook и TaskInstance
    ch_hook = ClickHouseHook(clickhouse_conn_id=ch_id, database=ch_bd)
    ti = context.get('ti') or context.get('task_instance')
    table = ti.task_id.replace('process_', '') if ti else ''

    # Опционально: проверка входных данных (сравнение счетчиков)
    if chk_in:
        chk_in_res= ch_hook.execute(f"""
            select toJSONString(map(
                'is_ok', (a.wf_cnt == b.cnt)::bool::text
                , 'wf_id', a.wf_id::text
                , 'wf_key', a.wf_key::text
                , 'wf_cnt', a.wf_cnt::text
                , 'chk_cnt', b.cnt::text
                ))
            from (
                select wf_id, wf_key, count(*) wf_cnt
                from gp_ue_exchange 
                where wf_name = '{table}'
                GROUP by 1,2
            ) a
            left join (
                select distinct wf_id, JSONExtract(wf_data, 'wf_key', 'String') tb_key, cnt
                from gp__gp_exchange a
                where type = 'GP_OUT' AND wf_name = '{table}'
            ) b on a.wf_id = b.wf_id and a.wf_key = b.tb_key 
            order by a.wf_id desc, a.wf_key desc
        """)
        ti.xcom_push(key='chk_in', value=chk_in_res)

        if [row for row in chk_in_res if json.loads(row[0]).get('is_ok') == 'false']:
            logging.error("Data check failed in chk_in task: %s", chk_in_res)
            raise AirflowFailException("Data check failed in chk_in task")

    # Определение FINAL при создании MergeTree с финальной компакцией
    final_types = [
        'ReplacingMergeTree',
        'SummingMergeTree',
        'AggregatingMergeTree',
        'CollapsingMergeTree',
        'VersionedCollapsingMergeTree',
    ]
    final = ''
    for t in final_types:
        if create and (t in create):
            final = ' FINAL'
            break

    # Формируем SELECT для вставки в целевую таблицу
    select = f"""
        select distinct toDateTime('{now}') as insert_time
            {fields}
            , a.wf_key as wf_key
            , a.wf_id as wf_id
        from gp_ue_exchange a
        where a.wf_name = '{table}'
    """

    # Создаём таблицу при необходимости, выполняем очистки/вставку и логирование
    if create: 
        ch_hook.execute(f""" 
            CREATE TABLE IF NOT EXISTS gp_{table} {ON_CLUSTER}
            {create} 
            as 
            {select} 
            limit 0 
        """)
    
    if clear_trg == 'drop': 
        res = ch_hook.execute(f""" select distinct {wf_key} from gp_ue_exchange where wf_name = '{table}' """)
        for key in res:
            ch_hook.execute(f""" ALTER TABLE gp_{table} {ON_CLUSTER} DROP PARTITION '{key[0]}' """)

    elif clear_trg == 'truncate':
        ch_hook.execute(f"""TRUNCATE TABLE gp_{table} {ON_CLUSTER}""")

    elif clear_trg == 'delete':
        res = ch_hook.execute(f"""select distinct {wf_key} from gp_ue_exchange where wf_name = '{table}' """)
        for key in res:
            ch_hook.execute(f""" DELETE FROM gp_{table} {ON_CLUSTER} WHERE {wf_key} = cast('{key[0]}', toTypeName({wf_key})) """)

    elif clear_trg == 'optimize': pass

    elif clear_trg is not None:
        logging.error(f"Unknown clear_trg option: {clear_trg}")
        raise AirflowFailException(f"Unknown clear_trg option: {clear_trg}")

    ch_hook.execute(f""" insert into gp_{table} {select} """)
    
    if clear_trg == 'optimize': ch_hook.execute(f""" OPTIMIZE TABLE gp_{table} {ON_CLUSTER} """)

    # проверка выходных данных (опционально)
    if chk_out:
        chk_out_res = ch_hook.execute(f"""
            select toJSONString(map(
                    'is_ok', (a.wf_cnt = b.cnt)::bool::text
                    , 'wf_id', a.wf_id::text
                    , 'key_cnt', a.key_cnt::text
                    , 'row_cnt', a.wf_cnt::text
                    , 'chk_cnt', b.cnt::text
                ))
            from (
                select wf_id, count(*) wf_cnt, count(distinct wf_key) key_cnt
                from gp_{table} {final}
                where insert_time = toDateTime('{now}')
                group by 1
                order by 1 desc
                limit 1
            ) a
            left join (
                select distinct wf_id, cnt
                from gp__gp_exchange a
                where type = 'GP_OUT' AND wf_name = '{table}'
            ) b on a.wf_id = b.wf_id 
        """ )
        ti.xcom_push(key='chk_out', value=chk_out_res)
        if [row for row in chk_out_res if json.loads(row[0]).get('is_ok') == 'false']:
            logging.error("Data check failed in chk_out task: %s", chk_out_res)
            raise AirflowFailException("Data check failed in chk_out task")

    # логируем факт загрузки в контрольную таблицу gp__gp_exchange
    ch_hook.execute(f"""
        insert into gp__gp_exchange
        select toDateTime('{now}') as insert_time
            , 'CH_IN' as type
            , '{table}' wf_name
            , count(*)::text cnt
            , sum(length(concat(a.*)))::text sum_len
            , toJSONString(map(
                'min_len', min(length(concat(a.*)))::text
                , 'max_len', max(length(concat(a.*)))::text
                , 'time', toString(nowInBlock()-now())
            )) wf_data
            , a.wf_key
            , a.wf_id
        from gp_{table} a {final}
        where a.insert_time = toDateTime('{now}')
        group by a.wf_id, a.wf_key
    """)
    logging.info("Inserted gp__gp_exchange entries for table %s", table)

    if drop_source: ch_hook.execute(f""" ALTER TABLE gp_ue_exchange {ON_CLUSTER} DROP PARTITION '{table}' """)

    # Возвращаем базовую статистику по таблице
    ret = ch_hook.execute(f"""
        select  toJSONString(map(
                'type', type
                , 'row_cnt', toString(row_cnt)
                , 'min_ctl', toString(min_ctl)
                , 'max_ctl', toString(max_ctl)
                --, 'cnt_ctl', toString(cnt_ctl)
                , 'min_its', toString(min_its)
                , 'max_its', toString(max_its)
                --, 'cnt_its', toString(cnt_its)
                --, 'time', toString(nowInBlock()-toDateTime('{now}')())
            ))
        from (
            select 'all' type, count(1) row_cnt
                , min(wf_id) min_ctl, max(wf_id)  max_ctl
                --, count(distinct wf_id) cnt_ctl
                , min(insert_time) min_its, max(insert_time)  max_its
                --, count(distinct insert_time) cnt_its
            from gp_{table} {final}
            union all
            select 'now' type, count(1) row_cnt
                , min(wf_id) min_ctl, max(wf_id)  max_ctl
                --, count(distinct wf_id) cnt_ctl
                , min(insert_time) min_its, max(insert_time)  max_its
                --, count(distinct insert_time) cnt_its   
            from gp_{table} {final}
            where insert_time = toDateTime('{now}')
        ) a
        order by a.type desc
    """)
    
    return ret

"""
DAG: 0_import_gp_ue_exchange

Краткое описание:
    Загружает универсальный обмен (gp_ue_exchange) и выполняет
    пост-процессинг по каждому wf_name.

Поведение и ключевые моменты:
    - load_exchange: вставляет сырые записи в таблицу gp_ue_exchange (SQL_INS).
    - chk_meta: проверяет агрегированную мета-информацию в _GP_EXCHANGE (фейлит при несоответствии).
    - process__gp_exchange: пишет контрольные записи в gp_exchange_log и формирует данные для ветвления.
    - branch_task: выбирает process_* задачи по наличию соответствующих wf_name.
    - process_any (process_<wf_name>): универсальная загрузка в gp_<wf_name>,
        создает таблицу при необходимости, поддерживает clear_trg: drop/truncate/delete/optimize,
        опциональные проверки chk_in/chk_out, логирование загрузок в gp__gp_exchange.
    - Вставки помечаются временем задачи (ti.start_date -> insert_time).
    - Использует ClickHouseHook/ClickHouseOperator (по умолчанию clickhouse_conn_id=ch_local).
    - XCom ключи: 'tables' (choose_branchs), 'chk_in', 'chk_out'.

Рекомендации:
    - Прогонять DAG в тестовой среде и проверять корректность схем контрольных таблиц.
"""

with DAG(
    dag_id='0_import_gp_ue_exchange',
    description="Загрузка универсального обмена из ПКАП",
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args=default_args,
    start_date=days_ago(1),
    schedule=[EXCHANGE_DATASET],
    tags=['DataLab', 'import', 'tfs', 'CI02420667', 'pkap', 'exchange'],
    catchup=False,
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    max_active_runs=1,
    max_active_tasks=1,
    # outlet=EXCHANGE_DATASET,
) as dag_import:

    # def list_s3_bucket() -> list:
    #     return list() if randint(0, 3) != 0 else ['yes']
    
    # list_files = PythonOperator(
    #     task_id = 'S3_list_files',
    #     python_callable = list_s3_bucket,
    #     do_xcom_push = True
    # )

    load_exchange = ClickHouseOperator(
        task_id="load_exchange",
        database=ch_bd,
        clickhouse_conn_id=ch_id,
        do_xcom_push = True,
        sql=SQL_INS
    )

    chk_meta = PythonOperator(
        task_id='chk_meta',
        python_callable=do_chk_meta,
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=choose_branchs,
    )
    
    process__gp_exchange = PythonOperator(
        task_id="process__gp_exchange",
        python_callable=process_any,
        op_kwargs = {
            'fields': """
                , 'GP_OUT' as "type"
                , JSONExtract(a.wf_data, 'wf_name', 'String') wf_name
                --, JSONExtract(a.wf_data, 'wf_key', 'String') tb_key
                , JSONExtract(a.wf_data, 'cnt', 'Int32') cnt
                , JSONExtract(a.wf_data, 'sum_len', 'Int32') sum_len
                , a.wf_data
            """,
            'create': """
                ENGINE = ReplacingMergeTree(insert_time)
                ORDER BY (wf_id, wf_name, wf_key, type, JSONExtract(wf_data, 'wf_key', 'String'))
            """,
            'drop_source': True,
            'clear_trg': 'optimize',
            # 'wf_key': 'wf_id',
            'chk_in': False,
            'chk_out': True,
            }
    )

    process_tb_tmp1 = PythonOperator(
        task_id="process_tb_tmp1",
        python_callable=process_any,
        op_kwargs = {
            'create': """
                ENGINE = ReplacingMergeTree(wf_id)
                ORDER BY id
            """,
            'fields': """
                , JSONExtract(a.wf_data, 'id', 'Int32') id
                , JSONExtract(a.wf_data, 'value', 'String') value
            """,
            'drop_source': True,
            'clear_trg': 'optimize',
        }
    )

    process_tb_tmp2 = PythonOperator(
        task_id="process_tb_tmp2",
        python_callable=process_any,
        op_kwargs = {
            'fields': """
                , JSONExtract(a.wf_data, 'id', 'DateTime') id
                , JSONExtract(a.wf_data, 'value', 'Int32') value
            """,
            'create': """
                ENGINE = MergeTree()
                PARTITION BY wf_key
                ORDER BY id
            """,
            'drop_source': True,
            'clear_trg': 'drop',
            }
    )
    
    process_tb_tmp3 = PythonOperator(
        task_id="process_tb_tmp3",
        python_callable=process_any,
        op_kwargs = {
            'create': """
                ENGINE = MergeTree()
                ORDER BY id
            """,
            'fields': """
                , JSONExtract(a.wf_data, 'id', 'Int32') id
                , JSONExtract(a.wf_data, 'value', 'String') value
            """,
            'drop_source': True,
            'clear_trg': 'delete',
        }
    )

    process__empty = DummyOperator(task_id='process__empty')

load_exchange >>  chk_meta >>  process__gp_exchange >> branch_task>> [
    process__empty
    , process_tb_tmp1
    , process_tb_tmp2
    , process_tb_tmp3
 ]
