# import pendulum
import json
from random import randint
from datetime import timedelta

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


def do_chk_meta(**context):
    dag_run = context['dag_run']
    if dag_run: logging.info("custom_business_event: %s", {"info": "some details", "run_id": dag_run.run_id})
    # dag_run.log_event(event="custom_business_event", extra='{"info": "some details"}')

    ch_hook = ClickHouseHook(clickhouse_conn_id=ch_id, database=ch_bd)
    ti = context.get('ti') or context.get('task_instance')

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
            where wf_name = '_GP_EXCHANGE'
                and JSONExtract(a.wf_data,'wf_name','String') = '_GP_EXCHANGE'
            group by 1
        ) a
        left join (
            select wf_id, count(distinct wf_name) bd_wf, count(*) bd_row
            from gp_ue_exchange
            where wf_name <> '_GP_EXCHANGE'
            GROUP by 1
        ) b on a.wf_id = b.wf_id
        order by a.wf_id desc, a.wf_key desc
    """)

    # chk_meta = ti.xcom_pull(task_ids="chk_meta", key="return_value") or []
    if [row for row in chk_meta if json.loads(row[0]).get('is_ok') == 'false']:
        logging.error("Data check failed in chk_meta task: %s", chk_meta)
        raise AirflowFailException("Data check failed in chk_meta task")
    return chk_meta

def choose_branchs(**context):
    ch_hook = ClickHouseHook(clickhouse_conn_id=ch_id, database=ch_bd)
    ti = context.get('ti') or context.get('task_instance')

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
    ti.xcom_push(key='tables', value=tables)

    # tables = ti.xcom_pull(task_ids="get_tables", key="return_value") or []
    downstream = context['task'].downstream_task_ids

    branches = ["process_" + row[0] for row in tables if ("process_" + row[0]) in downstream and row[0] != '_GP_EXCHANGE']
    return branches if branches else 'process__empty'

def process_any(fields, create='', drop_source=False, clear_trg=None, **context):

    ch_hook = ClickHouseHook(clickhouse_conn_id=ch_id, database=ch_bd)
    ti = context.get('ti') or context.get('task_instance')
    table = ti.task_id.replace('process_', '') if ti else ''
    # now= ch_hook.execute(f"""select now()""")[0][0]
    now = ti.execution_date.strftime("%Y-%m-%d %H:%M:%S")

    chk_in= ch_hook.execute(f"""
        select toJSONString(map(
            'is_ok', (a.wf_cnt == b.wf_cnt)::bool::text
            , 'wf_id', a.wf_id::text
            , 'wf_key', a.wf_key::text
            , 'wf_cnt', a.wf_cnt::text
            , 'chk_cnt', b.wf_cnt::text
            ))
        from (
            select wf_id, wf_key, count(*) wf_cnt
            from gp_ue_exchange 
            where wf_name = '{table}'
            GROUP by 1,2
        ) a
        left join (
            select distinct wf_id, wf_key, wf_cnt
            from gp_exchange_log a
            where wf_name = '{table}' and wf_type = 'GP_OUT'
        ) b on a.wf_id = b.wf_id and a.wf_key = b.wf_key 
        order by a.wf_id desc, a.wf_key desc
    """)

    if [row for row in chk_in if json.loads(row[0]).get('is_ok') == 'false']:
        logging.error("Data check failed in chk_in task: %s", chk_in)
        raise AirflowFailException("Data check failed in chk_in task")
    else:
        logging.info("Data check passed for table %s: %s", table, chk_in)
        ti.xcom_push(key='chk_in', value=chk_in)

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

    select = f"""
        select distinct toDateTime('{now}') as insert_time
            {fields}
            , a.wf_key as wf_key
            , a.wf_id as wf_id
        from gp_ue_exchange a
        where a.wf_name = '{table}'
    """

    if create: ch_hook.execute(f"""
        CREATE TABLE IF NOT EXISTS gp_{table} {ON_CLUSTER}
        {create}
        as {select} limit 0
    """)
    
    if clear_trg == 'drop': 
        res = ch_hook.execute(f""" select distinct wf_key from gp_ue_exchange where wf_name = '{table}' """)
        for key in res:
            ch_hook.execute(f""" ALTER TABLE gp_{table} {ON_CLUSTER} DROP PARTITION '{key[0]}' """)

    elif clear_trg == 'truncate':
        ch_hook.execute(f"""TRUNCATE TABLE gp_{table} {ON_CLUSTER}""")

    elif clear_trg == 'delete':
        res = ch_hook.execute(f"""select distinct wf_key from gp_ue_exchange where wf_name = '{table}' """)
        for key in res:
            ch_hook.execute(f""" DELETE FROM gp_{table} {ON_CLUSTER} WHERE wf_key = '{key[0]}' """)

    elif clear_trg == 'optimize': pass

    elif clear_trg is not None:
        logging.error(f"Unknown clear_trg option: {clear_trg}")
        raise AirflowFailException(f"Unknown clear_trg option: {clear_trg}")

    ch_hook.execute(f""" insert into gp_{table} {select} """)
    
    if clear_trg == 'optimize': ch_hook.execute(f""" OPTIMIZE TABLE gp_{table} {ON_CLUSTER} """)

    # log insert into gp_exchange_log for this load (GP_IN)
    ch_hook.execute(f"""
        insert into gp_exchange_log
        select distinct toDateTime('{now}') as insert_time
            , a.wf_id
            , 'GP_IN' as wf_type
            , {table} wf_name
            , a.wf_key
            , count(*) wf_cnt
        from gp_{table} a
        where a.insert_time = toDateTime('{now}')
        group by a.wf_id, a.wf_key
    """)
    logging.info("Inserted gp_exchange_log entries for table %s", table)

    # chk_out= ch_hook.execute(
    f"""
        select toJSONString(map(
            'is_ok', (a.wf_cnt == b.wf_cnt)::bool::text
            , 'wf_id', a.wf_id::text
            , 'wf_key', a.wf_key::text
            , 'wf_cnt', a.wf_cnt::text
            , 'chk_cnt', b.wf_cnt::text
            ))
        from (
            select wf_id, wf_key, count(*) wf_cnt
            from gp_{table} 
            where insert_time = toDateTime('{now}')
        ) a
        join (
            select wf_id, wf_key, count(*) wf_cnt
            from gp_{table} 
            where insert_time = toDateTime('{now}')
        ) b on a.wf_id = b.wf_id and a.wf_key = b.wf_key
        order by a.wf_id desc, a.wf_key desc
    """ #)
    # if [row for row in chk_out if json.loads(row[0]).get('is_ok') == 'false']:
    #     logging.error("Data check failed in chk_out task: %s", chk_out)
    #     raise AirflowFailException("Data check failed in chk_out task")
    # else:
    #     logging.info("Data check passed for table %s: %s", table, chk_out)

    if drop_source: ch_hook.execute(f""" ALTER TABLE gp_ue_exchange {ON_CLUSTER} DROP PARTITION '{table}' """)

    ret = ch_hook.execute(f"""
        select toJSONString(map(
                'type', type
                , 'row_cnt', toString(row_cnt)
                , 'min_ctl', toString(min_ctl)
                , 'max_ctl', toString(max_ctl)
                , 'min_its', toString(min_its)
                , 'max_its', toString(max_its)
            ))
        from (
            select 'all' type, count(1) row_cnt 
                , min(wf_id) min_ctl, max(wf_id)  max_ctl
                , min(insert_time) min_its, max(insert_time)  max_its
            from gp_{table} {final}
            union all
            select 'now' as type, count(1) row_cnt
                , min(wf_id) min_ctl, max(wf_id)  max_ctl
                , min(insert_time) min_its, max(insert_time)  max_its   
            from gp_{table} {final}
            where insert_time = toDateTime('{now}')
        ) a
        order by a.type desc
    """)
    
    return ret

exchange_log_sql=(f"""
    DROP TABLE IF EXISTS gp_exchange_log {ON_CLUSTER}
""", f"""
    CREATE TABLE IF NOT EXISTS gp_exchange_log {ON_CLUSTER}
    (
        insert_time DateTime
        , wf_id Int32
        , wf_type String
        , wf_name String
        , wf_key String
        , wf_cnt Int64
        , wf_data String
    )
    ENGINE = Log
""", f"""
    insert into gp_exchange_log
    select distinct toDateTime('{{{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}}}') as insert_time
        , a.wf_id
        , 'GP_OUT' wf_type
        , JSONExtract(a.wf_data, 'wf_name', 'String') wf_name
        , JSONExtract(a.wf_data, 'wf_key', 'String') wf_key
        , JSONExtract(a.wf_data, 'cnt', 'Int32') wf_cnt
        , a.wf_data
    from gp_ue_exchange a
    where a.wf_name = '_GP_EXCHANGE'
        -- and a.wf_id > (select ifnull(max(wf_id), 0) from gp_exchange_log)
""", f"""
    ALTER TABLE gp_ue_exchange {ON_CLUSTER} DROP PARTITION '_GP_EXCHANGE'
""", f"""
    select toJSONString(map(
        'type', type
        , 'row_cnt', toString(row_cnt)
        , 'min_ctl', toString(min_ctl)
        , 'max_ctl', toString(max_ctl)
        , 'min_its', toString(min_its)
        , 'max_its', toString(max_its)
        ))
    from (
        select 'all' as type, count(1) row_cnt 
            , min(wf_id) min_ctl, max(wf_id)  max_ctl
            , min(insert_time) min_its, max(insert_time)  max_its
        from gp_exchange_log
        union all
        select 'now' as type, count(1) row_cnt
            , min(wf_id) min_ctl, max(wf_id)  max_ctl
            , min(insert_time) min_its, max(insert_time)  max_its   
        from gp_exchange_log
        where insert_time = toDateTime('{{{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}}}')
    ) a
    order by a.type desc
""")


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
    
    process_exchange_log = ClickHouseOperator(
        task_id="process_exchange_log",
        clickhouse_conn_id=ch_id,
        database=ch_bd,
        # trigger_rule='all_done',
        sql=exchange_log_sql
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

load_exchange >>  chk_meta >>  process_exchange_log >> branch_task>> [
    process__empty
    , process_tb_tmp1
    , process_tb_tmp2
    , process_tb_tmp3
 ]
