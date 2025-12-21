from airflow import Dataset

TFS_IN_SCENARIO = '27671910'
TFS_IN_CONN_ID = 's3-tfs-hrplt' ###
TFS_IN_BUCKET = 'tfshrplt' ###
TFS_IN_TOPIC = 'TFS.DSSIGMA.IN'
TFS_IN_PREFIX = 'to/CAPUE/pkap1080_to_hrplt/pc1080.' ###
TFS_KAFKA_CALLBACK = 'CI06932748.analytics.datalab.export_tfs.tfs_common.tfs_message_delivery_callback'
# CH_BD = 'support'

ON_CLUSTER = ''
EXCHANGE_DATASET = Dataset(f's3://{TFS_IN_BUCKET}/{TFS_IN_PREFIX}gp_ue_exchange.csv')


default_args = dict(
    owner='DataLab (CI02420667)',
    # retries=2,
    # retry_delay=pendulum.duration(seconds=30),
    bucket_name=TFS_IN_BUCKET,
    bucket=TFS_IN_BUCKET,
    s3_bucket=TFS_IN_BUCKET,
    aws_conn_id=TFS_IN_CONN_ID,
    clickhouse_conn_id='ch_local',
    clickhouse_bd='support',
    do_xcom_push=True,
    depends_on_past=False,
    wait_for_downstream=False,
)

sql_ins = ("""
    truncate table support.gp_ue_exchange;
""","""
with exchange as(
	SELECT 1 as wf_id, 'tb_tmp1' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":%d,"value":"%s","p":%d}', number, toString(nowInBlock()), sleep(1)) as wf_data from numbers(9)
	union all
	SELECT 2 as wf_id, 'tb_tmp1' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":%d,"value":"%s","p":%d}', number*2, toString(nowInBlock()), sleep(1)) as wf_data from numbers(8)
	union all
	SELECT 3 as wf_id, 'tb_tmp1' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":%d,"value":"%s","p":%d}', number*3, toString(nowInBlock()), sleep(1)) as wf_data from numbers(7)
	union all
	SELECT 4 as wf_id, 'tb_tmp1' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":%d,"value":"%s","p":%d}', number*4, toString(nowInBlock()), sleep(1)) as wf_data from numbers(6)
	
	union ALL
	SELECT 1 as wf_id, 'tb_tmp2' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":"%s","value":%d,"p":%d}', toString(nowInBlock()), number, sleep(1)) as wf_data from numbers(9)
	union all
	SELECT 2 as wf_id, 'tb_tmp2' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":"%s","value":%d,"p":%d}', toString(nowInBlock()), number*2, sleep(1)) as wf_data from numbers(8)
	union all
	SELECT 3 as wf_id, 'tb_tmp2' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":"%s","value":%d,"p":%d}', toString(nowInBlock()), number*3, sleep(1)) as wf_data from numbers(7)

    union all
	SELECT 1 as wf_id, 'tb_tmp3' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":%d,"value":"%s","p":%d}', number, toString(nowInBlock()), sleep(1)) as wf_data from numbers(9)
	union all
	SELECT 2 as wf_id, 'tb_tmp3' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":%d,"value":"%s","p":%d}', number*2, toString(nowInBlock()), sleep(1)) as wf_data from numbers(8)
	union all
	SELECT 3 as wf_id, 'tb_tmp3' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":%d,"value":"%s","p":%d}', number*3, toString(nowInBlock()), sleep(1)) as wf_data from numbers(7)
	
    union all
	SELECT 1 as wf_id, 'tb_tmp4' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":%d,"value":"%s","p":%d}', number, toString(nowInBlock()), sleep(1)) as wf_data from numbers(9)
	union all
	SELECT 2 as wf_id, 'tb_tmp4' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":%d,"value":"%s","p":%d}', number*2, toString(nowInBlock()), sleep(1)) as wf_data from numbers(8)
	union all
	SELECT 3 as wf_id, 'tb_tmp4' as wf_name, toString(toDate(now())) as wf_key, printf('{"id":%d,"value":"%s","p":%d}', number*3, toString(nowInBlock()), sleep(1)) as wf_data from numbers(7)
	
), meta as (
	select a.wf_id, a.wf_name, a.wf_key, count(1) as cnt
		, sum(length(wf_data::text)) sum_len
		, min(length(wf_data::text)) min_len
		, max(length(wf_data::text)) max_len
		, toString(nowInBlock()-now()) "time"
	from exchange a
	group by 1,2,3
)
insert into support.gp_ue_exchange 
select * from exchange 
union all
select  wf_id, '_GP_EXCHANGE' as wf_name, toString(now()) as wf_key
	, printf('{"type":"OUT","wf_name":"%s","wf_key":"%s","cnt":%d,"sum_len":%d,"min_len":%d,"max_len":%d,"time":"%s"}', c.wf_name, c.wf_key, c.cnt, c.sum_len, c.min_len, c.max_len, c."time") as wf_data
from (
    select * from meta a
    union all
    select wf_id, '_GP_EXCHANGE', toString(now()), count(1) cnt
        , sum(sum_len) sum_len
        , min(min_len) min_len
        , max(max_len) max_len
        , toString(nowInBlock()-now()) "time"
    from meta b
	group by 1
) c
SETTINGS max_block_size = 1
""")

sql_init = ("""
    CREATE DATABASE IF NOT EXISTS support ;
""","""
    CREATE TABLE IF NOT EXISTS support.gp_ue_exchange
    (
        wf_id Int32 COMMENT 'Ключ пакета'
        , wf_name String COMMENT 'Источник'
        , wf_key String COMMENT 'Ключ пакета источника'
        , wf_data String COMMENT 'Данные в формате JSON'
    )
    ENGINE = MergeTree()
    ORDER BY wf_id
    partition by wf_name
    SETTINGS index_granularity = 8192
;
""")
SQL_INIT= sql_init
SQL_INS= sql_ins