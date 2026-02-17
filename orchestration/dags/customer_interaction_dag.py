from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import json
from datetime import datetime, timedelta

default_args = {
    'owner': 'omar',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def transform_with_hashmap(bucket_name, prefix, cleaned_prefix, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)

    seen_events = {}

    for key in keys:
        if not key.endswith('.json'): continue


        content = s3_hook.read_key(key, bucket_name)
        data = json.loads(content)


        events = data if isinstance(data, list) else [data]

        for event in events:
            event_id = event.get('event_id')

            if event_id not in seen_events:
                seen_events[event_id] = event


    cleaned_data = list(seen_events.values())
    s3_hook.load_string(
        string_data=json.dumps(cleaned_data),
        key=f"{cleaned_prefix}/cleaned_data.json",
        bucket_name=bucket_name,
        replace=True
    )

with DAG(
        'customer_event_elt_pipeline',
        default_args=default_args,
        schedule_interval='@hourly',
        catchup=False
) as dag:

    wait_for_s3_data = S3KeySensor(
        task_id='wait_for_s3_data',
        bucket_name='customer-event-elt-raw-dev-omarassed',
        bucket_key='raw-interactions/*.json',
        wildcard_match=True,
        aws_conn_id='aws_default',
        timeout=60 * 60,
        poke_interval=60
    )


    apply_transformation = PythonOperator(
        task_id='apply_hashmap_transformation',
        python_callable=transform_with_hashmap,
        op_kwargs={
            'bucket_name': 'customer-event-elt-raw-dev-omarassed',
            'prefix': 'raw-interactions/',
            'cleaned_prefix': 'cleaned-interactions'
        }
    )

    trigger_glue_crawler = GlueCrawlerOperator(
        task_id='trigger_glue_crawler',
        config={'Name': 'customer_event_crawler'},
        aws_conn_id='aws_default'
    )


    load_to_snowflake = SnowflakeSqlApiOperator(
        task_id='load_to_snowflake',
        snowflake_conn_id='snowflake_default',
        sql="COPY INTO CUSTOMER_EVENTS FROM @MY_S3_STAGE FILE_FORMAT = (TYPE = 'JSON');"
    )


    wait_for_s3_data >> apply_transformation >> trigger_glue_crawler >> load_to_snowflake