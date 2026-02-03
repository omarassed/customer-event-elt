from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'omar',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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
        timeout=60 * 60,  # 1 hour
        poke_interval=60  # Check every minute
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

    wait_for_s3_data >> trigger_glue_crawler >> load_to_snowflake