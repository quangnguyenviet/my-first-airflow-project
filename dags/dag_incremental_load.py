from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from scripts.extract.postgres_extractor import get_watermark, extract_orders_since
from scripts.load.postgres_loader import (
    upsert_fact_sales, resolve_surrogate_keys, update_watermark
)

OLTP_CONN_ID = "shopviet_oltp"
DWH_CONN_ID = "shopviet_dwh"
PIPELINE = "fact_sales_incremental"

default_args = {
    "owner": "quang",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False
}

def _get_wm(**ctx):
    wm = get_watermark(DWH_CONN_ID, PIPELINE)
    ctx["ti"].xcom_push(key="watermark", value=wm)
    return wm

def _extract(**ctx):
    wm = ctx["ti"].xcom_pull(key="watermark", task_ids="get_watermark")
    rows = extract_orders_since(OLTP_CONN_ID, wm)
    # XCom có giới hạn size - với batch lớn nên ghi ra file/S3
    ctx["ti"].xcom_push(key="row_count", value=len(rows))
    ctx["ti"].xcom_push(key="max_source_updated_at", value=max((r[9] for r in rows), default=None).isoformat() if rows else None)
    # Trong demo nhỏ có thể push luôn rows; production: ghi parquet rồi push path
    ctx["ti"].xcom_push(key="rows", value=[list(r) for r in rows])
    return len(rows)

def _load(**ctx):
    rows = ctx["ti"].xcom_pull(key="rows", task_ids="extract_orders")
    rows = [tuple(r) for r in rows] if rows else []
    resolved = resolve_surrogate_keys(DWH_CONN_ID, rows)
    n = upsert_fact_sales(DWH_CONN_ID, resolved)
    return n

def _update_wm(**ctx):
    new_wm = ctx["ti"].xcom_pull(key="max_source_updated_at", task_ids="extract_orders")
    if new_wm:
        update_watermark(DWH_CONN_ID, PIPELINE, new_wm)

with DAG(
    dag_id = "dag_incremental_load",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1, # tranh 2 run chay song song doc cung watermark
    tags=["shopviet", "incremental"]
) as dag:
    ensure_wm_table = SQLExecuteQueryOperator(
        task_id="ensure_watermark_table",
        conn_id=DWH_CONN_ID,
        sql = """
            create table if not exists etl_watermark (
                pipeline_name varchar(100) primary key,
                last_watermark timestamp not null,
                updated_at timestamp default now()
            );
            insert into etl_watermark (pipeline_name, last_watermark)
            values ('fact_sales_incremental', '2020-01-01')
            on conflict do nothing;
        """
    )
    get_wm = PythonOperator(
        task_id = "get_watermark",
        python_callable=_get_wm
    )
    extract = PythonOperator(
        task_id = "extract_orders",
        python_callable=_extract
    )
    load = PythonOperator(
        task_id="load_fact_sales",
        python_callable=_load
    )
    upd_wm = PythonOperator(
        task_id = "update_watermark",
        python_callable=_update_wm
    )
    ensure_wm_table >> get_wm >> extract >> load >> upd_wm