from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import chain


from scripts.extract.dwh_extractor import (
    extract_current_customers,
    extract_current_products,
    extract_customer_versions,
    extract_product_versions,
)
from scripts.extract.postgres_extractor import (
    extract_customers,
    extract_products,
    extract_orders_with_items,
)
from scripts.load.fact_loader import load_fact_sales
from scripts.load.scd2_loader import (
    load_customer_scd2_changes,
    load_product_scd2_changes,
)
from scripts.quality.scd2_checks import (
    check_no_null_fact_keys,
    check_single_current_customer,
    check_single_current_product,
    check_valid_date_ranges,
)
from scripts.transform.scd2_handler import (
    build_fact_rows,
    detect_customer_changes,
    detect_product_changes,
)


OLTP_CONN_ID = "shopviet_oltp"
DWH_CONN_ID = "shopviet_dwh"


default_args = {
    "owner": "quang",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def extract_customers_task(**context):
    return extract_customers(OLTP_CONN_ID)


def extract_products_task(**context):
    return extract_products(OLTP_CONN_ID)


def extract_current_customers_task(**context):
    return extract_current_customers(DWH_CONN_ID)


def extract_current_products_task(**context):
    return extract_current_products(DWH_CONN_ID)


def transform_customers_task(**context):
    ti = context["ti"]

    source_customers = ti.xcom_pull(
        task_ids="extract_customers",
    )
    current_customers = ti.xcom_pull(
        task_ids="extract_current_customers",
    )

    return detect_customer_changes(
        source_customers=source_customers,
        current_customers=current_customers,
    )


def transform_products_task(**context):
    ti = context["ti"]

    source_products = ti.xcom_pull(
        task_ids="extract_products",
    )
    current_products = ti.xcom_pull(
        task_ids="extract_current_products",
    )

    return detect_product_changes(
        source_products=source_products,
        current_products=current_products,
    )


def load_customers_task(**context):
    ti = context["ti"]

    result = ti.xcom_pull(
        task_ids="transform_customers_scd2",
    )

    return load_customer_scd2_changes(
        dwh_conn_id=DWH_CONN_ID,
        changes=result["changes"],
    )


def load_products_task(**context):
    ti = context["ti"]

    result = ti.xcom_pull(
        task_ids="transform_products_scd2",
    )

    return load_product_scd2_changes(
        dwh_conn_id=DWH_CONN_ID,
        changes=result["changes"],
    )


def extract_orders_task(**context):
    return extract_orders_with_items(OLTP_CONN_ID)


def transform_fact_task(**context):
    ti = context["ti"]

    orders = ti.xcom_pull(
        task_ids="extract_orders_with_items",
    )
    customer_versions = ti.xcom_pull(
        task_ids="extract_customer_versions",
    )
    product_versions = ti.xcom_pull(
        task_ids="extract_product_versions",
    )

    return build_fact_rows(
        orders=orders,
        customer_versions=customer_versions,
        product_versions=product_versions,
    )


def extract_customer_versions_task(**context):
    return extract_customer_versions(DWH_CONN_ID)


def extract_product_versions_task(**context):
    return extract_product_versions(DWH_CONN_ID)


def load_fact_task(**context):
    ti = context["ti"]

    fact_rows = ti.xcom_pull(
        task_ids="transform_fact_sales",
    )

    return load_fact_sales(
        dwh_conn_id=DWH_CONN_ID,
        fact_rows=fact_rows,
    )


def validate_scd2_task(**context):
    check_single_current_customer(DWH_CONN_ID)
    check_single_current_product(DWH_CONN_ID)
    check_valid_date_ranges(DWH_CONN_ID)
    check_no_null_fact_keys(DWH_CONN_ID)


with DAG(
    dag_id="dag_scd2_load",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["shopviet", "scd2"],
) as dag:
    extract_customers_op = PythonOperator(
        task_id="extract_customers",
        python_callable=extract_customers_task,
    )

    extract_products_op = PythonOperator(
        task_id="extract_products",
        python_callable=extract_products_task,
    )

    extract_current_customers_op = PythonOperator(
        task_id="extract_current_customers",
        python_callable=extract_current_customers_task,
    )

    extract_current_products_op = PythonOperator(
        task_id="extract_current_products",
        python_callable=extract_current_products_task,
    )

    transform_customers_op = PythonOperator(
        task_id="transform_customers_scd2",
        python_callable=transform_customers_task,
    )

    transform_products_op = PythonOperator(
        task_id="transform_products_scd2",
        python_callable=transform_products_task,
    )

    load_customers_op = PythonOperator(
        task_id="load_customers_scd2",
        python_callable=load_customers_task,
    )

    load_products_op = PythonOperator(
        task_id="load_products_scd2",
        python_callable=load_products_task,
    )

    extract_orders_op = PythonOperator(
        task_id="extract_orders_with_items",
        python_callable=extract_orders_task,
    )

    extract_customer_versions_op = PythonOperator(
        task_id="extract_customer_versions",
        python_callable=extract_customer_versions_task,
    )

    extract_product_versions_op = PythonOperator(
        task_id="extract_product_versions",
        python_callable=extract_product_versions_task,
    )

    transform_fact_op = PythonOperator(
        task_id="transform_fact_sales",
        python_callable=transform_fact_task,
    )

    load_fact_op = PythonOperator(
        task_id="load_fact_sales",
        python_callable=load_fact_task,
    )

    validate_op = PythonOperator(
        task_id="validate_scd2",
        python_callable=validate_scd2_task,
    )

    (
        [extract_customers_op, extract_current_customers_op]
        >> transform_customers_op
        >> load_customers_op
    )

    (
        [extract_products_op, extract_current_products_op]
        >> transform_products_op
        >> load_products_op
    )

   
    ([load_customers_op, load_products_op] >> extract_orders_op)
    ([load_customers_op, load_products_op] >> extract_customer_versions_op)
    ([load_customers_op, load_products_op] >> extract_product_versions_op)

    (
        [
            extract_orders_op,
            extract_customer_versions_op,
            extract_product_versions_op,
        ]
        >> transform_fact_op
        >> load_fact_op
        >> validate_op
    )