from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import date, datetime
import math
from pathlib import Path

OLTP_CONN_ID = "shopviet_oltp"
DWH_CONN_ID = "shopviet_dwh"



def load_dim_date() -> None:
    hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    start_date = date(2020, 1, 1)
    end_date = date.today()
    rows = []
    current_date = start_date

    while current_date <= end_date:
        rows.append(
            (
                int(current_date.strftime("%Y%m%d")),
                current_date,
                current_date.isoweekday(),
                current_date.strftime("%A"),
                current_date.month,
                current_date.strftime("%B"),
                math.ceil(current_date.month / 3),
                current_date.year,
                current_date.weekday() >= 5
            )
        )
        current_date = date.fromordinal(current_date.toordinal() + 1)
    hook.run("TRUNCATE TABLE dim_date RESTART IDENTITY CASCADE")
    hook.insert_rows(
        table="dim_date",
        rows=rows,
        target_fields=[
            "date_sk",
            "full_date",
            "day_of_week",
            "day_name",
            "month",
            "month_name",
            "quarter",
            "year",
            "is_weekend"
        ],
        replace=False,
        commit_every=1000
    )

def load_dim_customers():
    oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
    dwh = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    rows= oltp.get_records(
        sql="""
            SELECT customer_id, full_name, email, city, updated_at, created_at from customers
        """
    )
    dwh.run("TRUNCATE TABLE dim_customers RESTART IDENTITY CASCADE")
    dwh.insert_rows(
        table="dim_customers",
        rows=[
            (
                customer_id,
                full_name,
                email,
                city,
                created_at,
                None,
                True,
                updated_at
            )
            for customer_id, full_name, email, city, updated_at, created_at in rows
        ],
        target_fields=[
            "customer_id",
            "full_name",
            "email",
            "city",
            "valid_from",
            "valid_to",
            "is_current",
            "updated_at"
        ],
        commit_every=1000
    )

def load_dim_products():
    oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
    dwh = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    rows = oltp.get_records(
        sql="""
            SELECT product_id, product_name, category, price, updated_at, created_at FROM products
        """
    )
    dwh.run("TRUNCATE TABLE dim_products RESTART IDENTITY CASCADE")
    dwh.insert_rows(
        table="dim_products",
        target_fields=[
            "product_id",
            "product_name",
            "category",
            "price",
            "valid_from",
            "valid_to",
            "is_current",
            "updated_at"
        ],
        commit_every=1000,
        rows=[
            (
                product_id,
                product_name,
                category,
                price,
                created_at,
                None,
                True,
                updated_at
            )
            for product_id, product_name, category, price, updated_at, created_at in rows
        ]
    )

def load_fact_sales():
    oltp = PostgresHook(postgres_conn_id=OLTP_CONN_ID)
    dwh = PostgresHook(postgres_conn_id=DWH_CONN_ID)

    source_rows = oltp.get_records(
        sql="""
            SELECT o.order_id,
            oi.order_item_id,
            o.customer_id,
            oi.product_id,
            o.order_date::date,
            oi.quantity,
            oi.unit_price,
            o.status
            FROM orders o
            JOIN order_items oi on o.order_id = oi.order_id 
        """
    )

    customer_keys = dict(
        dwh.get_records(
            "SELECt customer_id, customer_sk FROM dim_customers WHERE is_current"
        )
    )

    product_keys = dict(
        dwh.get_records(
            "SELECT product_id, product_sk from dim_products where is_current"
        )
    )

    dwh.run("TRUNCATE TABLE fact_sales RESTART IDENTITY CASCADE")

    dwh.insert_rows(
        table="fact_sales",
        target_fields=[
            "order_id",
            "order_item_id",
            "customer_sk",
            "product_sk",
            "date_sk",
            "quantity",
            "unit_price",
            "total_amount",
            "order_status",
        ],
        rows=[
            (
                order_id,
                order_item_id,
                customer_keys.get(customer_id),
                product_keys.get(product_id),
                int(order_date.strftime("%Y%m%d")),
                quantity,
                unit_price,
                quantity*unit_price,
                order_status
            )
            for order_id, order_item_id, customer_id, product_id, order_date, quantity, unit_price, order_status in source_rows
        ],
        commit_every=1000
    )


with DAG(
    dag_id="dag_full_load_2",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    template_searchpath=[str(Path(__file__).parent / "sql")],
    tags=["phase-1", "full-load"],
) as dag:
    create_dwh_schema = SQLExecuteQueryOperator(
        task_id = "create_dwh_schema",
        conn_id=DWH_CONN_ID,
        sql="dwh_schema.sql"
    )
    
    load_dim_date_task = PythonOperator(
        task_id="load_dim_date",
        python_callable=load_dim_date
    )

    load_dim_customers_task = PythonOperator(
        task_id="load_dim_customers",
        python_callable=load_dim_customers
    )

    load_dim_products_task= PythonOperator(
        task_id="load_dim_products",
        python_callable=load_dim_products
    )

    load_fact_sales_task = PythonOperator(
        task_id="load_fact_sales",
        python_callable=load_fact_sales
    )

    create_dwh_schema >> [
        load_dim_date_task,
        load_dim_customers_task,
        load_dim_products_task
    ] >> load_fact_sales_task