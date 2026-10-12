from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta, datetime

OVERLAP = timedelta(minutes=10)

def get_watermark(conn_id:str, pipeline_name:str) -> str:
    """Đọc mốc watermark hiện tại từ dwh"""
    hook = PostgresHook(postgres_conn_id=conn_id)
    row = hook.get_first(
        "SELECT last_watermark FROM etl_watermark WHERE pipeline_name = %s",
        parameters=(pipeline_name,)
    )
    if not row:
        hook.run(
            "INSERT INTO etl_watermark (pipeline_name, last_watermark)"
            "VALUES (%s, '2020-01-01 00:00:00') ON CONFLICT DO NOTHING",
            parameters=(pipeline_name,)
        )
        return "2020-01-01T00:00:00"
    return row[0].isoformat()

def extract_orders_since(oltp_conn_id, watermark:str) -> list[tuple]:
    """
    Lấy các order_items của những order có updated_at > watermark.
    Trả về list các tuple sẵn sàng insert vào fact_sales.
    """
    oltp = PostgresHook(postgres_conn_id=oltp_conn_id)
    wm = datetime.fromisoformat(watermark) - OVERLAP

    sql = """
        SELECT 
            o.order_id,
            oi.order_item_id,
            o.customer_id,
            oi.product_id,
            o.order_date,
            oi.quantity,
            oi.unit_price,
            (oi.quantity * oi.unit_price)::numeric(12,2) AS total_amount,
            o.status,
            GREATEST(o.updated_at, oi.created_at) AS source_updated_at
        FROM orders o
        JOIN order_items oi on o.order_id = oi.order_id
        WHERE o.updated_at > %s
            OR oi.created_at > %s
        ORDER BY source_updated_at
    """
    return oltp.get_records(sql, parameters=(wm, wm))
