from airflow.providers.postgres.hooks.postgres import PostgresHook


FACT_UPSERT_SQL = """
    INSERT INTO fact_sales (
        order_id,
        order_item_id,
        customer_sk,
        product_sk,
        date_sk,
        quantity,
        unit_price,
        total_amount,
        order_status,
        source_updated_at
    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
    ON CONFLICT (order_item_id)
    DO UPDATE SET
        customer_sk = EXCLUDED.customer_sk,
        product_sk = EXCLUDED.product_sk,
        date_sk = EXCLUDED.date_sk,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        total_amount = EXCLUDED.total_amount,
        order_status = EXCLUDED.order_status,
        loaded_at = NOW(),
        source_updated_at = EXCLUDED.source_updated_at

"""


def load_fact_sales(
    dwh_conn_id: str,
    fact_rows: list[tuple],
) -> int:
    if not fact_rows:
        return 0

    hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    conn = hook.get_conn()

    try:
        with conn.cursor() as cursor:
            cursor.executemany(FACT_UPSERT_SQL, fact_rows)

        conn.commit()
        return len(fact_rows)

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()