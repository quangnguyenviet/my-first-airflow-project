from airflow.providers.postgres.hooks.postgres import PostgresHook

UPSERT_SQL = """
    INSERT INTO fact_sales (
        order_id, order_item_id, customer_sk, product_sk, date_sk,
        quantity, unit_price, total_amount, order_status, source_updated_at
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (order_item_id) DO UPDATE SET
        customer_sk = EXCLUDED.customer_sk,
        product_sk = EXCLUDED.product_sk,
        date_sk = EXCLUDED.date_sk,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        total_amount = EXCLUDED.total_amount,
        order_status = EXCLUDED.order_status,
        source_updated_at = EXCLUDED.source_updated_at,
        loaded_at = NOW();
    """

def upsert_fact_sales(dwh_conn_id: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    dwh = PostgresHook(postgres_conn_id = dwh_conn_id)
    conn = dwh.get_conn()
    cur = conn.cursor()
    try:
        cur.executemany(UPSERT_SQL, rows)
        conn.commit()
        return cur.rowcount
    finally:
        cur.close()
        conn.close()

def resolve_surrogate_keys(dwh_conn_id: str, rows: list[tuple]) -> list[tuple]:
    """
    Chuyển "customer_id, product_id, order_date) -> (customer_sk, product_sk, date_sk).
    Dùng 1 query duy nhất với VALUES để tránh N+1
    """
    dwh = PostgresHook(postgres_conn_id=dwh_conn_id)
    conn = dwh.get_conn()
    cur = conn.cursor()
    try:
        # Map customer_id -> customer_sk
        customer_ids = list({r[2] for r in rows})
        cur.execute(
            "SELECT customer_id, customer_sk FROM dim_customers "
            "Where is_current = TRUE AND customer_id = ANY(%s)",
            (customer_ids,)
        )
        cmap = dict(cur.fetchall())

        # Map product_id -> product_sk
        product_ids = list({r[3] for r in rows})
        cur.execute(
            "SELECT product_id, product_sk FROM dim_products "
            "WHERE is_current = TRUE AND product_id = ANY(%s)",
            (product_ids, )
        )
        pmap = dict(cur.fetchall())

        resolved = []
        for r in rows:
            order_id, oi_id, cust_id, prod_id, order_date, qty, price, amt, status, src_upd = r
            cust_sk = cmap.get(cust_id)
            prod_sk = pmap.get(prod_id)
            date_sk = int(order_date.strftime("%Y%m%d"))
            if cust_sk is None or prod_sk is None:
                print("cust_sk or prod_sk is none")
                continue
            resolved.append(
                (order_id, oi_id, cust_sk, prod_sk, date_sk, qty, price, amt, status, src_upd)
            )
        return resolved
    finally:
        cur.close()
        conn.close()

def update_watermark(dwh_conn_id: str, pipeline_name: str, new_wm):
    dwh = PostgresHook(postgres_conn_id=dwh_conn_id)
    dwh.run(
        """
        UPDATE etl_watermark
        SET last_watermark = %s, updated_at = NOW()
        WHERE pipeline_name = %s
        """,
        parameters=(new_wm, pipeline_name),
    )