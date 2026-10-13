from airflow.providers.postgres.hooks.postgres import PostgresHook

def _get_rows_as_dicts(hook: PostgresHook, sql: str) -> list[dict]:
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()
    return [dict(zip(columns, row)) for row in rows]

def extract_current_customers(dwh_conn_id: str) -> list[dict]:
    dwh = PostgresHook(postgres_conn_id=dwh_conn_id)
    sql = """
        select
            customer_sk,
            customer_id,
            full_name,
            email,
            city,
            valid_from,
            valid_to,
            is_current,
            updated_at
        from dim_customers
        where is_current = true
        order by customer_id
    """
    return _get_rows_as_dicts(dwh, sql)

def extract_current_products(dwh_conn_id: str):
    dwh = PostgresHook(postgres_conn_id=dwh_conn_id)
    sql = """
        select
            product_sk,
            product_id,
            product_name,
            category,
            price,
            valid_from,
            valid_to,
            is_current,
            updated_at
        from dim_products
        where is_current=true
        order by product_id
    """
    return _get_rows_as_dicts(dwh, sql)

def extract_customer_versions(dwh_conn_id: str) -> list[dict]:
    hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    sql = """
        select
            customer_sk,
            customer_id,
            full_name,
            email,
            city,
            valid_from,
            valid_to,
            is_current
        from dim_customers
        order by customer_id, valid_from
    """
    return _get_rows_as_dicts(hook, sql)

def extract_product_versions(dwh_conn_id: str) -> list[dict]:
    hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    sql = """
        select
            product_sk,
            product_id,
            product_name,
            category,
            price,
            valid_from,
            valid_to,
            is_current
        from dim_products
        order by product_id, valid_from
    """
    return _get_rows_as_dicts(hook, sql)

