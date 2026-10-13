from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_customer_scd2_changes(dwh_conn_id:str, changes: list[dict]) -> int:
    if not changes:
        return 0
    hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        for change in changes:
            values = change["values"]
            customer_id = change["business_key"]
            old_sk = change["old_sk"]
            effective_at = change["effective_at"]

            if old_sk is not None:
                cursor.execute(
                    """
                    update dim_customers
                    set valid_to = %s,
                        is_current = false,
                        updated_at = now()
                    where customer_sk = %s
                        and is_current = true
                    """,
                    (effective_at, old_sk,),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError(
                        f"Expected one current customer row to close "
                        f"for customer_id={customer_id}; "
                        f"updated {cursor.rowcount}"
                    )
            cursor.execute(
                """
                insert into dim_customers (
                    customer_id,
                    full_name,
                    email,
                    city,
                    valid_from,
                    valid_to,
                    is_current,
                    updated_at
                )
                values (%s, %s, %s, %s, %s, null, true, %s)
                """,
                (
                    values["customer_id"],
                    values["full_name"],
                    values["email"],
                    values["city"],
                    effective_at,
                    values["updated_at"],
                )
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
    
def load_product_scd2_changes(
    dwh_conn_id: str,
    changes: list[dict],
) -> int:
    if not changes:
        return 0

    hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    conn = hook.get_conn()

    try:
        with conn.cursor() as cursor:
            for change in changes:
                values = change["values"]
                product_id = change["business_key"]
                old_sk = change["old_sk"]
                effective_at = change["effective_at"]

                if old_sk is not None:
                    cursor.execute(
                        """
                        UPDATE dim_products
                        SET
                            valid_to = %s,
                            is_current = FALSE,
                            updated_at = NOW()
                        WHERE product_sk = %s
                          AND is_current = TRUE
                        """,
                        (effective_at, old_sk),
                    )

                    if cursor.rowcount != 1:
                        raise RuntimeError(
                            f"Expected one current product row to close "
                            f"for product_id={product_id}; "
                            f"updated {cursor.rowcount}"
                        )

                cursor.execute(
                    """
                    INSERT INTO dim_products (
                        product_id,
                        product_name,
                        category,
                        price,
                        valid_from,
                        valid_to,
                        is_current,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, NULL, TRUE, %s)
                    """,
                    (
                        values["product_id"],
                        values["product_name"],
                        values["category"],
                        values["price"],
                        effective_at,
                        values["updated_at"],
                    ),
                )

        conn.commit()
        return len(changes)

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()
