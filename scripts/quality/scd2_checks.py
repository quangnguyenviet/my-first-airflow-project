from airflow.providers.postgres.hooks.postgres import PostgresHook


def check_single_current_customer(
    dwh_conn_id: str,
) -> None:
    hook = PostgresHook(postgres_conn_id=dwh_conn_id)

    rows = hook.get_records(
        """
        SELECT customer_id, COUNT(*)
        FROM dim_customers
        WHERE is_current = TRUE
        GROUP BY customer_id
        HAVING COUNT(*) != 1
        """
    )

    if rows:
        raise ValueError(
            f"Customers with invalid current-version count: {rows}"
        )


def check_single_current_product(
    dwh_conn_id: str,
) -> None:
    hook = PostgresHook(postgres_conn_id=dwh_conn_id)

    rows = hook.get_records(
        """
        SELECT product_id, COUNT(*)
        FROM dim_products
        WHERE is_current = TRUE
        GROUP BY product_id
        HAVING COUNT(*) != 1
        """
    )

    if rows:
        raise ValueError(
            f"Products with invalid current-version count: {rows}"
        )


def check_no_null_fact_keys(
    dwh_conn_id: str,
) -> None:
    hook = PostgresHook(postgres_conn_id=dwh_conn_id)

    row = hook.get_first(
        """
        SELECT COUNT(*)
        FROM fact_sales
        WHERE customer_sk IS NULL
           OR product_sk IS NULL
           OR date_sk IS NULL
        """
    )

    invalid_count = row[0]

    if invalid_count != 0:
        raise ValueError(
            f"fact_sales contains {invalid_count} rows with NULL keys"
        )


def check_valid_date_ranges(
    dwh_conn_id: str,
) -> None:
    hook = PostgresHook(postgres_conn_id=dwh_conn_id)

    customer_invalid = hook.get_records(
        """
        SELECT customer_sk
        FROM dim_customers
        WHERE valid_from IS NULL
           OR (
                is_current = TRUE
                AND valid_to IS NOT NULL
           )
           OR (
                is_current = FALSE
                AND valid_to IS NULL
           )
           OR (
                valid_to IS NOT NULL
                AND valid_to <= valid_from
           )
        """
    )

    if customer_invalid:
        raise ValueError(
            f"Invalid customer dimension date ranges: "
            f"{customer_invalid}"
        )

    product_invalid = hook.get_records(
        """
        SELECT product_sk
        FROM dim_products
        WHERE valid_from IS NULL
           OR (
                is_current = TRUE
                AND valid_to IS NOT NULL
           )
           OR (
                is_current = FALSE
                AND valid_to IS NULL
           )
           OR (
                valid_to IS NOT NULL
                AND valid_to <= valid_from
           )
        """
    )

    if product_invalid:
        raise ValueError(
            f"Invalid product dimension date ranges: "
            f"{product_invalid}"
        )