from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk.bases.hook import BaseHook
import boto3
from tempfile import NamedTemporaryFile

load_dotenv(".env")

MINIO_CONN_ID = "minio_s3"
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

SCHEMA = pa.schema(
    [
        ("order_id", pa.int64()),
        ("order_item_id", pa.int64()),
        ("customer_id", pa.int64()),
        ("product_id", pa.int64()),
        ("order_date", pa.date32()),
        ("quantity", pa.int64()),
        ("unit_price", pa.decimal128(12,2)),
        ("total_amount", pa.decimal128(12, 2)),
        ("status", pa.string()),
        ("source_updated_at", pa.timestamp("us")),
    ]
)

def get_s3_client():
    connection = BaseHook.get_connection(MINIO_CONN_ID)
    extra = connection.extra_dejson

    endpoint_url = extra.get("endpoint_url")
    if not endpoint_url:
        raise ValueError(
            f"Connection {MINIO_CONN_ID!r} requires endpoint_url in Extra"
        )
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        region_name=extra.get("region_name", "us-east-1"),
        verify=extra.get("verify", True)
    )

def rows_to_table(rows: list[tuple]) -> pa.Table:
    records = [
        {
            "order_id": row[0],
            "order_item_id": row[1],
            "customer_id": row[2],
            "product_id": row[3],
            "order_date": row[4],
            "quantity": row[5],
            "unit_price": row[6],
            "total_amount": row[7],
            "status": row[8],
            "source_updated_at": row[9],
        }
        for row in rows
    ]
    return pa.Table.from_pylist(records, schema=SCHEMA)

def upload_rows_as_parquet(rows: list[tuple], object_key: str):
    table = rows_to_table(rows)

    with NamedTemporaryFile(suffix=".parquet", delete=False) as file:
        local_path = Path(file.name)
    try:
        pq.write_table(
            table,
            local_path,
            compression="snappy",
        )

        get_s3_client().upload_file(
            str(local_path),
            MINIO_BUCKET,
            object_key,
        )
    finally:
        local_path.unlink(missing_ok=True)

    return f"s3://{MINIO_BUCKET}/{object_key}"


def download_rows_from_parquet(object_key: str) -> list[tuple]:
    with NamedTemporaryFile(suffix=".parquet", delete=False) as file:
        local_path = Path(file.name)

    try:
        get_s3_client().download_file(
            MINIO_BUCKET,
            object_key,
            str(local_path),
        )

        table = pq.read_table(local_path)

        return [
            (
                row["order_id"],
                row["order_item_id"],
                row["customer_id"],
                row["product_id"],
                row["order_date"],
                row["quantity"],
                row["unit_price"],
                row["total_amount"],
                row["status"],
                row["source_updated_at"],
            )
            for row in table.to_pylist()
        ]
    finally:
        local_path.unlink(missing_ok=True)