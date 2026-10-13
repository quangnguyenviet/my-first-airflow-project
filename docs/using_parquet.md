Đúng rồi, mình đã hiểu nhầm ý bạn và bắt đầu sửa trực tiếp. Mình sẽ hoàn tác riêng các thay đổi vừa tạo, không đụng vào phần có sẵn của bạn, rồi hướng dẫn từng bước để bạn tự triển khai.

Ran terminal command: git --no-pager status --short; git --no-pager diff -- dags/dag_incremental_load.py docker-compose.yaml; Get-ChildItem -Path scripts -Recurse -File | Where-Object { $_.FullName -match 'storage|parquet_store' } | Select-Object -ExpandProperty FullName; Get-ChildItem -Path . -File -Filter 'Dockerfile' | Select-Object -ExpandProperty FullName; Get-ChildItem -Path . -File -Filter 'requirements.txt' | Select-Object -ExpandProperty FullName

Edit , , , 

Ran terminal command: $path = 'D:\evo\DE\my-airflow-project\scripts\storage\__pycache__\parquet_store.cpython-313.pyc'; if (Test-Path -LiteralPath $path) { Remove-Item -LiteralPath $path -Force }; $dir = 'D:\evo\DE\my-airflow-project\scripts\storage\__pycache__'; if (Test-Path -LiteralPath $dir) { $items = Get-ChildItem -LiteralPath $dir -Force; if ($items.Count -eq 0) { Remove-Item -LiteralPath $dir -Force } }; $dir = 'D:\evo\DE\my-airflow-project\scripts\storage'; if (Test-Path -LiteralPath $dir) { $items = Get-ChildItem -LiteralPath $dir -Force; if ($items.Count -eq 0) { Remove-Item -LiteralPath $dir -Force } }; git --no-pager status --short; git --no-pager diff -- dags/dag_incremental_load.py

Edit 

Đúng, mình đã hiểu nhầm yêu cầu ban đầu. Mình đã hoàn tác phần code vừa tạo; các thay đổi MinIO/Docker có sẵn của bạn vẫn được giữ nguyên.

Dưới đây là cách bạn tự triển khai theo hướng production.

## Kiến trúc nên dùng

Thay vì:

```python
ctx["ti"].xcom_push(key="rows", value=[list(r) for r in rows])
```

hãy làm:

```text
extract_orders
    ├── đọc dữ liệu từ PostgreSQL
    ├── ghi rows thành Parquet
    ├── upload Parquet lên MinIO
    └── XCom chỉ lưu s3://bucket/object-key

load_fact_sales
    ├── lấy URI từ XCom
    ├── download Parquet từ MinIO
    ├── đọc thành rows
    └── upsert vào DWH
```

XCom chỉ chứa chuỗi nhỏ, không chứa dữ liệu lớn.

---

## 1. Tạo dependency cố định

Tạo file `requirements.txt`:

```txt
boto3==1.42.44
pyarrow==23.0.1
```

Tạo `Dockerfile`:

```dockerfile
FROM apache/airflow:3.3.2

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
```

Trong `docker-compose.yaml`, tại phần `x-airflow-common`, đổi:

```yaml
image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.3.2}
# build: .
```

thành:

```yaml
image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.3.2}
build: .
```

Sau đó build lại Airflow:

```powershell
docker compose build
docker compose up -d
```

Không nên chỉ dùng `_PIP_ADDITIONAL_REQUIREMENTS` lâu dài trong production vì container sẽ cài package lại mỗi lần khởi động.

---

## 2. Tạo Airflow Connection cho MinIO

Trong Airflow UI:

```text
Admin → Connections → Add Connection
```

Nhập:

```text
Connection Id: minio_s3
Connection Type: Amazon Web Services
Login: giá trị MINIO_ROOT_USER (access key)
Password: giá trị MINIO_ROOT_PASSWORD (secret key)
```

Phần `Extra`:

```json
{
  "endpoint_url": "http://minio:9000",
  "region_name": "us-east-1"
}
```

Lưu ý:

- Từ container Airflow phải dùng `http://minio:9000`
- Không dùng `localhost:9000`
- `localhost` bên trong container là chính container Airflow, không phải MinIO
- Bucket hiện tại của bạn là `shopviet-data`

Nếu MinIO dùng HTTPS, đổi:

```json
{
  "endpoint_url": "https://minio:9000",
  "region_name": "us-east-1",
  "verify": true
}
```

Không nên lưu credential trực tiếp trong DAG hoặc source code.

---

## 3. Tạo helper ghi/đọc Parquet

Tạo thư mục:

```text
scripts/storage/
```

Tạo file `parquet_store.py`:

```python
from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.hooks.base import BaseHook


MINIO_CONN_ID = "minio_s3"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "shopviet-data")


SCHEMA = pa.schema(
    [
        ("order_id", pa.int64()),
        ("order_item_id", pa.int64()),
        ("customer_id", pa.int64()),
        ("product_id", pa.int64()),
        ("order_date", pa.date32()),
        ("quantity", pa.int64()),
        ("unit_price", pa.decimal128(12, 2)),
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
        verify=extra.get("verify", True),
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


def upload_rows_as_parquet(rows: list[tuple], object_key: str) -> str:
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
```

Điểm quan trọng là schema được khai báo cố định. Không nên để PyArrow tự suy luận schema mỗi lần vì khi batch rỗng hoặc có giá trị `NULL`, kiểu dữ liệu có thể không ổn định.

---

## 4. Sửa task extract

Trong `dag_incremental_load.py`, thêm import:

```python
from scripts.storage.parquet_store import upload_rows_as_parquet
```

Thay đoạn hiện tại:

```python
# Trong demo nhỏ có thể push luôn rows; production: ghi parquet rồi push path
ctx["ti"].xcom_push(key="rows", value=[list(r) for r in rows])
return len(rows)
```

bằng:

```python
object_key = (
    "bronze/fact_sales/"
    f"run_date={ctx['logical_date'].strftime('%Y-%m-%d')}/"
    f"run_id={ctx['run_id']}/"
    "orders.parquet"
)

parquet_uri = upload_rows_as_parquet(
    rows=rows,
    object_key=object_key,
)

ctx["ti"].xcom_push(
    key="parquet_uri",
    value=parquet_uri,
)

return len(rows)
```

Object key sẽ có dạng:

```text
bronze/fact_sales/run_date=2026-10-12/run_id=manual__.../orders.parquet
```

Không nên dùng cùng một tên cố định như:

```text
bronze/orders.parquet
```

vì lần chạy sau sẽ ghi đè lần chạy trước và khó audit/replay.

---

## 5. Sửa task load

Trong `dag_incremental_load.py`, thêm import:

```python
from scripts.storage.parquet_store import download_rows_from_parquet
```

Thay đoạn:

```python
rows = ctx["ti"].xcom_pull(key="rows", task_ids="extract_orders")
rows = [tuple(r) for r in rows] if rows else []
```

bằng:

```python
parquet_uri = ctx["ti"].xcom_pull(
    key="parquet_uri",
    task_ids="extract_orders",
)

if not parquet_uri:
    return 0

if not parquet_uri.startswith("s3://"):
    raise ValueError(f"Invalid Parquet URI: {parquet_uri}")

_, object_key = parquet_uri[5:].split("/", 1)

rows = download_rows_from_parquet(object_key)
```

Phần còn lại giữ nguyên:

```python
resolved = resolve_surrogate_keys(DWH_CONN_ID, rows)
n = upsert_fact_sales(DWH_CONN_ID, resolved)
return n
```

---

## 6. Vì sao nên dùng `run_id` trong path?

`run_id` giúp:

- retry không làm mất dữ liệu của run cũ
- có thể debug lại đúng file input
- audit được input của từng DAG run
- dễ replay một batch cụ thể
- tránh hai run ghi đè cùng một object

Với `max_active_runs=1`, bạn đã tránh được hai run chạy đồng thời, nhưng vẫn nên dùng object key bất biến.

---

## 7. Thứ tự cập nhật watermark

Flow hiện tại của bạn là:

```text
ensure_watermark_table
    >> get_watermark
    >> extract_orders
    >> load_fact_sales
    >> update_watermark
```

Đây là thứ tự đúng:

- Extract lỗi → watermark không đổi
- Upload Parquet lỗi → watermark không đổi
- Load DWH lỗi → watermark không đổi
- Chỉ update watermark sau khi load thành công

Không cập nhật watermark ngay sau khi extract.

---

## 8. Kiểm tra sau khi triển khai

### Kiểm tra Airflow Connection

Trong container scheduler hoặc worker:

```powershell
docker compose exec airflow-scheduler airflow connections get minio_s3
```

### Kiểm tra package trong worker

```powershell
docker compose exec airflow-worker python -c "import boto3, pyarrow; print('ok')"
```

### Kiểm tra bucket bằng MinIO client

```powershell
docker compose run --rm minio-init
```

Hoặc kiểm tra trên MinIO Console:

```text
http://localhost:9001
```

### Kiểm tra DAG

```powershell
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags show dag_incremental_load
```

### Kiểm tra log

Tìm các giá trị:

```text
parquet_uri
bronze/fact_sales/
orders.parquet
```

---

## Một lưu ý quan trọng trong schema hiện tại

Trong `postgres_extractor.py`, dữ liệu trả về có 10 cột:

```text
0  order_id
1  order_item_id
2  customer_id
3  product_id
4  order_date
5  quantity
6  unit_price
7  total_amount
8  status
9  source_updated_at
```

Schema Parquet phải giữ đúng thứ tự và kiểu dữ liệu này. Không nên ghi trực tiếp `list(row)` mà không đặt tên cột, vì sau này chỉ cần thay đổi câu SQL là dữ liệu có thể bị lệch cột mà không phát hiện ngay.

Kết quả cuối cùng là XCom chỉ chứa:

```text
s3://shopviet-data/bronze/fact_sales/run_date=.../run_id=.../orders.parquet
```

thay vì chứa toàn bộ dataset.