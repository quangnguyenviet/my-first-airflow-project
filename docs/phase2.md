# 📊 Hướng dẫn chi tiết: Incremental Load (Giai đoạn 2)

Tôi sẽ hướng dẫn bạn từng bước, từ **thiết kế watermark** → **viết code extract/load** → **DAG hoàn chỉnh** → **xử lý các cạm bẫy thường gặp**.

---

## 🎯 1. Tư duy cốt lõi của Incremental Load

Thay vì mỗi ngày `TRUNCATE + load lại tất cả`, bạn chỉ load **phần dữ liệu đã thay đổi kể từ lần chạy trước**.

**Ý tưởng đơn giản nhất:**
```
Lần 1: watermark = 2025-01-01 00:00
       → load orders WHERE updated_at > '2025-01-01'

Lần 2: watermark = 2025-01-01 01:00 (thời điểm task lần 1 chạy xong)
       → load orders WHERE updated_at > '2025-01-01 01:00'
```

**Cạm bẫy #1 – Tại sao không dùng `NOW()`?**
Vì dữ liệu có thể đến trễ (late-arriving data). Nếu bạn set watermark = `NOW()` sau khi load, thì bất kỳ record nào có `updated_at` nhỏ hơn `NOW()` nhưng insert sau đó sẽ **bị bỏ qua vĩnh viễn**.

✅ **Cách an toàn:** watermark = `MAX(updated_at)` của bản ghi đã load **trừ đi một khoảng overlap** (thường 5–15 phút) để bắt được late data.

---

## 🗂️ 2. Thiết kế bảng Watermark

Tạo trong DWH (chạy 1 lần):

```sql
-- sql/ddl/dwh_schema.sql (bổ sung)
CREATE TABLE IF NOT EXISTS etl_watermark (
    pipeline_name   VARCHAR(100) PRIMARY KEY,
    last_watermark  TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Khởi tạo mốc ban đầu (nếu chưa có)
INSERT INTO etl_watermark (pipeline_name, last_watermark)
VALUES ('fact_sales_incremental', '2020-01-01 00:00:00')
ON CONFLICT (pipeline_name) DO NOTHING;
```

Trong bảng `fact_sales`, thêm cột `source_updated_at` để dễ debug:

```sql
ALTER TABLE fact_sales ADD COLUMN source_updated_at TIMESTAMP;
```

---

## 🧩 3. Chiến lược UPSERT (Idempotency)

Bảng `fact_sales` đã có `UNIQUE(order_item_id)` → dùng `ON CONFLICT` để **update** nếu đã tồn tại (ví dụ đơn hàng đổi từ `pending` → `paid`).

```sql
INSERT INTO fact_sales (...)
VALUES (...)
ON CONFLICT (order_item_id) DO UPDATE SET
    customer_sk   = EXCLUDED.customer_sk,
    product_sk    = EXCLUDED.product_sk,
    date_sk       = EXCLUDED.date_sk,
    quantity      = EXCLUDED.quantity,
    unit_price    = EXCLUDED.unit_price,
    total_amount  = EXCLUDED.total_amount,
    order_status  = EXCLUDED.order_status,
    loaded_at     = NOW(),
    source_updated_at = EXCLUDED.source_updated_at;
```

👉 Với cách này, dù DAG chạy 100 lần trên cùng 1 batch, fact table **không bị duplicate**.

---

## 🏗️ 4. Cấu trúc file

```
scripts/
├── extract/postgres_extractor.py     ← lấy watermark + extract data
├── load/postgres_loader.py            ← upsert + update watermark
└── transform/                         ← (dùng cho Giai đoạn 3)
dags/
└── dag_incremental_load.py
```

---

## 🐍 5. Code chi tiết

### 5.1. `scripts/extract/postgres_extractor.py`

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import timedelta

OVERLAP = timedelta(minutes=10)  # bắt late-arriving data


def get_watermark(conn_id: str, pipeline_name: str) -> str:
    """Đọc mốc watermark hiện tại từ DWH."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    row = hook.get_first(
        "SELECT last_watermark FROM etl_watermark WHERE pipeline_name = %s",
        parameters=(pipeline_name,),
    )
    if not row:
        # Nếu chưa có, khởi tạo về mốc xa xưa
        hook.run(
            "INSERT INTO etl_watermark (pipeline_name, last_watermark) "
            "VALUES (%s, '2020-01-01') ON CONFLICT DO NOTHING",
            parameters=(pipeline_name,),
        )
        return "2020-01-01 00:00:00"
    return row[0].isoformat()


def extract_orders_since(oltp_conn_id: str, watermark: str) -> list[tuple]:
    """
    Lấy các order_items của những order có updated_at > watermark.
    Trả về list các tuple sẵn sàng insert vào fact_sales.
    """
    hook = PostgresHook(postgres_conn_id=oltp_conn_id)

    # Trừ overlap để tránh miss late data
    from datetime import datetime
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
        JOIN order_items oi ON oi.order_id = o.order_id
        WHERE o.updated_at > %s
           OR oi.created_at > %s
        ORDER BY source_updated_at
    """
    return hook.get_records(sql, parameters=(wm, wm))
```

### 5.2. `scripts/load/postgres_loader.py`

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


UPSERT_SQL = """
INSERT INTO fact_sales (
    order_id, order_item_id, customer_sk, product_sk, date_sk,
    quantity, unit_price, total_amount, order_status, source_updated_at
) VALUES %s
ON CONFLICT (order_item_id) DO UPDATE SET
    customer_sk       = EXCLUDED.customer_sk,
    product_sk        = EXCLUDED.product_sk,
    date_sk           = EXCLUDED.date_sk,
    quantity          = EXCLUDED.quantity,
    unit_price        = EXCLUDED.unit_price,
    total_amount      = EXCLUDED.total_amount,
    order_status      = EXCLUDED.order_status,
    source_updated_at = EXCLUDED.source_updated_at,
    loaded_at         = NOW();
"""


def upsert_fact_sales(dwh_conn_id: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        execute_values(cur, UPSERT_SQL, rows, page_size=1000)
        conn.commit()
        return cur.rowcount
    finally:
        cur.close()
        conn.close()


def resolve_surrogate_keys(dwh_conn_id: str, rows: list[tuple]) -> list[tuple]:
    """
    Chuyển (customer_id, product_id, order_date) → (customer_sk, product_sk, date_sk).
    Dùng 1 query duy nhất với VALUES để tránh N+1.
    """
    hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        # Map customer_id → customer_sk
        customer_ids = list({r[2] for r in rows})
        cur.execute(
            "SELECT customer_id, customer_sk FROM dim_customers "
            "WHERE is_current = TRUE AND customer_id = ANY(%s)",
            (customer_ids,),
        )
        cmap = dict(cur.fetchall())

        # Map product_id → product_sk
        product_ids = list({r[3] for r in rows})
        cur.execute(
            "SELECT product_id, product_sk FROM dim_products "
            "WHERE is_current = TRUE AND product_id = ANY(%s)",
            (product_ids,),
        )
        pmap = dict(cur.fetchall())

        resolved = []
        for r in rows:
            order_id, oi_id, cust_id, prod_id, order_date, qty, price, amt, status, src_upd = r
            cust_sk = cmap.get(cust_id)
            prod_sk = pmap.get(prod_id)
            date_sk = int(order_date.strftime("%Y%m%d"))
            if cust_sk is None or prod_sk is None:
                # Trong thực tế: log warning hoặc dùng unknown key (-1)
                continue
            resolved.append(
                (order_id, oi_id, cust_sk, prod_sk, date_sk, qty, price, amt, status, src_upd)
            )
        return resolved
    finally:
        cur.close()
        conn.close()


def update_watermark(dwh_conn_id: str, pipeline_name: str, new_wm):
    hook = PostgresHook(postgres_conn_id=dwh_conn_id)
    hook.run(
        """
        UPDATE etl_watermark
        SET last_watermark = %s, updated_at = NOW()
        WHERE pipeline_name = %s
        """,
        parameters=(new_wm, pipeline_name),
    )
```

> 💡 **Lưu ý quan trọng:** `get_first()` của PostgresHook trả về `Row` chứ không phải `tuple`, nhưng vẫn index được như tuple. Nếu bạn dùng Airflow version < 2.5, hãy dùng `hook.get_records(...)` và lấy `[0][0]`.

---

## 🎛️ 6. DAG hoàn chỉnh: `dags/dag_incremental_load.py`

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres import PostgresOperator

from scripts.extract.postgres_extractor import get_watermark, extract_orders_since
from scripts.load.postgres_loader import (
    upsert_fact_sales, resolve_surrogate_keys, update_watermark,
)

OLTP = "postgres_oltp"
DWH = "postgres_dwh"
PIPELINE = "fact_sales_incremental"

default_args = {
    "owner": "data-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _get_wm(**ctx):
    wm = get_watermark(DWH, PIPELINE)
    ctx["ti"].xcom_push(key="watermark", value=wm)
    return wm


def _extract(**ctx):
    wm = ctx["ti"].xcom_pull(key="watermark", task_ids="get_watermark")
    rows = extract_orders_since(OLTP, wm)
    # XCom có giới hạn size — với batch lớn nên ghi ra file/S3
    ctx["ti"].xcom_push(key="row_count", value=len(rows))
    ctx["ti"].xcom_push(key="max_source_updated_at",
                        value=max((r[9] for r in rows), default=None).isoformat()
                        if rows else None)
    # Trong demo nhỏ có thể push luôn rows; production: ghi Parquet rồi push path
    ctx["ti"].xcom_push(key="rows", value=[list(r) for r in rows])
    return len(rows)


def _load(**ctx):
    rows = ctx["ti"].xcom_pull(key="rows", task_ids="extract_orders")
    rows = [tuple(r) for r in rows] if rows else []
    resolved = resolve_surrogate_keys(DWH, rows)
    n = upsert_fact_sales(DWH, resolved)
    return n


def _update_wm(**ctx):
    new_wm = ctx["ti"].xcom_pull(key="max_source_updated_at",
                                  task_ids="extract_orders")
    if new_wm:
        update_watermark(DWH, PIPELINE, new_wm)


with DAG(
    dag_id="dag_incremental_load",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,      # 🔒 tránh 2 run chạy song song đọc cùng watermark
    tags=["shopviet", "incremental"],
) as dag:

    ensure_wm_table = PostgresOperator(
        task_id="ensure_watermark_table",
        postgres_conn_id=DWH,
        sql="""
            CREATE TABLE IF NOT EXISTS etl_watermark (
                pipeline_name VARCHAR(100) PRIMARY KEY,
                last_watermark TIMESTAMP NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            );
            INSERT INTO etl_watermark (pipeline_name, last_watermark)
            VALUES ('fact_sales_incremental', '2020-01-01')
            ON CONFLICT DO NOTHING;
        """,
    )

    get_wm = PythonOperator(task_id="get_watermark", python_callable=_get_wm)
    extract = PythonOperator(task_id="extract_orders", python_callable=_extract)
    load = PythonOperator(task_id="load_fact_sales", python_callable=_load)
    upd_wm = PythonOperator(task_id="update_watermark", python_callable=_update_wm)

    ensure_wm_table >> get_wm >> extract >> load >> upd_wm
```

> DAG này có **5 task**. Nếu bạn muốn đúng `len(dag.tasks) == 6` như đề test mẫu, thêm 1 task `validate` (ví dụ `check_positive_amount`) sau `load`.

---

## ⚠️ 7. Những cạm bẫy bạn PHẢI xử lý

| # | Vấn đề | Cách khắc phục |
|---|--------|----------------|
| 1 | **Late-arriving data** | Trừ `OVERLAP` (5–15 phút) khi đọc watermark |
| 2 | **Hai run chạy song song cùng đọc watermark** | `max_active_runs=1` + UPSERT idempotent |
| 3 | **Watermark update fail sau khi load thành công** | Update watermark ở task **cuối cùng**, không gộp vào task load |
| 4 | **XCom giới hạn 48KB** (mặc định) | Với batch lớn → ghi Parquet ra `/tmp` hoặc S3, chỉ push path |
| 5 | **NULL customer_id / product_id** | Dùng **Unknown Key (-1)** trong dim, không skip âm thầm |
| 6 | **Timezone mismatch** giữa OLTP và DWH | Đảm bảo cả 2 DB dùng `UTC`, hoặc `TIMESTAMPTZ` |
| 7 | **Backfill** | Khi backfill, phải tắt incremental (chạy full cho ngày đó) hoặc dùng `execution_date` làm mốc tạm |

---

## 🧪 8. Test cho giai đoạn 2

```python
# tests/test_incremental.py
from scripts.extract.postgres_extractor import OVERLAP

def test_overlap_is_positive():
    assert OVERLAP.total_seconds() > 0

def test_watermark_query_idempotent(mocker):
    """Chạy 2 lần với cùng data → fact_sales không tăng thêm dòng."""
    # Mock PostgresHook trả về cùng 1 batch 2 lần
    # Assert rowcount ở lần 2 = 0 (do ON CONFLICT update chính nó)
    ...
```

---

## 🚀 9. Thứ tự thực hiện để bạn không bị rối

1. **Chạy migration** — thêm cột `source_updated_at` + tạo bảng `etl_watermark`.
2. **Seed data** — insert vài order, chạy DAG, kiểm tra `fact_sales` có dữ liệu.
3. **Update 1 order** — `UPDATE orders SET status='paid', updated_at=NOW() WHERE order_id=1`.
4. **Trigger DAG lần 2** — chỉ có 1 order_item được update (nhờ `ON CONFLICT`), **không** insert dòng mới.
5. **Kiểm tra watermark** — `SELECT * FROM etl_watermark` phải tăng.
6. **Chạy lại DAG lần 3 ngay lập tức** — rowcount = 0 (vì watermark đã vượt qua `max(source_updated_at)`), chứng minh idempotent.

---

## 🎓 10. Câu hỏi tự kiểm tra

1. Tại sao phải trừ `OVERLAP` mà không set watermark = `MAX(source_updated_at)`?
2. Nếu OLTP insert order với `updated_at` cũ hơn watermark (backdate), pipeline có bắt được không? Cần thêm gì?
3. Tại sao phải tách task `update_watermark` ra khỏi task `load_fact_sales`?
4. `max_active_runs=1` bảo vệ điều gì cụ thể?

---

Bạn muốn tôi:
- ✍️ **Viết luôn `scripts/extract` + `scripts/load` đầy đủ** (kèm xử lý Unknown Key -1)?
- 🧪 **Cung cấp test idempotency** chạy được với pytest + mock?
- 🔄 **Mở rộng sang late-arriving data** (dùng cột `event_time` riêng thay vì `updated_at`)?
- 📦 **Chuyển XCom sang ghi Parquet** để chạy được với batch hàng trăm nghìn dòng?

Cho tôi biết bạn muốn đào sâu phần nào! 💪