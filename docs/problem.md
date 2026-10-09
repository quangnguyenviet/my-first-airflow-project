# 🛒 Đề bài luyện tập: Xây dựng ETL Pipeline với Airflow + PostgreSQL

## 📋 Bối cảnh nghiệp vụ

Bạn là Data Engineer tại một công ty thương mại điện tử **"ShopViet"**. Công ty có một hệ thống bán hàng (OLTP) chạy trên PostgreSQL, ghi nhận các giao dịch hàng ngày. Ban giám đốc cần một **báo cáo doanh thu hàng ngày** và **phân tích hành vi khách hàng**, nhưng không thể query trực tiếp trên database production vì sẽ ảnh hưởng đến hiệu năng.

**Nhiệm vụ của bạn:** Xây dựng một pipeline ETL tự động bằng Airflow để:
1. Trích xuất dữ liệu từ database nguồn (OLTP).
2. Biến đổi và làm sạch dữ liệu.
3. Nạp vào Data Warehouse (mô hình hình sao - Star Schema) để phục vụ báo cáo.

---

## 🗄️ 1. Thiết kế Database

### 1.1. Source Database (OLTP) - `shopviet_oltp`

Đây là database mô phỏng hệ thống bán hàng. Bạn cần tạo schema và seed data.

```sql
-- Bảng khách hàng
CREATE TABLE customers (
    customer_id     SERIAL PRIMARY KEY,
    full_name       VARCHAR(100) NOT NULL,
    email           VARCHAR(150) UNIQUE NOT NULL,
    phone           VARCHAR(20),
    city            VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Bảng sản phẩm
CREATE TABLE products (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(50),
    price           NUMERIC(12, 2) NOT NULL,
    stock_quantity  INT DEFAULT 0,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Bảng đơn hàng
CREATE TABLE orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INT REFERENCES customers(customer_id),
    order_date      TIMESTAMP NOT NULL,
    status          VARCHAR(20) DEFAULT 'pending', -- pending, paid, shipped, delivered, cancelled
    total_amount    NUMERIC(12, 2),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Chi tiết đơn hàng
CREATE TABLE order_items (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INT REFERENCES orders(order_id),
    product_id      INT REFERENCES products(product_id),
    quantity        INT NOT NULL,
    unit_price      NUMERIC(12, 2) NOT NULL,
    created_at      TIMESTAMP DEFAULT NOW()
);
```

### 1.2. Destination Database (Data Warehouse) - `shopviet_dwh`

Đây là nơi chứa dữ liệu đã được biến đổi theo mô hình hình sao.

```sql
-- Dimension: Khách hàng
CREATE TABLE dim_customers (
    customer_sk     SERIAL PRIMARY KEY,      -- Surrogate Key
    customer_id     INT NOT NULL,             -- Business Key từ nguồn
    full_name       VARCHAR(100),
    email           VARCHAR(150),
    city            VARCHAR(50),
    valid_from      TIMESTAMP NOT NULL,
    valid_to        TIMESTAMP,                -- NULL = record hiện tại
    is_current      BOOLEAN DEFAULT TRUE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Dimension: Sản phẩm
CREATE TABLE dim_products (
    product_sk      SERIAL PRIMARY KEY,
    product_id      INT NOT NULL,
    product_name    VARCHAR(200),
    category        VARCHAR(50),
    price           NUMERIC(12, 2),
    valid_from      TIMESTAMP NOT NULL,
    valid_to        TIMESTAMP,
    is_current      BOOLEAN DEFAULT TRUE,
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Dimension: Thời gian
CREATE TABLE dim_date (
    date_sk         INT PRIMARY KEY,          -- YYYYMMDD
    full_date       DATE NOT NULL,
    day_of_week     INT,
    day_name        VARCHAR(10),
    month           INT,
    month_name      VARCHAR(10),
    quarter         INT,
    year            INT,
    is_weekend      BOOLEAN
);

-- Fact: Doanh thu bán hàng
CREATE TABLE fact_sales (
    sale_sk         BIGSERIAL PRIMARY KEY,
    order_id        INT NOT NULL,
    order_item_id   INT NOT NULL,
    customer_sk     INT REFERENCES dim_customers(customer_sk),
    product_sk      INT REFERENCES dim_products(product_sk),
    date_sk         INT REFERENCES dim_date(date_sk),
    quantity        INT NOT NULL,
    unit_price      NUMERIC(12, 2) NOT NULL,
    total_amount    NUMERIC(12, 2) NOT NULL,
    order_status    VARCHAR(20),
    loaded_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE(order_item_id)                     -- Đảm bảo idempotent
);
```

---

## 🎯 2. Yêu cầu Pipeline

Chia thành **4 giai đoạn** với độ khó tăng dần. Bạn nên làm tuần tự từng giai đoạn.

### Giai đoạn 1: Full Load (Cơ bản)

**Mục tiêu:** Làm quen với `PostgresHook`, `PostgresOperator`, và cấu trúc DAG.

**Yêu cầu DAG `dag_full_load.py`:**
- **Schedule:** `@daily`
- **Tasks:**
  1. `create_dwh_schema`: Tạo các bảng DWH nếu chưa tồn tại (dùng `PostgresOperator`).
  2. `load_dim_date`: Sinh dữ liệu cho `dim_date` từ 2020-01-01 đến hôm nay (dùng `PythonOperator` + `PostgresHook`).
  3. `load_dim_customers`: Truncate và load toàn bộ khách hàng vào `dim_customers`.
  4. `load_dim_products`: Truncate và load toàn bộ sản phẩm vào `dim_products`.
  5. `load_fact_sales`: Join `orders` + `order_items` và load vào `fact_sales`.
- **Dependencies:** `create_dwh_schema >> [load_dim_date, load_dim_customers, load_dim_products] >> load_fact_sales`

**Kỹ năng luyện tập:**
- Tạo connection trong Airflow UI (Admin → Connections).
- Sử dụng `PostgresHook` để lấy và insert dữ liệu.
- Xử lý NULL, cast kiểu dữ liệu.

---

### Giai đoạn 2: Incremental Load (Trung cấp)

**Mục tiêu:** Chỉ load dữ liệu mới/thay đổi, không load lại toàn bộ.

**Yêu cầu DAG `dag_incremental_load.py`:**
- **Schedule:** `@hourly`
- **Yêu cầu chi tiết:**
  1. Thêm cột `last_loaded_at` vào bảng `fact_sales` hoặc dùng một bảng `etl_watermark` để lưu mốc thời gian đã load.
  2. Mỗi lần chạy, chỉ lấy các `orders` có `updated_at > last_watermark`.
  3. Sử dụng **UPSERT** (`INSERT ... ON CONFLICT ... DO UPDATE`) để tránh trùng lặp.
  4. Sau khi load xong, cập nhật lại watermark.

**Kỹ năng luyện tập:**
- Xử lý idempotency (chạy lại không gây duplicate).
- Quản lý state giữa các lần chạy.
- Sử dụng `XCom` để truyền watermark giữa các task.

---

### Giai đoạn 3: SCD Type 2 (Nâng cao)

**Mục tiêu:** Theo dõi lịch sử thay đổi của khách hàng và sản phẩm.

**Yêu cầu:**
- Khi một khách hàng thay đổi `city` hoặc `full_name`:
  - Record cũ: set `valid_to = NOW()`, `is_current = FALSE`.
  - Record mới: insert với `valid_from = NOW()`, `is_current = TRUE`.
- Viết custom logic này trong một hàm Python, gọi từ `PythonOperator`.
- Đảm bảo `fact_sales` luôn tham chiếu đến đúng `customer_sk` tại thời điểm giao dịch (`order_date`).

**Kỹ năng luyện tập:**
- Hiểu và triển khai SCD Type 2.
- Xử lý logic phức tạp trong Python.
- Join fact với dimension dựa trên khoảng thời gian (`BETWEEN valid_from AND valid_to`).

---

### Giai đoạn 4: Data Quality & Monitoring (Chuyên nghiệp)

**Mục tiêu:** Đảm bảo chất lượng dữ liệu và có cảnh báo khi lỗi.

**Yêu cầu:**
1. **Data Quality Checks** (task chạy sau khi load):
   - `check_no_null_keys`: Không có `customer_sk`, `product_sk`, `date_sk` bị NULL trong `fact_sales`.
   - `check_positive_amount`: `total_amount > 0`.
   - `check_referential_integrity`: Mọi `customer_sk` trong fact đều tồn tại trong dim.
   - Nếu check fail → raise exception để task fail.
2. **Alerting:**
   - Cấu hình `on_failure_callback` cho DAG để gửi thông báo (giả lập bằng cách log ra console hoặc ghi vào bảng `etl_alerts`).
3. **SLA:**
   - Đặt `sla=timedelta(hours=2)` cho DAG. Nếu vượt quá, ghi cảnh báo.
4. **Retry:**
   - Cấu hình `retries=3`, `retry_delay=timedelta(minutes=5)` cho các task quan trọng.

**Kỹ năng luyện tập:**
- Viết custom `on_failure_callback`.
- Sử dụng `BranchPythonOperator` để rẽ nhánh khi data quality fail.
- Cấu hình retry, timeout, SLA.

---

## 🧪 3. Yêu cầu Testing

Viết test với `pytest`:

```python
# tests/test_dag_integrity.py
def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="dag_incremental_load")
    assert dag is not None
    assert len(dag.tasks) == 6

def test_dag_no_cycles(dagbag):
    dag = dagbag.get_dag(dag_id="dag_incremental_load")
    assert not dag.test_cycle()
```

```python
# tests/test_transformations.py
def test_calculate_total_amount():
    # Test hàm tính total_amount
    items = [{"quantity": 2, "unit_price": 100}, {"quantity": 1, "unit_price": 50}]
    assert calculate_total(items) == 250
```

---

## 📦 4. Cấu trúc dự án gợi ý

```
shopviet-etl/
├── dags/
│   ├── dag_full_load.py
│   ├── dag_incremental_load.py
│   └── dag_scd2_load.py
├── scripts/
│   ├── __init__.py
│   ├── extract/
│   │   └── postgres_extractor.py
│   ├── transform/
│   │   ├── scd2_handler.py
│   │   └── dim_date_generator.py
│   ├── load/
│   │   └── postgres_loader.py
│   └── quality/
│       └── data_checks.py
├── sql/
│   ├── ddl/
│   │   ├── oltp_schema.sql
│   │   └── dwh_schema.sql
│   └── seed/
│       └── seed_data.sql
├── tests/
│   ├── test_dag_integrity.py
│   └── test_transformations.py
├── config/
│   └── connections.yaml
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

---

## 🎁 5. Bonus Challenges (Tự chọn)

Nếu bạn muốn thử thách bản thân hơn:

1. **DAG Factory:** Định nghĩa DAG bằng file YAML thay vì code Python thuần.
2. **dbt Integration:** Thay vì dùng Python để transform, dùng `dbt` và gọi từ Airflow qua `BashOperator`.
3. **Idempotent Backfill:** Cho phép chạy lại DAG cho một khoảng ngày bất kỳ mà không gây duplicate.
4. **Partitioning:** Partition bảng `fact_sales` theo tháng để tăng hiệu năng query.
5. **Great Expectations:** Tích hợp thư viện `great_expectations` để kiểm tra chất lượng dữ liệu chuyên nghiệp hơn.

---

## ✅ 6. Tiêu chí đánh giá

| Tiêu chí | Mô tả |
|---|---|
| **Đúng nghiệp vụ** | Dữ liệu trong DWH khớp với OLTP sau khi transform |
| **Idempotent** | Chạy lại DAG không tạo duplicate |
| **Incremental** | Chỉ xử lý dữ liệu mới, không full load |
| **Có test** | DAG integrity test + unit test cho transform logic |
| **Có monitoring** | Alert khi fail, có SLA, có retry |
| **Code sạch** | Tách biệt logic (scripts/) và orchestration (dags/) |

---

## 🚀 Gợi ý bắt đầu

1. Viết file `docker-compose.yaml` mở rộng: thêm 2 service PostgreSQL (một cho OLTP, một cho DWH).
2. Chạy `sql/ddl/oltp_schema.sql` và `sql/seed/seed_data.sql` để tạo dữ liệu mẫu.
3. Tạo connections trong Airflow UI:
   - `postgres_oltp` → trỏ đến OLTP DB.
   - `postgres_dwh` → trỏ đến DWH DB.
4. Bắt đầu với Giai đoạn 1, sau khi pass mới chuyển sang Giai đoạn 2.

---

Bạn muốn tôi:
- **Viết luôn file `docker-compose.yaml` mở rộng** cho 2 database PostgreSQL chưa?
- **Cung cấp seed data mẫu** để bạn có dữ liệu test ngay?
- **Giải thích chi tiết hơn về SCD Type 2** hoặc một phần nào khác?

Cho tôi biết để tôi hỗ trợ sâu hơn nhé! 💪