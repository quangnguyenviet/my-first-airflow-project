CREATE TABLE IF NOT EXISTS dim_customers (
    customer_sk SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    full_name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(50),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_sk SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(50),
    price NUMERIC(12, 2),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week INT,
    day_name VARCHAR(10),
    month INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_sk BIGSERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    order_item_id INT NOT NULL UNIQUE,
    customer_sk INT REFERENCES dim_customers(customer_sk),
    product_sk INT REFERENCES dim_products(product_sk),
    date_sk INT REFERENCES dim_date(date_sk),
    quantity INT NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    order_status VARCHAR(20),
    loaded_at TIMESTAMP NOT NULL DEFAULT NOW(),
    source_updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_watermark (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_watermark TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Khởi tạo mốc ban đầu (nếu chưa có)
INSERT INTO etl_watermark (pipeline_name, last_watermark)
VALUES ('fact_sales_incremental', '2020-01-01 00:00:00')
ON CONFLICT (pipeline_name) DO NOTHING;