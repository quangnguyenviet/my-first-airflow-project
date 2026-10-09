INSERT INTO customers (full_name, email, phone, city)
VALUES
    ('Nguyen Van An', 'an.nguyen@example.com', '0901000001', 'Ha Noi'),
    ('Tran Thi Binh', 'binh.tran@example.com', '0901000002', 'Da Nang'),
    ('Le Minh Chau', 'chau.le@example.com', '0901000003', 'Ho Chi Minh'),
    ('Pham Duc Dung', 'dung.pham@example.com', '0901000004', 'Can Tho'),
    ('Hoang Thu Ha', 'ha.hoang@example.com', '0901000005', 'Hai Phong')
ON CONFLICT (email) DO NOTHING;

INSERT INTO products (product_name, category, price, stock_quantity)
SELECT v.product_name, v.category, v.price, v.stock_quantity
FROM (
    VALUES
        ('Ao thun ShopViet', 'Thoi trang', 199000.00, 100),
        ('Quan jean xanh', 'Thoi trang', 499000.00, 60),
        ('Giay the thao', 'Giay dep', 899000.00, 40),
        ('Balo laptop', 'Phu kien', 350000.00, 75),
        ('Tai nghe Bluetooth', 'Dien tu', 650000.00, 50),
        ('Ban phim co', 'Dien tu', 1200000.00, 30)
) AS v(product_name, category, price, stock_quantity)
WHERE NOT EXISTS (
    SELECT 1
    FROM products p
    WHERE p.product_name = v.product_name
);

INSERT INTO orders (customer_id, order_date, status, total_amount)
SELECT c.customer_id, v.order_date, v.status, v.total_amount
FROM (
    VALUES
        ('an.nguyen@example.com', TIMESTAMP '2026-10-01 09:15:00', 'delivered', 697000.00),
        ('binh.tran@example.com', TIMESTAMP '2026-10-02 14:30:00', 'shipped', 899000.00),
        ('chau.le@example.com', TIMESTAMP '2026-10-03 10:00:00', 'paid', 1000000.00),
        ('dung.pham@example.com', TIMESTAMP '2026-10-04 16:45:00', 'delivered', 1200000.00),
        ('ha.hoang@example.com', TIMESTAMP '2026-10-05 11:20:00', 'cancelled', 650000.00)
) AS v(email, order_date, status, total_amount)
JOIN customers c ON c.email = v.email
WHERE NOT EXISTS (
    SELECT 1
    FROM orders o
    WHERE o.customer_id = c.customer_id
      AND o.order_date = v.order_date
);

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT o.order_id, p.product_id, v.quantity, v.unit_price
FROM (
    VALUES
        (TIMESTAMP '2026-10-01 09:15:00', 'Ao thun ShopViet', 1, 199000.00),
        (TIMESTAMP '2026-10-01 09:15:00', 'Balo laptop', 1, 350000.00),
        (TIMESTAMP '2026-10-01 09:15:00', 'Tai nghe Bluetooth', 1, 148000.00),
        (TIMESTAMP '2026-10-02 14:30:00', 'Giay the thao', 1, 899000.00),
        (TIMESTAMP '2026-10-03 10:00:00', 'Ao thun ShopViet', 2, 199000.00),
        (TIMESTAMP '2026-10-03 10:00:00', 'Balo laptop', 1, 602000.00),
        (TIMESTAMP '2026-10-04 16:45:00', 'Ban phim co', 1, 1200000.00),
        (TIMESTAMP '2026-10-05 11:20:00', 'Tai nghe Bluetooth', 1, 650000.00)
) AS v(order_date, product_name, quantity, unit_price)
JOIN orders o ON o.order_date = v.order_date
JOIN products p ON p.product_name = v.product_name
WHERE NOT EXISTS (
    SELECT 1
    FROM order_items oi
    WHERE oi.order_id = o.order_id
      AND oi.product_id = p.product_id
);
