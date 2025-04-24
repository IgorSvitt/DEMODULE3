CREATE DATABASE IF NOT EXISTS db;
USE db;

DROP TABLE IF EXISTS orders;
CREATE TABLE orders
(
    order_id       UInt64,
    user_id        UInt64,
    order_date     DateTime,
    total_amount   Decimal(10,2),
    payment_status LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY order_id;

DROP TABLE IF EXISTS order_items;
CREATE TABLE order_items
(
    item_id        UInt64,
    order_id       UInt64,
    product_name   String,
    product_price  Decimal(10,2),
    quantity       UInt32
)
ENGINE = MergeTree
ORDER BY (order_id, item_id);

INSERT INTO orders
SELECT *
FROM s3(
    'https://storage.yandexcloud.net/storages/orders.csv',
    'CSV',
    'order_id UInt64, user_id UInt64, order_date DateTime, total_amount Decimal(10,2), payment_status String'
);

INSERT INTO order_items
SELECT *
FROM s3(
    'https://storage.yandexcloud.net/storages/order_items.txt',
    'CSV',
    'item_id UInt64, order_id UInt64, product_name String, product_price Decimal(10,2), quantity UInt32'
) SETTINGS format_csv_delimiter=';';

-- 4. Группировка по payment_status
SELECT
    payment_status,
    count()         AS orders_cnt,
    sum(total_amount) AS orders_sum,
    avg(total_amount) AS avg_order
FROM orders
GROUP BY payment_status
ORDER BY orders_cnt DESC;

-- 5. JOIN с order_items: товары
SELECT
    count(*)                               AS item_rows,
    sum(oi.quantity)                       AS total_qty,
    sum(oi.product_price * oi.quantity)    AS items_sum,
    avg(oi.product_price)                  AS avg_product_price
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id;

-- 6. Ежедневная динамика: количество, сумма, средний amount
SELECT
    toDate(order_date)  AS day,
    count()             AS orders_cnt,
    sum(total_amount)   AS orders_sum,
    avg(total_amount)   AS avg_order
FROM orders
GROUP BY day
ORDER BY day;

-- 7. Самые активные пользователи (по сумме)
SELECT
    user_id,
    count()           AS orders_cnt,
    sum(total_amount) AS total_spent
FROM orders
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;

-- 8. Самые активные пользователи (по количеству)
SELECT
    user_id,
    count()           AS orders_cnt,
    sum(total_amount) AS total_spent
FROM orders
GROUP BY user_id
ORDER BY orders_cnt DESC, total_spent DESC
LIMIT 10;
