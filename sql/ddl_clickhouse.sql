CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.orders_raw (
    order_id UInt32,
    user_id UInt32,
    order_number UInt16,
    order_dow UInt8,
    order_hour_of_day UInt8,
    days_since_prior_order Nullable(UInt16),
    eval_set String,
    product_id UInt32,
    product_name String,
    aisle_id UInt16,
    aisle String,
    department_id UInt16,
    department String,
    add_to_cart_order UInt16,
    reordered UInt8,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (order_id, product_id);

CREATE VIEW IF NOT EXISTS analytics.orders AS
SELECT
    order_id,
    user_id,
    order_number,
    order_dow,
    order_hour_of_day,
    days_since_prior_order,
    eval_set,
    product_id,
    product_name,
    aisle_id,
    aisle,
    department_id,
    department,
    add_to_cart_order,
    reordered,
    ingested_at
FROM analytics.orders_raw
WHERE order_id > 0
    AND user_id > 0
    AND product_id > 0
    AND product_name != '';
