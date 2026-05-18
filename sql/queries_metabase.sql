-- Q01 | KPI / Number Card | Total unique orders
-- Business insight: Measures how many distinct orders have entered the pipeline.
SELECT
    uniqExact(order_id) AS total_unique_orders
FROM analytics.orders_raw;

-- Q02 | KPI / Number Card | Total unique users
-- Business insight: Shows how many customers are represented in the dataset.
SELECT
    uniqExact(user_id) AS total_unique_users
FROM analytics.orders_raw;

-- Q03 | KPI / Number Card | Total products sold
-- Business insight: Counts product-level line items as the total basket volume.
SELECT
    count() AS total_products_sold
FROM analytics.orders_raw;

-- Q04 | KPI / Number Card | Overall reorder rate
-- Business insight: Indicates the share of order lines that are repeat purchases.
SELECT
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct
FROM analytics.orders_raw;

-- Q05 | KPI / Number Card | Average products per order
-- Business insight: Captures average basket size across all unique orders.
SELECT
    round(count() / uniqExact(order_id), 2) AS avg_products_per_order
FROM analytics.orders_raw;

-- Q06 | Bar Chart | Top 10 most ordered products
-- Business insight: Highlights products with the strongest demand by frequency.
SELECT
    product_name,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY
    product_id,
    product_name
ORDER BY total_items_sold DESC
LIMIT 10;

-- Q07 | Bar Chart | Top 10 departments by total items sold
-- Business insight: Compares category-level contribution to total item volume.
SELECT
    department,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY department
ORDER BY total_items_sold DESC
LIMIT 10;

-- Q08 | Bar Chart | Top 10 aisles by total items sold
-- Business insight: Finds store aisles that dominate basket composition.
SELECT
    aisle,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY aisle
ORDER BY total_items_sold DESC
LIMIT 10;

-- Q09 | Bar Chart | Top 10 products with highest reorder rate
-- Business insight: Finds sticky products among items ordered at least 5 times.
SELECT
    product_name,
    count() AS total_items_sold,
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct
FROM analytics.orders_raw
GROUP BY
    product_id,
    product_name
HAVING total_items_sold >= 5
ORDER BY reorder_rate_pct DESC, total_items_sold DESC
LIMIT 10;

-- Q10 | Distribution Chart | Order count by day of week
-- Business insight: Reveals weekly shopping patterns from Sunday to Saturday.
SELECT
    order_dow,
    concat(
        toString(order_dow),
        ' - ',
        arrayElement(
            ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
            order_dow + 1
        )
    ) AS day_of_week,
    uniqExact(order_id) AS total_orders
FROM analytics.orders_raw
GROUP BY order_dow
ORDER BY order_dow;

-- Q11 | Distribution Chart | Order count by hour of day
-- Business insight: Produces a heatmap-ready view of daily demand peaks.
SELECT
    order_hour_of_day AS hour_of_day,
    uniqExact(order_id) AS total_orders
FROM analytics.orders_raw
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;

-- Q12 | Distribution Chart | Bucketed order size distribution
-- Business insight: Shows whether baskets are usually small, medium, or large.
WITH order_sizes AS (
    SELECT
        order_id,
        count() AS product_count
    FROM analytics.orders_raw
    GROUP BY order_id
),
bucketed AS (
    SELECT
        intDiv(product_count - 1, 5) * 5 + 1 AS bucket_start
    FROM order_sizes
)
SELECT
    concat(
        toString(bucket_start),
        '-',
        toString(bucket_start + 4)
    ) AS cart_size_bucket,
    count() AS total_orders
FROM bucketed
GROUP BY bucket_start
ORDER BY bucket_start;

-- Q13 | Scatter / Bubble | Product order volume vs reorder rate
-- Business insight: Separates popular products from products with loyal demand.
SELECT
    product_name,
    department AS bubble_group,
    uniqExact(order_id) AS total_orders,
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct,
    count() AS bubble_size
FROM analytics.orders_raw
GROUP BY
    product_id,
    product_name,
    department
ORDER BY total_orders DESC;

-- Q14 | Scatter / Bubble | User order frequency vs average order size
-- Business insight: Identifies high-frequency users and their basket depth.
WITH user_order_sizes AS (
    SELECT
        user_id,
        order_id,
        count() AS products_in_order
    FROM analytics.orders_raw
    GROUP BY
        user_id,
        order_id
)
SELECT
    user_id,
    count() AS order_frequency,
    round(avg(products_in_order), 2) AS avg_order_size
FROM user_order_sizes
GROUP BY user_id
ORDER BY order_frequency DESC, avg_order_size DESC;

-- Q15 | Time / Trend | Order volume by days since prior order
-- Business insight: Shows how long customers usually wait before reordering.
SELECT
    days_since_prior_order,
    uniqExact(order_id) AS total_orders
FROM analytics.orders_raw
WHERE days_since_prior_order IS NOT NULL
GROUP BY days_since_prior_order
ORDER BY days_since_prior_order;

-- Q16 | Time / Trend | Reorder rate across order number
-- Business insight: Tracks customer loyalty as order sequence increases.
SELECT
    order_number,
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct,
    uniqExact(order_id) AS total_orders
FROM analytics.orders_raw
GROUP BY order_number
ORDER BY order_number;

-- Q17 | Pivot / Cohort | Reorder rate by department and day of week
-- Business insight: Compares department loyalty patterns across weekdays.
SELECT
    department,
    order_dow,
    arrayElement(
        ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
        order_dow + 1
    ) AS day_name,
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY
    department,
    order_dow
ORDER BY
    department,
    order_dow;

-- Q18 | Pivot / Cohort | Average cart size by hour and day of week
-- Business insight: Supports operational planning by time window.
WITH order_sizes AS (
    SELECT
        order_id,
        any(order_dow) AS order_dow,
        any(order_hour_of_day) AS order_hour_of_day,
        count() AS cart_size
    FROM analytics.orders_raw
    GROUP BY order_id
)
SELECT
    order_dow,
    arrayElement(
        ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
        order_dow + 1
    ) AS day_name,
    order_hour_of_day,
    round(avg(cart_size), 2) AS avg_cart_size,
    count() AS total_orders
FROM order_sizes
GROUP BY
    order_dow,
    order_hour_of_day
ORDER BY
    order_dow,
    order_hour_of_day;

-- Q19 | Funnel / Table | Product table with reorder rate and department rank
-- Business insight: Ranks products inside each department by sales volume.
WITH product_metrics AS (
    SELECT
        department,
        product_id,
        product_name,
        count() AS total_items_sold,
        uniqExact(order_id) AS unique_orders,
        round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct
    FROM analytics.orders_raw
    GROUP BY
        department,
        product_id,
        product_name
)
SELECT
    department,
    product_name,
    total_items_sold,
    unique_orders,
    reorder_rate_pct,
    rank() OVER (
        PARTITION BY department
        ORDER BY total_items_sold DESC
    ) AS rank_within_department
FROM product_metrics
ORDER BY
    department,
    rank_within_department;

-- Q20 | Funnel / Table | Top 20 users by total orders placed
-- Business insight: Finds the most active customers by distinct order count.
SELECT
    user_id,
    uniqExact(order_id) AS total_orders,
    count() AS total_items_bought,
    round(count() / uniqExact(order_id), 2) AS avg_products_per_order
FROM analytics.orders_raw
GROUP BY user_id
ORDER BY total_orders DESC, total_items_bought DESC
LIMIT 20;
