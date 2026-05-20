-- Q01 | Number | Total unique orders
-- Business insight: Shows how many distinct orders are available for analysis.
SELECT
    uniqExact(order_id) AS total_unique_orders
FROM analytics.orders_raw;

-- Q02 | Number | Total unique users
-- Business insight: Measures the customer base represented in the loaded data.
SELECT
    uniqExact(user_id) AS total_unique_users
FROM analytics.orders_raw;

-- Q03 | Number | Total products sold
-- Business insight: Counts all product-level rows as total item sales volume.
SELECT
    count() AS total_products_sold
FROM analytics.orders_raw;

-- Q04 | Gauge | Overall reorder rate
-- Business insight: Tracks what percentage of purchased items are repeat buys.
SELECT
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct
FROM analytics.orders_raw;

-- Q05 | Progress | Average products per order
-- Business insight: Summarizes basket depth as the average number of items per order.
SELECT
    round(count() / uniqExact(order_id), 2) AS avg_products_per_order
FROM analytics.orders_raw;

-- Q06 | Bar | Top 10 most ordered products
-- Business insight: Identifies products with the highest demand by item frequency.
SELECT
    product_name,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY
    product_id,
    product_name
ORDER BY total_items_sold DESC
LIMIT 10;

-- Q07 | Row | Top 10 products by reorder rate
-- Business insight: Finds sticky products among items ordered at least five times.
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

-- Q08 | Bar | Top 10 departments by total items sold
-- Business insight: Compares department contribution to total sales volume.
SELECT
    department,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY department
ORDER BY total_items_sold DESC
LIMIT 10;

-- Q09 | Row | Top 10 aisles by total items sold
-- Business insight: Highlights the most active aisles for assortment planning.
SELECT
    aisle,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY aisle
ORDER BY total_items_sold DESC
LIMIT 10;

-- Q10 | Pie | Department share of item sales
-- Business insight: Shows how much each department contributes to item volume.
SELECT
    department,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY department
ORDER BY total_items_sold DESC;

-- Q11 | Line | Order count by hour of day
-- Business insight: Reveals peak ordering hours for operational planning.
SELECT
    order_hour_of_day AS hour_of_day,
    uniqExact(order_id) AS total_orders
FROM analytics.orders_raw
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;

-- Q12 | Bar | Order count by day of week
-- Business insight: Compares shopping volume from Sunday to Saturday.
SELECT
    order_dow,
    arrayElement(
        ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
        order_dow + 1
    ) AS day_name,
    uniqExact(order_id) AS total_orders
FROM analytics.orders_raw
GROUP BY order_dow
ORDER BY order_dow;

-- Q13 | Area | Order size distribution
-- Business insight: Shows whether most carts are small, medium, or large.
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

-- Q14 | Scatter | Product volume versus reorder rate
-- Business insight: Separates high-volume products from products with loyal repeat demand.
SELECT
    product_name,
    uniqExact(order_id) AS total_orders,
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY
    product_id,
    product_name
HAVING total_items_sold >= 3
ORDER BY total_orders DESC;

-- Q15 | Scatter | User order frequency versus average basket size
-- Business insight: Finds users who order often and also buy many products per order.
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

-- Q16 | Line | Reorder rate across order number
-- Business insight: Tracks whether customers become more likely to reorder over time.
SELECT
    order_number,
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct
FROM analytics.orders_raw
GROUP BY order_number
ORDER BY order_number;

-- Q17 | Line | Order volume by days since prior order
-- Business insight: Shows the most common reorder waiting periods.
SELECT
    days_since_prior_order,
    uniqExact(order_id) AS total_orders
FROM analytics.orders_raw
WHERE days_since_prior_order IS NOT NULL
GROUP BY days_since_prior_order
ORDER BY days_since_prior_order;

-- Q18 | Combo | Order count and average cart size by day of week
-- Business insight: Compares traffic volume and basket depth on the same weekday chart.
WITH order_sizes AS (
    SELECT
        order_id,
        any(order_dow) AS order_dow,
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
    uniqExact(order_id) AS total_orders,
    round(avg(cart_size), 2) AS avg_cart_size
FROM order_sizes
GROUP BY order_dow
ORDER BY order_dow;

-- Q19 | Waterfall | Department contribution to total item sales
-- Business insight: Shows which departments add the largest item-volume contribution.
SELECT
    department,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY department
ORDER BY total_items_sold DESC
LIMIT 12;

-- Q20 | Table | Reorder rate by department and day of week
-- Business insight: Compares department loyalty across weekdays in a SQL-generated pivot table.
SELECT
    department,
    if(
        countIf(order_dow = 0) = 0,
        NULL,
        round(sumIf(reordered, order_dow = 0) / countIf(order_dow = 0) * 100, 2)
    ) AS sun_reorder_rate_pct,
    if(
        countIf(order_dow = 1) = 0,
        NULL,
        round(sumIf(reordered, order_dow = 1) / countIf(order_dow = 1) * 100, 2)
    ) AS mon_reorder_rate_pct,
    if(
        countIf(order_dow = 2) = 0,
        NULL,
        round(sumIf(reordered, order_dow = 2) / countIf(order_dow = 2) * 100, 2)
    ) AS tue_reorder_rate_pct,
    if(
        countIf(order_dow = 3) = 0,
        NULL,
        round(sumIf(reordered, order_dow = 3) / countIf(order_dow = 3) * 100, 2)
    ) AS wed_reorder_rate_pct,
    if(
        countIf(order_dow = 4) = 0,
        NULL,
        round(sumIf(reordered, order_dow = 4) / countIf(order_dow = 4) * 100, 2)
    ) AS thu_reorder_rate_pct,
    if(
        countIf(order_dow = 5) = 0,
        NULL,
        round(sumIf(reordered, order_dow = 5) / countIf(order_dow = 5) * 100, 2)
    ) AS fri_reorder_rate_pct,
    if(
        countIf(order_dow = 6) = 0,
        NULL,
        round(sumIf(reordered, order_dow = 6) / countIf(order_dow = 6) * 100, 2)
    ) AS sat_reorder_rate_pct,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY department
ORDER BY total_items_sold DESC;

-- Q21 | Table | Product performance table with rank inside department
-- Business insight: Ranks products within each department by item volume and repeat demand.
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

-- Q22 | Table | Top 20 users by order activity
-- Business insight: Finds the most active users and their average basket size.
SELECT
    user_id,
    uniqExact(order_id) AS total_orders,
    count() AS total_items_bought,
    round(count() / uniqExact(order_id), 2) AS avg_products_per_order
FROM analytics.orders_raw
GROUP BY user_id
ORDER BY total_orders DESC, total_items_bought DESC
LIMIT 20;

-- Q23 | Funnel | Cart position retention
-- Business insight: Shows how many orders reach each add-to-cart position.
SELECT
    concat('Item ', toString(add_to_cart_order)) AS cart_step,
    uniqExact(order_id) AS orders_reaching_step
FROM analytics.orders_raw
WHERE add_to_cart_order <= 10
GROUP BY add_to_cart_order
ORDER BY add_to_cart_order;

-- Q24 | Sankey | Department to aisle item flow
-- Business insight: Shows how item volume flows from broad departments into aisles.
SELECT
    department AS source,
    aisle AS target,
    count() AS total_items_sold
FROM analytics.orders_raw
GROUP BY
    department,
    aisle
ORDER BY total_items_sold DESC
LIMIT 30;

-- Q25 | Table | Department scorecard
-- Business insight: Compares overall department performance in one table.
SELECT
    department,
    count() AS total_items_sold,
    uniqExact(product_id) AS unique_products,
    uniqExact(order_id) AS unique_orders,
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct
FROM analytics.orders_raw
GROUP BY department
ORDER BY total_items_sold DESC;

-- Q26 | Bar | Average basket and reorder behavior by reorder interval segment
-- Business insight: Shows whether shorter reorder intervals relate to larger carts and stronger repeat buying.
SELECT
    CASE
        WHEN days_since_prior_order < 7 THEN '< 7 days'
        WHEN days_since_prior_order < 15 THEN '7-14 days'
        WHEN days_since_prior_order < 22 THEN '15-21 days'
        ELSE '> 21 days'
    END AS interval_segment,
    uniqExact(order_id) AS total_orders,
    round(count() / uniqExact(order_id), 2) AS avg_basket_size,
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct
FROM analytics.orders_raw
WHERE days_since_prior_order IS NOT NULL
GROUP BY interval_segment
ORDER BY min(days_since_prior_order);

-- Q27 | Bar | New versus returning product share by day
-- Business insight: Compares whether orders contain more new products or reordered products on each weekday.
WITH order_composition AS (
    SELECT
        order_id,
        any(order_dow) AS order_dow,
        round(sumIf(1, reordered = 0) / count() * 100, 2) AS pct_new_products,
        round(sumIf(1, reordered = 1) / count() * 100, 2) AS pct_returning_products
    FROM analytics.orders_raw
    GROUP BY order_id
)
SELECT
    order_dow,
    arrayElement(
        ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'],
        order_dow + 1
    ) AS day_name,
    round(avg(pct_new_products), 2) AS avg_pct_new,
    round(avg(pct_returning_products), 2) AS avg_pct_returning
FROM order_composition
GROUP BY order_dow
ORDER BY order_dow;

-- Q28 | Table | Top aisles per department
-- Business insight: Finds the leading aisles inside each department for drill-down category analysis.
WITH aisle_metrics AS (
    SELECT
        department,
        aisle,
        count() AS total_items_sold,
        round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct,
        rank() OVER (
            PARTITION BY department
            ORDER BY count() DESC
        ) AS rank_in_department
    FROM analytics.orders_raw
    GROUP BY
        department,
        aisle
)
SELECT
    department,
    aisle,
    total_items_sold,
    reorder_rate_pct,
    rank_in_department
FROM aisle_metrics
WHERE rank_in_department <= 3
ORDER BY
    department,
    rank_in_department;

-- Q29 | Bar | User behavior by order frequency bucket
-- Business insight: Segments users by order frequency to compare basket size and reorder behavior.
WITH user_stats AS (
    SELECT
        user_id,
        uniqExact(order_id) AS order_count,
        round(count() / uniqExact(order_id), 2) AS avg_basket_size,
        round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct
    FROM analytics.orders_raw
    GROUP BY user_id
)
SELECT
    CASE
        WHEN order_count = 1 THEN '1 order'
        WHEN order_count <= 5 THEN '2-5 orders'
        WHEN order_count <= 10 THEN '6-10 orders'
        ELSE '> 10 orders'
    END AS frequency_bucket,
    count() AS total_users,
    round(avg(avg_basket_size), 2) AS avg_basket_size,
    round(avg(reorder_rate_pct), 2) AS avg_reorder_rate_pct
FROM user_stats
GROUP BY frequency_bucket
ORDER BY min(order_count);

-- Q30 | Line | Reorder rate by hour of day
-- Business insight: Complements hourly order volume by showing when repeat buying is strongest.
SELECT
    order_hour_of_day AS hour_of_day,
    uniqExact(order_id) AS total_orders,
    round(sum(reordered) / count() * 100, 2) AS reorder_rate_pct
FROM analytics.orders_raw
GROUP BY order_hour_of_day
ORDER BY order_hour_of_day;