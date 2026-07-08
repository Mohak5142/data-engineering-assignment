CREATE TABLE superstore_raw AS
SELECT * FROM superstore_raw;

CREATE TABLE customers AS
SELECT DISTINCT customer_id, customer_name, segment, country, city, state, postal_code, region
FROM superstore_raw;

CREATE TABLE products AS
SELECT DISTINCT product_id, product_name, category, sub_category
FROM superstore_raw;

CREATE TABLE orders AS
SELECT DISTINCT order_id, order_date, ship_date, ship_mode, customer_id
FROM superstore_raw;


SELECT order_id, customer_id, sales
FROM superstore_raw
WHERE sales > (SELECT AVG(sales) FROM superstore_raw)
ORDER BY sales DESC
LIMIT 10;


SELECT customer_id, customer_name, order_id, sales
FROM superstore_raw s1
WHERE sales = (
    SELECT MAX(sales)
    FROM superstore_raw s2
    WHERE s2.customer_id = s1.customer_id
)
ORDER BY sales DESC
LIMIT 10;


SELECT customer_id, customer_name, total_sales
FROM (
    SELECT customer_id, customer_name, ROUND(SUM(sales),2) AS total_sales
    FROM superstore_raw
    GROUP BY customer_id
) sub
WHERE total_sales > (SELECT AVG(total_sales) FROM (
    SELECT SUM(sales) AS total_sales FROM superstore_raw GROUP BY customer_id
))
ORDER BY total_sales DESC
LIMIT 10;


WITH customer_sales AS (
    SELECT customer_id, customer_name, segment, region,
           ROUND(SUM(sales),2) AS total_sales,
           ROUND(AVG(sales),2) AS avg_sales,
           COUNT(DISTINCT order_id) AS total_orders,
           ROUND(SUM(profit),2) AS total_profit
    FROM superstore_raw
    GROUP BY customer_id
)
SELECT * FROM customer_sales
ORDER BY total_sales DESC
LIMIT 10;


WITH category_summary AS (
    SELECT category,
           ROUND(SUM(sales),2) AS total_sales,
           ROUND(SUM(profit),2) AS total_profit,
           SUM(quantity) AS total_qty,
           ROUND(AVG(discount),2) AS avg_discount
    FROM superstore_raw
    GROUP BY category
)
SELECT * FROM category_summary
ORDER BY total_sales DESC;


WITH order_totals AS (
    SELECT order_id, customer_name, region,
           ROUND(SUM(sales),2) AS order_total
    FROM superstore_raw
    GROUP BY order_id
),
avg_order AS (
    SELECT ROUND(AVG(order_total),2) AS avg_val FROM order_totals
)
SELECT o.order_id, o.customer_name, o.region, o.order_total, a.avg_val
FROM order_totals o, avg_order a
WHERE o.order_total > a.avg_val
ORDER BY o.order_total DESC
LIMIT 10;


SELECT customer_name, region, ROUND(SUM(sales),2) AS total_sales,
       ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(sales) DESC) AS row_num
FROM superstore_raw
GROUP BY customer_id, region
ORDER BY region, row_num
LIMIT 20;


SELECT customer_name, segment,
       ROUND(SUM(sales),2) AS total_sales,
       RANK() OVER (ORDER BY SUM(sales) DESC) AS sales_rank
FROM superstore_raw
GROUP BY customer_id
ORDER BY sales_rank
LIMIT 10;


SELECT customer_name, category,
       ROUND(SUM(sales),2) AS total_sales,
       DENSE_RANK() OVER (PARTITION BY category ORDER BY SUM(sales) DESC) AS dense_rnk
FROM superstore_raw
GROUP BY customer_id, category
ORDER BY category, dense_rnk
LIMIT 20;


SELECT order_date,
       ROUND(SUM(sales),2) AS daily_sales,
       ROUND(SUM(SUM(sales)) OVER (ORDER BY order_date),2) AS running_total
FROM superstore_raw
GROUP BY order_date
ORDER BY order_date
LIMIT 15;


WITH customer_totals AS (
    SELECT customer_id, customer_name, segment, region,
           ROUND(SUM(sales),2) AS total_sales,
           ROUND(SUM(profit),2) AS total_profit,
           COUNT(DISTINCT order_id) AS num_orders
    FROM superstore_raw
    GROUP BY customer_id
),
ranked AS (
    SELECT *,
           RANK() OVER (ORDER BY total_sales DESC) AS overall_rank,
           RANK() OVER (PARTITION BY region ORDER BY total_sales DESC) AS region_rank,
           RANK() OVER (PARTITION BY segment ORDER BY total_sales DESC) AS segment_rank
    FROM customer_totals
)
SELECT customer_name, segment, region, total_sales, total_profit,
       num_orders, overall_rank, region_rank, segment_rank
FROM ranked
ORDER BY overall_rank
LIMIT 15;


WITH customer_totals AS (
    SELECT customer_id, customer_name, segment,
           ROUND(SUM(sales),2) AS total_sales,
           ROUND(SUM(profit),2) AS total_profit,
           COUNT(DISTINCT order_id) AS num_orders
    FROM superstore_raw
    GROUP BY customer_id
),
ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY segment ORDER BY total_sales DESC) AS rnk
    FROM customer_totals
)
SELECT customer_name, segment, total_sales, total_profit, num_orders, rnk
FROM ranked
WHERE rnk <= 3
ORDER BY segment, rnk;


WITH base AS (
    SELECT region, segment,
           ROUND(SUM(sales),2) AS total_sales,
           ROUND(SUM(profit),2) AS total_profit,
           COUNT(DISTINCT order_id) AS num_orders
    FROM superstore_raw
    GROUP BY region, segment
)
SELECT region, segment, total_sales, total_profit, num_orders,
       RANK() OVER (PARTITION BY region ORDER BY total_sales DESC) AS rank_in_region
FROM base
ORDER BY region, rank_in_region;


WITH product_sales AS (
    SELECT category, product_name,
           ROUND(SUM(sales),2) AS total_sales,
           DENSE_RANK() OVER (PARTITION BY category ORDER BY SUM(sales) DESC) AS rnk
    FROM superstore_raw
    GROUP BY category, product_name
)
SELECT category, product_name, total_sales, rnk
FROM product_sales
WHERE rnk <= 3
ORDER BY category, rnk;
