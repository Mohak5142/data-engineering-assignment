-- ====================================================================
-- E-COMMERCE ORDER ANALYTICS SYSTEM — SQL QUERIES
-- ====================================================================

-- --------------------------------------------------------------------
-- BASIC QUERIES
-- --------------------------------------------------------------------

-- 1. Total revenue per category (revenue = quantity * unit_price * (1 - discount_percent/100))
-- QUERY_1_START
SELECT 
    p.category,
    ROUND(SUM(oi.quantity * oi.unit_price * (1.0 - oi.discount_percent / 100.0)), 2) AS total_revenue
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.status != 'CANCELLED'
GROUP BY p.category
ORDER BY total_revenue DESC;
-- QUERY_1_END

-- 2. Top 10 customers by total order value
-- QUERY_2_START
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_type,
    ROUND(SUM(oi.quantity * oi.unit_price * (1.0 - oi.discount_percent / 100.0)), 2) AS total_order_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.status != 'CANCELLED' AND c.customer_id != 'C_UNKNOWN'
GROUP BY c.customer_id, c.customer_name, c.customer_type
ORDER BY total_order_value DESC
LIMIT 10;
-- QUERY_2_END

-- 3. Month-wise order count for the last 12 months
-- QUERY_3_START
SELECT 
    strftime('%Y-%m', order_date) AS month,
    COUNT(order_id) AS order_count
FROM orders
WHERE order_date >= date('now', '-12 months')
GROUP BY month
ORDER BY month ASC;
-- QUERY_3_END


-- --------------------------------------------------------------------
-- INTERMEDIATE QUERIES
-- --------------------------------------------------------------------

-- 4. Customers who placed orders but never had any item delivered
-- QUERY_4_START
SELECT DISTINCT
    c.customer_id,
    c.customer_name,
    c.email
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_id NOT IN (
    SELECT DISTINCT customer_id 
    FROM orders 
    WHERE status = 'DELIVERED'
) AND c.customer_id != 'C_UNKNOWN'
ORDER BY c.customer_id;
-- QUERY_4_END

-- 5. Products that had more returns than purchases
-- QUERY_5_START
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(CASE WHEN oi.quantity < 0 THEN ABS(oi.quantity) ELSE 0 END) AS returned_quantity,
    SUM(CASE WHEN oi.quantity > 0 THEN oi.quantity ELSE 0 END) AS purchased_quantity
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.product_name, p.category
HAVING returned_quantity > purchased_quantity
ORDER BY returned_quantity DESC;
-- QUERY_5_END

-- 6. Return rate (returned items / total items) per category
-- QUERY_6_START
SELECT 
    p.category,
    SUM(CASE WHEN oi.quantity < 0 THEN ABS(oi.quantity) ELSE 0 END) AS returned_items,
    SUM(CASE WHEN oi.quantity > 0 THEN oi.quantity ELSE 0 END) AS purchased_items,
    ROUND(
        SUM(CASE WHEN oi.quantity < 0 THEN ABS(oi.quantity) ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN oi.quantity > 0 THEN oi.quantity ELSE 0 END), 0), 
        2
    ) AS return_rate_percent
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.category
ORDER BY return_rate_percent DESC;
-- QUERY_6_END


-- --------------------------------------------------------------------
-- ADVANCED QUERIES (WINDOW FUNCTIONS, CTES, SUBQUERIES)
-- --------------------------------------------------------------------

-- 7. Running total of revenue per region, ordered by date
-- QUERY_7_START
WITH regional_daily_revenue AS (
    SELECT 
        o.region_code,
        DATE(o.order_date) AS order_day,
        SUM(oi.quantity * oi.unit_price * (1.0 - oi.discount_percent / 100.0)) AS daily_revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status != 'CANCELLED'
    GROUP BY o.region_code, order_day
)
SELECT 
    region_code,
    order_day,
    ROUND(daily_revenue, 2) AS daily_revenue,
    ROUND(SUM(daily_revenue) OVER (
        PARTITION BY region_code 
        ORDER BY order_day 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS running_total_revenue
FROM regional_daily_revenue
ORDER BY region_code, order_day;
-- QUERY_7_END

-- 8. DENSE_RANK products by revenue within each category (ties get the same rank)
-- QUERY_8_START
WITH product_revenue AS (
    SELECT 
        p.category,
        p.product_id,
        p.product_name,
        ROUND(SUM(oi.quantity * oi.unit_price * (1.0 - oi.discount_percent / 100.0)), 2) AS total_revenue
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.status != 'CANCELLED'
    GROUP BY p.category, p.product_id, p.product_name
)
SELECT 
    category,
    product_id,
    product_name,
    total_revenue,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS category_rank
FROM product_revenue
ORDER BY category, category_rank;
-- QUERY_8_END

-- 9. LAG/LEAD: days between consecutive orders per customer; flag customers with average gap > 30 days as "At Risk"
-- QUERY_9_START
WITH customer_order_gaps AS (
    SELECT 
        customer_id,
        order_id,
        order_date,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date,
        JULIANDAY(order_date) - JULIANDAY(LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)) AS days_since_last_order
    FROM orders
    WHERE customer_id != 'C_UNKNOWN'
),
avg_customer_gaps AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS total_orders,
        ROUND(AVG(days_since_last_order), 2) AS avg_days_between_orders
    FROM customer_order_gaps
    WHERE prev_order_date IS NOT NULL
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.customer_name,
    g.total_orders,
    g.avg_days_between_orders,
    CASE 
        WHEN g.avg_days_between_orders > 30 THEN 'At Risk'
        ELSE 'Active'
    END AS risk_status
FROM avg_customer_gaps g
JOIN customers c ON g.customer_id = c.customer_id
ORDER BY g.avg_days_between_orders DESC;
-- QUERY_9_END

-- 10. Multi-level CTE: monthly revenue per customer -> categorize as High (>10000)/Medium (5000-10000)/Low (<5000) -> count of customers per category per month
-- QUERY_10_START
WITH monthly_customer_revenue AS (
    SELECT 
        o.customer_id,
        strftime('%Y-%m', o.order_date) AS month,
        SUM(oi.quantity * oi.unit_price * (1.0 - oi.discount_percent / 100.0)) AS revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status != 'CANCELLED' AND o.customer_id != 'C_UNKNOWN'
    GROUP BY o.customer_id, month
),
customer_tiers AS (
    SELECT 
        customer_id,
        month,
        revenue,
        CASE 
            WHEN revenue > 10000 THEN 'High'
            WHEN revenue >= 5000 THEN 'Medium'
            ELSE 'Low'
        END AS revenue_category
    FROM monthly_customer_revenue
)
SELECT 
    month,
    revenue_category,
    COUNT(customer_id) AS customer_count,
    ROUND(SUM(revenue), 2) AS category_revenue
FROM customer_tiers
GROUP BY month, revenue_category
ORDER BY month ASC, revenue_category DESC;
-- QUERY_10_END

-- 11. NTILE: split customers into 4 quartiles by lifetime value, label them Platinum/Gold/Silver/Bronze
-- QUERY_11_START
WITH customer_ltv AS (
    SELECT 
        o.customer_id,
        ROUND(SUM(oi.quantity * oi.unit_price * (1.0 - oi.discount_percent / 100.0)), 2) AS lifetime_value
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status != 'CANCELLED' AND o.customer_id != 'C_UNKNOWN'
    GROUP BY o.customer_id
),
quartiled_customers AS (
    SELECT 
        customer_id,
        lifetime_value,
        NTILE(4) OVER (ORDER BY lifetime_value DESC) AS quartile
    FROM customer_ltv
)
SELECT 
    qc.customer_id,
    c.customer_name,
    qc.lifetime_value,
    qc.quartile,
    CASE qc.quartile
        WHEN 1 THEN 'Platinum'
        WHEN 2 THEN 'Gold'
        WHEN 3 THEN 'Silver'
        WHEN 4 THEN 'Bronze'
    END AS tier_label
FROM quartiled_customers qc
JOIN customers c ON qc.customer_id = c.customer_id
ORDER BY qc.lifetime_value DESC;
-- QUERY_11_END

-- 12. Year-over-year comparison of monthly revenue vs. the same month the previous year, handling missing prior-year data
-- QUERY_12_START
WITH monthly_revenue AS (
    SELECT 
        strftime('%Y-%m', o.order_date) AS year_month,
        CAST(strftime('%Y', o.order_date) AS INTEGER) AS year,
        CAST(strftime('%m', o.order_date) AS INTEGER) AS month,
        ROUND(SUM(oi.quantity * oi.unit_price * (1.0 - oi.discount_percent / 100.0)), 2) AS revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status != 'CANCELLED'
    GROUP BY year_month, year, month
)
SELECT 
    curr.year_month AS current_month,
    curr.revenue AS current_revenue,
    prev.year_month AS prior_year_month,
    COALESCE(prev.revenue, 0.0) AS prior_year_revenue,
    ROUND(curr.revenue - COALESCE(prev.revenue, 0.0), 2) AS revenue_diff,
    CASE 
        WHEN prev.revenue IS NULL OR prev.revenue = 0 THEN NULL
        ELSE ROUND(((curr.revenue - prev.revenue) * 100.0 / prev.revenue), 2)
    END AS yoy_growth_percent
FROM monthly_revenue curr
LEFT JOIN monthly_revenue prev 
    ON curr.month = prev.month AND curr.year = prev.year + 1
ORDER BY current_month ASC;
-- QUERY_12_END

-- 13. First vs. most recent purchased category per customer, with a category_shift flag (Yes/No)
-- QUERY_13_START
WITH customer_category_orders AS (
    SELECT 
        o.customer_id,
        p.category,
        o.order_date,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date ASC, oi.item_id ASC) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.order_date DESC, oi.item_id DESC) AS rn_desc
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE o.customer_id != 'C_UNKNOWN'
),
first_categories AS (
    SELECT customer_id, category AS first_category
    FROM customer_category_orders
    WHERE rn_asc = 1
),
latest_categories AS (
    SELECT customer_id, category AS latest_category
    FROM customer_category_orders
    WHERE rn_desc = 1
)
SELECT 
    fc.customer_id,
    c.customer_name,
    fc.first_category,
    lc.latest_category,
    CASE 
        WHEN fc.first_category != lc.latest_category THEN 'Yes'
        ELSE 'No'
    END AS category_shift
FROM first_categories fc
JOIN latest_categories lc ON fc.customer_id = lc.customer_id
JOIN customers c ON fc.customer_id = c.customer_id
ORDER BY fc.customer_id;
-- QUERY_13_END

-- 14. Cumulative revenue distribution — % of total revenue from top N% of customers
-- QUERY_14_START
WITH customer_rev AS (
    SELECT 
        o.customer_id,
        SUM(oi.quantity * oi.unit_price * (1.0 - oi.discount_percent / 100.0)) AS revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE o.status != 'CANCELLED' AND o.customer_id != 'C_UNKNOWN'
    GROUP BY o.customer_id
),
ranked_customers AS (
    SELECT 
        customer_id,
        revenue,
        ROW_NUMBER() OVER (ORDER BY revenue DESC) AS rank,
        COUNT(*) OVER () AS total_customers,
        SUM(revenue) OVER () AS grand_total_revenue,
        SUM(revenue) OVER (ORDER BY revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
    FROM customer_rev
)
SELECT 
    rank,
    customer_id,
    ROUND(revenue, 2) AS customer_revenue,
    ROUND((rank * 100.0 / total_customers), 2) AS customer_percentile,
    ROUND(cumulative_revenue, 2) AS cumulative_revenue,
    ROUND((cumulative_revenue * 100.0 / grand_total_revenue), 2) AS cumulative_revenue_percent
FROM ranked_customers
ORDER BY rank ASC;
-- QUERY_14_END

-- 15. Cohort analysis — group customers by registration month, track how many ordered in month 0/1/2/3, and compute retention rate per month
-- QUERY_15_START
WITH customer_cohorts AS (
    SELECT 
        customer_id,
        strftime('%Y-%m', registration_date) AS cohort_month
    FROM customers
),
customer_order_months AS (
    SELECT DISTINCT
        o.customer_id,
        strftime('%Y-%m', o.order_date) AS order_month
    FROM orders o
    WHERE o.customer_id != 'C_UNKNOWN'
),
cohort_orders AS (
    SELECT 
        cc.customer_id,
        cc.cohort_month,
        com.order_month,
        (CAST(strftime('%Y', com.order_month || '-01') AS INTEGER) - CAST(strftime('%Y', cc.cohort_month || '-01') AS INTEGER)) * 12 +
        (CAST(strftime('%m', com.order_month || '-01') AS INTEGER) - CAST(strftime('%m', cc.cohort_month || '-01') AS INTEGER)) AS month_number
    FROM customer_cohorts cc
    JOIN customer_order_months com ON cc.customer_id = com.customer_id
    WHERE month_number >= 0 AND month_number <= 3
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(customer_id) AS total_cohort_size
    FROM customer_cohorts
    GROUP BY cohort_month
),
monthly_active AS (
    SELECT 
        cohort_month,
        month_number,
        COUNT(DISTINCT customer_id) AS active_customers
    FROM cohort_orders
    GROUP BY cohort_month, month_number
)
SELECT 
    cs.cohort_month,
    cs.total_cohort_size,
    ma.month_number,
    COALESCE(ma.active_customers, 0) AS active_customers,
    ROUND(COALESCE(ma.active_customers, 0) * 100.0 / cs.total_cohort_size, 2) AS retention_rate_percent
FROM cohort_sizes cs
LEFT JOIN monthly_active ma ON cs.cohort_month = ma.cohort_month
ORDER BY cs.cohort_month ASC, ma.month_number ASC;
-- QUERY_15_END

-- 16. Self-join to find products frequently bought together (exclude duplicate/reversed pairs)
-- QUERY_16_START
SELECT 
    p1.product_id AS product_1_id,
    p1.product_name AS product_1_name,
    p2.product_id AS product_2_id,
    p2.product_name AS product_2_name,
    COUNT(DISTINCT item1.order_id) AS times_bought_together
FROM order_items item1
JOIN order_items item2 ON item1.order_id = item2.order_id AND item1.product_id < item2.product_id
JOIN products p1 ON item1.product_id = p1.product_id
JOIN products p2 ON item2.product_id = p2.product_id
JOIN orders o ON item1.order_id = o.order_id
WHERE o.status != 'CANCELLED'
GROUP BY p1.product_id, p1.product_name, p2.product_id, p2.product_name
HAVING times_bought_together > 1
ORDER BY times_bought_together DESC
LIMIT 15;
-- QUERY_16_END
