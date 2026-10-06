-- AI/BI dashboard datasets (Data tab -> Create from SQL). One block = one dataset.
-- Filters (add on the canvas): Date range -> date, Segment -> segment, Customer -> customer_name.

-- 1) kpi_sales
SELECT d.date, f.segment, c.customer_name, f.order_id, f.amount
FROM dbr_dev_ua5816bd.lena066636_gold.fact_orders f
JOIN dbr_dev_ua5816bd.lena066636_gold.dim_date     d ON f.date_key     = d.date_key
JOIN dbr_dev_ua5816bd.lena066636_gold.dim_customer c ON f.customer_key = c.customer_key;
-- counters on this dataset: SUM(amount) = revenue, COUNT(order_id) = orders, AVG(amount) = average order value

-- 2) revenue_by_hour
SELECT timestampadd(HOUR, t.hour_of_day, CAST(d.date AS TIMESTAMP)) AS hour,
       t.day_part, f.segment,
       count(*) AS orders, round(sum(f.amount), 2) AS revenue
FROM dbr_dev_ua5816bd.lena066636_gold.fact_orders f
JOIN dbr_dev_ua5816bd.lena066636_gold.dim_date d ON f.date_key = d.date_key
JOIN dbr_dev_ua5816bd.lena066636_gold.dim_time t ON f.time_key = t.time_key
GROUP BY 1, 2, 3;

-- 3) top_customers
SELECT c.customer_name, g.segment, g.orders, g.revenue, g.avg_order_value
FROM dbr_dev_ua5816bd.lena066636_gold.agg_customer_sales g
JOIN dbr_dev_ua5816bd.lena066636_gold.dim_customer c ON g.customer_key = c.customer_key;
-- bar chart: customer_name by revenue, sorted descending, top 10

-- 4) revenue_by_segment
SELECT d.date, f.segment, count(*) AS orders, round(sum(f.amount), 2) AS revenue
FROM dbr_dev_ua5816bd.lena066636_gold.fact_orders f
JOIN dbr_dev_ua5816bd.lena066636_gold.dim_date d ON f.date_key = d.date_key
GROUP BY d.date, f.segment;

-- 5) order_amount_distribution
SELECT d.date, f.segment,
       CASE WHEN f.amount < 50 THEN '1) under 50'
            WHEN f.amount < 100 THEN '2) 50-100'
            WHEN f.amount < 150 THEN '3) 100-150'
            ELSE '4) 150+' END AS amount_bucket,
       count(*) AS orders
FROM dbr_dev_ua5816bd.lena066636_gold.fact_orders f
JOIN dbr_dev_ua5816bd.lena066636_gold.dim_date d ON f.date_key = d.date_key
GROUP BY 1, 2, 3;

-- 6) revenue_by_day_part
SELECT d.date, t.day_part, f.segment, count(*) AS orders, round(sum(f.amount), 2) AS revenue
FROM dbr_dev_ua5816bd.lena066636_gold.fact_orders f
JOIN dbr_dev_ua5816bd.lena066636_gold.dim_date d ON f.date_key = d.date_key
JOIN dbr_dev_ua5816bd.lena066636_gold.dim_time t ON f.time_key = t.time_key
GROUP BY d.date, t.day_part, f.segment;
-- bar chart: day_part by revenue (night, morning, afternoon, evening)
