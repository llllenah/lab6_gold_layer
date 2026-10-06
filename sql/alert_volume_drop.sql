-- Alert query: data volume drop.
-- Alert condition: Value column = volume_drop, operator ">", threshold 0. Notify: email.
WITH ranked AS (
  SELECT new_rows, run_ts, row_number() OVER (ORDER BY run_ts DESC) AS rn
  FROM dbr_dev_ua5816bd.lena066636_gold.pipeline_volume_log
  WHERE table_name = 'fact_orders'
),
base AS (SELECT avg(new_rows) AS avg_prev FROM ranked WHERE rn BETWEEN 2 AND 6),
cur  AS (SELECT new_rows AS latest_rows FROM ranked WHERE rn = 1)
SELECT
  cur.latest_rows,
  round(base.avg_prev, 0) AS avg_prev_rows,
  CASE WHEN cur.latest_rows < 0.5 * base.avg_prev THEN 1 ELSE 0 END AS volume_drop
FROM cur CROSS JOIN base;
