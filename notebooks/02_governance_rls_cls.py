dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("gold_schema", "lena066636_gold")
dbutils.widgets.text("analyst_principal", "account users")

catalog = dbutils.widgets.get("catalog")
gold = f"{catalog}.{dbutils.widgets.get('gold_schema')}"
analyst = dbutils.widgets.get("analyst_principal")

spark.sql(f"USE CATALOG {catalog}")
tables = ["dim_customer", "dim_date", "fact_orders", "agg_daily_sales", "agg_hourly_sales", "agg_customer_sales"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Object permissions
# MAGIC Analysts can read the gold tables and nothing else. No write privileges.

# COMMAND ----------

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {gold} TO `{analyst}`")
for t in tables:
    spark.sql(f"GRANT SELECT ON TABLE {gold}.{t} TO `{analyst}`")

display(spark.sql(f"SHOW GRANTS ON SCHEMA {gold}"))
display(spark.sql(f"SHOW GRANTS ON TABLE {gold}.fact_orders"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Access table
# MAGIC One row per user and segment. `full_access` controls the column mask.
# MAGIC The current user is seeded with both segments and full access.

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {gold}.security_access (
        user_email STRING,
        segment STRING,
        full_access BOOLEAN
    )
""")
spark.sql(f"DELETE FROM {gold}.security_access WHERE user_email = current_user()")
spark.sql(f"""
    INSERT INTO {gold}.security_access
    SELECT current_user(), s, true FROM VALUES ('premium'), ('standard') AS t(s)
""")
display(spark.table(f"{gold}.security_access"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Row-Level Security
# MAGIC A user sees only the orders of customer segments listed for them in `security_access`.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {gold}.rls_segment_filter(row_segment STRING)
    RETURNS BOOLEAN
    RETURN EXISTS (
        SELECT 1 FROM {gold}.security_access a
        WHERE a.user_email = current_user() AND a.segment = row_segment
    )
""")
spark.sql(f"ALTER TABLE {gold}.fact_orders SET ROW FILTER {gold}.rls_segment_filter ON (segment)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Column-Level Security
# MAGIC `customer_name` is personal data. Only users with `full_access = true` see it,
# MAGIC everyone else sees `***`.
# MAGIC
# MAGIC If the workspace rejects subqueries in masks, replace the body with
# MAGIC `CASE WHEN is_account_group_member('<your_group>') THEN name ELSE '***' END`.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {gold}.cls_mask_customer(name STRING)
    RETURNS STRING
    RETURN CASE WHEN EXISTS (
        SELECT 1 FROM {gold}.security_access a
        WHERE a.user_email = current_user() AND a.full_access
    ) THEN name ELSE '***' END
""")
spark.sql(f"ALTER TABLE {gold}.dim_customer ALTER COLUMN customer_name SET MASK {gold}.cls_mask_customer")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Test: full access

# COMMAND ----------

q = f"""
    SELECT f.segment, count(*) AS orders, round(sum(f.amount), 2) AS revenue,
           min(c.customer_name) AS sample_customer
    FROM {gold}.fact_orders f
    JOIN {gold}.dim_customer c ON f.customer_key = c.customer_key
    GROUP BY f.segment ORDER BY f.segment
"""
display(spark.sql(q))   # expected: premium and standard, real customer names

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Test: restricted user (simulated on the current user)
# MAGIC Only the standard segment, customer names hidden.

# COMMAND ----------

spark.sql(f"DELETE FROM {gold}.security_access WHERE user_email = current_user() AND segment <> 'standard'")
spark.sql(f"UPDATE {gold}.security_access SET full_access = false WHERE user_email = current_user()")
display(spark.sql(q))   # expected: only standard, sample_customer = ***

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Restore full access

# COMMAND ----------

spark.sql(f"DELETE FROM {gold}.security_access WHERE user_email = current_user()")
spark.sql(f"""
    INSERT INTO {gold}.security_access
    SELECT current_user(), s, true FROM VALUES ('premium'), ('standard') AS t(s)
""")
display(spark.sql(q))
