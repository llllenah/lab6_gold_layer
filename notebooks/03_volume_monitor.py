# Databricks notebook source
# Data-volume monitoring for the alert. Each run logs how many new rows reached gold.fact_orders.
# The simulation modes create a normal baseline and then a sudden volume drop.

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("gold_schema", "lena066636_gold")
dbutils.widgets.dropdown("mode", "log_real", ["log_real", "simulate_baseline", "simulate_drop"])

gold = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('gold_schema')}"
mode = dbutils.widgets.get("mode")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {gold}.pipeline_volume_log (
        run_ts TIMESTAMP,
        table_name STRING,
        total_rows BIGINT,
        new_rows BIGINT,
        scenario STRING
    )
""")

def last_total():
    r = spark.sql(f"""
        SELECT total_rows FROM {gold}.pipeline_volume_log
        WHERE table_name = 'fact_orders' ORDER BY run_ts DESC LIMIT 1
    """).first()
    return r[0] if r else 0

# COMMAND ----------

if mode == "log_real":
    total = spark.table(f"{gold}.fact_orders").count()
    spark.sql(f"INSERT INTO {gold}.pipeline_volume_log VALUES (current_timestamp(), 'fact_orders', {total}, {total - last_total()}, 'real')")

elif mode == "simulate_baseline":
    # six normal loads of about 1000 new orders each (simulated, marked in the scenario column)
    for i in range(6):
        spark.sql(f"""
            INSERT INTO {gold}.pipeline_volume_log
            VALUES (current_timestamp() - INTERVAL {7 - i} HOURS, 'fact_orders', {10000 + i * 1000}, {980 + i * 10}, 'simulated_baseline')
        """)

elif mode == "simulate_drop":
    # the next load brings almost nothing: volume drop
    spark.sql(f"""
        INSERT INTO {gold}.pipeline_volume_log
        VALUES (current_timestamp(), 'fact_orders', 16005, 5, 'simulated_drop')
    """)

display(spark.sql(f"SELECT * FROM {gold}.pipeline_volume_log WHERE table_name = 'fact_orders' ORDER BY run_ts DESC LIMIT 10"))

# COMMAND ----------
# MAGIC %md
# MAGIC Alert query: `sql/alert_volume_drop.sql`. It returns `volume_drop = 1` when the latest load
# MAGIC has less than 50% of the average of the five loads before it.
