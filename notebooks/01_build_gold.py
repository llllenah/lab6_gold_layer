# Databricks notebook source

from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("silver_schema", "lena066636_silver")
dbutils.widgets.text("silver_table", "orders")
dbutils.widgets.text("gold_schema", "lena066636_gold")

catalog = dbutils.widgets.get("catalog")
silver_table = f"{catalog}.{dbutils.widgets.get('silver_schema')}.{dbutils.widgets.get('silver_table')}"
gold_schema = dbutils.widgets.get("gold_schema")
gold = f"{catalog}.{gold_schema}"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

orders = (
    spark.table(silver_table)
    .select(
        F.col("order_id").cast("int").alias("order_id"),
        F.trim("customer").alias("customer"),
        F.col("amount").cast("double").alias("amount"),
        F.to_timestamp("ts").alias("order_ts"),
    )
    .where("order_id IS NOT NULL AND customer IS NOT NULL AND amount IS NOT NULL AND order_ts IS NOT NULL")
    .dropDuplicates(["order_id"])
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Customer dimension

# COMMAND ----------

spend = orders.groupBy("customer").agg(F.sum("amount").alias("total_spend"))
dim_customer = (
    spend
    .withColumn("customer_key", F.row_number().over(Window.orderBy("customer")).cast("int"))
    .withColumn("spend_bucket", F.ntile(5).over(Window.orderBy(F.col("total_spend").desc())))
    .withColumn("segment", F.when(F.col("spend_bucket") == 1, "premium").otherwise("standard"))
    .select("customer_key", F.col("customer").alias("customer_name"), "segment")
)
dim_customer.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold}.dim_customer")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Fact table

# COMMAND ----------

fact_orders = (
    orders.alias("o")
    .join(F.broadcast(spark.table(f"{gold}.dim_customer")).alias("c"), F.col("o.customer") == F.col("c.customer_name"))
    .select(
        F.col("o.order_id").alias("order_id"),
        F.date_format(F.to_date("o.order_ts"), "yyyyMMdd").cast("int").alias("date_key"),
        F.hour("o.order_ts").cast("int").alias("time_key"),
        F.col("c.customer_key").alias("customer_key"),
        F.col("c.segment").alias("segment"),
        F.col("o.amount").alias("amount"),
    )
)
fact_orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold}.fact_orders")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregations (business-ready tables for the dashboard)

# COMMAND ----------

fact = spark.table(f"{gold}.fact_orders")

agg_daily = (
    fact.groupBy("date_key")
    .agg(F.count("*").alias("orders"),
         F.round(F.sum("amount"), 2).alias("revenue"),
         F.round(F.avg("amount"), 2).alias("avg_order_value"))
)
agg_daily.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold}.agg_daily_sales")

agg_hourly = (
    fact.groupBy("date_key", "time_key")
    .agg(F.count("*").alias("orders"), F.round(F.sum("amount"), 2).alias("revenue"))
)
agg_hourly.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold}.agg_hourly_sales")

agg_customer = (
    fact.groupBy("customer_key", "segment")
    .agg(F.count("*").alias("orders"),
         F.round(F.sum("amount"), 2).alias("revenue"),
         F.round(F.avg("amount"), 2).alias("avg_order_value"))
)
agg_customer.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold}.agg_customer_sales")

# COMMAND ----------

comments = {
    "dim_customer": "Dimension: customer with a spend-based segment (premium, standard). customer_name is masked.",
    "fact_orders": "Fact: one row per order. Row filter by segment.",
    "agg_daily_sales": "Aggregation: orders, revenue and average order value per day.",
    "agg_hourly_sales": "Aggregation: orders and revenue per hour.",
    "agg_customer_sales": "Aggregation: orders, revenue and average order value per customer.",
}
for t, c in comments.items():
    spark.sql(f"COMMENT ON TABLE {gold}.{t} IS '{c}'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

for t in list(comments) + ["dim_date", "dim_time"]:
    print(t, spark.table(f"{gold}.{t}").count())

display(spark.sql(f"""
    SELECT sum(CASE WHEN d.date_key     IS NULL THEN 1 ELSE 0 END) AS no_date,
           sum(CASE WHEN t.time_key     IS NULL THEN 1 ELSE 0 END) AS no_time,
           sum(CASE WHEN c.customer_key IS NULL THEN 1 ELSE 0 END) AS no_customer,
           count(*) AS fact_rows
    FROM {gold}.fact_orders f
    LEFT JOIN {gold}.dim_date     d ON f.date_key     = d.date_key
    LEFT JOIN {gold}.dim_time     t ON f.time_key     = t.time_key
    LEFT JOIN {gold}.dim_customer c ON f.customer_key = c.customer_key
"""))

display(spark.sql(f"""
    SELECT segment, count(*) AS orders, round(sum(amount), 2) AS revenue
    FROM {gold}.fact_orders GROUP BY segment ORDER BY segment
"""))
