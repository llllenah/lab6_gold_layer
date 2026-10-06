# Databricks notebook source

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("gold_schema", "lena066636_gold")
dbutils.widgets.text("start_year", "2024")
dbutils.widgets.text("end_year", "2030")
dbutils.widgets.dropdown("recreate", "no", ["no", "yes"])

catalog = dbutils.widgets.get("catalog")
gold_schema = dbutils.widgets.get("gold_schema")
gold = f"{catalog}.{gold_schema}"
start_year = int(dbutils.widgets.get("start_year"))
end_year = int(dbutils.widgets.get("end_year"))
recreate = dbutils.widgets.get("recreate") == "yes"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_date

# COMMAND ----------

if recreate or not spark.catalog.tableExists(f"{gold}.dim_date"):
    dim_date = (
        spark.sql(f"SELECT explode(sequence(to_date('{start_year}-01-01'), to_date('{end_year}-12-31'), interval 1 day)) AS date")
        .select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            "date",
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.dayofmonth("date").alias("day"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        )
    )
    dim_date.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold}.dim_date")
    spark.sql(f"COMMENT ON TABLE {gold}.dim_date IS 'Dimension: calendar day. Loaded once for several years, not rebuilt with the facts.'")
    print("dim_date created")
else:
    print("dim_date already exists, skipped (set recreate = yes to rebuild)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## dim_time

# COMMAND ----------

if recreate or not spark.catalog.tableExists(f"{gold}.dim_time"):
    dim_time = (
        spark.range(0, 24).withColumnRenamed("id", "hour_of_day")
        .select(
            F.col("hour_of_day").cast("int").alias("time_key"),
            F.col("hour_of_day").cast("int").alias("hour_of_day"),
            F.concat(F.lpad(F.col("hour_of_day").cast("string"), 2, "0"), F.lit(":00-"),
                     F.lpad(F.col("hour_of_day").cast("string"), 2, "0"), F.lit(":59")).alias("hour_label"),
            F.when(F.col("hour_of_day") < 6, "night")
             .when(F.col("hour_of_day") < 12, "morning")
             .when(F.col("hour_of_day") < 18, "afternoon")
             .otherwise("evening").alias("day_part"),
            F.col("hour_of_day").between(9, 17).alias("is_business_hours"),
        )
    )
    dim_time.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{gold}.dim_time")
    spark.sql(f"COMMENT ON TABLE {gold}.dim_time IS 'Dimension: hour of the day with the day part (night, morning, afternoon, evening).'")
    print("dim_time created")
else:
    print("dim_time already exists, skipped")

display(spark.table(f"{gold}.dim_time").orderBy("time_key"))
