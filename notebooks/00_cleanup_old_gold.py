# Databricks notebook source
# One-off cleanup: removes the tables built by the first (crypto) version of the gold layer.
# Run once before 01_build_gold. Safe to rerun.

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("gold_schema", "lena066636_gold")
gold = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('gold_schema')}"

for t in ["fact_prices", "agg_daily_prices", "agg_hourly_volume", "dim_asset", "dim_source", "security_access"]:
    spark.sql(f"DROP TABLE IF EXISTS {gold}.{t}")
for f in ["rls_symbol_filter", "cls_mask_volume"]:
    spark.sql(f"DROP FUNCTION IF EXISTS {gold}.{f}")

display(spark.sql(f"SHOW TABLES IN {gold}"))
