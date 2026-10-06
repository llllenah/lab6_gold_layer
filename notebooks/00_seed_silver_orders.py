# Databricks notebook source

import random
from datetime import datetime, timedelta

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("silver_schema", "lena066636_silver")
dbutils.widgets.text("silver_table", "orders")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
table = f"{catalog}.{silver_schema}.{dbutils.widgets.get('silver_table')}"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")

if spark.catalog.tableExists(table):
    print(f"{table} already exists, nothing to do")
else:
    random.seed(42)
    now = datetime.now().replace(microsecond=0)
    rows = [
        (str(i), f"cust_{i % 50}", str(round(random.uniform(5, 200), 2)), (now - timedelta(minutes=i)).isoformat())
        for i in range(1000)
    ]
    spark.createDataFrame(rows, "order_id string, customer string, amount string, ts string") \
        .write.format("delta").saveAsTable(table)
    print(f"{table} created: {spark.table(table).count()} rows")
