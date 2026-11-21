# -*- coding: utf-8 -*-
# Stage 3 - Cleaning and preprocessing (REBUILT script, Spark 1.6.2 / Python 2.6+ compatible)

# Input : Hive table project_db.dataco_raw
# Output: hdfs:///project_cleaned (Parquet) with the columns Stage 5 and Stage 7 need:
#         product_card_id, product_name, order_item_id, total_quantity, total_sales,
#         order_year, order_month, order_region (plus category_name, department_name, product_price)
#
# Run: export SPARK_MAJOR_VERSION=1 ; spark-submit stage3_preprocessing.py

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import (
    col, trim, unix_timestamp, from_unixtime, year, month, coalesce, count, lit
)

RAW_TABLE = "project_db.dataco_raw"
OUTPUT_PATH = "hdfs:///project_cleaned"

# Raw column names (as in the Stage 2 validation queries)
RAW_COLUMNS = {
    "product_id": "product_card_id",
    "product_name": "product_name",
    "item_id": "order_item_id",
    "quantity": "order_item_quantity",
    "sales": "sales",
    "price": "product_price",
    "category": "category_name",
    "department": "department_name",
    "date": "order_date",
    "region": "order_region",
}

# Date patterns tried in order (DataCo exports use e.g. 1/31/2018 22:56)
DATE_FORMATS = ["M/d/yyyy H:mm", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "M/d/yyyy"]

conf = SparkConf().setAppName("Stage3_Cleaning_Preprocessing")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

raw = sqlContext.table(RAW_TABLE)
total_raw = raw.count()
print("Raw records:", total_raw)

# 1. Select, trim and cast
parsed_ts = coalesce(*[unix_timestamp(trim(col(RAW_COLUMNS["date"])), f) for f in DATE_FORMATS])

df = raw.select(
    col(RAW_COLUMNS["product_id"]).cast("int").alias("product_card_id"),
    trim(col(RAW_COLUMNS["product_name"])).alias("product_name"),
    col(RAW_COLUMNS["item_id"]).cast("long").alias("order_item_id"),
    col(RAW_COLUMNS["quantity"]).cast("int").alias("total_quantity"),
    col(RAW_COLUMNS["sales"]).cast("double").alias("total_sales"),
    col(RAW_COLUMNS["price"]).cast("double").alias("product_price"),
    trim(col(RAW_COLUMNS["category"])).alias("category_name"),
    trim(col(RAW_COLUMNS["department"])).alias("department_name"),
    trim(col(RAW_COLUMNS["region"])).alias("order_region"),
    parsed_ts.alias("order_ts"),
)

# 2. Derive year and month from the parsed order date
df = df.withColumn("order_date_parsed", from_unixtime(col("order_ts")))
df = df.withColumn("order_year", year(col("order_date_parsed")))
df = df.withColumn("order_month", month(col("order_date_parsed")))

# 3. Remove missing / invalid / negative records
df = df.filter(
    col("product_card_id").isNotNull() &
    col("product_name").isNotNull() & (col("product_name") != "") &
    col("order_item_id").isNotNull() &
    col("total_quantity").isNotNull() & (col("total_quantity") >= 0) &
    col("total_sales").isNotNull() & (col("total_sales") >= 0) &
    col("product_price").isNotNull() & (col("product_price") >= 0) &
    col("category_name").isNotNull() &
    col("order_region").isNotNull() & (col("order_region") != "") &
    col("order_ts").isNotNull()
)

# 4. Remove duplicate order items
df = df.dropDuplicates(["order_item_id"])

cleaned = df.select(
    "product_card_id", "product_name", "order_item_id",
    "total_quantity", "total_sales", "product_price",
    "category_name", "department_name",
    "order_year", "order_month", "order_region",
)

cleaned.write.mode("overwrite").parquet(OUTPUT_PATH)

# 5. Quick validation of what was written
check = sqlContext.read.parquet(OUTPUT_PATH)
print("Cleaned records:", check.count())
print("Removed records:", total_raw - check.count())
check.printSchema()
check.show(5, False)

sc.stop()
