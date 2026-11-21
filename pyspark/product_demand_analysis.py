# -*- coding: utf-8 -*-
# Stage 5 - Product demand analysis (historical retained PySpark script, Spark 1.6.2)
# Run: export SPARK_MAJOR_VERSION=1 ; spark-submit product_demand_analysis.py
# Input : hdfs:///project_cleaned (Parquet, produced by stage3_preprocessing.py)
# Output: hdfs:///project_analysis/{product_demand,sales_summary,monthly_demand,regional_demand,product_ranking}

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import (
    col, sum, count, avg, row_number
)
from pyspark.sql.window import Window

conf = SparkConf().setAppName("Product_Demand_Analysis")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

input_path = "hdfs:///project_cleaned"
df = sqlContext.read.parquet(input_path)

df.cache()

print("Total records:", df.count())
print("Dataset schema:")
df.printSchema()

product_demand = df.groupBy(
    "product_card_id",
    "product_name"
).agg(
    sum("total_quantity").alias("total_quantity_sold"),
    count("order_item_id").alias("total_transactions"),
    avg("total_quantity").alias("average_quantity"),
    sum("total_sales").alias("total_revenue")
)

product_demand = product_demand.orderBy(
    col("total_quantity_sold").desc()
)

print("PRODUCT-WISE DEMAND")
product_demand.show(20, False)

product_demand.write.mode("overwrite").parquet(
    "hdfs:///project_analysis/product_demand"
)

sales_summary = df.agg(
    sum("total_sales").alias("total_revenue"),
    sum("total_quantity").alias("total_quantity_sold"),
    count("order_item_id").alias("total_transactions"),
    avg("total_sales").alias("average_sales_per_transaction")
)

print("OVERALL SALES SUMMARY")
sales_summary.show(1, False)

sales_summary.write.mode("overwrite").parquet(
    "hdfs:///project_analysis/sales_summary"
)

monthly_demand = df.groupBy(
    "order_year",
    "order_month"
).agg(
    sum("total_quantity").alias("total_quantity_sold"),
    count("order_item_id").alias("total_transactions"),
    sum("total_sales").alias("total_revenue")
)

monthly_demand = monthly_demand.orderBy(
    col("order_year"),
    col("order_month")
)

print("MONTHLY DEMAND")
monthly_demand.show(50, False)

monthly_demand.write.mode("overwrite").parquet(
    "hdfs:///project_analysis/monthly_demand"
)

regional_demand = df.groupBy(
    "order_region"
).agg(
    sum("total_quantity").alias("total_quantity_sold"),
    count("order_item_id").alias("total_transactions"),
    sum("total_sales").alias("total_revenue")
)

regional_demand = regional_demand.orderBy(
    col("total_quantity_sold").desc()
)

print("REGIONAL DEMAND")
regional_demand.show(50, False)

regional_demand.write.mode("overwrite").parquet(
    "hdfs:///project_analysis/regional_demand"
)

ranking_window = Window.orderBy(
    col("total_quantity_sold").desc(),
    col("product_card_id").asc()
)

product_ranking = product_demand.withColumn(
    "product_rank",
    row_number().over(ranking_window)
)

product_ranking = product_ranking.orderBy(
    col("product_rank")
)

print("PRODUCT RANKING")
product_ranking.show(118, False)

product_ranking.write.mode("overwrite").parquet(
    "hdfs:///project_analysis/product_ranking"
)

print("STAGE 5 ANALYSIS COMPLETED")
print("Outputs saved in: hdfs:///project_analysis")

print("Product demand records:", product_demand.count())
print("Monthly demand records:", monthly_demand.count())
print("Regional demand records:", regional_demand.count())
print("Product ranking records:", product_ranking.count())

sc.stop()
