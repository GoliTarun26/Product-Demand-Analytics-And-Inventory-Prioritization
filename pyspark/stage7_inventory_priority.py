# -*- coding: utf-8 -*-
# Stage 7 - Demand-based inventory prioritization (REBUILT script, Spark 1.6.2 / Python 2.6+ compatible)
#

# Percentile direction (documented): products are ordered from HIGHEST to LOWEST total_quantity_sold,
# so the top 25% of products by demand are "High" priority.
# This is demand-based only: it does not use stock on hand, reorder points, lead times or safety stock.
#
# Input : hdfs:///project_analysis/product_demand   (written by product_demand_analysis.py)
# Output: hdfs:///project_analysis/inventory_priority, hdfs:///project_analysis/priority_summary
#
# Run: export SPARK_MAJOR_VERSION=1 ; spark-submit stage7_inventory_priority.py

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.functions import col, when, percent_rank, sum, count, lit
from pyspark.sql.window import Window

conf = SparkConf().setAppName("Stage7_Inventory_Prioritization")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

base = "hdfs:///project_analysis"
product_demand = sqlContext.read.parquet(base + "/product_demand")
print("Products read:", product_demand.count())

# percent_rank: 0.0 for the highest-demand product, 1.0 for the lowest-demand product
demand_window = Window.orderBy(col("total_quantity_sold").desc(), col("product_card_id").asc())

ranked = product_demand.withColumn("demand_percentile", percent_rank().over(demand_window))

inventory_priority = ranked.withColumn(
    "priority",
    when(col("demand_percentile") <= 0.25, "High")
    .when(col("demand_percentile") <= 0.75, "Medium")
    .otherwise("Low")
)

inventory_priority = inventory_priority.select(
    "product_card_id", "product_name",
    "total_quantity_sold", "total_transactions", "total_revenue",
    "demand_percentile", "priority"
).orderBy(col("demand_percentile").asc(), col("product_card_id").asc())

print("INVENTORY PRIORITY")
inventory_priority.show(20, False)
inventory_priority.write.mode("overwrite").parquet(base + "/inventory_priority")

priority_summary = inventory_priority.groupBy("priority").agg(
    count("product_card_id").alias("product_count"),
    sum("total_quantity_sold").alias("total_quantity_sold"),
    sum("total_revenue").alias("total_revenue")
).orderBy(
    when(col("priority") == "High", 1).when(col("priority") == "Medium", 2).otherwise(3)
)

print("PRIORITY SUMMARY")
priority_summary.show(10, False)
priority_summary.write.mode("overwrite").parquet(base + "/priority_summary")

print("STAGE 7 COMPLETED. Outputs saved in:", base)
sc.stop()
