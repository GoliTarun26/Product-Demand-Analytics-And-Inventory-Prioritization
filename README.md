# 📊 Product Demand Analytics and Inventory Prioritization

**👨‍💻 Project Developer:** G. Tarun

## 📌 Project Overview

Product Demand Analytics and Inventory Prioritization is a Big Data project developed using the **DataCo Supply Chain Dataset**. The main objective of this project is to analyze product demand, sales performance, regional demand patterns, and product rankings to support data-driven inventory prioritization.

The project uses Big Data technologies such as Hadoop, HDFS, Hive, MapReduce, and Apache Spark to process and analyze large volumes of supply chain data. R is used for data visualization and dashboard development.

## 📂 Dataset Information

- **Dataset:** DataCo Supply Chain Dataset
- **Total Records:** 180,519
- **Total Columns:** 53
- **Distinct Products:** 118
- **Categories:** 50
- **Departments:** 11

## 🛠️ Technologies Used

- 🐘 **Hadoop & HDFS:** Distributed storage and processing of large datasets.
- 🐝 **Apache Hive:** Data validation, querying, and analytical operations.
- ☕ **Java MapReduce:** Product-wise quantity aggregation.
- ⚡ **Apache Spark:** Product demand, sales, monthly demand, regional demand, and product ranking analysis.
- 📈 **R & RStudio:** Data visualization and dashboard development.
- 🐍 **Python:** Data preprocessing and CSV conversion utilities.

## 🔄 Project Workflow

1. 📥 **Data Loading:** Uploaded the original dataset into HDFS and created a raw Hive table.
2. 🔍 **Data Validation:** Performed record count, missing value, duplicate, and data quality checks using Hive.
3. 🧹 **Data Preprocessing:** Cleaned the dataset, converted data types, derived date fields, and prepared the data in Parquet format.
4. ⚙️ **MapReduce Processing:** Implemented product-wise quantity aggregation using Java MapReduce.
5. ⚡ **Product Demand Analysis:** Used Apache Spark to analyze product demand, overall sales, monthly demand, regional demand, and product rankings.
6. 🐝 **Hive Analytics:** Executed analytical queries to identify top-performing products, categories, and regions.
7. 📦 **Inventory Prioritization:** Classified products into High, Medium, and Low demand categories based on demand percentiles.
8. 📊 **Data Visualization:** Exported analytical results into CSV files for visualization and dashboard development using R.

## 📊 Key Analysis Performed

- 📦 Product-wise demand analysis.
- 💰 Overall sales and revenue analysis.
- 📅 Monthly demand analysis.
- 🌍 Regional demand comparison.
- 🏆 Product ranking based on quantity sold.
- 🛍️ Category-wise performance analysis.
- 📋 Demand-based inventory prioritization.

## 🎯 Key Results

| Metric | Result |
|---|---:|
| Total Transactions | 180,519 |
| Total Quantity Sold | 384,079 units |
| Total Revenue | 36.78 million (approx.) |
| Distinct Products | 118 |
| Highest Revenue Region | Western Europe |

## 🚀 Project Outcome

The project demonstrates how Big Data technologies can be used to process and analyze supply chain datasets efficiently. The analytical results help identify high-demand products, understand regional sales patterns, evaluate product performance, and support demand-based inventory prioritization.

## 👨‍💻 Developer

**G. Tarun**

🎓 Big Data Analytics Project
