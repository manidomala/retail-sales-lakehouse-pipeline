# Retail Sales Data Lakehouse Pipeline

## Project Overview

This project is an end-to-end Retail Sales Data Lakehouse Pipeline built using Python, PySpark, Spark SQL, Delta Lake, Databricks, AWS S3, and GitHub.

The pipeline follows the Medallion Architecture pattern:

* Bronze Layer: Raw ingested data
* Silver Layer: Cleaned and transformed data
* Gold Layer: Business-ready analytics tables

The goal of this project is to demonstrate a real-world data engineering workflow where raw retail transaction data is ingested from cloud storage, processed using Spark, stored as Delta tables, and queried for business insights.

---

## Tech Stack

* Python
* PySpark
* Spark SQL
* Delta Lake
* Databricks
* AWS S3
* Git
* GitHub

---

## Project Architecture

```text
Raw CSV Data
   ↓
AWS S3 / Databricks Volume
   ↓
Bronze Delta Table
   ↓
Silver Delta Table
   ↓
Gold Delta Tables
   ↓
SQL Analytics
```

---

## Project Structure

```text
retail-sales-lakehouse-pipeline/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── data/
│   ├── raw/
│   │   └── .gitkeep
│   └── sample/
│       └── .gitkeep
│
├── notebooks/
│   ├── 01_ingest_to_bronze.py
│   ├── 02_bronze_to_silver.py
│   ├── 03_silver_to_gold.py
│   └── 04_sql_analytics.sql
│
├── sql/
│   ├── create_catalog_schema.sql
│   ├── bronze_tables.sql
│   ├── silver_tables.sql
│   ├── gold_tables.sql
│   └── business_queries.sql
│
└── docs/
    ├── architecture.md
    ├── data_dictionary.md
    └── project_explanation.md
```

---

## Dataset

The dataset contains online retail transaction data with columns such as:

| Column      | Description              |
| ----------- | ------------------------ |
| InvoiceNo   | Invoice number           |
| StockCode   | Product code             |
| Description | Product description      |
| Quantity    | Quantity purchased       |
| InvoiceDate | Date and time of invoice |
| UnitPrice   | Unit price of item       |
| CustomerID  | Customer identifier      |
| Country     | Customer country         |

The raw dataset is stored in AWS S3 or a Databricks Volume.

Example S3 path:

```text
s3://retail-sales-lakehouse-mani/raw/online_retail.csv
```

Example Databricks Volume path:

```text
/Volumes/retail_lakehouse/bronze/raw_files/online_retail.csv
```

---

## Phase 1: Project Setup

Created the local project folder structure with separate folders for data, notebooks, SQL scripts, and documentation.

Commands used:

```bash
mkdir retail-sales-lakehouse-pipeline
cd retail-sales-lakehouse-pipeline

mkdir -p data/raw data/sample notebooks sql docs

touch README.md requirements.txt .gitignore
```

---

## Phase 2: Git Setup

Initialized Git and connected the project to GitHub.

```bash
git init
git branch -M main
git add .
git commit -m "Initial project structure"
git remote add origin https://github.com/YOUR_USERNAME/retail-sales-lakehouse-pipeline.git
git push -u origin main
```

---

## Phase 3: AWS S3 Setup

Created an AWS S3 bucket to store the retail sales data.

S3 folder structure:

```text
s3://retail-sales-lakehouse-mani/
├── raw/
├── bronze/
├── silver/
└── gold/
```

Uploaded the raw CSV file to:

```text
s3://retail-sales-lakehouse-mani/raw/online_retail.csv
```

---

## Phase 4: Databricks Setup

Created the required catalog and schemas in Databricks.

```sql
CREATE CATALOG IF NOT EXISTS retail_lakehouse;

CREATE SCHEMA IF NOT EXISTS retail_lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS retail_lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS retail_lakehouse.gold;
```

---

## Phase 5: Bronze Layer

The Bronze layer stores raw ingested retail sales data.

Bronze notebook:

```text
notebooks/01_ingest_to_bronze.py
```

Bronze table:

```text
retail_lakehouse.bronze.retail_sales_raw
```

Additional metadata columns:

* ingestion_timestamp
* source_file

---

## Phase 6: Silver Layer

The Silver layer contains cleaned and standardized data.

Silver notebook:

```text
notebooks/02_bronze_to_silver.py
```

Silver table:

```text
retail_lakehouse.silver.retail_sales_cleaned
```

Transformations performed:

* Renamed columns to snake_case
* Converted data types
* Removed invalid records
* Removed null invoice and product records
* Standardized text columns
* Created total_amount column
* Created transaction_type column

---

## Phase 7: Gold Layer

The Gold layer contains business-ready aggregated tables.

Gold notebook:

```text
notebooks/03_silver_to_gold.py
```

Gold tables:

| Table                                       | Description                     |
| ------------------------------------------- | ------------------------------- |
| retail_lakehouse.gold.daily_sales_summary   | Daily revenue and order summary |
| retail_lakehouse.gold.country_sales_summary | Country-level sales summary     |
| retail_lakehouse.gold.product_sales_summary | Product-level sales summary     |
| retail_lakehouse.gold.monthly_sales_summary | Monthly sales summary           |

---

## Phase 8: SQL Analytics

Business queries are stored in:

```text
notebooks/04_sql_analytics.sql
sql/business_queries.sql
```

Analytics created:

* Top countries by revenue
* Top products by revenue
* Monthly revenue trend
* Daily revenue trend
* Average order value by country

---

## Key Business Insights

This pipeline helps answer business questions such as:

* Which countries generate the highest revenue?
* Which products are the top sellers?
* What is the monthly revenue trend?
* What is the daily sales trend?
* What is the average order value by country?

---

## How to Run the Project

1. Clone the repository.

```bash
git clone https://github.com/YOUR_USERNAME/retail-sales-lakehouse-pipeline.git
cd retail-sales-lakehouse-pipeline
```

2. Upload the retail sales CSV file to AWS S3 or Databricks Volume.

3. Run the SQL setup script in Databricks.

```text
sql/create_catalog_schema.sql
```

4. Run the notebooks in this order:

```text
01_ingest_to_bronze.py
02_bronze_to_silver.py
03_silver_to_gold.py
04_sql_analytics.sql
```

5. Validate tables in Databricks SQL Editor.

---

## Git Workflow Used

After each project phase, changes were committed and pushed to GitHub.

Example:

```bash
git status
git add .
git commit -m "Add meaningful commit message"
git push
```

---

## Best Practices Followed

* Used Medallion Architecture
* Separated raw, cleaned, and business-ready data
* Used Delta Lake tables
* Added metadata columns during ingestion
* Used meaningful folder structure
* Stored code separately from data
* Ignored large CSV files using `.gitignore`
* Used Git commits after each phase
* Added documentation for architecture and data dictionary

---

## Final Output

The project produces clean business-ready Delta tables that can be used for analytics, reporting, and dashboarding.

Final Gold tables:

```text
retail_lakehouse.gold.daily_sales_summary
retail_lakehouse.gold.country_sales_summary
retail_lakehouse.gold.product_sales_summary
retail_lakehouse.gold.monthly_sales_summary
```

---

## Author

Manichandra Domala
