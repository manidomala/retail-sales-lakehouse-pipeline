# Retail Sales Data Lakehouse Pipeline

## Project Overview

This project is an end-to-end **Retail Sales Data Lakehouse Pipeline** built using **Python, PySpark, SQL, Databricks, Delta Lake, AWS S3, and GitHub**.

The goal of this project is to simulate a real-world data engineering pipeline where raw retail transaction data is stored in a cloud data lake, processed through **Bronze, Silver, and Gold layers**, and made available for business analytics.

---

## Business Problem

Retail companies generate large volumes of sales transaction data from invoices, customers, products, countries, and sales transactions.

Raw retail data is often not directly usable for analytics because it may contain:

- Null values
- Duplicate records
- Invalid quantities
- Invalid prices
- Inconsistent column names
- Incorrect data types

This project solves the problem by building a structured lakehouse pipeline that:

1. Ingests raw retail sales data
2. Stores raw data in the Bronze layer
3. Cleans and transforms data in the Silver layer
4. Creates business-ready Gold tables
5. Performs SQL-based analytics

---

## Tech Stack

| Technology | Purpose |
|---|---|
| Python | Programming and transformation logic |
| PySpark | Distributed data processing |
| Spark SQL | SQL-based data analysis |
| Databricks | Lakehouse platform |
| Delta Lake | Reliable table storage |
| AWS S3 | Cloud storage/data lake |
| Git | Version control |
| GitHub | Code repository and portfolio |

---

## Architecture

```text
Retail CSV Dataset
        |
        v
AWS S3 / Databricks Volume
        |
        v
Bronze Layer
Raw Delta Table
        |
        v
Silver Layer
Cleaned Delta Table
        |
        v
Gold Layer
Business Aggregated Tables
        |
        v
SQL Analytics