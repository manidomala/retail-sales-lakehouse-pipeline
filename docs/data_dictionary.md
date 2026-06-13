# Data Dictionary

## Bronze Table

Table: `retail_lakehouse.bronze.retail_sales_raw`

| Column | Description |
|---|---|
| InvoiceNo | Invoice number |
| StockCode | Product code |
| Description | Product description |
| Quantity | Quantity purchased |
| InvoiceDate | Invoice timestamp |
| UnitPrice | Product unit price |
| CustomerID | Customer identifier |
| Country | Customer country |
| ingestion_timestamp | Data ingestion timestamp |
| source_file | Source file path |

## Silver Table

Table: `retail_lakehouse.silver.retail_sales_cleaned`

| Column | Description |
|---|---|
| invoice_no | Invoice number |
| stock_code | Product code |
| description | Standardized product description |
| quantity | Quantity purchased |
| invoice_timestamp | Converted invoice timestamp |
| unit_price | Product unit price |
| customer_id | Customer identifier |
| country | Standardized country |
| total_amount | quantity × unit_price |
| transaction_type | SALE or RETURN |

## Gold Tables

### daily_sales_summary

Daily revenue and order metrics.

### country_sales_summary

Country-level revenue and customer metrics.

### product_sales_summary

Product-level revenue and quantity metrics.

### monthly_sales_summary

Monthly revenue and customer metrics.