SELECT *
FROM retail_lakehouse.bronze.retail_sales_raw
LIMIT 10;

DESCRIBE TABLE retail_lakehouse.bronze.retail_sales_raw;

SELECT COUNT(*) AS total_records
FROM retail_lakehouse.bronze.retail_sales_raw;