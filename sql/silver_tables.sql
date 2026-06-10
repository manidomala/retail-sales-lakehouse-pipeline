SELECT * FROM retail_lakehouse.silver.retail_sales_cleaned
LIMIT 10;

SELECT count(*) AS total_clean_records
FROM retail_lakehouse.silver.retail_sales_cleaned;

SELECT country, COUNT(*) AS total_orders
FROM retail_lakehouse.silver.retail_sales_cleaned
GROUP BY country
ORDER BY total_orders DESC;