SELECT * 
FROM retail_lakehouse.gold.daily_sales_summary
ORDER BY sales_date;

SELECT *
FROM retail_lakehouse.gold.country_sales_summary
ORDER BY total_revenue DESC;

SELECT *
FROM retail_lakehouse.gold.product_sales_summary
ORDER BY product_revenue DESC;

SELECT *
FROM retail_lakehouse.gold.monthly_sales_summary
ORDER BY invoice_year, invoice_month_number;