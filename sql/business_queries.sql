-- Top 10 countries by Revenue
SELECT country, total_revenue, total_orders, unique_customers
FROM retail_lakehouse.gold.country_sales_summary
ORDER BY total_revenue DESC
LIMIT 10;

-- Top 10 products by revenue
SELECT stock_code, description, product_revenue
FROM retail_lakehouse.gold.product_sales_summary
ORDER BY product_revenue DESC
LIMIT 10;

-- Monthly revenue trend
SELECT sales_year, sales_month, monthly_revenue, total_orders, unique_customers
FROM retail_lakehouse.gold.monthly_sales_summary
ORDER BY sales_year, sales_month;

-- Daily revenue trend
SELECT sales_date, daily_revenue, total_orders, unique_customers
FROM retail_lakehouse.gold.daily_sales_summary
ORDER BY sales_date;

-- Average order value by country
SELECT
    country,
    round((total_revenue / total_quantity_sold),2) AS average_order_value
FROM retail_lakehouse.gold.country_sales_summary
WHERE total_orders >0
ORDER BY average_order_value DESC;