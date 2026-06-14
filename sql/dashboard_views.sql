CREATE OR REPLACE VIEW
retail_lakehouse.gold.vw_executive_kpis
AS
SELECT
    gross_sales,
    return_value,
    net_revenue,
    total_orders,
    total_customers,
    units_sold,
    average_order_value,
    return_rate
FROM retail_lakehouse.gold.sales_kpi_summary;

-- Monthly trend view
CREATE OR REPLACE VIEW
retail_lakehouse.gold.vw_monthly_sales_trend
AS
SELECT
    invoice_year,
    invoice_month_number,
    invoice_month,
    invoice_year_month,
    monthly_revenue,
    total_orders,
    unique_customers,
    units_sold,
    revenue_growth_percentage
FROM retail_lakehouse.gold.monthly_sales_summary;

-- Country view
CREATE OR REPLACE VIEW
retail_lakehouse.gold.vw_country_performance
AS
SELECT
    country,
    total_revenue,
    total_orders,
    unique_customers,
    total_quantity_sold,
    ROUND(total_revenue / NULLIF(total_orders, 0), 2)
        AS average_order_value
FROM retail_lakehouse.gold.country_sales_summary;

-- Product view
CREATE OR REPLACE VIEW
retail_lakehouse.gold.vw_product_performance
AS
SELECT
    stock_code,
    description,
    product_revenue,
    quantity_sold,
    order_count,
    unique_customers,
    average_selling_price
FROM retail_lakehouse.gold.product_sales_summary;

-- Customer view
CREATE OR REPLACE VIEW
retail_lakehouse.gold.vw_customer_performance
AS
SELECT
    customer_id,
    country,
    customer_revenue,
    total_orders,
    total_units,
    first_purchase_date,
    last_purchase_date,
    average_order_value,
    customer_segment
FROM retail_lakehouse.gold.customer_sales_summary;

-- Return view
CREATE OR REPLACE VIEW
retail_lakehouse.gold.vw_return_analysis
AS
SELECT
    stock_code,
    description,
    sold_quantity,
    returned_quantity,
    sales_value,
    return_value,
    return_transactions,
    product_return_rate
FROM retail_lakehouse.gold.return_analysis;