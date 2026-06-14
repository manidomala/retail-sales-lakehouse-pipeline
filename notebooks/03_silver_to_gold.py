# Retail Sales Lakehouse Pipeline
# Silver to Gold Transformation
#
# Source:
# retail_lakehouse.silver.retail_sales_cleaned
#
# Gold Tables:
# 1. order_detail_fact
# 2. sales_kpi_summary
# 3. daily_sales_summary
# 4. monthly_sales_summary
# 5. country_sales_summary
# 6. product_sales_summary
# 7. customer_sales_summary
# 8. return_analysis
# 9. weekday_sales_summary
# ============================================================

# Import required PySpark functions

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Create the Gold schema if it does not already exist

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS retail_lakehouse.gold
""")


# Read cleaned data from the Silver table

df_silver = spark.table(
    "retail_lakehouse.silver.retail_sales_cleaned"
)

print("Silver table loaded successfully.")

display(df_silver.limit(10))


# Separate successful sales and returned transactions

sales_only = (
    df_silver
    .filter(F.col("transaction_type") == "SALE")
)

returns_only = (
    df_silver
    .filter(F.col("transaction_type") == "RETURN")
)

print(f"Sales records: {sales_only.count()}")
print(f"Return records: {returns_only.count()}")


# Reusable function for writing DataFrames to Gold Delta tables

def write_gold_table(dataframe, table_name):
    """
    Writes a DataFrame to the retail_lakehouse.gold schema
    as a managed Delta table.
    """

    full_table_name = f"retail_lakehouse.gold.{table_name}"

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    print(f"Gold table created successfully: {full_table_name}")


# ============================================================
# Gold Table 1: Order Detail Fact
# ============================================================
#
# Provides transaction-level data for detailed analysis,
# dashboard drill-downs and ad-hoc reporting.

order_detail_fact = (
    df_silver
    .select(
        "invoice_no",
        "stock_code",
        "description",
        "quantity",
        "absolute_quantity",
        "unit_price",
        "total_amount",
        "gross_amount",
        "customer_id",
        "customer_type",
        "country",
        "invoice_timestamp",
        "invoice_date",
        "invoice_year",
        "invoice_month_number",
        "invoice_month",
        "invoice_year_month",
        "invoice_quarter",
        "invoice_day",
        "invoice_day_number",
        "transaction_type",
        "ingestion_timestamp",
        "source_file"
    )
)

write_gold_table(
    order_detail_fact,
    "order_detail_fact"
)

# ============================================================
# Gold Table 2: Sales KPI Summary
# ============================================================
#
# Creates one summarized row containing the primary
# business performance metrics.

sales_metrics = (
    sales_only
    .agg(
        F.round(F.sum("total_amount"),2).alias("gross_sales"),
        F.countDistinct("invoice_no").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum("quantity").alias("units_sold")
    )
)


return_metrics = (
    returns_only
    .agg(
        F.round(F.sum(F.abs(F.col("total_amount"))),2).alias("return_value"),
        F.countDistinct("invoice_no").alias("returned_orders"),
        F.sum("absolute_quantity").alias("returned_units")
    )
)


sales_kpi_summary = (
    sales_metrics
    .crossJoin(return_metrics)

    # Replace null values when no sales or returns exist
    .fillna(
        {
            "gross_sales": 0.0,
            "total_orders": 0,
            "total_customers": 0,
            "units_sold": 0,
            "return_value": 0.0,
            "returned_orders": 0,
            "returned_units": 0
        }
    )

    # Revenue remaining after deducting returns
    .withColumn(
        "net_revenue",
        F.round(
            F.col("gross_sales") -
            F.col("return_value"),
            2
        )
    )

    # Average revenue generated per successful sales order
    .withColumn("average_order_value",
        F.when(
            F.col("total_orders") > 0,
            F.round(F.col("gross_sales") /F.col("total_orders"),2)
        )
        .otherwise(F.lit(0.0))
    )

    # Percentage of all orders that were returned
    .withColumn(
        "return_rate",
        F.when(
            (
                F.col("total_orders") + F.col("returned_orders")
            ) > 0,
            F.round(
                (
                    F.col("returned_orders") /
                    (
                        F.col("total_orders") +
                        F.col("returned_orders")
                    )
                ) * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )

    # Percentage of gross sales value that was returned
    .withColumn(
        "return_value_percentage",
        F.when(
            F.col("gross_sales") > 0,
            F.round(
                (
                    F.col("return_value") /
                    F.col("gross_sales")
                ) * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )

    .withColumn(
        "last_updated_timestamp",
        F.current_timestamp()
    )
)

write_gold_table(
    sales_kpi_summary,
    "sales_kpi_summary"
)


# ============================================================
# Gold Table 3: Daily Sales Summary
# ============================================================
#
# Provides daily sales performance for time-series dashboards.

daily_sales = (
    sales_only
    .withColumn(
        "sales_date",
        F.to_date(F.col("invoice_timestamp"))
    )
    .groupBy("sales_date")
    .agg(
        F.round(F.sum("total_amount"),2).alias("daily_revenue"),
        F.countDistinct("invoice_no").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("total_quantity_sold")
    )
    .withColumn(
        "average_order_value",
        F.when(
            F.col("total_orders") > 0,
            F.round(
                F.col("daily_revenue") /F.col("total_orders"),2
            )
        ).otherwise(F.lit(0.0))
    )
    .orderBy("sales_date")
)

write_gold_table(
    daily_sales,
    "daily_sales_summary"
)

# ============================================================
# Gold Table 4: Monthly Sales Summary
# ============================================================
#
# Provides monthly sales performance and month-over-month
# revenue growth.

monthly_sales = (
    sales_only
    .groupBy(
        "invoice_year",
        "invoice_month_number",
        "invoice_month",
        "invoice_year_month"
    )
    .agg(
        F.round(F.sum("total_amount"),2).alias("monthly_revenue"),
        F.countDistinct("invoice_no").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("units_sold")
    )
)


# Window used to retrieve the previous month's revenue

month_window = Window.orderBy(
    "invoice_year",
    "invoice_month_number"
)

monthly_sales = (
    monthly_sales
    .withColumn(
        "previous_month_revenue",
        F.lag("monthly_revenue").over(month_window)
    )
    .withColumn(
        "revenue_growth_percentage",
        F.when(
            F.col("previous_month_revenue").isNull(),
            F.lit(None).cast("double")
        )
        .when(
            F.col("previous_month_revenue") == 0,
            F.lit(None).cast("double")
        )
        .otherwise(
            F.round(
                (
                    (
                        F.col("monthly_revenue") -F.col("previous_month_revenue")
                    ) /
                    F.col("previous_month_revenue")
                ) * 100,
                2
            )
        )
    )
    .withColumn(
        "average_order_value",
        F.when(
            F.col("total_orders") > 0,
            F.round(
                F.col("monthly_revenue") /
                F.col("total_orders"),
                2
            )
        ).otherwise(F.lit(0.0))
    )
    .orderBy(
        "invoice_year",
        "invoice_month_number"
    )
)

write_gold_table(
    monthly_sales,
    "monthly_sales_summary"
)


# ============================================================
# Gold Table 5: Country Sales Summary
# ============================================================
#
# Provides revenue, order and customer performance by country.

country_sales = (
    sales_only
    .groupBy("country")
    .agg(
        F.round(F.sum("total_amount"),2).alias("total_revenue"),
        F.countDistinct("invoice_no").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("total_quantity_sold")
    )
    .withColumn(
        "average_order_value",
        F.when(
            F.col("total_orders") > 0,
            F.round(
                F.col("total_revenue") /F.col("total_orders"),2
            )
        ).otherwise(F.lit(0.0))
    )
    .orderBy(
        F.col("total_revenue").desc()
    )
)

write_gold_table(
    country_sales,
    "country_sales_summary"
)


# ============================================================
# Gold Table 6: Product Sales Summary
# ============================================================
#
# Provides revenue and sales performance for each product.

product_sales = (
    sales_only
    .groupBy(
        "stock_code",
        "description"
    )
    .agg(
        F.round(F.sum("total_amount"),2).alias("product_revenue"),
        F.sum("quantity").alias("quantity_sold"),
        F.countDistinct("invoice_no").alias("order_count"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn(
        "average_selling_price",
        F.when(
            F.col("quantity_sold") > 0,
            F.round(
                F.col("product_revenue") /F.col("quantity_sold"),2
            )
        ).otherwise(F.lit(0.0))
    )
    .orderBy(
        F.col("product_revenue").desc()
    )
)

write_gold_table(
    product_sales,
    "product_sales_summary"
)


# ============================================================
# Gold Table 7: Customer Sales Summary
# ============================================================
#
# Provides customer-level metrics and value-based customer
# segmentation.

customer_sales = (
    sales_only
    .filter(
        F.col("customer_id").isNotNull()
    )
    .groupBy(
        "customer_id",
        "country"
    )
    .agg(
        F.round(F.sum("total_amount"),2).alias("customer_revenue"),
        F.countDistinct("invoice_no").alias("total_orders"),
        F.sum("quantity").alias("total_units"),
        F.min("invoice_date").alias("first_purchase_date"),
        F.max("invoice_date").alias("last_purchase_date")
    )
    .withColumn(
        "average_order_value",
        F.when(
            F.col("total_orders") > 0,
            F.round(
                F.col("customer_revenue") /F.col("total_orders"),2
            )
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "customer_segment",
        F.when(
            F.col("customer_revenue") >= 5000,"HIGH VALUE"
        )
        .when(
            F.col("customer_revenue") >= 1000,"MEDIUM VALUE"
        )
        .otherwise("LOW VALUE")
    )
    .orderBy(
        F.col("customer_revenue").desc()
    )
)

write_gold_table(
    customer_sales,
    "customer_sales_summary"
)


# ============================================================
# Gold Table 8: Return Analysis
# ============================================================
#
# Compares product sales with product returns and calculates
# product-level return rates.

product_returns = (
    returns_only
    .groupBy(
        "stock_code",
        "description"
    )
    .agg(
        F.sum("absolute_quantity").alias("returned_quantity"),
        F.round(F.sum(F.abs(F.col("total_amount"))),2).alias("return_value"),
        F.countDistinct("invoice_no").alias("return_transactions"),
        F.countDistinct("customer_id").alias("customers_returning")
    )
)


product_sales_base = (
    sales_only
    .groupBy(
        "stock_code",
        "description"
    )
    .agg(
        F.sum("quantity").alias("sold_quantity"),
        F.round(F.sum("total_amount"),2).alias("sales_value")
    )
)


return_analysis = (
    product_sales_base
    .join(
        product_returns,
        on=["stock_code", "description"],
        how="left"
    )
    .fillna(
        {
            "returned_quantity": 0,
            "return_value": 0.0,
            "return_transactions": 0,
            "customers_returning": 0
        }
    )
    .withColumn(
        "product_return_rate",
        F.when(
            F.col("sold_quantity") > 0,
            F.round(
                (
                    F.col("returned_quantity") /F.col("sold_quantity")
                ) * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )
    .withColumn(
        "return_value_percentage",
        F.when(
            F.col("sales_value") > 0,
            F.round(
                (
                    F.col("return_value") /F.col("sales_value")
                ) * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )
    .orderBy(
        F.col("return_value").desc()
    )
)

write_gold_table(
    return_analysis,
    "return_analysis"
)


# ============================================================
# Gold Table 9: Weekday Sales Summary
# ============================================================
#
# Provides sales performance by day of the week.

weekday_sales = (
    sales_only
    .groupBy(
        "invoice_day",
        "invoice_day_number"
    )
    .agg(
        F.round(F.sum("total_amount"),2).alias("revenue"),
        F.countDistinct("invoice_no").alias("orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("units_sold")
    )
    .withColumn(
        "average_order_value",
        F.when(
            F.col("orders") > 0,
            F.round(
                F.col("revenue") /F.col("orders"),2
            )
        ).otherwise(F.lit(0.0))
    )
    .orderBy("invoice_day_number")
)

write_gold_table(
    weekday_sales,
    "weekday_sales_summary"
)


# ============================================================
# Validate Created Gold Tables
# ============================================================

gold_tables = spark.sql("""
    SHOW TABLES IN retail_lakehouse.gold
""")

display(gold_tables)


# Display important Gold table outputs

print("Sales KPI Summary")
display(sales_kpi_summary)

print("Daily Sales Summary")
display(daily_sales)

print("Monthly Sales Summary")
display(monthly_sales)

print("Country Sales Summary")
display(country_sales)

print("Product Sales Summary")
display(product_sales)

print("Customer Sales Summary")
display(customer_sales)

print("Return Analysis")
display(return_analysis)

print("Weekday Sales Summary")
display(weekday_sales)

print("Order Detail Fact")
display(order_detail_fact.limit(100))


print("Silver-to-Gold transformation completed successfully.")