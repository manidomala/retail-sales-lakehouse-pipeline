from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
    count,
    round,
    desc,
    to_date,
    month,
    year
)

df_silver = spark.table("retail_lakehouse.silver.retail_sales_cleaned")

# Gold Table 1: Daily Sales Summary
daily_sales = (
    df_silver
    .withColumn("sales_date", to_date(col("invoice_timestamp")))
    .groupBy("sales_date")
    .agg(
        round(sum("total_amount"), 2).alias("daily_revenue"),
        countDistinct("invoice_no").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("total_quantity_sold")
    )
    .orderBy("sales_date")
)

daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable("retail_lakehouse.gold.daily_sales_summary")


# Gold Table 2: Country Sales Summary
country_sales = (
    df_silver
    .groupBy("country")
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        countDistinct("invoice_no").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        sum("quantity").alias("total_quantity_sold")
    )
    .orderBy(desc("total_revenue"))
)

country_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable("retail_lakehouse.gold.country_sales_summary")


# Gold Table 3: Product Sales Summary
product_sales = (
    df_silver
    .groupBy("stock_code", "description")
    .agg(
        round(sum("total_amount"), 2).alias("product_revenue"),
        sum("quantity").alias("quantity_sold"),
        countDistinct("invoice_no").alias("order_count")
    )
    .orderBy(desc("product_revenue"))
)

product_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable("retail_lakehouse.gold.product_sales_summary")


# Gold Table 4: Monthly Sales Summary
monthly_sales = (
    df_silver
    .withColumn("sales_year", year(col("invoice_timestamp")))
    .withColumn("sales_month", month(col("invoice_timestamp")))
    .groupBy("sales_year", "sales_month")
    .agg(
        round(sum("total_amount"), 2).alias("monthly_revenue"),
        countDistinct("invoice_no").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers")
    )
    .orderBy("sales_year", "sales_month")
)

monthly_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable("retail_lakehouse.gold.monthly_sales_summary")


display(daily_sales)
display(country_sales)
display(product_sales)
display(monthly_sales)