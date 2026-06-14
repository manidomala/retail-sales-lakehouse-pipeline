from pyspark.sql.functions import (
    col,
    count,
    sum,
    when,
    current_timestamp,
    lit
)

df_bronze = spark.table(
    "retail_lakehouse.bronze.retail_sales_raw"
)

df_silver = spark.table(
    "retail_lakehouse.silver.retail_sales_cleaned"
)

bronze_count = df_bronze.count()
silver_count = df_silver.count()

null_customer_count = (
    df_silver
    .filter(col("customer_id").isNull())
    .count()
)

duplicate_count = (
    df_silver
    .groupBy(
        "invoice_no",
        "stock_code",
        "customer_id",
        "invoice_timestamp"
    )
    .count()
    .filter(col("count") > 1)
    .count()
)

invalid_price_count = (
    df_silver
    .filter(col("unit_price") <= 0)
    .count()
)

invalid_quantity_count = (
    df_silver
    .filter(col("quantity") == 0)
    .count()
)

quality_data = [
    (
        bronze_count,
        silver_count,
        bronze_count - silver_count,
        null_customer_count,
        duplicate_count,
        invalid_price_count,
        invalid_quantity_count
    )
]

quality_columns = [
    "bronze_record_count",
    "silver_record_count",
    "rejected_record_count",
    "null_customer_count",
    "duplicate_record_count",
    "invalid_price_count",
    "invalid_quantity_count"
]

df_quality = spark.createDataFrame(
    quality_data,
    quality_columns
).withColumn(
    "check_timestamp",
    current_timestamp()
)

df_quality.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "retail_lakehouse.gold.data_quality_metrics"
    )

display(df_quality)