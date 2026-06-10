from pyspark.sql.functions import (col,trim,upper,to_timestamp,round,when)

df_bronze = spark.table("retail_lakehouse.bronze.retail_sales_raw")

# Rename columns
df_silver = (
    df_bronze
    .withColumnRenamed("InvoiceNo","invoice_no")
    .withColumnRenamed("StockCode","stock_code")
    .withColumnRenamed("Description","description")
    .withColumnRenamed("Quantity","quantity")
    .withColumnRenamed("InvoiceDate","invoice_date")
    .withColumnRenamed("UnitPrice","unit_price")
    .withColumnRenamed("CustomerID","customer_id")
    .withColumnRenamed("Country","country")
    )

# 
df_silver = (
    df_silver
    .withColumn("description",trim(upper(col("description"))))
    .withColumn("country",trim(upper(col("country"))))
    .withColumn("invoice_timestamp", to_timestamp(col("invoice_date"), 'M/d/yyyy H:mm'))
    .withColumn("quantity", col("quantity").cast("int"))    
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("total_amount", round(col("quantity") * col("unit_price"), 2))
)

df_silver = (
    df_silver
    .filter(col("invoice_no").isNotNull())
    .filter(col("stock_code").isNotNull())
    .filter(col("quantity")>0)
    .filter(col("unit_price")>0)
    .filter(col("invoice_timestamp").isNotNull())
)

df_silver = (
    df_silver
    .withColumn(
        "transaction_type",
        when(col("invoice_no").startswith("C"), "RETURN")
        .otherwise("SALE")
    )
)

display(df_silver.limit(100))

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable("retail_lakehouse.silver.retail_sales_cleaned")

display(
    spark.sql(
        """SELECT *
        FROM retail_lakehouse.silver.retail_sales_cleaned
        LIMIT 100"""
    )
)