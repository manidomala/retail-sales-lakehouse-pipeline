from pyspark.sql.functions import current_timestamp, col

# Read raw data from Databricks Volume
raw_file_path = "/Volumes/retail_lakehouse/bronze/raw_files/online_retail.csv"

df_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(raw_file_path)
    .select("*", col("_metadata.file_path").alias("source_file"))
)

display(df_raw)

# Add ingestion metadata
df_bronze = (
    df_raw
    .withColumn("ingestion_timestamp", current_timestamp())
)

display(df_bronze)

# Write Bronze Delta table
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable("retail_lakehouse.bronze.retail_sales_raw")

# Validate Bronze table using Spark SQL
spark.sql("""
SELECT *
FROM retail_lakehouse.bronze.retail_sales_raw
LIMIT 10
""").show()

spark.sql("""
SELECT COUNT(*) AS bronze_record_count
FROM retail_lakehouse.bronze.retail_sales_raw
""").show()