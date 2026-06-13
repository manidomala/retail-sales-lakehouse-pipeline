# Architecture

This project follows the Medallion Architecture pattern.

## Data Flow

Raw CSV data is uploaded to AWS S3 or Databricks Volume.

The data is then processed through three layers:

1. Bronze Layer
   - Raw data ingestion
   - Metadata columns added
   - Stored as Delta table

2. Silver Layer
   - Data cleaning
   - Type conversion
   - Null and invalid record removal
   - Business columns created

3. Gold Layer
   - Aggregated business tables
   - Daily sales summary
   - Country sales summary
   - Product sales summary
   - Monthly sales summary

## Tools Used

- Python
- PySpark
- Spark SQL
- Delta Lake
- Databricks
- AWS S3
- GitHub