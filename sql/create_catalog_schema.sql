CREATE CATALOG IF NOT EXISTS retail_lakehouse;

CREATE SCHEMA IF NOT EXISTS retail_lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS retail_lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS retail_lakehouse.gold;

CREATE VOLUME IF NOT EXISTS retail_lakehouse.bronze.raw_files;