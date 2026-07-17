# Week 2 - Silver Layer

## Objective
Transform raw Bronze data into clean, validated, and analytics-ready Silver Delta tables.

## Tasks Completed

- Read Bronze Delta tables
- Explored schema and sample data
- Removed duplicate records
- Removed null records
- Standardized column data types
- Identified 10 corrupted eBay records
- Removed corrupted records
- Converted eBay price column from String to Double
- Created Silver Delta tables
- Verified record counts and schemas

## Silver Tables Created

- silver_ebay
- silver_laptop
- silver_warranty

## Final Record Counts

| Table | Records |
|--------|---------|
| silver_ebay | 4187 |
| silver_laptop | 893 |
| silver_warranty | 893 |

## Technologies Used

- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Git & GitHub

## Key Learning

- Data quality validation
- Duplicate removal
- Null handling
- Data type conversion
- Delta table creation
- ETL pipeline design