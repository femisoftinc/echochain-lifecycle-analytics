# EchoChain – Week 1 Development Summary

## Project Overview

EchoChain is a Circular Economy and Secondary Market Lifecycle Analytics project designed to combine internal manufacturing data with secondary-market data.

The project aims to analyze product lifecycle, resale value, warranty failures, and refurbishment opportunities.

## Week 1 Objectives

The main objectives for Week 1 were:

- Understand the project requirements and datasets
- Explore the available datasets
- Set up the Scrapy web scraping infrastructure
- Extract mock secondary-market data
- Set up the Databricks environment
- Initialize raw data storage
- Ingest datasets using PySpark
- Create Bronze Delta tables
- Prepare the project for Power BI integration

## Datasets

Three datasets are currently used in the EchoChain project:

### 1. Laptop Dataset

Contains internal laptop product and specification information.

**Records:** 893

This dataset represents the internal product/SKU data that will later be matched with secondary-market listings.

### 2. Warranty Dataset

Contains warranty and product failure information.

**Records:** 893

This dataset will be used to identify component and product reliability patterns.

### 3. Secondary Market Dataset

Contains secondary-market product listings, including pricing, product condition, and other listing information.

**Records:** 4,197

This dataset represents external secondary-market data.

## Data Understanding

A Jupyter Notebook was created to:

- Load the datasets using Pandas
- Inspect dataset dimensions
- Examine columns and data types
- Generate descriptive statistics
- Understand the role of each dataset in the EchoChain architecture

Notebook:

`notebooks/01_Data_Understanding.ipynb`

## Web Scraping Infrastructure

A Scrapy project was initialized for collecting secondary-market data.

The first spider was created and tested using a mock scraping website.

The spider successfully:

- Sent HTTP requests
- Parsed HTML content
- Extracted product information
- Handled pagination
- Exported scraped results to JSON

Scrapy project:

`echochain_scraper/`

## Databricks Lakehouse Setup

A Databricks workspace was configured for the EchoChain project.

A Unity Catalog managed volume was created:

`/Volumes/workspace/default/echochain_raw`

The three raw CSV datasets were uploaded to this volume.

## PySpark Data Ingestion

The raw CSV files were loaded into PySpark DataFrames.

The following record counts were verified:

- Secondary-market records: 4,197
- Laptop records: 893
- Warranty records: 893

Column names were standardized to make them compatible with Delta Lake.

## Bronze Layer

The raw datasets were stored as Delta tables:

- `workspace.default.bronze_ebay`
- `workspace.default.bronze_laptop`
- `workspace.default.bronze_warranty`

The Bronze tables preserve the ingested source data and form the first layer of the EchoChain Lakehouse architecture.

Databricks notebook:

`notebooks/databricks/01_Bronze_Layer.py`

## Technologies Used

- Python
- Pandas
- Jupyter Notebook
- Scrapy
- Databricks
- Apache Spark
- PySpark
- Delta Lake
- Unity Catalog
- Git
- GitHub
- Visual Studio Code

## Week 1 Status

### Data Engineering

- [x] Dataset acquisition
- [x] Dataset understanding
- [x] Scrapy project setup
- [x] Mock web scraping
- [x] Databricks workspace setup
- [x] Raw data upload
- [x] PySpark ingestion
- [x] Bronze Delta tables
- [x] Record-count validation

### Analytics and BI

- [ ] Connect Power BI Desktop to Databricks
- [ ] Verify access to initial Databricks tables from Power BI

## Next Step

Complete the Power BI connection to the Databricks SQL Warehouse. After successful connection and table verification, the Week 1 implementation will be fully complete.