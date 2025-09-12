# SQL Data Warehouse Project

A comprehensive data warehousing solution built with SQL Server, showcasing ETL pipelines, dimensional modeling, and analytical querying.

## 🛠️ Technologies Used
- Microsoft SQL Server
- SQL Server Management Studio (SSMS)

## 📦 Features
- Data ingestion from CSV files
- Fact and dimension table modeling (star Schema)
- Performance-optimized queries

## 🚀 Project Overview

This project demonstrates the creation of a modern data warehouse using SQL Server. It includes:
- ETL processes to extract, transform, and load data
- Star schema design for efficient querying
- Analytical queries for business insights
- Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.

### 🧱 Data Architecture

The data architecture for this project follows three-tiered architecture containing Bronze, Silver, and Gold layers:

<img width="1544" height="778" alt="data_architecture" src="https://github.com/user-attachments/assets/5cf1f6ca-9652-4794-8608-4bca774adc4b" />

1. Bronze Layer: Captures unprocessed data directly from source systems, importing it in its original format from CSV files into the SQL Server database.
2. Silver Layer: Focuses on refining the data through cleaning, formatting, and normalization steps to ensure it's structured and ready for analytical use.
3. Gold Layer: Contains curated, analysis-ready data organized in a star schema, optimized for business intelligence, reporting, and decision-making.

### 🔄 ETL Pipelines
Designs robust Extract, Transform, Load (ETL) workflows to move data from source systems into the warehouse.
- Automates ingestion from flat files
- Applies transformation logic for consistency and quality
- Loads data into structured tables for downstream use
### 🧮 Data Modeling
Constructs a dimensional model using fact and dimension tables tailored for analytical performance.
- Star schema architecture ensures fast query execution
- Supports slicing and dicing across multiple business dimensions
### 📊 Analytics & Reporting
Delivers actionable insights through SQL-based queries and visual dashboards.
- Builds reusable analytical queries for KPIs and trends
- Enables data-driven decision-making via structured reports




