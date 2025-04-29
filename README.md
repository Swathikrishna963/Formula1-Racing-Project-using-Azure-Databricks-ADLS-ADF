# 🏎️ Formula 1 Racing Data Engineering Project

This project demonstrates the design and implementation of a scalable, end-to-end data pipeline using **Azure Data Factory (ADF)**, **Azure Databricks (PySpark)**, and **Azure Data Lake Storage (ADLS Gen2)** to process and analyze Formula 1 racing data.

---

## 📌 Project Objective

To build a modern data platform on Azure that enables ingestion, transformation, and modeling of Formula 1 race data, supporting downstream analytics and business intelligence use cases.

---

## 🔧 Technologies Used

- **Azure Data Factory (ADF)** – for orchestrating data movement and transformation pipelines
- **Azure Databricks** – for scalable data processing using PySpark and Delta Lake
- **Azure Data Lake Storage Gen2 (ADLS)** – for storing raw and processed data
- **Delta Lake** – for ACID-compliant data architecture and time travel capabilities
- **PySpark** – for data wrangling, transformation, and enrichment

---

## 🗂️ Project Structure

### 🏁 Bronze Layer – Raw Data Ingestion
- Raw CSV and JSON files are stored in the **bronze** zone of ADLS.
- ADF pipelines ingest the data from public datasets or simulated uploads.

### 🏎️ Silver Layer – Cleaned & Processed Data
- Databricks notebooks clean and transform raw data using **PySpark**.
- Data is standardized and joined across domains (e.g., drivers, races, constructors).
- Stored as Delta tables for efficient querying and versioning.

### 🥇 Gold Layer – Aggregated Business-Ready Data
- Aggregations and business logic are applied (e.g., top drivers, team rankings).
- These curated datasets are ready for consumption by BI tools like Power BI.

---

## 📊 Datasets Used

The dataset includes historical Formula 1 data:
- Drivers
- Constructors (Teams)
- Circuits
- Races & Results
- Lap Times
- Pit Stops
- Qualifying Sessions

Public dataset source: [Ergast Developer API](http://ergast.com/mrd/) and Kaggle

---

## 🔁 Workflow Overview

1. **Data Ingestion**  
   - ADF pipelines copy raw files into ADLS (Bronze layer).

2. **Data Processing**  
   - Azure Databricks notebooks process and transform data into Silver layer.
   - Complex joins, filtering, and enrichment logic using PySpark.

3. **Data Modeling**  
   - Gold layer datasets are derived for BI and reporting.

4. **Orchestration & Automation**  
   - ADF triggers Databricks notebooks using pipeline activities.
   - Monitoring handled through ADF and Azure Log Analytics.

---

## 🚀 Key Features

- End-to-end **data lake architecture** with Bronze, Silver, and Gold zones
- Robust ETL using **ADF + Databricks integration**
- Scalable processing with **PySpark and Delta Lake**
- Handles **structured and semi-structured** data (CSV, JSON, Parquet)
- CI/CD readiness for production environments
- Ready for future enhancements in **Power BI**, **Azure Synapse**, or **ML**

---

## 📈 Future Enhancements

- Power BI dashboards to visualize race and driver performance trends
- Stream processing using **Azure Event Hubs** and **Structured Streaming**
- Machine Learning models for predicting race outcomes or driver rankings
- Integration with APIs for real-time or near real-time F1 updates

---

## 📁 Sample Folder Structure
