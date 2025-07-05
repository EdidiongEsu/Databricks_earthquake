# End-to-End Earthquake Data Engineering & Analytics Pipeline with Azure, Databricks & Power BI
## Navigation / Quick Access
Quickly move to the section you are interested in by clicking on the appropriate link:
- [Overview](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#overview)
- [Project Objective](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#project-objective)
- [Project Architecture](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#project-architecture)
- [Dataset](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#dataset)
- [Reproducing Project](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#reproducing-project) (long section)
- [Dashboard](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#dashboard)

---
## Overview

Every day, hundreds of earthquakes shake the earth — some minor, others devastating. Understanding where, when, and how they occur is crucial for monitoring natural hazards, informing infrastructure planning, and protecting communities.

This project extracts and analyzes real-time earthquake data from the [United States Geological Survey (USGS)](https://www.usgs.gov/programs/earthquake-hazards), an agency that tracks seismic activity around the globe. The data is ingested daily via Azure Data Factory, transformed in Azure Databricks using the **medallion architecture** (bronze → silver → gold), stored in **Microsoft Fabric Lakehouse**, and visualized using **Power BI**.

The pipeline demonstrates how raw JSON data from a public API can be converted into structured, trusted insights using modern, cloud-native tools. By the end of the pipeline, users can explore:

- 🌍 Earthquake hotspots by country and region  
- 📈 Magnitude trends over time  
- 🚨 Significant seismic events by signal strength  
- 🕒 Time-based patterns of earthquake activity

This project showcases how scalable data engineering workflows can power decision-ready dashboards, turning global sensor data into actionable intelligence for analysts, researchers, and the public.

---

## Project Objective

- ✅ Automate daily ingestion of global earthquake data from the USGS API using Azure Data Factory  
- ✅ Transform and enrich raw data using Azure Databricks with a medallion architecture (bronze → silver → gold)  
- ✅ Store trusted and structured data in **Microsoft Fabric Lakehouse**  
- ✅ Build interactive **Power BI** dashboards that uncover patterns, trends, and anomalies in global seismic activity  

-----
## Project Architecture
![Alt text](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/img/Earthquake%20architecture.gif)

This architecture illustrates the end-to-end data pipeline used in this project, leveraging Azure and Microsoft Fabric services to move from raw ingestion to visual insights.

### 🔄 End-to-End Pipeline Flow

1. **🔁 Ingestion – Azure Data Factory**  
   - Daily earthquake data is ingested from the USGS API.  
   - Azure Data Factory orchestrates the process and stores the data in **Azure Data Lake**.

2. **⚙️ Transformation – Azure Databricks (Medallion Architecture)**  
   - Data is processed through three structured layers:
     - **Bronze Layer**: Raw ingestion and flattening
     - **Silver Layer**: Cleansing, filtering, standardization
     - **Gold Layer**: Aggregated and enriched for reporting

3. **🏠 Storage – Microsoft Fabric Lakehouse**  
   - The gold-layer data is loaded into **Microsoft Fabric Lakehouse** for scalable storage and advanced analytics.

4. **📊 Visualization – Power BI**  
   - Fabric Lakehouse feeds directly into **Power BI**, enabling dynamic dashboards and reports for stakeholders.

✅ This architecture ensures a reliable, scalable, and analytics-ready pipeline from API to dashboard.


----
## Dataset
### 🌍 Source: USGS Earthquake API

This project collects seismic data from the [United States Geological Survey (USGS) Earthquake API](https://earthquake.usgs.gov/fdsnws/event/1/), which provides detailed information about global earthquake events.

- **API Endpoint:** [https://earthquake.usgs.gov/fdsnws/event/1/](https://earthquake.usgs.gov/fdsnws/event/1/)
- **Data Format:** GeoJSON
- **Ingestion:** Daily via Azure Data Factory
- **Dynamic Parameters:**
  - `starttime`: set dynamically during ingestion
  - `endtime`: optional, defaults to the same as `starttime`

📘 [API Documentation](https://earthquake.usgs.gov/fdsnws/event/1/)

### Gold Layer Schema (Final Output)

The final dataset is produced in the **gold layer** after cleaning, enrichment, and transformation in Databricks. This output is ready for analytics or visualization.

```text
|-- id: string (nullable = true)
|-- longitude: double (nullable = true)
|-- latitude: double (nullable = true)
|-- elevation: double (nullable = true)
|-- title: string (nullable = true)
|-- place_description: string (nullable = true)
|-- sig: long (nullable = true)
|-- mag: double (nullable = true)
|-- magType: string (nullable = true)
|-- time: timestamp (nullable = true)
|-- updated: timestamp (nullable = true)
|-- country_code: string (nullable = true)
|-- sig_class: string (nullable = false)
```

----

## Reproducing Project

This project uses Azure Data Factory to move data into an Azure Databricks notebook (bronze layer). Follow the steps below to set up the environment.

---

### 1. **Create a Free Azure Account**

If you don’t already have one, start by creating a free Azure account:

🔗 [https://azure.microsoft.com/free](https://azure.microsoft.com/free)

You’ll get free credits and access to services like Data Factory and Databricks.

---

### 2. **Create a Resource Group**

A resource group organizes related Azure resources.

- In Azure Portal, search for **"Resource groups"**
- Click **Create**
- Choose a region and give it a name (e.g., `data-project-rg`)

---

### 3. **Set Up Azure Data Factory (ADF)**

- In Azure Portal, search **“Data Factory”**
- Click **Create**
- Select your resource group, choose a name (e.g., `datafactory-dev`), and location
- Leave version as **V2**
- Once deployed, click **Author & Monitor** to open the ADF Studio

📘 [Create Data Factory](https://learn.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-portal)

---

### 4. **Create a Databricks Workspace**

- In Azure Portal, search **“Databricks”**
- Click **Create**
- Fill in:
  - Workspace name (e.g., `databricks-dev`)
  - Same region as ADF
  - Pricing tier: Standard
- After deployment, click **Launch Workspace** to open the Databricks UI

📘 [Create Azure Databricks Workspace](https://learn.microsoft.com/en-us/azure/databricks/getting-started/)

---

### 5. **Create a Databricks Cluster & Notebook**

Inside the Databricks workspace:

- Go to **Compute** → Click **Create Cluster**
  - Runtime: 11.3 LTS or higher
  - Keep other settings default for development
- Go to **Workspace** → Your username → Click **New > Notebook**
  - Name: `bronze_ingest`
  - Language: Python or PySpark

---

### 6. **Generate Access Token (for ADF to Databricks)**

- In Databricks, click your profile (top-right) → **User Settings**
- Go to **Access Tokens** → **Generate New Token**
- Copy the token (save it securely)

📘 [Access Tokens in Databricks](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth#--personal-access-tokens)

---

### 7. **Connect ADF to Databricks**

Back in ADF Studio:

- Go to **Manage > Linked Services > + New**
- Choose **Azure Databricks**
- Fill in:
  - Workspace URL (from Azure portal)
  - Access token
  - Cluster ID (found in Databricks > Compute > cluster > URL)

Now ADF can run your Databricks notebooks as activities in a pipeline.

📘 [Run Databricks from ADF](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-using-databricks-notebook)

---

✅ You’re all set. Next, create your pipeline in ADF and point it to the Databricks notebook to run your bronze-layer ingestion logic.
