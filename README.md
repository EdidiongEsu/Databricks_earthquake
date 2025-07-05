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

This project uses Azure Data Factory to move data through Azure Databricks notebooks (bronze, silver, gold layers). Follow the steps below to set up the environment.

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
  - Pricing tier: Standard. There is a trial tier that is limited for 14 days also
- After deployment, click **Launch Workspace** to open the Databricks UI

📘 [Create Azure Databricks Workspace](https://learn.microsoft.com/en-us/azure/databricks/getting-started/)

---

### 5. **Create a Databricks Cluster & Notebook**

Inside the Databricks workspace:

- Go to **Compute** → Click **Create Cluster**
  - Runtime: 11.3 LTS for mInimal Configuration. Choose cluster to turn off after 30 mins of inactivity to save cost.
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

### 8. 🧪 Configure Earthquake Pipeline in ADF Using Databricks Notebooks

After setting up the Azure environment and connecting to Databricks, the next step is to configure the earthquake pipeline using a series of Databricks notebooks that follow the **medallion architecture**: Bronze → Silver → Gold.

Each notebook runs as an activity in the ADF pipeline, and parameters are passed between them dynamically.

---

#### 📁 ADF Pipeline Overview

The ADF pipeline orchestrates the daily execution of the Databricks notebooks. Below is the structure of the pipeline:

![ADF Pipeline](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/img/adf_pipeline.PNG)

#### 🟫 Bronze Notebook

The [**Bronze Notebook**](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/code/Databricks%20notebooks/Bronze%20Notebook.ipynb) ingests raw earthquake data and stores it in json format in the Bronze layer of the lakehouse.

##### 🔧 Base Parameters

The notebook takes two date parameters — `start_date` and `end_date` — which are dynamically generated in ADF using the following expressions:

```json
{
  "start_date": "@formatDateTime(addDays(utcNow(), -1), 'yyyy-MM-dd')",
  "end_date": "@formatDateTime(utcNow(), 'yyyy-MM-dd')"
}
```

![Bronze Base Paramters](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/img/base_parameters.PNG)

At the end of this step, the raw earthquake data is stored in JSON format in the Bronze layer of Azure Data Lake Gen2, ready for further processing in Databricks.

---

#### ⚙️ Silver Notebook

The [**Silver Notebook**](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/code/Databricks%20notebooks/Silver%20Notebook.ipynb) notebook processes the JSON data from the Bronze layer by flattening nested fields (like coordinates and properties), renaming columns, and handling missing values.

##### 🔧 Base Parameters
```json
{
  "bronze_params": "@string(activity('Bronze Notebook').output.runOutput)"
}
```
The cleaned and flattened data is saved in parquet format in the Silver layer of Azure Data Lake Gen2, making it ready for enrichment.

---

#### 🏅 Gold Notebook

The [**Gold Notebook**](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/code/Databricks%20notebooks/Gold%20Notebook.ipynb) enriches the Silver data by assigning a `country_code` to each earthquake event using reverse geocoding, and classifies each event's significance (`sig`) as **Low**, **Moderate**, or **High** to support downstream analytics.

##### 🔧 Base Parameters
```json
{
  "bronze_params": "@string(activity('Bronze Notebook').output.runOutput)",
  "silver_params": "@string(activity('Silver Notebook').output.runOutput)"
}
```
The enriched dataset, now with country codes and significance classifications, is stored in Delta format in the Gold layer and made available to Microsoft Fabric Lakehouse for downstream analytics,


---

#### Triggers
A daily Trigger is created and time set to run the pipeline daily. This is after debug run has been done and everything confirmed to work fine
![Pipeline runs](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/img/pipelineruns.PNG)

---
### 9. 🏞️ Moving Data to Microsoft Fabric Lakehouse

After enrichment in the Gold notebook including the addition of fields like `country_code, The output files are passed into a **Fabrics Notebook** for final processing

The [**Fabrics Notebook**](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/code/Fabrics%20Notebook/Merge%20Data.ipynb) performs the following:
- Merges partitioned output files
- Applies any final transformations (e.g., standardizing `country_name`)
- Outputs a clean, unified **Delta table** in the **Microsoft Fabric Lakehouse**

This structured table serves as the final dataset for reporting and analytics.

Key benefits:
- Stored in Delta format within the OneLake architecture  
- Optimized for low-latency querying  
- Natively available to Power BI through Direct Lake mode

---
## Dashboard
### Visualizing in Power BI

The data in the Fabric Lakehouse is visualized using **Power BI** to deliver insights on seismic activity trends, frequency, significance distribution, and geographic patterns.

![Combined Power BI Visualization](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/img/Combined_Viz2.PNG)

Dashboards include:
- Top Countries with Earthquakes
- Magnitude distribution charts and the Earthquake with the most impact
- Geographic mapping of earthquake events
- Severity class breakdown (Low / Moderate / High) and lots more!

#### Looking Closer:
The pipeline has been paused to save costs, so the Visualization link is currently unavailable for public consumption.

![Page 1 Power BI Visualization](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/img/Viz2_Page1.PNG)

![Page 2 Power BI Visualization](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/img/Viz2_Page2.PNG)


## Contact
Please reach out to me on [LinkedIn](https://www.linkedin.com/in/edidiong-esu-1942a2129/) for thoughts and/or issues encountered during reproduction of project. Let's chat! :star:.

**Happy Coding!** :computer:


