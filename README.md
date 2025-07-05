# End-to-End Earthquake Data Pipeline with Azure, Databricks & Power BI
## still building
## Navigation / Quick Access
Quickly move to the section you are interested in by clicking on the appropriate link:
- [Overview](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#overview)
- [Project Objective](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#project-objective)
- [Project Architecture](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#project-architecture)
- [Dataset](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#dataset)
- [Technologies](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#technologies)
- [Reproducing Project](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#reproducing-project) (long section)
- [Dashboard](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#dashboard)

---
## Overview
This project extracts, moves, and analyzes USGS's [United States Geological Survey]([https://ride.capitalbikeshare.com](https://www.usgs.gov/programs/earthquake-hazards)) data.
The USGS monitors and reports on earthquakes, assesses earthquake impacts and hazards, and conducts targeted research on the causes and effects of earthquakes.



---





-----
## Project Architecture
![Alt text](https://github.com/EdidiongEsu/Databricks_earthquake/blob/main/img/Earthquake%20architecture.gif)

The data architecture is an overview of the end-to-end pipeline which include:
- Ingesting of source dataset to Azure Datalake using Azure Data Factory
- 
- Moving data and staging in a dataware house which is big query
- transforming the data using dbt via dbt cloud
- Creation of dashboard with Power BI



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
