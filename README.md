# End-to-End Earthquake Data Pipeline with Azure, Databricks & Power BI

## Navigation / Quick Access
Quickly move to the section you are interested in by clicking on the appropriate link:
- [Overview](https://github.com/EdidiongEsu/capital_bikeshare/tree/main#overview)
- [Project Objective](https://github.com/EdidiongEsu/capital_bikeshare/tree/main#project-objective)
- [Project Architecture](https://github.com/EdidiongEsu/Databricks_earthquake/tree/main#project-architecture)

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
