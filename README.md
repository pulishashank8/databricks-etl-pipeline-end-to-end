#  Databricks End-to-End ETL Pipeline Project
This project demonstrates a production-grade ETL pipeline on Azure Databricks, leveraging Delta Lake for structured storage across Bronze, Silver, and Gold layers. The pipeline handles both batch and streaming data, applies Slowly Changing Dimensions (SCD) Type 1 & 2, and is orchestrated using Databricks Workflows.
# Description:
Implemented an end-to-end ETL pipeline on Azure Databricks using Delta Lake (Bronze, Silver, Gold). Built parameterized ingestion, data cleansing, and SCD Type 1 & 2 transformations. Orchestrated with Databricks Workflows, producing Dim/Fact tables for analytics.

# Key techniques used:

Delta Lake Storage: ACID-compliant, schema enforcement, optimized reads/writes
Multi-Layer Architecture: Bronze (raw), Silver (cleansed), Gold (analytics-ready)
SCD Type 1 & 2: Supports both overwrite and historical tracking of dimensional data
Delta Live Tables (DLT): Handles incremental batch & streaming ingestion
Automated Orchestration: Databricks Workflows ensure seamless Bronze → Silver → Gold flow

##  Pipeline Overview

The pipeline consists of three key layers: Bronze, Silver, and Gold. Each layer is designed for a specific stage in the data processing pipeline.

1. Data Ingestion (Bronze Layer)
   
-Ingests raw data (e.g., orders, customers, products) from Azure Data Lake using parameterized workflows for flexibility.
-Stores ingested data in Delta tables for initial processing.
-Makes raw datasets available in the Delta Lake catalog for downstream use

2. Data Transformation (Silver Layer)
   
   - The Silver layer involves the cleaning and transformation of raw data from the Bronze layer.
   - Common transformations include data cleansing (removal of nulls, duplicates, etc.) and filtering.
   - Cleaned data is stored in Delta tables within the Silver layer, and these tables are cataloged for further use.
   - The Silver layer prepares data for more complex transformations in the Gold layer.

3. Advanced Transformations & SCD (Gold Layer)
Applies Slowly Changing Dimensions (SCD) logic:
Type 1: Overwrites outdated records with the latest values.
Type 2: Preserves historical records with versioning for change tracking.
Builds analytics-ready dimensional and fact tables (e.g., DimCustomer, DimProduct, FactOrders).
Prepares curated data for reporting, dashboards, and advanced analytics.

**Key Technologies & Techniques Used**

 **Azure Databricks:** Used as the orchestration engine for building the pipeline and managing the Databricks Jobs.

 **Delta Lake:** For storing data in an optimized, ACID-compliant format. Ensures data consistency and provides support for schema evolution and enforcement.

 **Delta Live Tables (DLT)**: Managed batch and streaming data pipelines with automatic handling of incremental data processing.

 **SCD Type 1 & Type 2:** Used for handling changes in dimensional data over time:

- SCD Type 1 for overwriting outdated records with the latest version.

- SCD Type 2 for maintaining historical records when attributes change.

## 🔁 ETL Pipeline Flow

1. **Bronze Layer**  
   - Ingest raw `orders`, `customers`, and `products` data into Delta tables.

2. **Silver Layer**  
   - Clean, transform, and normalize the data.
   - Handle nulls, types, and dates.

3. **Gold Layer**  
   - Create:
     - `DimCustomer` with surrogate keys
     - `DimProduct` with validation logic
     - `FactOrders` by joining Silver orders with Dim tables

4. **Workflow**  
   - Orchestrated in Databricks Workflows to automate Bronze → Gold flow.

---

## Project Structure


databricks-etl-pipeline-project/                                                                                                                                               
├── notebooks/      # All .py notebooks for each ETL layer (bronze, silver, gold)                                                                                              
├── data_sample/    # Sample datasets used for local or testing purposes                                                                                                        
├── sql/            # SQL scripts for table creation (optional)                                                                                                                
├── pipeline/       # Workflow screenshots & architecture diagrams                                                                                                             
└── README.md       # Project overview and documentation

## Technologies Used

- Azure Databricks (Community Edition compatible)
- PySpark (DataFrame API)
- Delta Lake
- Azure Data Lake Storage (optional)
- Databricks Workflows

---

##  How to Run

1. Upload the `notebooks/` to your Databricks Workspace.
2. Upload CSVs (if needed) to your DBFS or mount to Azure.
3. Create and configure your Workflow using `pipeline/workflow_screenshot.png` as reference.
4. Run from **Bronze → Silver → Gold** notebooks.
5. Check output Delta tables using Catalog Explorer.

---

##  Final Output

- `databricks_cata.gold.DimCustomer`
- `databricks_cata.gold.DimProducts`
- `databricks_cata.gold.FactOrders`

Each table is queryable via Delta Lake with clean, deduplicated, and surrogate-key-joined records.

  Passionate about Data Engineering & Analytics

##  Future Enhancements

- Integrate with Power BI or Looker for dashboard visualization
- Add unit testing for transformation logic using `pytest`
- Add CI/CD deployment using GitHub Actions for notebook promotion
- Incorporate data quality checks using expectations in Delta Live Tables
- Use Azure Data Factory to trigger pipelines via REST API
- Execute the full pipeline on a paid subscription to avoid cluster quota limits


