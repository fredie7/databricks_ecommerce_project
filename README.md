## E-Commerce Analytics Project Featuring Databricks Unity Catalog, Autoloader, Azure Datalake Storage, Delta Tables, Data Streaming, Slowly Changing Dimension & Pipeline Workflow.

First, an Azure Resource Group is created and configured to include both the Data Lake Storage and the Azure Databricks workspace. Next, the datasets in Parquet format are loaded into the Azure Data Lake.

To ensure data governance, the project begins by creating a Unity Catalog metastore, which enables data lineage tracking and auditing. After that, the Databricks workspace is assigned to the newly created metastore.

For future automated data loading, Databricks Autoloader is implemented to incrementally load data files from Azure Data Lake Storage into the bronze layer.

The layers:
Here’s a running pipeline of the entire analytics process

<div align="center">
  <img src="https://github.com/fredie7/databricks_ecommerce_project/blob/main/pipeline.png?raw=true" />
  <br>
   <sub><b>Fig 1.</b> Workflow</sub>
</div>
