### A simulation od Apple's E-Commerce Analytics Project Featuring Databricks Unity Catalog, Autoloader, Medallion Architectural Azure Datalake Storage, Delta Tables, Data Streaming, Slowly Changing Dimension & Pipeline Workflow.

First, an Azure Resource Group is created and configured to include both the Data Lake Storage and the Azure Databricks workspace. Next, the datasets are loaded into the Azure Data Lake.

#### PROJECT ARCHITECTURE
<div align="center">
  <img src="https://github.com/fredie7/databricks_ecommerce_project/blob/main/images/Screenshot%20(5510).png?raw=true" />
  <br>
   <sub><b>Fig 1.</b> Workflow</sub>
</div>

To ensure data governance, the project begins by creating a Unity Catalog metastore, which enables data lineage tracking and auditing. After that, the Databricks workspace is assigned to the newly created metastore.

#### PIPELINE WORKFLOW:
For future automated data loading, Databricks Autoloader is implemented to incrementally load data files from Azure Data Lake Storage into the bronze layer.
<table align="center">
  <tr>
    <td align="center">
      <img src="https://github.com/fredie7/databricks_ecommerce_project/blob/main/images/incremental_pipeline.png?raw=true" height="300"><br>
      <sub><b></b> Bronze & Sliver Incremental Pipeline</sub>
    </td>
    <td align="center">
      <img src="https://github.com/fredie7/databricks_ecommerce_project/blob/main/images/fact_tables_pipeline.png?raw=true" height="300"><br>
      <sub><b></b> Fact tables Pipeline</sub>
    </td>
  </tr>
</table>


#### VISUALIZATION

<div align="center">
  <img src="https://github.com/fredie7/databricks_ecommerce_project/blob/main/images/visualization.png?raw=truee" />
  <br>
   <sub><b>Fig 1.</b> Workflow</sub>
</div>
