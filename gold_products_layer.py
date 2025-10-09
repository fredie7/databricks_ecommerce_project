# Import dependencies
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Define the Silver layer's source and the datalake path repectively
silver_table = "databricks_catalog.silver.products"
datalake_path = "abfss://gold@databricksstorageukk.dfs.core.windows.net/dim_products"
gold_table = "databricks_catalog.gold.dim__products"


#  Read as stream from Silver layer but ignore non-data change commits
retrieved_data = spark.readStream.option('skipChangeCommits', True).table(silver_table)

# Filter the data of null values and add timestamp
cleaned_data = (
    retrieved_data.filter(col("product_id").isNotNull() & col("product_name").isNotNull())
      .withColumn("processed_at", current_timestamp())
)

#  Stream-write the data to Delta gold layer
(
    cleaned_data.writeStream
    .format("delta")
    .option("checkpointLocation", f"{datalake_path}/products")
    .outputMode("append")            
    .trigger(availableNow=True)        
    .start(datalake_path)
    .awaitTermination()                
)

# Register a table on top of the deltalake in the catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {gold_table}
    USING DELTA
    LOCATION '{datalake_path}'
""")

