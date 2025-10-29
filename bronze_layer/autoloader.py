
# Define the widgets to select the file name dynamically 
dbutils.widgets.text("file_name","products")

# Read the file name
source_file_name = dbutils.widgets.get("file_name")

# Read-stream the products data
df_products = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","csv")\
        .option("cloudFiles.schemaLocation",f"abfss://bronze@marketstorage1111.dfs.core.windows.net/checkpoint_{source_file_name}")\
            .load(f"abfss://source@marketstorage1111.dfs.core.windows.net/{source_file_name}")

# Write the data to the bronze layer as a stream
df_products.writeStream.format("parquet")\
    .outputMode("append")\
        .option("checkpointLocation", f"abfss://bronze@marketstorage1111.dfs.core.windows.net/checkpoint_{source_file_name}")\
            .option("path",f"abfss://bronze@marketstorage1111.dfs.core.windows.net/{source_file_name}")\
                .trigger(once=True)\
                    .start()
