# Create a parameter for the intended parquet file to be process
dbutils.widgets.text("file_name","")
# Read the file
item_file_name = dbutils.widgets.get("file_name")
# STream-read the file
data = spark.readStream.format('cloudFiles') \
    .option('cloudFiles.format', 'parquet') \
    .option('cloudFiles.schemaLocation', f'abfss://bronze@databricksstorageukk.dfs.core.windows.net/checkpoint_{item_file_name}') \
    .load(f'abfss://source@databricksstorageukk.dfs.core.windows.net/{item_file_name}')

# Stream-Write the file to the bronze layer
data.writeStream.format("parquet") \
    .outputMode("append") \
    .option("checkpointLocation", f"abfss://bronze@databricksstorageukk.dfs.core.windows.net/checkpoint_{item_file_name}") \
    .option("path", f"abfss://bronze@databricksstorageukk.dfs.core.windows.net/{item_file_name}") \
    .trigger(once=True) \
    .start()


