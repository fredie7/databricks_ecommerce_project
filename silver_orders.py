# Import required dependencies
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Read the data from the bronze layer
data = spark.read.format('parquet').load('abfss://bronze@databricksstorageukk.dfs.core.windows.net/orders')
# Drop the _rescued_data column
data = data.drop("_rescued_data")
display(data)

# Refactor the order_date column to a timestamp
data = data.withColumn("order_date", to_timestamp(col("order_date")))

# Extract the year from the order_date column
data = data.withColumn("year", year(col("order_date")))
display(data)

# Wrte the transformed data to a Delta table
data.write.format("delta").mode("overwrite").save("abfss://silver@databricksstorageukk.dfs.core.windows.net/orders")

#Create a table in the silver layer of the databricks_catalog
%sql
CREATE TABLE IF NOT EXISTS databricks_catalog.silver.orders
USING DELTA
LOCATION "abfss://silver@databricksstorageukk.dfs.core.windows.net/orders"

