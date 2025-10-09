# Import dependencies
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Read the data from the bronze layer
data = spark.read.format("parquet").load("abfss://bronze@databricksstorageukk.dfs.core.windows.net/customers")

# Drop the _rescued_data column which had helped prevent schema mismatch in the datalake
data = data.drop("_rescued_data")

# Get the full name from the first and last name, and drop the first and last name
data = data.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
data = data.drop("first_name", "last_name","domain")
data.display()

# Write the data to the silver layer
data.write.format("delta").mode("overwrite").save("abfss://silver@databricksstorageukk.dfs.core.windows.net/customers")

# Create the table within databricks catalog on the silver layer
%sql
CREATE TABLE IF NOT EXISTS databricks_catalog.silver.customers
USING DELTA
LOCATION "abfss://silver@databricksstorageukk.dfs.core.windows.net/customers"

