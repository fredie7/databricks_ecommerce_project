# Import the dependencies
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read the region data from the bronze layer of the catalog
data = spark.read.table("databricks_catalog.bronze.regions")

# Drop the _rescued_data column
data = data.drop("_rescued_data")

# Write the data to the silver layer of the datalake
data.write.format("delta") \
    .mode("overwrite") \
        .save("abfss://silver@databricksstorageukk.dfs.core.windows.net/regions")

# Create a table in the silver layer of the catalog
%sql
CREATE TABLE IF NOT EXISTS databricks_catalog.silver.regions
USING DELTA
LOCATION "abfss://silver@databricksstorageukk.dfs.core.windows.net/regions"

