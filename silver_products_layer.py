# Import dependencies
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read data from bronze layer
data = spark.read.format("parquet") \
    .load("abfss://bronze@databricksstorageukk.dfs.core.windows.net/products")

# Drop rescued data column
data = data.drop("_rescued_data")

# Create a reusable function to calculate discounted price
%sql
CREATE OR REPLACE FUNCTION databricks_catalog.bronze.discount_price(price_parm double) 
RETURNS double
LANGUAGE SQL 
RETURN ROUND(price_parm * 0.95,2)

# Call the function to calculate discounted price
data = data.withColumn('discounted_price', expr("databricks_catalog.bronze.discount_price(price)"))

# Create a Delta table in the silver layer
data.write.format("delta") \
    .mode("overwrite") \
        .option("path", "abfss://silver@databricksstorageukk.dfs.core.windows.net/products") \
            .save()
# Create a table in the silver layer of the unity catalog
%sql
CREATE TABLE IF NOT EXISTS databricks_catalog.silver.products
USING DELTA
LOCATION "abfss://silver@databricksstorageukk.dfs.core.windows.net/products"

