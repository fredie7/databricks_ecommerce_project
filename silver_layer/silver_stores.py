# Import dependencies
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read the stores data
df_stores = spark.read.format("parquet")\
    .load("abfss://bronze@marketstorage1111.dfs.core.windows.net/stores")

# Peek at the stores data
df_stores.limit(50).display()

# Helper function to remove white spaces across columns
def remove_white_spaces(data):
    for column in data.columns:
        data = data.withColumn(column, trim(col(column)))
    return data

# Remove white spaces across columns
df_stores = remove_white_spaces(df_stores)

# Utility function to revert column names into lowercase characters
def to_lower_case(data):
    cols = [c.lower() for c in data.columns]
    return data.toDF(*cols)

# Revert column names to lower case
df_stores = to_lower_case(df_stores)

# Peek at changes
df_stores.limit(2).display()

# Utility to count null values
def count_nulls(data):
    null_counts = data.select([count(when(isnull(c), c)).alias(c) for c in data.columns])
    return null_counts
# Count null values
count_nulls(df_stores).display()

# Utility function to check duplicates
def check_duplcates(data, id):
    d_counts = data.groupBy(id).count().filter("count > 1")
    return d_counts
  
# Check for duplicates
check_duplcates(df_stores, 'store_id').display()

# Drop _rescued_data
df_stores = df_stores.drop('_rescued_data')

# Peek at changes
df_stores.limit(2).display()

# Write to delta
df_stores.write.format("delta")\
    .mode("overwrite")\
        .save("abfss://silver@marketstorage1111.dfs.core.windows.net/stores")
