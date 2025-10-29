# Import dependencies
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read warranty data
df_warranty = spark.read.format("parquet")\
    .load("abfss://bronze@marketstorage1111.dfs.core.windows.net/warranty")

# Peek at the data
df_warranty.limit(56).display()

# Helper function to remove white spaces across columns
def remove_white_spaces(data):
    for column in data.columns:
        data = data.withColumn(column, trim(col(column)))
        return data
# Remove white spaces across columns
df_warranty = remove_white_spaces(df_warranty)

# Derive the claim year
df_warranty = df_warranty.withColumn("claim_year", year(col("claim_date")))

# Show table
df_warranty.limit(2).display()


# Utility to count null values
def count_nulls(data):
    null_counts = data.select([count(when(isnull(c), c)).alias(c) for c in data.columns])
    return null_counts

# Check for null values
count_nulls(df_warranty).display()

# Utility function to check duplicates
def check_duplcates(data, id):
    dup_counts = data.groupBy(id).count().filter("count > 1")
    return dup_counts
# Check for duplicates
check_duplcates(df_warranty, "claim_id").display()

# Drop _rescued_data
df_warranty = df_warranty.drop("_rescued_data")

# Preview the data
df_warranty.limit(2).display()

# Write to delta
df_warranty.write.mode("overwrite")\
    .format("delta")\
        .save("abfss://silver@marketstorage1111.dfs.core.windows.net/warranty")
