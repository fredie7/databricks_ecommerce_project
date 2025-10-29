# Import dependencies
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read sales data
df_sales = spark.read.format("parquet")\
    .load("abfss://bronze@marketstorage1111.dfs.core.windows.net/sales")

# Peek at the data
df_sales.limit(10).display()

#Convert the quantity column to an integer type
df_sales = df_sales.withColumn("quantity", col("quantity").cast("integer"))

# Convert sale_date into date datatype
df_sales = df_sales.withColumn("sale_date", to_date(col("sale_date"), "dd-MM-yyyy"))

df_sales.limit(2).display()

# Check for null values
df_sales.select([count(when(col(c).isNull(), c)).alias(c) for c in df_sales.columns])

# Utility function to check duplicates
def check_duplcates(data, id):
    duplicate_counts = data.groupBy(id).count().filter("count > 1")
    return duplicate_counts

# Check for duplicates
check_duplcates(df_sales, 'sale_id').display()

# Drop _rescued_data
df_sales = df_sales.drop('_rescued_data')

# Preview data
df_sales.limit(10).display()

# Write to delta
df_sales.write.format("delta")\
    .mode("overwrite")\
        .save("abfss://silver@marketstorage1111.dfs.core.windows.net/sales")
