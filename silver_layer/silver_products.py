# Inport dependencies
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read products data
df_products = spark.read.format("parquet")\
    .load("abfss://bronze@marketstorage1111.dfs.core.windows.net/products")

# Peek on the table
df_products.limit(3).display()

# Utility to count null values
def count_nulls(data):
    null_counts = data.select([count(when(isnull(c), c)).alias(c) for c in data.columns])
    return null_counts

# Count null values
count_nulls(df_products).display()

# Utility function to check duplicates
def check_duplcates(data, id):
    dup_counts = data.groupBy(id).count().filter("count > 1")
    return dup_counts


# Utility function to revert column names into lowercase characters
def to_lower_case(data):
    cols = [c.lower() for c in data.columns]
    return data.toDF(*cols)

# Convert column names to lowercase 
df_products = to_lower_case(df_products)

# Peek at athe table to confirm changes
df_products.limit(2).display()

# Drop undesirable _rescued_data
df_products = df_products.drop("_rescued_data")

# Derive the product's lunch year
df_products = df_products.withColumn("year", year("launch_date"))

#Confirm table's new outlook
df_products.limit(2).display()

# Separate the product's name from the version in 2 different columns
df_products = df_products \
    .withColumn(
        "product_name_flag",
        regexp_replace(col("product_name"), r"\s*\(.*?\)", "")
    ) \
    .withColumn(
        "product_version",
        regexp_extract(col("product_name"), r'\((.*?)\)', 1)
    )


# Replace the empty spaces in the product_version columns with single version since it's just 1
df_products = df_products.withColumn(
    "product_version",
    when(
        (col("product_version").isNull()) | (col("product_version") == ""), 
        "single version"
    ).otherwise(col("product_version"))
)

# Peek at the table to confirm changes
df_products.limit(2).display()

# Tweak launch_date data type
df_products = df_products.withColumn("launch_date", to_date(col("launch_date")))

# Cast produt price to hint
df_products = df_products.withColumn("price",col("price").cast("int"))

# Drop original product name column
df_products = df_products.drop("product_name")

# Rename product_name_flag column to product_name
df_products = df_products.withColumnRenamed("product_name_flag","product_name")

# Peek at the table to confirm changes
df_products.limit(2).display()

# Write the data to delta
df_products.write.format("delta").mode("overwrite") \
    .save("abfss://silver@marketstorage1111.dfs.core.windows.net/products")
# Check for duplicates
check_duplcates(df_products, 'Product_ID').display()
