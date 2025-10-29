# Import dependencies
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Read category data
df_category = spark.read.format("parquet")\
    .load("abfss://bronze@marketstorage1111.dfs.core.windows.net/category")

# Peek at the category data
df_category.display()

# Rid category name of possible leading or trailing spaces
df_category = df_category.withColumn("category_name", trim(col("category_name")))

# Utility function to check duplicates
def check_duplcates(data, id):
    duplicate_counts = data.groupBy(id).count().filter("count > 1")
    return duplicate_counts

# Check for duplicates
check_duplcates(df_category, "category_id").show()

# Drop _rescued_data column
df_category = df_category.drop("_rescued_data")

# Confirm changes
df_category.display()

# Write to delta
df_category.write.format("delta")\
    .mode("overwrite")\
        .save("abfss://silver@marketstorage1111.dfs.core.windows.net/category")

