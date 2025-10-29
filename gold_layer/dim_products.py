# Introduce sirrogate key to the SCD Type-1 o track incremental loads
#Import dependencies

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read data
df_products = spark.sql("SELECT * FROM market_catalog.silver.products")

# Switch between initial and subsequent loads
initial_load = 0

# Divide new and existing records
if initial_load == 0:
    # For initial load
    df_products_old = spark.sql('''SELECT product_id, product_key, created_at, updated_at 
                                FROM market_catalog.gold.products''')

else:
     # For subsequent loads
    df_products_old = spark.sql('''SELECT 0 AS product_id, 0 AS product_key, 0 AS created_at, 0 AS updated_at 
                                FROM market_catalog.silver.products 
                                WHERE 1=0''')

#Rename columns for exiting records
df_products_old = df_products_old.withColumnRenamed("product_key","prev_product_key")\
    .withColumnRenamed("created_at","prev_created_at")\
    .withColumnRenamed("updated_at","prev_updated_at")\
    .withColumnRenamed("product_id","prev_product_id")

# Confirm changes
df_products_old.limit(2).display()

# Apply join with the old records
df_join = df_products.join(df_products_old, df_products['product_id'] == df_products_old['prev_product_id'], "left")

# Confirm changes
df_join.limit(3).display()

# Separaing new vs existing reords
df_products_new = df_join.filter(df_join.prev_product_id.isNull())
df_products_existing = df_join.filter(df_join.prev_product_id.isNotNull())

# Drop unwanted columns
df_products_existing = df_products_existing.drop("prev_product_id", "prev_updated_at")
# Rename prev_product_key to product_key
df_products_existing = df_products_existing.withColumnRenamed("prev_product_key","product_key")
# Rename created_at column
df_products_existing = df_products_existing.withColumnRenamed("prev_created_at","created_at")
# Type cast created_at
df_products_existing = df_products_existing.withColumn("created_at", to_timestamp(df_products_existing["created_at"]))
# Recreate updated_at
df_products_existing = df_products_existing.withColumn("updated_at", current_timestamp())

# Create surrogate key
df_products = df_products.withColumn("product_key", monotonically_increasing_id() + lit(1))

# Show the new schema going forward
df_products_existing.printSchema()

# Get maximum surrogate key
if initial_load == 1:
    max_surrogate_key = 0
else:
    df_max_surrogate_key = spark.sql("select max(product_key) as max_surrogate_key from market_catalog.gold.products")
    # Convert df_max_surrogate_key into max_sirrogate_key
    max_surrogate_key = df_max_surrogate_key.collect()[0]['max_surrogate_key']

# Add max surrogate key to new products
df_products_new = df_products_new.withColumn("product_key", col("product_key") + lit(max_surrogate_key))

# Union of df_products_new & df_products_existing
df_products_data = df_products_new.unionByName(df_products_existing)

if spark.catalog.tableExists("market_catalog.gold.products"):
    #when product_key is matched, update all columns
    #when product_key is not matched, insert all columns
    delta_pbj = DeltaTable.forPath(spark, "abfss://gold@marketstorage1111.dfs.core.windows.net/dim_products")
    delta_pbj.alias("t").merge(df_products_data.alias("s"), "t.product_key = s.product_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    
else:
    df_products_data.write.mode("overwrite").\
        format("delta")\
            .option("path","abfss://gold@marketstorage1111.dfs.core.windows.net/dim_products")\
                .saveAsTable("market_catalog.gold.products")

#optimize table
# spark.sql("OPTIMIZE market_catalog.gold.products ZORDER BY (product_key)")
    
