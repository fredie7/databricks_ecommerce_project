# Prepare the fact table using slowly chsnging dimension - Type 1

# Import dependencies
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import DeltaTable

# Preview products table
%sql
SELECT * FROM market_catalog.gold.products;

# Preview stores table
%sql
SELECT * FROM market_catalog.gold.dim_stores;

# Preview sales data
%sql
SELECT * FROM market_catalog.silver.sales;

# Preview warranty data
%sql
SELECT * FROM market_catalog.silver.warranty;

# Preview waranty data
%sql
SELECT * FROM market_catalog.silver.category;

# Prepare fact_sale table
# Load the tables
df_sales = spark.table("market_catalog.silver.sales")
df_products = spark.table("market_catalog.gold.products")
df_category = spark.table("market_catalog.gold.dim_category")
df_warranty = spark.table("market_catalog.gold.dim_warranty")
df_stores = spark.table("market_catalog.gold.dim_stores")

# Perform the joins
df_fact_sales = (
    df_sales.alias("s")
    .join(df_products.alias("p"), col("s.product_id") == col("p.product_id"), "left")
    .join(df_category.alias("c"), col("p.category_id") == col("c.category_id"), "left")
    .join(df_warranty.alias("w"), col("s.sale_id") == col("w.sale_id"), "left")
    .join(df_stores.alias("st"), col("s.store_id") == col("st.store_id"), "left")
    .select(
        col("s.sale_id").alias("sale_id"),
        col("p.product_key").alias("product_key"),
        col("s.store_id").alias("store_id"),
        col("p.category_id").alias("category_id"),
        col("w.claim_id").alias("claim_id"),
        col("s.sale_date").alias("sale_date"),
        round(col("p.price"),2).alias("price"),
        col("s.quantity").alias("quantity"),
        round(col("p.price") * col("s.quantity"), 2).alias("revenue")
    )
)

# Write the table to delta format and merge the data into the table colection on the catalog if it exists
# Otherwise, create a delta live table in data lake and save one to the catalog
if spark.catalog.tableExists("market_catalog.gold.fact__sales"):
    delta_obj = DeltaTable.forName(spark, "market_catalog.gold.fact__sales")
    delta_obj.alias("t")\
        .merge(
            df_fact_sales.alias("s"),
            "t.sale_id = s.sale_id AND t.product_key = s.product_key"
        )\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()
else:
    df_fact_sales.write.format("delta")\
        .option("path","abfss://gold@marketstorage1111.dfs.core.windows.net/face__sales")\
            .saveAsTable("market_catalog.gold.fact__sales")
