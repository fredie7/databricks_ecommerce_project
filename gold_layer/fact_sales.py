# Create a delta live table for the fact_sales data
# Import dependencies

import dlt
from pyspark.sql.functions import *

# Define data quality check
data_expections = {
    "rule1": "sale_id IS NOT NULL"
}

# Define a Delta Live Table to stream from the silver layer and stage sales data
@dlt.table(
    comment="Stage data from silver sales as a streaming table"
)
# Apply data quality check
@dlt.expect_all_or_drop(data_expections)

def fact_sales_stage():
    # Load the tables to reconcile attributes in the fact table
    df_sales = spark.table("market_catalog.silver.sales")
    df_products = spark.table("market_catalog.gold.products")
    df_category = spark.table("market_catalog.gold.dim_category")
    df_warranty = spark.table("market_catalog.gold.dim_warranty")
    df_stores = spark.table("market_catalog.gold.dim_stores")

    # Join the dimension tables fom the gold layer with the fact table in the silver layer
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
            round(col("s.sale_date"),2).alias("sale_date"),
            col("p.price").alias("price"),
            col("s.quantity").alias("quantity"),
            round(col("p.price") * col("s.quantity"), 2).alias("revenue")
        )
    )
  # Return the staged data to register as a streaming table
    return df_fact_sales

# Define a DLT view to prepare the staged data for loading into the fact table  
@dlt.view(
    comment="View to prepare sales fact data for SCD type 1 load"
)

# Read the streaming data from the staging table
def fact_sales_view():
    return dlt.read_stream("fact_sales_stage")

# Create fact_sales streaming table
dlt.create_streaming_table("fact_sales")

# Apply Slowly Changing Dimension Type 1 logic to stick with upsert in the fact_sales table
dlt.apply_changes(
    target="fact_sales", # Target table to update 
    source="fact_sales_view", # Source data fro the DLT view
    keys=["sale_id"], # Primary key for unique record identificaion
    sequence_by=col("sale_date"), # Track order of changes
    stored_as_scd_type=1, # Perform upsert
)
