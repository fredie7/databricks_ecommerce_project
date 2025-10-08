from pyspark.sql import functions as F

# Define table and storage paths
orders_table = "databricks_catalog.silver.orders"
din_customers = "databricks_catalog.gold.dim__customers"
dim_products = "databricks_catalog.gold.dim__products"
fact_table = "databricks_catalog.gold.fact__orders"
datalake_path = "abfss://gold@databricksstorageukk.dfs.core.windows.net/fact__orders"

# Load data
orders_data = spark.table(orders_table)
# Filter for current customers
customers_data = spark.table(din_customers).filter(F.col("is_current") == True)
products_data = spark.table(dim_products)

# Join orders with dimension tables
fact_orders_data = (
    orders_data
    .join(customers_data, on="customer_id", how="left")
    .join(products_data, on="product_id", how="left")
    .select(
        F.col("order_id"),
        F.col("surrogate_id").alias("customer_key"),
        F.col("product_id"),
        F.col("order_date"),
        F.col("quantity"),
        F.col("total_amount"),
        F.col("price"),
        F.round(F.col("quantity") * F.col("discounted_price"), 2).alias("discounted_total"),
        F.col("year")
    )
)

# Save to datalake
fact_orders_data.write.format("delta").mode("overwrite").save(datalake_path)

# Create a table on the datalake
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {fact_table}
    USING DELTA
    LOCATION '{datalake_path}'
    """)
