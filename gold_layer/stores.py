# Create a delta live table for the stores data

# Import dependencies
import dlt
from pyspark.sql.functions import *

# Create a collection for data quality checks
data_expections = {
    "rule1": "store_id IS NOT NULL"
}

# Define a delta live table to stream from the silver layer and stage stores data
@dlt.table(
    comment="Stage data from silver stores as a streaming table"
)

# Apply quality check
@dlt.expect_all_or_drop(data_expections)

# Continuously stream data from the silver layer table residing in "market_catalog.silver.stores"
def dim_stores_stage():
    return spark.readStream.table("market_catalog.silver.stores")

# Define a DLT view to prepare the staged data for loading into the stores dimension table 
@dlt.view(
    comment="View to prepare stores data for SCD type 1 load"
)

# Read the streaming data from the staging table
def dim_stores_view():
    return dlt.read_stream("dim_stores_stage")

# Read the streaming data from the staged stores table
dlt.create_streaming_table("dim_stores")

# Apply SCD Type-1
dlt.apply_changes(
    target="dim_stores", # Target table
    source="dim_stores_view", # Source data
    keys=["store_id"], # Primary key for unique identification
    sequence_by=col("store_id"), # Track changes
    stored_as_scd_type=1 # Register upsert
)
