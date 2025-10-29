# Create a delta live table for the category data

# Import dependencies

import dlt
from pyspark.sql.functions import *

# Define quality check
data_expections = {
    "rule1": "category_id IS NOT NULL"
}

# Define DLT to stream from silver_category layer
@dlt.table(
    comment="Stage data from silver category as a streaming table"
)

# Apply data testing
@dlt.expect_all_or_drop(data_expections)

# Stream data continuously from the market_catalog_silver_category
def dim_category_stage():
    return spark.readStream.table("market_catalog.silver.category")

# Define a DLT view to prepare the staged data for loading
@dlt.view(
    comment="View to prepare category data for SCD type 1 load"
)

# Read the streaming data from the staging category table
def dim_category_view():
    return dlt.read_stream("dim_category_stage")

# Read the streaming data from the now staged category table
dlt.create_streaming_table("dim_category")

# Apply SCD - Type 1
dlt.apply_changes(
    target="dim_category", # The target table
    source="dim_category_view", # Source
    keys=["category_id"], # Primary key
    sequence_by=col("category_id"), # Track changes by the id since date feature was not available
    stored_as_scd_type=1, # Register upsert
)
