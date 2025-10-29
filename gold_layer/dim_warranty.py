# Create a delta live table for the warranty data

# Import dependencies

import dlt
from pyspark.sql.functions import *

# Define data quality check
data_expections = {
    "rule1": "claim_id IS NOT NULL"
}

# Define a delta live table to stream from the silver layer to have warranty data staged
@dlt.table(
    comment="Stage data from silver warranty as a streaming table"
)

# Apply data quality check
@dlt.expect_all_or_drop(data_expections)

# Continuously stream data from the silver layer table residing in "market_catalog.silver.warranty"
def dim_warranty_stage():
    return spark.readStream.table("market_catalog.silver.warranty")

# Define a DLT view to prepare the staged data for loading into the warranty dimension table 
@dlt.view(
    comment="View to prepare warranty data for SCD type 1 load"
)

# Read the streaming data from the staging table
def dim_warranty_view():
    return dlt.read_stream("dim_warranty_stage")

# Read the streaming data from the staged warranty table
dlt.create_streaming_table("dim_warranty")

# Apply SCD Type-1
dlt.apply_changes(
    target="dim_warranty",
    source="dim_warranty_view",
    keys=["claim_id"],
    sequence_by=col("claim_date"),
    stored_as_scd_type=1,
)
