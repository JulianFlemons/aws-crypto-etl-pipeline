import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType
)

# ==============================================================================
# JOB SETUP
# ==============================================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ==============================================================================
# UPDATED SCHEMA (FLATTENED)
# Fixed: Removed nested StructTypes because the source data is likely flat.
# ==============================================================================
json_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("current_price", DoubleType(), True),   # Changed to flat Double
    StructField("market_cap", DoubleType(), True),      # Changed to flat Double
    StructField("total_volume", DoubleType(), True)     # Changed to flat Double
])

# ==============================================================================
# EXTRACT (S3 RAW)
# ==============================================================================
raw_df = spark.read \
    .schema(json_schema) \
    .json("s3://julian-crypto-s3-bucket/raw/")

# ==============================================================================
# TRANSFORM (FLATTENED MAPPING)
# ==============================================================================
flattened_df = raw_df.select(
    F.col("id").alias("coin_id"),
    F.col("symbol").alias("coin_symbol"),
    F.col("current_price").alias("price_usd"),        # Mapping directly to flat field
    F.col("market_cap").alias("market_cap_usd"),      # Mapping directly to flat field
    F.col("total_volume").alias("volume_24h_usd"),    # Mapping directly to flat field
    F.col("ingestion_timestamp").alias("source_timestamp")
)

# ==============================================================================
# CLEAN + CAST
# ==============================================================================
final_df = flattened_df.select(
    F.col("coin_id"),
    F.col("coin_symbol"),
    F.col("price_usd").cast(DoubleType()),
    F.col("market_cap_usd").cast(DoubleType()),
    F.col("volume_24h_usd").cast(DoubleType()),
    F.to_timestamp("source_timestamp").alias("source_timestamp"),
    F.current_timestamp().alias("load_date")
).filter(F.col("coin_id").isNotNull())


print("Sample of processed data:")
final_df.show(5)

# ==============================================================================
# LOAD (S3 PROCESSED ZONE)
# ==============================================================================
final_df.write \
    .mode("overwrite") \
    .parquet("s3://julian-crypto-s3-bucket/processed/fact_market_data/")

job.commit()
