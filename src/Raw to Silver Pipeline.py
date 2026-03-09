# Databricks notebook source
# =========================================
# PATH CONFIGURATION
# =========================================

base_path = "/Volumes/workspace/default/spark1/Medallion Architecture with Data Pre Processing"

raw_path = f"{base_path}/Raw"
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"
metadata_path = f"{base_path}/metadata"

print("Raw Path:", raw_path)
print("Bronze Path:", bronze_path)
print("Silver Path:", silver_path)
print("Metadata Path:", metadata_path)

# COMMAND ----------

# =========================================
# CHECK IF RAW FILES EXIST
# =========================================

raw_files = dbutils.fs.ls(raw_path)

if len(raw_files) == 0:
    print("No raw files found. Skipping Bronze ingestion.")
    raw_data_available = False
else:
    print(f"{len(raw_files)} raw files found.")
    raw_data_available = True

# COMMAND ----------

# =========================================
# RAW → BRONZE INGESTION (SAFE)
# =========================================

from pyspark.sql.functions import current_timestamp

if raw_data_available:

    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(raw_path)
    )

    bronze_df = raw_df.withColumn("arrival_ts", current_timestamp())

    bronze_df.write.mode("append").parquet(bronze_path)

    print("Bronze ingestion completed.")

else:
    print("Bronze ingestion skipped.")

# COMMAND ----------

# =========================================
# SAFE METADATA CHECK
# =========================================

try:
    metadata_files = dbutils.fs.ls(metadata_path)
    metadata_available = len(metadata_files) > 0
except:
    metadata_available = False

print("Metadata exists:", metadata_available)

# COMMAND ----------

# =========================================
# GET LAST WATERMARK
# =========================================

if metadata_available:
    metadata_df = spark.read.parquet(metadata_path)
    last_watermark = metadata_df.first()["last_watermark"]
    print("Last Watermark:", last_watermark)
else:
    last_watermark = None
    print("First run detected.")

# COMMAND ----------

# =========================================
# CHECK IF BRONZE EXISTS
# =========================================

try:
    bronze_files = dbutils.fs.ls(bronze_path)
    bronze_available = len(bronze_files) > 0
except:
    bronze_available = False

print("Bronze exists:", bronze_available)

# COMMAND ----------

# =========================================
# READ BRONZE SAFELY
# =========================================

from pyspark.sql.functions import col

if bronze_available:
    bronze_df = spark.read.parquet(bronze_path)
    print("Bronze loaded.")
else:
    print("Bronze does not exist yet. Pipeline exiting.")
    dbutils.notebook.exit("No Bronze Data")

# COMMAND ----------

# =========================================
# APPLY INCREMENTAL FILTER
# =========================================

if last_watermark:
    bronze_incremental_df = bronze_df.filter(
        col("arrival_ts") > last_watermark
    )
else:
    bronze_incremental_df = bronze_df

display(bronze_incremental_df)

# COMMAND ----------

# =========================================
# CAST TO SILVER SCHEMA
# =========================================

from pyspark.sql.functions import expr

silver_cast_df = bronze_incremental_df.select(
    expr("try_cast(id as int)").alias("id"),
    expr("try_cast(longitude as double)").alias("longitude"),
    expr("try_cast(latitude as double)").alias("latitude"),
    expr("try_cast(housing_median_age as int)").alias("housing_median_age"),
    expr("try_cast(total_rooms as int)").alias("total_rooms"),
    expr("try_cast(total_bedrooms as int)").alias("total_bedrooms"),
    expr("try_cast(population as int)").alias("population"),
    expr("try_cast(households as int)").alias("households"),
    expr("try_cast(median_income as double)").alias("median_income"),
    expr("try_cast(median_house_value as int)").alias("median_house_value"),
    col("ocean_proximity"),
    col("arrival_ts")
)

display(silver_cast_df)

# COMMAND ----------

# =========================================
# FILTER INVALID IDs
# =========================================

silver_valid_df = silver_cast_df.filter(
    col("id").isNotNull()
)

display(silver_valid_df)

# COMMAND ----------

# =========================================
# KEEP LATEST RECORD PER ID (DETERMINISTIC)
# =========================================

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("id").orderBy(col("arrival_ts").desc())

silver_batch_df = (
    silver_valid_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

display(silver_batch_df)

# COMMAND ----------

# =========================================
# UPSERT INTO SILVER (LATEST WINS)
# =========================================

if not metadata_available:

    silver_batch_df.write.mode("overwrite").parquet(silver_path)
    print("First run: Silver created.")

else:

    existing_silver_df = spark.read.parquet(silver_path)

    combined_df = existing_silver_df.unionByName(silver_batch_df)

    window_spec = Window.partitionBy("id").orderBy(col("arrival_ts").desc())

    silver_updated_df = (
        combined_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    silver_updated_df.write.mode("overwrite").parquet(silver_path)

    print("Incremental run: Silver updated.")

# COMMAND ----------

# =========================================
# UPDATE WATERMARK
# =========================================

from pyspark.sql.functions import max as spark_max

if bronze_incremental_df.take(1):

    new_watermark = bronze_incremental_df.select(
        spark_max("arrival_ts")
    ).first()[0]

    spark.createDataFrame(
        [(new_watermark,)],
        ["last_watermark"]
    ).write.mode("overwrite").parquet(metadata_path)

    print("Watermark updated to:", new_watermark)

else:
    print("No new data. Watermark unchanged.")

# COMMAND ----------

# =========================================
# ORCHESTRATION: TRIGGER GOLD LAYER
# =========================================

try:
    print("\n========================================")
    print("SILVER COMPLETE - STARTING GOLD LAYER")
    print("========================================\n")
    
    gold_notebook_path = "/Workspace/Users/fa23-bcs-065@cuiatd.edu.pk/DE P1/v3_gold_layer"
    
    dbutils.notebook.run(gold_notebook_path, timeout_seconds=3600)
    
    print("\n========================================")
    print("PIPELINE COMPLETE: SILVER → GOLD")
    print("========================================")
    
except Exception as e:
    print(f"Gold layer execution failed: {str(e)}")
    raise