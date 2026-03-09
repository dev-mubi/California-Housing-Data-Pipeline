# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Analytics Tables
# MAGIC This notebook creates 5 gold tables for dashboard insights.

# COMMAND ----------

# =========================================
# PATH CONFIGURATION
# =========================================

base_path = "/Volumes/workspace/default/spark1/Medallion Architecture with Data Pre Processing"

silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"

print("Silver Path:", silver_path)
print("Gold Path:", gold_path)

# COMMAND ----------

# =========================================
# LOAD SILVER DATA
# =========================================

from pyspark.sql.functions import avg, count, round, when, col

silver_df = spark.read.parquet(silver_path)

print("Silver data loaded.")
silver_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 1: House Price by Ocean Proximity

# COMMAND ----------

# =========================================
# GOLD TABLE 1: HOUSE PRICE BY OCEAN PROXIMITY
# =========================================

gold_ocean_proximity = (
    silver_df
    .groupBy("ocean_proximity")
    .agg(
        round(avg("median_house_value"), 2).alias("avg_house_price"),
        round(avg("median_income"), 2).alias("avg_income"),
        count("id").alias("house_count")
    )
    .orderBy("avg_house_price")
)

gold_ocean_proximity.write.mode("overwrite").parquet(f"{gold_path}/house_price_by_ocean")

print("Gold Table 1 created: house_price_by_ocean")
gold_ocean_proximity.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 2: Income vs House Price

# COMMAND ----------

# =========================================
# GOLD TABLE 2: INCOME VS HOUSE PRICE
# =========================================

income_bucketed = silver_df.withColumn(
    "income_bucket",
    when(col("median_income") < 2, "0-2")
    .when(col("median_income") < 4, "2-4")
    .when(col("median_income") < 6, "4-6")
    .when(col("median_income") < 8, "6-8")
    .otherwise("8+")
)

gold_income_price = (
    income_bucketed
    .groupBy("income_bucket")
    .agg(
        round(avg("median_house_value"), 2).alias("avg_house_price"),
        count("id").alias("house_count")
    )
    .orderBy("income_bucket")
)

gold_income_price.write.mode("overwrite").parquet(f"{gold_path}/income_vs_house_price")

print("Gold Table 2 created: income_vs_house_price")
gold_income_price.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 3: Population Density vs Price

# COMMAND ----------

# =========================================
# GOLD TABLE 3: POPULATION DENSITY VS PRICE
# =========================================

population_bucketed = silver_df.withColumn(
    "population_bucket",
    when(col("population") < 500, "0-500")
    .when(col("population") < 1000, "500-1000")
    .when(col("population") < 2000, "1000-2000")
    .when(col("population") < 5000, "2000-5000")
    .otherwise("5000+")
)

gold_population_analysis = (
    population_bucketed
    .groupBy("population_bucket")
    .agg(
        round(avg("median_house_value"), 2).alias("avg_house_price"),
        round(avg("total_rooms"), 2).alias("avg_rooms"),
        count("id").alias("house_count")
    )
    .orderBy("population_bucket")
)

gold_population_analysis.write.mode("overwrite").parquet(f"{gold_path}/population_analysis")

print("Gold Table 3 created: population_analysis")
gold_population_analysis.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 4: Housing Age Impact

# COMMAND ----------

# =========================================
# GOLD TABLE 4: HOUSING AGE IMPACT
# =========================================

age_bucketed = silver_df.withColumn(
    "age_bucket",
    when(col("housing_median_age") < 10, "0-10")
    .when(col("housing_median_age") < 20, "10-20")
    .when(col("housing_median_age") < 30, "20-30")
    .when(col("housing_median_age") < 40, "30-40")
    .otherwise("40+")
)

gold_house_age_analysis = (
    age_bucketed
    .groupBy("age_bucket")
    .agg(
        round(avg("median_house_value"), 2).alias("avg_house_price"),
        count("id").alias("house_count")
    )
    .orderBy("age_bucket")
)

gold_house_age_analysis.write.mode("overwrite").parquet(f"{gold_path}/house_age_analysis")

print("Gold Table 4 created: house_age_analysis")
gold_house_age_analysis.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table 5: Geographic Price Distribution

# COMMAND ----------

# =========================================
# GOLD TABLE 5: GEOGRAPHIC PRICE DISTRIBUTION
# =========================================

from pyspark.sql.functions import col

gold_geo_price_distribution = (
    silver_df
    .select(
        col("longitude"),
        col("latitude"),
        col("median_house_value"),
        col("population")
    )
    .filter(
        (col("longitude").isNotNull()) &
        (col("latitude").isNotNull()) &
        (col("median_house_value").isNotNull()) &
        (col("population").isNotNull())
    )
)

# Write to Gold layer
gold_geo_price_distribution.write \
    .mode("overwrite") \
    .parquet(f"{gold_path}/geo_price_distribution")

print("Gold Table 5 created: geo_price_distribution")

gold_geo_price_distribution.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: All Gold Tables Created

# COMMAND ----------

print("\n========================================")
print("GOLD LAYER COMPLETE")
print("========================================")
print("✓ Gold Table 1: house_price_by_ocean")
print("✓ Gold Table 2: income_vs_house_price")
print("✓ Gold Table 3: population_analysis")
print("✓ Gold Table 4: house_age_analysis")
print("✓ Gold Table 5: geo_price_distribution")
print("========================================")