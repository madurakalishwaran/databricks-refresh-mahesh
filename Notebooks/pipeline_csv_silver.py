# Databricks notebook source
from pyspark.sql import SparkSession, functions as F


spark = SparkSession.builder.appName("pipeline -- csv_silver_table").getOrCreate()

file_path = "/Volumes/workspace/default/my_datas/chocolate_sales.csv"

# Read csv
csv_df = (spark
          .read
          .format("csv")
          .option("header", "true")
          .load(file_path)
)

# Write to delta_bronze
(
    csv_df.write
    .format("delta")
    .mode("overwrite") 
    .saveAsTable("delta_bronze")
)

# Read from delta_bronze
bronze_df = spark.read.table("delta_bronze")
delta_bronze_count = bronze_df.count()

# Data Casting
temp_siver_df = bronze_df.withColumns({
    "Shipdate": F.to_date("Shipdate", "dd-MMM-yy"),
    "Amount": F.col("Amount").cast("double"),
    "Boxes": F.col("Boxes").cast("integer")
})

# Define invalid record conditions
invalid_conditions = (
    (F.col("Amount").isNull() | (F.col("Amount") < 0) | F.isnan("Amount")) |
    (F.col("Boxes").isNull() | (F.col("Boxes") < 0) | F.isnan("Boxes")) |
    F.col("Shipdate").isNull()
)

# Create a temporary DataFrame with validity flag
temp_silver_is_record_df = temp_siver_df.withColumn("is_valid_record", ~(invalid_conditions))

# Filter Valid Records
silver_valid_df = temp_silver_is_record_df.filter("is_valid_record").drop("is_valid_record")
silver_valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("delta_silver_valid") 

# Filter Invalid Records
silver_invalid_df = temp_silver_is_record_df.filter("NOT is_valid_record").drop("is_valid_record")
silver_invalid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("delta_quarantine") 

silver_valid_count = silver_valid_df.count()
silver_quarantine_count = silver_invalid_df.count()

print(f"bronze count: {delta_bronze_count}")
print(f"silver valid count: {silver_valid_count}")
print(f"silver quarantine count: {silver_quarantine_count}")