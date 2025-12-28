# Databricks notebook source
from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Incremental_Learning").getOrCreate()

# --- STEP 1: BRONZE LAYER ---
# We stamp the data the moment it is read from the CSV
file_path = "/Volumes/workspace/default/my_datas/sample-chocolate-sales-data-all.csv"

csv_df = (spark.read
                .format("csv")
                .option("header", "true")
                .load(file_path)
                .withColumn("ingestion_timestamp", F.current_timestamp()) # Stamp here!
)

# IMPORTANT: We use APPEND so we don't overwrite the timestamps of old records
(
    csv_df.write
    .format("delta")
    .mode("append") 
    .option("mergeSchema", "true")
    .saveAsTable("delta_bronze")
)

# --- STEP 2: SILVER LAYER (TRANSFORMATIONS) ---
delta_temp_silver = spark.read.table("delta_bronze")

# We only process data that hasn't been cleaned yet (Optional, but good practice)
# For this exercise, we process the whole bronze but append to silver
delta_temp_silver_cast = delta_temp_silver.withColumns({
    "Shipdate": F.to_date("Shipdate", "dd-MMM-yy"),
    "Amount": F.col("Amount").cast("double"),
    "Boxes": F.col("Boxes").cast("integer")
})

# Validation Logic
invalid_shipdate = F.col("Shipdate").isNull()
invalid_amount = (F.col("Amount") <= 0) | F.col("Amount").isNull() | F.isnan("Amount")
invalid_boxes = (F.col("Boxes") < 0) | F.col("Boxes").isNull() | F.isnan("Boxes")
invalid_list = invalid_amount | invalid_boxes | invalid_shipdate

delta_temp_silver_is_record = delta_temp_silver_cast.withColumn("is_valid_record", ~(invalid_list))

# Filter Valid Records
# We use APPEND here as well to keep the historical timestamps intact
delta_silver_valid = delta_temp_silver_is_record.filter("is_valid_record")
delta_silver_valid.write.format("delta").mode("overwrite").saveAsTable("delta_silver_valid_temp") 

# --- STEP 3: FACT LAYER (INCREMENTAL LOAD) ---
target_table = "delta_facts_sales"

if not spark.catalog.tableExists(target_table):
    print("🚀 Table does not exist. Performing initial load...")
    delta_silver_valid.write.format("delta").mode("overwrite").saveAsTable(target_table)
    print(f"Initial Count: {spark.table(target_table).count()}")

else:
    print("🔄 Table exists. Checking for new data...")
    
    # 1. Find the latest timestamp already in the Fact table
    max_timestamp = spark.table(target_table).select(F.max("ingestion_timestamp")).collect()[0][0]
    print(f"Last ingestion was at: {max_timestamp}")

    # 2. Filter the Silver Valid data for only rows NEWER than our max_timestamp
    # This prevents the doubling!
    new_records = delta_silver_valid.filter(F.col("ingestion_timestamp") > max_timestamp)
    
    new_count = new_records.count()
    
    if new_count > 0:
        new_records.write.format("delta").mode("append").saveAsTable(target_table)
        print(f"✅ Successfully appended {new_count} new records.")
    else:
        print("⏸️ No new records found (Watermark filtered all rows).")

# Final verification
print("Total records in delta_facts_sales: ", spark.table(target_table).count())