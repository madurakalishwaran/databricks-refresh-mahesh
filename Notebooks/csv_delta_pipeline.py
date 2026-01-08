from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import uuid
import datetime

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

bronze_df = spark.read.table("delta_bronze")


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

temp_silver_df = spark.read.table("delta_silver_valid")
delta_facts_sales_object = DeltaTable.forName(spark, "delta_facts_sales")

if not spark.catalog.tableExists("delta_facts_sales"):
    
    print("Table not exists. Creating one...")
    (
        temp_silver_df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("last_updated_timestamp", F.lit(None).cast("timestamp"))
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable("delta_facts_sales")
    )

    print("Number of records in the table: ", temp_silver_df.count())

else:
    print("Table exists. processing updated and new records..")

    change_condition = """
        NOT (t.Shipdate <=> s.Shipdate) OR 
        NOT (t.Amount <=> s.Amount) OR 
        NOT (t.Boxes <=> s.Boxes)
    """

    all_columns = temp_silver_df.columns
    base_map = {c: F.col(f"s.{c}") for c in all_columns}
    
    update_map = base_map.copy()
    update_map['last_updated_timestamp'] = F.current_timestamp()

    insert_map = base_map.copy()
    insert_map['ingestion_timestamp'] = F.current_timestamp()

    delta_merge = (
        delta_facts_sales_object.alias("t")
        .merge(temp_silver_df.alias("s"), "t.ShipmentID = s.ShipmentID")
        .whenMatchedUpdate(condition=change_condition, set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )

table_name = "pipeline_run_metrics"
schema = StructType(
    [
        StructField("run_id", StringType(), nullable=False),
        StructField("run_timestamp", TimestampType(), nullable=False),
        StructField("bronze_row_count", LongType(), nullable=False),
        StructField("silver_valid_count", LongType(), nullable=False),
        StructField("silver_quarantine_count", LongType(), nullable=False),
        StructField("fact_rows_inserted", LongType(), nullable=False),
        StructField("fact_rows_updated", LongType(), nullable=False),
        StructField("run_status", StringType(), nullable=False)
    ]
)

if not spark.catalog.tableExists(table_name):
    
    table_df = spark.createDataFrame([], schema=schema)    
    table_df.write.mode("overwrite").saveAsTable(table_name)

run_id = str(uuid.uuid4())
run_status = "success"
run_timestamp = datetime.datetime.now()

# default values

delta_bronze_count = 0
silver_valid_count = 0
silver_quarantine_count = 0
rows_inserted = 0
rows_updated = 0

try:
    
    delta_bronze_count = bronze_df.count()
    silver_valid_count = silver_valid_df.count()
    silver_quarantine_count = silver_invalid_df.count()

    latest_history = delta_facts_sales_object.history(1).collect()[0]
    metrics = latest_history["operationMetrics"]
    rows_updated = int(metrics.get("numTargetRowsUpdated", 0))
    rows_inserted = int(metrics.get("numTargetRowsInserted", 0))

except Exception as e:
    
    run_status = "failure"
    print(f"Pipeline failed with error: {e}")
    raise e

finally:

    final_stats = [
        (
            run_id,
            run_timestamp,
            delta_bronze_count,
            silver_valid_count,
            silver_quarantine_count,
            rows_inserted,
            rows_updated,
            run_status
        )
    ]

    spark.createDataFrame(final_stats, schema=schema).write.mode("append").saveAsTable(table_name)

df= spark.read.table("pipeline_status_table")
df.show()