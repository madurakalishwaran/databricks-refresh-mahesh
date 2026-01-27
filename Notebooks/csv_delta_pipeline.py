from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import uuid
import datetime

# variable declaration

spark = SparkSession.builder.appName("pipeline -- csv_silver_table").getOrCreate()
source_path = "/Volumes/workspace/default/my_datas/chocolate_sales.csv"

bronze_df = None
silver_valid_df = None
silver_quarantine_df = None

run_id = str(uuid.uuid4())
run_status = "success"
run_timestamp = datetime.datetime.now()
bronze_row_count = 0
silver_valid_count = 0
silver_quarantine_count = 0
fact_rows_inserted = 0
fact_rows_updated = 0

metrics_table = "pipeline_run_metrics"
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

def ingest_csv_to_bronze(spark, source_path: str, bronze_table: str):
    
    """
    Reads source CSV snapshot and writes to Bronze Delta table.
    Returns bronze_df and bronze_row_count.
    """

    csv_df = (spark
          .read
          .format("csv")
          .option("header", "true")
          .load(source_path)
    )

    (
    csv_df.write
    .format("delta")
    .mode("overwrite") 
    .saveAsTable(bronze_table)
    )
    
    bronze_df = spark.read.table(bronze_table)
    bronze_row_count = bronze_df.count()

    return bronze_df, bronze_row_count

def apply_silver_quality_rules(bronze_df):
    """
    Applies business quality rules on Bronze data.
    Splits records into Silver valid and quarantine datasets.
    Returns both DataFrames and their row counts.
    """

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

    temp_silver_is_record_df = temp_siver_df.withColumn("is_valid_record", ~(invalid_conditions))

    # Filter Valid Records
    silver_valid_df = temp_silver_is_record_df.filter("is_valid_record").drop("is_valid_record")
    silver_valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("delta_silver_valid") 

    # Filter Invalid Records
    silver_quarantine_df = temp_silver_is_record_df.filter("NOT is_valid_record").drop("is_valid_record")
    silver_quarantine_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("delta_quarantine")

    silver_valid_count = silver_valid_df.count()
    silver_quarantine_count = silver_quarantine_df.count()

    return silver_valid_df, silver_quarantine_df, silver_valid_count, silver_quarantine_count

def merge_into_fact_table(silver_valid_df, fact_table: str):
    """
    Performs Delta MERGE into fact table using ShipmentID.
    Inserts new records and updates changed records.
    Returns inserted and updated row counts.
    """

    if not spark.catalog.tableExists(fact_table):
    
        print("Table not exists. Creating one...")
        (
            silver_valid_df
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("last_updated_timestamp", F.lit(None).cast("timestamp"))
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(fact_table)
        )

    else:
        print("Table exists. processing updated and new records..")

        delta_facts_sales_object = DeltaTable.forName(spark, fact_table)

        change_condition = """
            NOT (t.Shipdate <=> s.Shipdate) OR 
            NOT (t.Amount <=> s.Amount) OR 
            NOT (t.Boxes <=> s.Boxes)
        """

        all_columns = silver_valid_df.columns
        base_map = {c: F.col(f"s.{c}") for c in all_columns}
    
        update_map = base_map.copy()
        update_map['last_updated_timestamp'] = F.current_timestamp()

        insert_map = base_map.copy()
        insert_map['ingestion_timestamp'] = F.current_timestamp()

        delta_merge = (
            delta_facts_sales_object.alias("t")
            .merge(silver_valid_df.alias("s"), "t.ShipmentID = s.ShipmentID")
            .whenMatchedUpdate(condition=change_condition, set=update_map)
            .whenNotMatchedInsert(values=insert_map)
            .execute()
        )

    latest_history = delta_facts_sales_object.history(1).collect()[0]
    metrics = latest_history["operationMetrics"]
    fact_rows_updated = int(metrics.get("numTargetRowsUpdated", 0))
    fact_rows_inserted = int(metrics.get("numTargetRowsInserted", 0))

    return fact_rows_inserted, fact_rows_updated

def write_pipeline_metrics(
    run_id: str,
    bronze_row_count: int,
    silver_valid_count: int,
    silver_quarantine_count: int,
    fact_rows_inserted: int,
    fact_rows_updated: int,
    run_status: str,
    metrics_table: str
):
    """
    Persists one row of pipeline execution metrics.
    Must never fail the pipeline.
    """

    final_stats = [
            (
                run_id,
                run_timestamp,
                bronze_row_count,
                silver_valid_count,
                silver_quarantine_count,
                fact_rows_inserted,
                fact_rows_updated,
                run_status
        )
    ]
    spark.createDataFrame(final_stats, schema=schema).write.mode("append").saveAsTable(metrics_table)
    
try:
    
    bronze_df, bronze_row_count = ingest_csv_to_bronze(spark, source_path, "delta_bronze")
    silver_valid_df, silver_quarantine_df, silver_valid_count, silver_quarantine_count = apply_silver_quality_rules(bronze_df)
    fact_rows_inserted, fact_rows_updated = merge_into_fact_table(silver_valid_df, "delta_facts_sales")

except Exception as e:
    
    run_status = "failure"
    print(f"Pipeline failed with error: {e}")
    raise e

finally:

    write_pipeline_metrics(
        run_id,
        bronze_row_count,
        silver_valid_count,
        silver_quarantine_count,
        fact_rows_inserted,
        fact_rows_updated,
        run_status,
        metrics_table
    )
    
df= spark.read.table("pipeline_run_metrics")
df.orderBy(F.col("run_timestamp").desc()).display()