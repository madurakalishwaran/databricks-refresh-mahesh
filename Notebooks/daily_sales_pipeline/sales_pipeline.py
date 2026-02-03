from pyspark.sql import SparkSession, DataFrame
from typing import Tuple, Dict, Any
from pyspark.sql.functions import col
import re
from delta import DeltaTable
import uuid
import datetime
from sales_pipeline_config import PipelineConfig

cfg = PipelineConfig()
spark = SparkSession.builder.appName("Sales").getOrCreate()

def processed_file_rows() -> Dict[str, Any]:
    filename_df = spark.read.table("sales_processed_files")
    processed_rows = {row.file_name: row.processed_timestamp for row in filename_df.select("file_name", "processed_timestamp").collect()}
    return processed_rows

def create_run_metadata(file_name: str) -> Dict[str, Any]:
    run_metadata = {
        "run_id": str(uuid.uuid4()),
        "run_timestamp": datetime.datetime.now(),
        "bronze_row_count": 0,
        "silver_valid_count": 0,
        "silver_quarantine_count": 0,
        "fact_rows_inserted": 0,
        "fact_rows_updated": 0,
        "run_status": "success",
        "file_name": file_name
    }

    return run_metadata

def process_single_file(file_name: str, last_modified_time: datetime) -> None:

    # ================= RUN INITIALIZATION =================
    run_metadata = create_run_metadata(file_name)
            
    print("New/Updated file found: ", file_name)
    print("Processing file: ", file_name)

    try:
        
        SOURCE_PATH = cfg.dir_path + file_name + ".csv"
        # ================= BRONZE INGESTION =================
        bronze_df, run_metadata =  ingest_csv_bronze(SOURCE_PATH, run_metadata)
        
        # ================= SILVER TRANSFORMATION =================
        silver_df, run_metadata = transform_silver(bronze_df, run_metadata)
        print("Total_rows: ", run_metadata["bronze_row_count"])
        print("Valid_rows: ", run_metadata["silver_valid_count"])
        print("Invalid_rows: ", run_metadata["silver_quarantine_count"])

        # ================= FACT MERGE =================
        run_metadata = incremental_loads(silver_df, run_metadata)
        print("new_rows: ", run_metadata["fact_rows_inserted"])
        print("rows_updated: ", run_metadata["fact_rows_updated"])
        
        # ================= FILE HISTORY UPDATE =================
        update_processed_history(file_name, last_modified_time)
        print("Processed files list:")
        spark.sql("SELECT * FROM sales_processed_files").show()

        run_metadata["run_status"] = "success"

    except Exception as e:
        run_metadata["run_status"] = "failure"
        print(f"Error processing {file_name}: {e}")

    finally:
        try:
            print("Attempting to log pipeline metrics...")
            # ================= METRICS LOGGING =================
            write_pipeline_metrics(run_metadata)

        except Exception as e:
            print(f"CRITICAL WARNING: Metrics logging failed! Error: {e}")

def ingest_csv_bronze(SOURCE_PATH: str, run_metadata: Dict[str, Any]) -> Tuple[DataFrame, Dict[str, Any]]:
    bronze_df = spark.read.format("csv").option("header", "true").load(SOURCE_PATH)
    
    # clean column names - using Regex
    bronze_df = bronze_df.select([
        col(c)
        .alias(re.sub(r'[^a-zA-Z0-9]+', '_', c)
            .lower()
            .strip('_')) 
        for c in bronze_df.columns
    ])

    bronze_df.write.format('delta').mode("overwrite").saveAsTable(cfg.bronze_table)
    bronze_df = spark.read.table(cfg.bronze_table)
    run_metadata["bronze_row_count"] = spark.table(cfg.bronze_table).count()
    return bronze_df, run_metadata

def transform_silver(bronze_df: DataFrame, run_metadata: Dict[str, Any]) -> Tuple[DataFrame, Dict[str, Any]]:
    bronze_df = (
        bronze_df
        .withColumns(
            {
                "order_id": col('order_id').cast('long'),
                "units_sold": col('units_sold').cast('int'),
                "unit_price": col('unit_price').cast('double') 
            }
        )
    )

    # Business conditions to filter rows
    invalid_condition = (
        (col('order_id').isNull()) | 
        (col('units_sold') <= 0) | 
        (col('unit_price') <= 0)
    )

    # Retrieve valid & invalid rows based on Business conditions
    silver_df = bronze_df.filter(~invalid_condition)
    quarantine_df = bronze_df.filter(invalid_condition)

    # Write Valid & Invlaid rows to delta
    silver_df.write.format('delta').mode("overwrite").saveAsTable(cfg.silver_table)
    quarantine_df.write.format('delta').mode("overwrite").saveAsTable(cfg.quarantine_table)

    # Get Silver_valid and Silver_invalid counts
    run_metadata["silver_valid_count"]  = spark.table(cfg.silver_table).count()
    run_metadata["silver_quarantine_count"] = spark.table(cfg.quarantine_table).count()

    return silver_df, run_metadata

def incremental_loads(silver_df: DataFrame, run_metadata: Dict[str, Any]) -> Dict[str, Any]:

    # condtion for incremental load
    change_condition = """
        NOT (t.units_sold <=> s.units_sold) OR
        NOT (t.unit_price <=> s.unit_price)
    """
    # Condition for merge
    merge_condition = "t.order_id = s.order_id"

    # Check if table exists an create one and overwrite with silver valid data
    if not spark.catalog.tableExists(cfg.fact_table):
        print("Fact table does not exist. Creating one..")
        silver_df.write.format('delta').mode("overwrite").saveAsTable(cfg.fact_table)
        fact_table_count = spark.table(cfg.fact_table).count()
        print(f"Total No of rows in fact table: {fact_table_count}")

    # If table exists, merge the upsert data
    else:
        print("Table exists. Upserting rows..")

        # Create delta table object for sales_fact
        fact_df = DeltaTable.forName(spark, cfg.fact_table)
        fact_df.alias("t").merge(
            silver_df.alias("s"), 
            merge_condition
            ).whenMatchedUpdateAll(
                condition=change_condition
                ).whenNotMatchedInsertAll().execute()
            
        latest_history = fact_df.history(1).collect()[0]
        metrics = latest_history["operationMetrics"]
        run_metadata["fact_rows_inserted"] = int(metrics.get("numTargetRowsInserted", 0))
        run_metadata["fact_rows_updated"] = int(metrics.get("numTargetRowsUpdated", 0))
    
    return run_metadata

def update_processed_history(file_name: str, last_modified_time: datetime) -> None:
    target_schema = spark.table("sales_processed_files").schema
    
    write_list = [file_name, last_modified_time]
    filename_df = spark.createDataFrame([write_list], schema=target_schema)
    delta_df = DeltaTable.forName(spark, "sales_processed_files")
    delta_df.alias("t").merge(
        filename_df.alias("s"), 
        "t.file_name = s.file_name"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def write_pipeline_metrics(metrics_data) -> None:
        
        target_schema = spark.table('sales_pipeline_metrics').schema
        metrics_df = spark.createDataFrame([metrics_data], schema=target_schema)
        metrics_df.write.format('delta').mode("append").saveAsTable('sales_pipeline_metrics')
        print("Metrics appended successfully")

def main() -> None:
    
    all_files = dbutils.fs.ls(cfg.dir_path)
    task_to_run = []
    # ================= FILE DISCOVERY =================
    processed_rows = processed_file_rows()
    
    for l in all_files:
        file_name = l.name.removesuffix(".csv")
        last_modified_time = datetime.datetime.fromtimestamp(l.modificationTime / 1000)

        is_new = file_name not in processed_rows
        is_updated = False
        if not is_new:
            is_updated = last_modified_time > processed_rows[file_name]
       
        if is_new or is_updated:
            task_to_run.append(
                {
                    "name": file_name,
                    "mtime": last_modified_time
                }
            )
    if not task_to_run:
        print("No new files found")
        return
    
    for task in task_to_run:
            process_single_file(task["name"], task["mtime"])

if __name__ == "__main__":
    main()            
    
    
