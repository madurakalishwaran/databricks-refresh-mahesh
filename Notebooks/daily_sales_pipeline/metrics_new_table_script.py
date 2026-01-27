from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("new_metrics_table").getOrCreate()

'''

tables_to_drop = ["sales_bronze", "sales_silver", "sales_quarantine", "sales_fact", "sales_pipeline_metrics"]

for t in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

if spark.catalog.tableExists("sales_pipeline_metrics"):
    print("Table exists.")
else:
    print("Table is not created.")'''








