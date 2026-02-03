"""
This file defines the Constant variable for sales_pipeline.py
"""
from dataclasses import dataclass

@dataclass
class PipelineConfig:
    
    dir_path: str = "/Volumes/workspace/default/my_datas/daily_sales/"
    bronze_table: str = "sales_bronze"
    silver_table: str = "sales_silver"
    quarantine_table: str = "sales_quarantine"
    fact_table: str = "sales_fact"