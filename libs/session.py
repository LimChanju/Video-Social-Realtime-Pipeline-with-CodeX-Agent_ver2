"""
Spark session helpers and configuration loading.

Usage:
    from libs.session import build_spark
    spark = build_spark("my_app")
"""
import os
from pyspark.sql import SparkSession


def build_spark(app_name: str, extra_conf: dict | None = None) -> SparkSession:
    """Create a SparkSession with Delta support and basic warehouse config."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", os.getenv("SPARK_WAREHOUSE_DIR", "warehouse"))
    )
    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)
    return builder.getOrCreate()
