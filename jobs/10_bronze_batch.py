"""Bronze batch ingest: landing JSON -> Delta with Bloom prefilter and reservoir sampling.

- Maintains fixed schema + ingest_date/source partitions
- Captures per-video reservoir sample to `data/bronze/_sample`
- Applies Bloom filter using last 7 days of bronze data to drop obvious duplicates

Run with:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/10_bronze_batch.py
"""
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

LANDING = os.getenv("LANDING_DIR", "data/landing")
BRONZE_BASE = os.getenv("BRONZE_DIR", "data/bronze")
BRONZE_DELTA = os.path.join(BRONZE_BASE, "delta")
BRONZE_SAMPLE = os.path.join(BRONZE_BASE, "_sample")
SAMPLE_SIZE = int(os.getenv("RESERVOIR_K", "50"))

spark = (
    SparkSession.builder.appName("bronze_batch")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", "warehouse")
    .getOrCreate()
)

schema = T.StructType(
    [
        T.StructField("post_id", T.StringType(), True),
        T.StructField("video_id", T.StringType(), True),
        T.StructField("author_id", T.StringType(), True),
        T.StructField("text", T.StringType(), True),
        T.StructField("lang", T.StringType(), True),
        T.StructField("ts", T.LongType(), True),
    ]
)

raw_df = spark.read.schema(schema).json(LANDING)

incoming = (
    raw_df.select("post_id", "video_id", "author_id", "text", "lang", "ts")
    .withColumn("ingest_date", F.current_date())
    .withColumn("source", F.lit("mock_api"))
)

# Reservoir sampling per video_id (pre-Bloom) for quick profiling
rand_col = F.rand()
window = Window.partitionBy("video_id").orderBy(rand_col)
reservoir_sample = (
    incoming.withColumn("_rand", rand_col)
    .withColumn("_rn", F.row_number().over(window))
    .filter(F.col("_rn") <= SAMPLE_SIZE)
    .drop("_rand", "_rn")
)

(reservoir_sample.write.mode("append").partitionBy("ingest_date").json(BRONZE_SAMPLE))

# Bloom filter built from the last 7 days of bronze data
bf = None
if Path(BRONZE_DELTA).exists():
    try:
        recent_ids = (
            spark.read.format("delta")
            .load(BRONZE_DELTA)
            .where(F.col("ingest_date") >= F.date_sub(F.current_date(), 7))
            .select("post_id")
            .na.drop()
        )
        bf_row = recent_ids.agg(F.expr("bloom_filter(post_id, 100000, 0.01) as bf")).first()
        bf = bf_row["bf"] if bf_row else None
    except Exception:
        bf = None

if bf is None:
    filtered = incoming
else:
    bf_df = spark.createDataFrame([(bf,)], schema="bf BINARY")
    filtered = (
        incoming.crossJoin(bf_df)
        .filter(~F.expr("might_contain(bf, post_id)"))
        .drop("bf")
    )

(
    filtered.write.format("delta")
    .mode("append")
    .partitionBy("ingest_date", "source")
    .save(BRONZE_DELTA)
)

print("Bronze appended. Rows:", filtered.count())