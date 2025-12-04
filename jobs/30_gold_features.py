"""Gold features: HLL-style approx uniques + CDF-based top-pct labeling -> Delta.
Run with:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/30_gold_features.py
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import functions as F
from libs.session import build_spark
from libs.metrics import label_top_pct
from libs.config import load_logging_config

load_dotenv("conf/.env")
load_logging_config()

SILVER  = os.getenv("SILVER_DIR", "data/silver")
GOLD    = os.getenv("GOLD_DIR", "data/gold")
BRONZE_DELTA = os.getenv("BRONZE_DELTA", "data/bronze/delta")
TOP_PCT = float(os.getenv("TOP_PCT", "0.9"))

spark = build_spark("gold_features_snippet")

# 변경: Silver에서 approx_count_distinct(author_id)로 uniq_authors_est 생성
#       Gold에서는 Silver의 uniq_authors_est를 그대로 사용
metrics = spark.read.format("delta").load(f"{SILVER}/social_metrics")

has_uniq = "uniq_authors_est" in metrics.columns

# Engagement: sum windowed counts per video
eng = metrics.groupBy("video_id").agg(F.sum("count").alias("engagement_24h"))

# Uniques: prefer silver column; else recompute from bronze; else null
if has_uniq:
    uniq = metrics.groupBy("video_id").agg(F.max("uniq_authors_est").alias("uniq_authors_est"))
elif Path(BRONZE_DELTA).exists():
    bronze = spark.read.format("delta").load(BRONZE_DELTA)
    uniq_key = "author_id" if "author_id" in bronze.columns else "post_id"
    uniq = bronze.groupBy("video_id").agg(F.approx_count_distinct(uniq_key).alias("uniq_authors_est"))
else:
    uniq = eng.select("video_id").withColumn("uniq_authors_est", F.lit(None).cast("bigint"))

features = eng.join(uniq, "video_id", "left")

# 삽입 포인트 E: CDF-based cutoff for top_pct engagement label
labeled, cut = label_top_pct(features, "engagement_24h", TOP_PCT, label_col="label")

(labeled.write.format("delta").mode("overwrite").save(f"{GOLD}/features"))
print("Gold features written. P{:.0f} cut = {}".format(TOP_PCT*100, cut))
