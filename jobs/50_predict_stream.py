"""Prediction micro-batch appender:
- Loads gold/features
- Computes a simple score (toy) and appends to gold/predictions with timestamp.
"""
import os
from dotenv import load_dotenv
from pyspark.sql import functions as F
from libs.session import build_spark

load_dotenv("conf/.env")
GOLD = os.getenv("GOLD_DIR", "data/gold")

spark = build_spark("predict_append")

features = spark.read.format("delta").load(f"{GOLD}/features")

# Toy scoring: weighted sum of engagement + uniques, normalized
pred = (
    features
    .withColumn("score_raw", F.col("engagement_24h") + 0.1 * F.col("uniq_authors_est"))
    .withColumn("score", F.col("score_raw") / (F.col("score_raw") + F.lit(1)))
    .withColumn("event_time", F.current_timestamp())
    .select("video_id", "score", "event_time")
)

(
    pred.write.format("delta")
    .mode("append")
    .save(f"{GOLD}/predictions")
)
print("Appended predictions (toy).")
