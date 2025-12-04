"""
Silver streaming: landing -> sliding window + watermark -> Delta sink.

Run with:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/20_silver_stream.py

Then drop JSON files into data/landing.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -----------------------------
# Environment variables
# -----------------------------

LANDING    = os.getenv("LANDING_DIR", "data/landing")
SILVER     = os.getenv("SILVER_DIR", "data/silver")

# 기존: checkpointLocation 하드코딩(chk/silver)
# 변경: checkpoint is now configurable + descriptive
# streaming job이 여러 개일 경우 checkpoint 충돌 방지 + 관리 쉬움
CHECKPOINT = os.getenv("SILVER_CHECKPOINT", "chk/silver_social_metrics")


WATERMARK  = os.getenv("WATERMARK", "10 minutes")
WIN        = os.getenv("WINDOW_SIZE", "1 hour")
SLIDE      = os.getenv("WINDOW_SLIDE", "5 minutes")

# -----------------------------
# Spark Session
# -----------------------------

spark = (
    SparkSession.builder.appName("silver_stream")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", "warehouse")
    .getOrCreate()
)

# -----------------------------
# Schema
# -----------------------------

schema = """
post_id string,
text string,
lang string,
ts long,
author_id string,
video_id string
"""

# -----------------------------
# Streaming Source
# -----------------------------

raw = (
    spark.readStream
    .format("json")
    .schema(schema)
    .load(LANDING)
)

# -----------------------------
# Cleaning
# -----------------------------

clean = (
    raw
    .filter(
        (F.col("post_id").isNotNull()) &
        (F.col("lang") == "en")
    )

    # 기존:
    # .withColumn("event_time", to_timestamp((col("ts")/1000).cast("timestamp")))
    # - cast("timestamp") 방식은 milliseconds → timestamp 변환이 정확하지 않음
    #
    # 변경: 보다 안정적인 변환 패턴 적용
    .withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("ts") / 1000)))
    #   from_unixtime(ts/1000) → UNIX seconds로 변환
    #   to_timestamp → 안정적인 timestamp 생성
    #   실무에서 쓰는 정석 패턴이며 timezone 문제도 덜함.

    # watermark 동일하지만 event_time이 정확해져서 처리 안정성 향상
    .withWatermark("event_time", WATERMARK)

    # 중복 제거 기준은 동일
    .dropDuplicates(["post_id"])
)

# -----------------------------
# Sliding Window Aggregation
# -----------------------------

win = (
    clean
    # 기존: window(col("event_time"), ...) + col("video_id")
    # 변경: F.window(F.col(...)) + F.col(...)
    # [WHY] 모든 함수 호출을 F alias로 통일 → 유지보수성/가독성 개선
    .groupBy(
        F.window(F.col("event_time"), WIN, SLIDE),
        F.col("video_id")
    )
    .count()
)

# -----------------------------
# Streaming Sink (Delta)
# -----------------------------

q = (
    win.writeStream
    .format("delta")

    # [OLD] .option("checkpointLocation", "chk/silver")
    # [NEW] .option("checkpointLocation", CHECKPOINT)
    # [WHY] checkpoint를 job 단위로 명확히 분리 → 다른 job과 충돌하지 않음
    .option("checkpointLocation", CHECKPOINT)

    .outputMode("append")
    .start(f"{SILVER}/social_metrics")
)

q.awaitTermination()
