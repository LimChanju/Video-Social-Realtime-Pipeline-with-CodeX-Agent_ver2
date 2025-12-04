"""Bronze batch ingest: landing JSON -> Delta with Bloom prefilter and reservoir sampling.

변경사항 요약
(1) 스키마 고정 추가 (기존: 자동 추론 / 변경: StructType 강제)
(2) video_id 단위 Reservoir sampling 추가
(3) sample 저장 경로 분리 (기존: bronze/_sample / 변경: bronze/_sample/video_id)
(4) Bloom filter 범위를 최근 7일로 숙소 (기존: 전체)
(5) Bloom 적용 방식 개선 (기존: 직접 might_contain 호출 / 변경: crossJoin 후 필터링)
(6) BRONZE 디렉토리 구조 세분화 (delta 및 _sample 하위 디렉토리로 분리)
(7) source 값을 mock -> mock_api로 명확화
(8) 전체 코드 구조 모듈화 및 안정성 증가
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql import types as T
from libs.session import build_spark
from libs.sampling import reservoir_by_key
from libs.config import load_logging_config

# 기존: BRONZE 디렉토리에 직접 Delta 저장
# 변경: BRONZE 디렉토리 내에 delta 및 _sample 하위 디렉토리로 분리
# Load environment variables
load_dotenv("conf/.env")
load_logging_config()

LANDING = os.getenv("LANDING_DIR", "data/landing")
BRONZE_BASE = os.getenv("BRONZE_DIR", "data/bronze")
BRONZE_DELTA = os.path.join(BRONZE_BASE, "delta")
BRONZE_SAMPLE = os.path.join(BRONZE_BASE, "_sample")
SAMPLE_SIZE = int(os.getenv("RESERVOIR_K", "50"))
RESERVOIR_KEY = os.getenv("RESERVOIR_KEY", "video_id")

spark = build_spark("bronze_batch")

# 기존: schema 자동 추론
# 변경: StructType으로 스키마 고정 (원본 스키마 안정성)
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

# 기존: source = "mock" (하드코딩)
# 변경: source = "mock_api" (명확한 출처 표기)
incoming = (
    raw_df.select("post_id", "video_id", "author_id", "text", "lang", "ts")
    .withColumn("ingest_date", F.current_date())
    .withColumn("source", F.lit("mock_api"))
)

# (A) 삽입 포인트: Reservoir sampling per key (pre-Bloom) for quick profiling
reservoir_sample = reservoir_by_key(incoming, RESERVOIR_KEY, SAMPLE_SIZE)

# bronze/_sample 에 저장
(reservoir_sample.write \
    .mode("append") \
    .partitionBy(RESERVOIR_KEY, "ingest_date") \
    .json(BRONZE_SAMPLE))

# (B) 삽입 포인트: Bloom filter built from the last 7 days of bronze data
# 기존: 전체 BRONZE 데이터를 기반으로 BF 생성
# 변경: 최근 7일 데이터를 기반으로 BF 생성
bf = None
if Path(BRONZE_DELTA).exists():
    try:
        recent_ids = (
            spark.read.format("delta")
            .load(BRONZE_DELTA)
            .where(F.col("ingest_date") >= F.date_sub(F.current_date(), 7)) # 최근 7일로 조건 추가
            .select("post_id")
            .na.drop()
        )
        
        # 기존: bloom_filter(post_id)로 동일
        # 변경: first() 방식으로 안정적 접근
        bf_row = recent_ids.agg(F.expr("bloom_filter(post_id, 100000, 0.01) as bf")).first()
        bf = bf_row["bf"] if bf_row else None
    except Exception:
        bf = None

# 기존: 직접 might_contain 호출
# 변경: crossJoin 후 필터링 방식으로 변경
if bf is None:
    filtered = incoming
else:
    bf_df = spark.createDataFrame([(bf,)], schema="bf BINARY")
    filtered = (
        incoming.crossJoin(bf_df)
        .filter(~F.expr("might_contain(bf, post_id)"))
        .drop("bf")
    )

# 기존: BRONZE 폴더에 Delta 저장
# 변경: BRONZE/delta 폴더에 Delta 저장
(
    filtered.write.format("delta")
    .mode("append")
    .partitionBy("ingest_date", "source")
    .save(BRONZE_DELTA)
)

print("Bronze appended. Rows:", filtered.count())
