"""Streamlit realtime view: CDF/PDF visualization + Bloom presence check + predictions table.
Run with: streamlit run app/app.py
"""
import os
from dotenv import load_dotenv
import streamlit as st
from pyspark.sql import functions as F
from libs.session import build_spark
from libs.config import load_app_config, load_logging_config

load_dotenv("conf/.env")
load_logging_config()
cfg = load_app_config()
GOLD = cfg.get("data", {}).get("gold_dir", os.getenv("GOLD_DIR", "data/gold"))
SILVER = cfg.get("data", {}).get("silver_dir", os.getenv("SILVER_DIR", "data/silver"))
TOP_PCT = float(cfg.get("data", {}).get("top_pct", os.getenv("TOP_PCT", "0.9")))

st.set_page_config(page_title="Realtime Video Insights", layout="wide")

spark = build_spark("streamlit_view")

# Auto-refresh 옵션 (비활성화)
refresh = st.sidebar.button("Refresh data")
if refresh:
    if hasattr(st, "experimental_rerun"):
        st.experimental_rerun()
    elif hasattr(st, "rerun"):
        st.rerun()

query = st.text_input("영상 ID 검색 (video_id) 또는 키워드", "")

try:
    features = spark.read.format("delta").load(f"{GOLD}/features")
    predictions = spark.read.format("delta").load(f"{GOLD}/predictions")
    metrics = spark.read.format("delta").load(f"{SILVER}/social_metrics")
except Exception:
    st.warning("데이터가 아직 없습니다. 파이프라인을 먼저 실행하세요.")
    st.stop()

col1, col2, col3 = st.columns(3)

# CDF cut for engagement
cut = features.approxQuantile("engagement_24h", [TOP_PCT], 0.001)[0]
col1.metric(f"Top {int(TOP_PCT*100)}% cut (engagement_24h)", f"{cut:,.2f}")

# PDF histogram (bucketed)
hist_df = (
    features.selectExpr("width_bucket(engagement_24h, 0, 10000, 30) as bucket")
    .groupBy("bucket").count()
    .orderBy("bucket")
).toPandas()
if not hist_df.empty:
    col2.bar_chart(hist_df.set_index("bucket"))

# Bloom presence check (최근 7일; fallback: gold)
try:
    recent_vids = (
        metrics
        .filter(F.col("window.end") >= F.date_sub(F.current_timestamp(), 7))
        .select("video_id")
        .distinct()
    )
    recent_count = recent_vids.count()
    col3.metric("Recent video_ids (7d)", recent_count)
    bloom_source = recent_vids if recent_count > 0 else features.select("video_id").distinct()

    def check_with_bloom(df, q):
        """Try bloom_filter/might_contain; fallback to exact match if functions missing."""
        try:
            bf_row = df.agg(F.bloom_filter("video_id", 100000, 0.01).alias("bf")).collect()[0]
            bf = bf_row["bf"]
            exists = (
                spark.createDataFrame([(q,)], "video_id string")
                .select(F.might_contain(F.lit(bf), F.col("video_id")).alias("maybe"))
            ).collect()[0]["maybe"]
            return exists, "bloom"
        except Exception:
            exists = df.filter(F.col("video_id") == q).head(1) is not None
            return exists, "exact"

    if bloom_source.head(1) and query:
        exists, mode = check_with_bloom(bloom_source, query)
        source_name = "최근 7일" if recent_count > 0 else "gold"
        note = "Bloom" if mode == "bloom" else "Exact"
        col3.info(f"{note} 기반 존재 여부 ({source_name}): {exists}")
    elif not bloom_source.head(1):
        col3.warning("Bloom 계산 불가: video_id 없음.")
except Exception as e:
    col3.warning(f"Bloom 필터 계산 실패: {e}")

# Filter by query if provided
filt_features = (
    features
    if not query
    else features.filter((F.col("video_id") == query) | (F.col("video_id").contains(query)))
)
filt_preds = (
    predictions
    if not query
    else predictions.filter((F.col("video_id") == query) | (F.col("video_id").contains(query)))
)

st.subheader("Features (latest)")
st.dataframe(filt_features.orderBy(F.desc("engagement_24h")).limit(200).toPandas(), use_container_width=True)

st.subheader("Predictions (latest)")
st.dataframe(filt_preds.orderBy(F.desc("event_time")).limit(200).toPandas(), use_container_width=True)
