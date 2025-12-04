"""Multi-model benchmarking + Pareto-optimal selection (AUC↑, latency↓, feature_count↓).
Run with:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/40_train_pareto.py
"""
import os
import time
from typing import List, Dict, Tuple
from dotenv import load_dotenv
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from libs.session import build_spark
from libs.pareto import pareto_front

load_dotenv("conf/.env")
GOLD = os.getenv("GOLD_DIR", "data/gold")

# 모델 training용 Spark 세션 생성
spark = build_spark("pareto_training")

# Load labeled features
df = spark.read.format("delta").load(f"{GOLD}/features").fillna(0)
feat_cols = ["engagement_24h", "uniq_users_est"]

# Basic index/assemble for tabular features (extend with text/vector features as needed)
si = StringIndexer(inputCol="label", outputCol="label_idx")
va = VectorAssembler(inputCols=feat_cols, outputCol="features")

# Candidate models (다목적: 성능 vs 지연 vs 피처수)
candidates = [
    ("lr", LogisticRegression(labelCol="label_idx", maxIter=50)),
    ("rf", RandomForestClassifier(labelCol="label_idx", numTrees=200, maxDepth=10)),
    ("gbt", GBTClassifier(labelCol="label_idx", maxIter=80, maxDepth=6)),
]

evaluator = BinaryClassificationEvaluator(labelCol="label_idx", metricName="areaUnderPR")

def evaluate_models() -> List[Dict]:
    results = []
    for name, clf in candidates:
        pipe = Pipeline(stages=[si, va, clf])
        model = pipe.fit(df)
        t0 = time.time()
        pred = model.transform(df)
        latency_ms = (time.time() - t0) * 1000
        aucpr = evaluator.evaluate(pred)
        feat_count = len(feat_cols)
        results.append(
            {
                "name": name,
                "aucpr": aucpr,
                "latency_ms": latency_ms,
                "feat_count": feat_count,
            }
        )
    return results

def select_pareto(results: List[Dict]) -> List[Dict]:
    # Maximize aucpr, minimize latency/feat_count by negating the latter two
    points: List[Tuple[float, float, float]] = [
        (r["aucpr"], -r["latency_ms"], -r["feat_count"]) for r in results
    ]
    front_pts = set(pareto_front(points, maximize=True))
    return [r for r, p in zip(results, points) if p in front_pts]

results = evaluate_models()
pareto = select_pareto(results)

print("All models:")
for r in results:
    print(r)

print("Pareto front:")
for r in pareto:
    print(r)
