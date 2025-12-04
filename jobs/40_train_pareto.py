"""Snippet: multi-model training metrics + Pareto front selection.
Run with:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/40_train_pareto.py
"""
import os, time
from dotenv import load_dotenv
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from libs.session import build_spark
from libs.pareto import pareto_front

load_dotenv("conf/.env")
GOLD = os.getenv("GOLD_DIR","data/gold")

spark = build_spark("pareto_training_snippet")

df = spark.read.format("delta").load(f"{GOLD}/features").fillna(0)
# Minimal features (no text here). In your real pipeline, include text features.
feat_cols = ["engagement_24h","uniq_users_est"]
si = StringIndexer(inputCol="label", outputCol="label_idx")
va = VectorAssembler(inputCols=feat_cols, outputCol="features")

candidates = [
  ("lr", LogisticRegression(labelCol="label_idx", maxIter=50)),
  ("rf", RandomForestClassifier(labelCol="label_idx", numTrees=200, maxDepth=10)),
  ("gbt", GBTClassifier(labelCol="label_idx", maxIter=80, maxDepth=6)),
]

evaluator = BinaryClassificationEvaluator(labelCol="label_idx", metricName="areaUnderPR")

results = []
for name, clf in candidates:
    pipe = Pipeline(stages=[si, va, clf]).fit(df)
    t0 = time.time(); pred = pipe.transform(df)
    latency_ms = (time.time()-t0)*1000
    aucpr = evaluator.evaluate(pred)
    feat_count = len(feat_cols)
    results.append({"name":name,"aucpr":aucpr,"latency_ms":latency_ms,"feat_count":feat_count})

# Pareto front selection using shared util.
# We maximize aucpr while minimizing latency/feat_count => negate those for maximizing.
points = [(r["aucpr"], -r["latency_ms"], -r["feat_count"]) for r in results]
front_pts = set(pareto_front(points, maximize=True))
pareto = [r for r, p in zip(results, points) if p in front_pts]

print("All:", results)
print("Pareto-front:", pareto)
