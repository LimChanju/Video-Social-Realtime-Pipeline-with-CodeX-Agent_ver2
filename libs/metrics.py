"""
Metrics helpers: CDF/PDF style cutoffs and labeling utilities.
"""
from pyspark.sql import DataFrame, functions as F


def cdf_cut(df: DataFrame, value_col: str, top_pct: float, rel_error: float = 0.001):
    """
    Return the value cutoff for the given top percentile (e.g., 0.9 for top 10%).
    """
    vals = df.approxQuantile(value_col, [top_pct], rel_error)
    return vals[0] if vals else None


def label_top_pct(df: DataFrame, value_col: str, top_pct: float, label_col: str = "label", rel_error: float = 0.001):
    """
    Add a binary label column indicating membership in the top percentile.
    """
    cut = cdf_cut(df, value_col, top_pct, rel_error)
    return df.withColumn(label_col, (F.col(value_col) >= F.lit(cut)).cast("int")), cut


def approx_pdf(df: DataFrame, value_col: str, bins: int = 20):
    """
    Approximate a PDF via Spark histogram; suitable for small/medium data.
    Returns (bin_edges, counts).
    """
    # hist is collected driver-side; keep bins modest
    values = df.select(value_col).rdd.map(lambda r: r[0])
    return values.histogram(bins)
