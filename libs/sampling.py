"""
Sampling utilities (reservoir-like via window row_number).
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def reservoir_by_key(df: DataFrame, key_col: str, k: int, seed: float | None = None) -> DataFrame:
    """
    Approximate reservoir sampling per key using random order + row_number.

    Args:
        df: Input DataFrame.
        key_col: Column to partition by (e.g., video_id).
        k: Sample size per key.
        seed: Optional seed for reproducibility.
    """
    rand_col = F.rand(seed)
    window = Window.partitionBy(key_col).orderBy(rand_col)
    return (
        df.withColumn("_rand", rand_col)
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") <= k)
        .drop("_rand", "_rn")
    )
