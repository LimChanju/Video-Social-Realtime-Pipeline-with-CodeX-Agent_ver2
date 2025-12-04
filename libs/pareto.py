"""
Pareto frontier utilities.
"""
from typing import Iterable, List, Sequence
from pyspark.sql import DataFrame


def _dominates(a: Sequence[float], b: Sequence[float], maximize: bool = True) -> bool:
    """Return True if point a dominates point b."""
    if not maximize:
        a = [-x for x in a]
        b = [-x for x in b]
    return all(x >= y for x, y in zip(a, b)) and any(x > y for x, y in zip(a, b))


def pareto_front(points: Iterable[Sequence[float]], maximize: bool = True) -> List[Sequence[float]]:
    """
    Compute the non-dominated set (Pareto front) from an iterable of points.
    Intended for small/medium collections (collected to driver).
    """
    pts = list(points)
    front: List[Sequence[float]] = []
    for p in pts:
        if any(_dominates(q, p, maximize) for q in pts if q is not p):
            continue
        front.append(p)
    return front


def pareto_front_df(df: DataFrame, cols: Sequence[str], maximize: bool = True) -> List[Sequence[float]]:
    """
    Compute Pareto front from a Spark DataFrame by collecting specified columns.
    Use only for small datasets; for large sets, consider an iterative/approx approach.
    """
    pts = df.select(*cols).collect()
    return pareto_front([tuple(r[c] for c in cols) for r in pts], maximize=maximize)
