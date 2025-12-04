"""
I/O helpers for NDJSON and small DataFrame samples.
"""
import json
from pathlib import Path
from typing import Iterable
from pyspark.sql import DataFrame


def write_ndjson(path: str | Path, records: Iterable[dict]) -> None:
    """Write iterable of dicts to newline-delimited JSON."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def read_ndjson(path: str | Path) -> Iterable[dict]:
    """Yield dicts from a newline-delimited JSON file."""
    with Path(path).open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def save_df_sample(df: DataFrame, path: str | Path, n: int = 100, fmt: str = "json", mode: str = "overwrite") -> None:
    """
    Save a small sample of a DataFrame to local storage.
    """
    path = str(path)
    df.limit(n).write.mode(mode).format(fmt).save(path)
