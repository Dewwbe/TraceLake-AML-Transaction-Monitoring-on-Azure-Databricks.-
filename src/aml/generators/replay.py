"""Stream replay: write a transaction file as time-ordered micro-batch files into the landing zone.

This simulates a feed. Each file is a CSV with a header. Run with ``--sleep`` to emit files over
time while a streaming job is running; without it all files are written immediately.
"""

from __future__ import annotations

import time
from pathlib import Path

import pandas as pd


def write_microbatches(
    df: pd.DataFrame, out_dir: str | Path, rows_per_file: int = 1000, sleep_seconds: float = 0.0,
    prefix: str = "txn",
) -> list[Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for i, start in enumerate(range(0, len(df), rows_per_file), start=1):
        path = out / f"{prefix}_{i:06d}.csv"
        df.iloc[start:start + rows_per_file].to_csv(path, index=False)
        written.append(path)
        if sleep_seconds:
            time.sleep(sleep_seconds)
    return written
