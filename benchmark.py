"""
Benchmark Utility — Measures pipeline throughput across different thread configurations.
Run this script directly to see how concurrency affects performance on your machine.
"""

import csv
import json
import logging
import os
import random
import sqlite3
import string
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

def generate_csv(path: str, n_rows: int = 50_000):
    """Generate a synthetic CSV file with realistic-ish columns."""
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id", "name", "email", "age", "score", "joined_at", "active"],
        )
        writer.writeheader()
        for i in range(n_rows):
            writer.writerow(
                {
                    "id": i,
                    "name": "".join(random.choices(string.ascii_lowercase, k=8)).capitalize(),
                    "email": f"user{i}@example.com",
                    "age": random.randint(18, 90),
                    "score": round(random.uniform(0, 100), 2),
                    "joined_at": f"202{random.randint(0,4)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                    "active": random.choice(["true", "false"]),
                }
            )


def generate_sqlite(path: str, n_rows: int = 50_000):
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users "
        "(id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, score REAL, joined_at TEXT, active TEXT)"
    )
    conn.executemany(
        "INSERT INTO users VALUES (?,?,?,?,?,?,?)",
        [
            (
                i,
                "".join(random.choices(string.ascii_lowercase, k=8)).capitalize(),
                f"user{i}@example.com",
                random.randint(18, 90),
                round(random.uniform(0, 100), 2),
                f"202{random.randint(0,4)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                random.choice(["true", "false"]),
            )
            for i in range(n_rows)
        ],
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Benchmark scenarios
# ---------------------------------------------------------------------------

@dataclass
class BenchmarkResult:
    scenario: str
    workers: int
    records: int
    duration_seconds: float

    @property
    def throughput(self) -> float:
        return self.records / self.duration_seconds if self.duration_seconds > 0 else 0


def _simulate_transform(record: Dict[str, Any]) -> Dict[str, Any]:
    """Lightweight CPU + IO simulation for a transform."""
    time.sleep(0.00001)  # Simulate a tiny IO wait
    record["name"] = record.get("name", "").upper()
    record["score"] = float(record.get("score", 0)) * 1.1
    return record


def benchmark_csv_concurrent_read(csv_path: str, worker_counts: List[int] = None) -> List[BenchmarkResult]:
    """Benchmark reading a CSV with different numbers of threads (simulated parallel chunks)."""
    from etl.extractors.file_extractors import CSVExtractor
    results = []
    worker_counts = worker_counts or [1, 2, 4, 8]

    for workers in worker_counts:
        start = time.perf_counter()
        extractor = CSVExtractor(csv_path, max_workers=workers)
        count = sum(1 for _ in extractor())
        elapsed = time.perf_counter() - start
        results.append(BenchmarkResult("CSV Read", workers, count, elapsed))
        print(f"  CSV Read  | workers={workers:2d} | {count:>7,} records | {elapsed:.3f}s | {count/elapsed:>10,.0f} rec/s")

    return results


def benchmark_transform_throughput(records: int = 20_000, worker_counts: List[int] = None) -> List[BenchmarkResult]:
    """Benchmark transform stage with growing thread pool sizes."""
    from queue import Queue
    results = []
    worker_counts = worker_counts or [1, 2, 4, 8, 16]

    raw_data = [
        {"id": i, "name": f"user_{i}", "score": float(i % 100)}
        for i in range(records)
    ]

    for workers in worker_counts:
        output = []
        lock = __import__("threading").Lock()

        def process_slice(slice_: List[Dict]) -> int:
            count = 0
            for record in slice_:
                result = _simulate_transform(dict(record))
                with lock:
                    output.append(result)
                count += 1
            return count

        chunk_size = max(1, records // workers)
        slices = [raw_data[i:i + chunk_size] for i in range(0, records, chunk_size)]

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(process_slice, s) for s in slices]
            total = sum(f.result() for f in as_completed(futures))
        elapsed = time.perf_counter() - start

        results.append(BenchmarkResult("Transform", workers, total, elapsed))
        print(f"  Transform | workers={workers:2d} | {total:>7,} records | {elapsed:.3f}s | {total/elapsed:>10,.0f} rec/s")

    return results


def benchmark_sqlite_parallel_read(db_path: str, worker_counts: List[int] = None) -> List[BenchmarkResult]:
    """Benchmark SQLite extraction with different levels of chunk parallelism."""
    from etl.extractors.sql_extractor import SQLiteExtractorConfig, SQLiteExtractor
    results = []
    worker_counts = worker_counts or [1, 2, 4, 8]

    for workers in worker_counts:
        cfg = SQLiteExtractorConfig(
            db_path=db_path,
            query="SELECT * FROM users",
            chunk_size=1000,
            pool_size=workers,
            parallel_chunks=workers,
        )
        extractor = SQLiteExtractor(cfg)

        start = time.perf_counter()
        count = sum(1 for _ in extractor())
        elapsed = time.perf_counter() - start

        results.append(BenchmarkResult("SQLite Read", workers, count, elapsed))
        print(f"  SQLite    | workers={workers:2d} | {count:>7,} records | {elapsed:.3f}s | {count/elapsed:>10,.0f} rec/s")

    return results


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_all_benchmarks(n_rows: int = 20_000):
    print("\n" + "=" * 70)
    print(f"  ETL Pipeline — Multithreading Benchmark  ({n_rows:,} records)")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "benchmark.csv")
        db_path = os.path.join(tmpdir, "benchmark.db")

        print("\nGenerating test data…")
        generate_csv(csv_path, n_rows)
        generate_sqlite(db_path, n_rows)
        print("Done.\n")

        print("── CSV Concurrent Read ──────────────────────────────────")
        benchmark_csv_concurrent_read(csv_path)

        print("\n── Transform Throughput (ThreadPoolExecutor) ───────────")
        benchmark_transform_throughput(n_rows)

        print("\n── SQLite Parallel Chunk Read ───────────────────────────")
        benchmark_sqlite_parallel_read(db_path)

    print("\n" + "=" * 70)
    print("  Benchmark complete. Check above for rec/s at each worker count.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    run_all_benchmarks()
