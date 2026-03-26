"""
Example: Full ETL Pipeline
==========================
Demonstrates a complete CSV → Transform → CSV + SQLite pipeline
using the multithreaded Pipeline orchestrator.

Run:
    python examples/csv_to_sqlite_pipeline.py
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
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-18s] %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)

# ---------------------------------------------------------------------------
# Inline minimal stubs so the example runs without installing the package
# ---------------------------------------------------------------------------
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.core.pipeline import Pipeline, PipelineConfig
from etl.extractors.file_extractors import CSVExtractor
from etl.extractors.sql_extractor import SQLiteExtractorConfig, SQLiteExtractor
from etl.transformers.transformers import (
    RequiredFieldsValidator,
    TypeCaster,
    FieldNormalizer,
    DateParser,
    DuplicateFilter,
    RecordHasher,
    FieldEnricher,
    chain,
)
from etl.loaders.loaders import CSVLoader, SQLiteLoader, MultiLoader


# ---------------------------------------------------------------------------
# 1. Generate sample data
# ---------------------------------------------------------------------------

def generate_sample_csv(path: str, n: int = 5_000):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["id", "name", "email", "age", "score", "joined_at", "active"]
        )
        writer.writeheader()
        for i in range(n):
            # Inject some dirty data
            writer.writerow({
                "id": i if i % 50 != 0 else "",          # some missing IDs
                "name": "".join(random.choices(string.ascii_lowercase, k=7)).capitalize(),
                "email": f"user{i}@example.com" if i % 30 != 0 else "",
                "age": random.randint(15, 95),
                "score": round(random.uniform(-10, 110), 2),
                "joined_at": f"202{random.randint(0,4)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
                "active": random.choice(["true", "false", "TRUE", "False ", " true"]),
            })


# ---------------------------------------------------------------------------
# 2. Build & run the pipeline
# ---------------------------------------------------------------------------

def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_csv = os.path.join(tmpdir, "users_raw.csv")
        output_csv = os.path.join(tmpdir, "users_clean.csv")
        output_db = os.path.join(tmpdir, "users.db")

        print(f"\nGenerating 5,000 sample records → {input_csv}")
        generate_sample_csv(input_csv, n=5_000)

        # ── Extractor ─────────────────────────────────────────────────────
        extractor = CSVExtractor(input_csv, max_workers=2)

        # ── Transformers (chained) ─────────────────────────────────────────
        transform = chain(
            RequiredFieldsValidator(["id", "email"]),
            TypeCaster({"id": int, "age": int, "score": float}),
            FieldNormalizer(fields=["name", "active"]),
            DateParser(fields=["joined_at"]),
            DuplicateFilter(key_fields=["id"]),
            RecordHasher(key_fields=["id", "email"]),
            FieldEnricher({
                "active": lambda r: r.get("active", "").strip().lower() in ("true", "1", "yes"),
                "score_category": lambda r: (
                    "high" if float(r.get("score", 0)) >= 70
                    else "mid" if float(r.get("score", 0)) >= 40
                    else "low"
                ),
            }),
        )

        # ── Loaders ────────────────────────────────────────────────────────
        csv_out = CSVLoader(output_csv)
        db_out = SQLiteLoader(output_db, table="users", upsert=True)
        loader = MultiLoader(csv_out, db_out, max_workers=2)

        # ── Pipeline ───────────────────────────────────────────────────────
        config = PipelineConfig(
            name="Users-ETL",
            extract_workers=2,
            transform_workers=6,
            load_workers=3,
            batch_size=250,
        )

        pipeline = Pipeline(config)
        pipeline.add_extractor(extractor)
        pipeline.add_transformer(transform)
        pipeline.add_loader(loader)

        print("\nRunning ETL pipeline…\n")
        report = pipeline.run()

        # ── Results ────────────────────────────────────────────────────────
        print("\n" + "=" * 55)
        print("  Pipeline Report")
        print("=" * 55)
        print(f"  Status   : {report['status'].upper()}")
        print(f"  Duration : {report['duration_seconds']:.3f}s")
        print()
        for stage in report["stages"]:
            print(f"  {stage['name'].upper():<12} {stage['records']:>6,} records  ({stage['duration_seconds']:.3f}s)")
        print()
        metrics = report["metrics"]
        print(f"  Extracted : {metrics['extracted']:>6,}")
        print(f"  Transformed: {metrics['transformed']:>6,}")
        print(f"  Loaded    : {metrics['loaded']:>6,}")
        print(f"  Skipped   : {metrics['skipped']:>6,}")
        print(f"  Failed    : {metrics['failed']:>6,}")
        print("=" * 55)

        # Quick verification from SQLite
        conn = sqlite3.connect(output_db)
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        sample = conn.execute(
            "SELECT id, name, score_category, _hash FROM users LIMIT 3"
        ).fetchall()
        conn.close()

        print(f"\n  SQLite rows written : {count:,}")
        print("  Sample rows:")
        for row in sample:
            print(f"    id={row[0]}  name={row[1]}  category={row[2]}  hash={row[3][:12]}…")
        print()


if __name__ == "__main__":
    main()
