# ⚡ ETL Pipeline

<div align="center">

![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-22c55e?style=for-the-badge)
![Dependencies](https://img.shields.io/badge/Dependencies-Zero-f97316?style=for-the-badge)
![Threads](https://img.shields.io/badge/Concurrency-Multithreaded-8b5cf6?style=for-the-badge)

**A production-grade, zero-dependency ETL framework powered by Python's native threading primitives.**  
Extract from CSV/JSON/SQLite → Transform with composable rules → Load to multiple targets, all concurrently.

[Getting Started](#-getting-started) · [Architecture](#-architecture) · [Usage](#-usage) · [Benchmarks](#-benchmarks) · [API Reference](#-api-reference)

</div>

---

## ✨ Features

| Feature | Detail |
|---|---|
| 🔀 **True Multithreading** | Each ETL stage runs in its own `ThreadPoolExecutor` — independently tunable |
| 🔁 **Producer-Consumer Queues** | Bounded `Queue`s between stages provide natural backpressure |
| 🗂️ **Multiple Sources** | CSV, JSON, JSON Lines, SQLite — single file or entire directories |
| 🔗 **Composable Transforms** | Chain validators, cleaners, enrichers, and deduplicators |
| 🎯 **Fan-out Loading** | Write to CSV + SQLite simultaneously with `MultiLoader` |
| 🔒 **Thread-Safe by Design** | Locks on metrics, dedup sets, file writes, and DB connections |
| 📊 **Built-in Benchmarks** | Measure throughput at different worker counts out of the box |
| 🔌 **Zero Dependencies** | Uses only Python's standard library — no pip installs required |

---

## 🏗️ Architecture

The pipeline uses a **three-stage producer-consumer model**. Each stage is decoupled by a bounded queue, so fast stages don't overrun slow ones.

```
╔══════════════════════════════════════════════════════════════════════╗
║                         ETL Pipeline                                 ║
║                                                                      ║
║  ┌─────────────────┐                   ┌──────────────────────────┐  ║
║  │   EXTRACT        │                   │   TRANSFORM              │  ║
║  │                  │                   │                          │  ║
║  │  Thread 1 ──┐   │  bounded Queue    │  Thread 1 ──┐           │  ║
║  │  Thread 2 ──┼──►│ ════════════════► │  Thread 2 ──┼──┐        │  ║
║  │  Thread 3 ──┘   │  (backpressure)   │  Thread 3 ──┘  │        │  ║
║  │                  │                   │  Thread 4 ──┘  │        │  ║
║  └─────────────────┘                   └────────────────│─────────┘  ║
║                                                          │            ║
║                                          bounded Queue   │            ║
║                                         ═════════════════▼            ║
║                                        ┌──────────────────────────┐  ║
║                                        │   LOAD                   │  ║
║                                        │                          │  ║
║                                        │  Thread 1 → CSV          │  ║
║                                        │  Thread 2 → SQLite       │  ║
║                                        │  Thread 3 → JSON         │  ║
║                                        └──────────────────────────┘  ║
╚══════════════════════════════════════════════════════════════════════╝
```

### Concurrency Design Decisions

```
┌─────────────────────┬────────────────────────────────────────────────────┐
│ Concern             │ Solution                                            │
├─────────────────────┼────────────────────────────────────────────────────┤
│ Stage decoupling    │ Bounded Queue — natural backpressure                │
│ Completion signals  │ threading.Event — no polling                        │
│ Shared metrics      │ threading.Lock over a dict                          │
│ SQLite connections  │ Queue-based pool — one connection per thread        │
│ Parallel file reads │ as_completed() streams results as files finish      │
│ Deduplication       │ Lock-guarded set — safe across transform threads    │
│ Load retries        │ Per-loader retry loop with exponential backoff      │
└─────────────────────┴────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
etl-pipeline/
│
├── etl/
│   ├── core/
│   │   ├── pipeline.py        ← Orchestrator: queues, events, thread pools, metrics
│   │   ├── thread_pool.py     ← Named pool manager with per-worker stats & resizing
│   │   └── benchmark.py       ← Throughput benchmarks across worker counts
│   │
│   ├── extractors/
│   │   ├── file_extractors.py ← CSV, JSON, JSONL, Directory (concurrent multi-file)
│   │   └── sql_extractor.py   ← SQLite: connection pool + parallel chunk reads
│   │
│   ├── transformers/
│   │   └── transformers.py    ← 11 composable transforms + chain() combinator
│   │
│   └── loaders/
│       └── loaders.py         ← CSV, JSON Lines, SQLite + MultiLoader fan-out
│
├── examples/
│   └── csv_to_sqlite_pipeline.py   ← Full end-to-end working example
│
├── tests/
│   └── test_pipeline.py       ← Unit + integration tests (no external deps)
│
├── requirements.txt           ← pytest (dev only)
├── setup.py
└── pyproject.toml
```

---

## 🚀 Getting Started

```bash
# 1. Clone the repo
git clone https://github.com/your-username/etl-pipeline.git
cd etl-pipeline

# 2. No install needed for core — zero external dependencies!
#    For running tests:
pip install pytest

# 3. Run the example pipeline
python examples/csv_to_sqlite_pipeline.py

# 4. Run benchmarks
python -m etl.core.benchmark

# 5. Run tests
pytest tests/ -v
```

**Expected example output:**
```
EXTRACT       5,000 records  (0.013s)
TRANSFORM     4,767 records  (0.081s)
LOAD          4,767 records  (0.216s)

Extracted :  5,000
Transformed:  4,767
Loaded    :  4,767
Skipped   :    233   ← dirty records caught by validators
Failed    :      0
```

---

## 💡 Usage

### Minimal pipeline

```python
from etl.core.pipeline import Pipeline, PipelineConfig
from etl.extractors.file_extractors import CSVExtractor
from etl.transformers.transformers import RequiredFieldsValidator, TypeCaster, chain
from etl.loaders.loaders import SQLiteLoader

pipeline = Pipeline(PipelineConfig(
    name="my-pipeline",
    extract_workers=4,    # tune for I/O bound work
    transform_workers=8,  # tune for CPU bound work
    load_workers=4,
    batch_size=500,
))

pipeline.add_extractor(CSVExtractor("data/users.csv"))
pipeline.add_transformer(chain(
    RequiredFieldsValidator(["id", "email"]),
    TypeCaster({"id": int, "score": float}),
))
pipeline.add_loader(SQLiteLoader("output/users.db", table="users"))

report = pipeline.run()
print(report["metrics"])
# {'extracted': 50000, 'transformed': 49231, 'loaded': 49231, 'failed': 0, 'skipped': 769}
```

### Read multiple files concurrently

```python
# All files read in parallel, results merged into one stream
extractor = CSVExtractor(
    ["data/jan.csv", "data/feb.csv", "data/mar.csv"],
    max_workers=3,
)
pipeline.add_extractor(extractor)
```

### Partition a large SQLite table

```python
from etl.extractors.sql_extractor import TablePartitioner

# Splits the table into 4 rowid ranges — each reads concurrently
for extractor in TablePartitioner("data.db", "orders", n_partitions=4).extractors():
    pipeline.add_extractor(extractor)
```

### Write to multiple targets simultaneously

```python
from etl.loaders.loaders import CSVLoader, SQLiteLoader, MultiLoader

pipeline.add_loader(MultiLoader(
    CSVLoader("output/clean.csv"),
    SQLiteLoader("output/data.db", table="results"),
    max_workers=2,  # both loaders run in parallel
))
```

### Lifecycle hooks

```python
pipeline.add_hook("pre_extract",  lambda p: print(f"Starting {p.config.name}"))
pipeline.add_hook("post_load",    lambda p: print(f"Done: {p.metrics}"))
```

### Graceful cancellation

```python
import threading

def cancel_after(pipeline, seconds):
    threading.Timer(seconds, pipeline.cancel).start()

cancel_after(pipeline, 30)  # auto-cancel after 30 seconds
pipeline.run()
```

---

## 📊 Benchmarks

Run `python -m etl.core.benchmark` to see how concurrency improves throughput on your machine:

```
── Transform Throughput (ThreadPoolExecutor) ────────────────
  Transform | workers= 1 |  20,000 records | 2.902s |   6,891 rec/s
  Transform | workers= 2 |  20,000 records | 2.545s |   7,859 rec/s
  Transform | workers= 4 |  20,000 records | 0.964s |  20,747 rec/s  ✓ sweet spot
  Transform | workers= 8 |  20,000 records | 1.110s |  18,015 rec/s
  Transform | workers=16 |  20,000 records | 1.079s |  18,529 rec/s

── SQLite Parallel Chunk Read ───────────────────────────────
  SQLite    | workers= 1 |  20,000 records | 0.284s |  70,422 rec/s
  SQLite    | workers= 2 |  20,000 records | 0.191s | 104,712 rec/s
  SQLite    | workers= 4 |  20,000 records | 0.097s | 206,185 rec/s  ✓ sweet spot
```

> **Note:** Python's GIL means threading shines brightest on **I/O-bound** work (file reads, DB queries). For pure CPU transforms, consider `multiprocessing` for further gains.

---

## 📖 API Reference

### PipelineConfig

```python
@dataclass
class PipelineConfig:
    name: str                         # Pipeline identifier (appears in logs)
    extract_workers: int = 4          # Extractor thread pool size
    transform_workers: int = 8        # Transformer thread pool size
    load_workers: int = 4             # Loader thread pool size
    batch_size: int = 500             # Records per queue batch
    queue_maxsize: int = 1000         # Max batches between stages (backpressure)
    timeout_seconds: int = 300        # Queue put/get timeout
    retry_attempts: int = 3           # Loader retry count on failure
    retry_delay_seconds: float = 1.0  # Seconds between retries (×attempt)
```

### Extractors

| Class | Description |
|---|---|
| `CSVExtractor(paths, max_workers)` | Single or multi-file CSV; parallel reads with `as_completed()` |
| `JSONExtractor(paths, record_path, jsonlines)` | JSON array, nested-path, or JSON Lines |
| `DirectoryExtractor(directory, recursive)` | Auto-detects all CSV/JSON in a folder |
| `SQLiteExtractor(config)` | Chunked reads via a `Queue`-based connection pool |
| `TablePartitioner(db, table, n_partitions)` | Splits table by rowid → N independent extractors |

### Transformers

| Class | Description |
|---|---|
| `RequiredFieldsValidator(fields)` | Drop records with missing/empty fields |
| `SchemaValidator(schema, strict)` | Type-validate; optionally reject extra fields |
| `RangeValidator(field, min, max)` | Drop records with out-of-range numeric values |
| `FieldNormalizer(fields)` | Strip whitespace, normalize Unicode |
| `TypeCaster(casts)` | Cast fields to target Python types |
| `DateParser(fields, output_format)` | Parse many date formats → ISO 8601 |
| `FieldDropper(fields)` | Remove unwanted fields from records |
| `FieldRenamer(mapping)` | Rename fields via `{old: new}` dict |
| `FieldEnricher(enrichments)` | Add computed/derived fields |
| `RecordHasher(key_fields)` | Attach SHA-256 fingerprint from key fields |
| `DuplicateFilter(key_fields)` | Thread-safe in-memory deduplication via `Lock` |
| `chain(*transformers)` | Compose transforms; propagates `None` to drop records |

### Loaders

| Class | Description |
|---|---|
| `CSVLoader(path)` | Thread-safe append-mode CSV writer (lock per batch) |
| `JSONLoader(path)` | Thread-safe JSON Lines writer |
| `SQLiteLoader(db, table, upsert)` | Bulk `executemany`; auto-creates table; WAL mode |
| `MultiLoader(*loaders)` | Fan-out to N loaders concurrently |

---

## 🧪 Testing

```bash
pytest tests/ -v --tb=short
```

Test coverage includes:

- ✅ Single and multi-file CSV/JSON extraction
- ✅ Concurrent SQLite chunk reads with order preservation
- ✅ All 11 transformer correctness cases
- ✅ `DuplicateFilter` thread-safety under 100 concurrent threads
- ✅ Thread-safe CSV, SQLite, and JSON loader writes
- ✅ Full end-to-end pipeline integration (extract → transform → load → verify)

---

## 🤝 Contributing

1. Fork the repo and create a feature branch
2. Add tests for any new extractors, transformers, or loaders
3. Run `pytest tests/ -v` — all tests must pass
4. Open a pull request with a clear description

---

## 📄 License

MIT © 2024 — free to use, modify, and distribute.
