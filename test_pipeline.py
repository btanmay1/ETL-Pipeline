"""
Tests — Unit and integration tests for the ETL pipeline.
Run: pytest tests/ -v
"""

import csv
import json
import os
import sqlite3
import tempfile
import threading
import time
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Extractors
# ---------------------------------------------------------------------------

class TestCSVExtractor:
    def _write_csv(self, path, rows):
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def test_single_file(self, tmp_path):
        from etl.extractors.file_extractors import CSVExtractor
        data = [{"id": str(i), "name": f"user_{i}"} for i in range(100)]
        p = tmp_path / "test.csv"
        self._write_csv(str(p), data)

        extractor = CSVExtractor(str(p))
        results = list(extractor())
        assert len(results) == 100
        assert results[0]["id"] == "0"

    def test_multi_file_concurrent(self, tmp_path):
        from etl.extractors.file_extractors import CSVExtractor
        files = []
        for i in range(4):
            p = tmp_path / f"part_{i}.csv"
            self._write_csv(str(p), [{"id": str(j + i * 50), "val": j} for j in range(50)])
            files.append(str(p))

        extractor = CSVExtractor(files, max_workers=4)
        results = list(extractor())
        assert len(results) == 200

    def test_missing_file_raises(self):
        from etl.extractors.file_extractors import CSVExtractor
        with pytest.raises(FileNotFoundError):
            CSVExtractor("/no/such/file.csv")


class TestJSONExtractor:
    def test_json_array(self, tmp_path):
        from etl.extractors.file_extractors import JSONExtractor
        data = [{"id": i} for i in range(50)]
        p = tmp_path / "data.json"
        p.write_text(json.dumps(data))

        results = list(JSONExtractor(str(p))())
        assert len(results) == 50

    def test_jsonlines(self, tmp_path):
        from etl.extractors.file_extractors import JSONExtractor
        p = tmp_path / "data.jsonl"
        p.write_text("\n".join(json.dumps({"id": i}) for i in range(30)))

        results = list(JSONExtractor(str(p), jsonlines=True)())
        assert len(results) == 30

    def test_nested_path(self, tmp_path):
        from etl.extractors.file_extractors import JSONExtractor
        data = {"response": {"data": [{"id": i} for i in range(10)]}}
        p = tmp_path / "nested.json"
        p.write_text(json.dumps(data))

        results = list(JSONExtractor(str(p), record_path="response.data")())
        assert len(results) == 10


class TestSQLiteExtractor:
    def _create_db(self, path, n=200):
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
        conn.executemany(
            "INSERT INTO items VALUES (?,?)",
            [(i, f"val_{i}") for i in range(n)],
        )
        conn.commit()
        conn.close()

    def test_basic_extraction(self, tmp_path):
        from etl.extractors.sql_extractor import SQLiteExtractor, SQLiteExtractorConfig
        db = str(tmp_path / "test.db")
        self._create_db(db, n=300)

        cfg = SQLiteExtractorConfig(
            db_path=db,
            query="SELECT * FROM items",
            chunk_size=100,
            pool_size=4,
            parallel_chunks=4,
        )
        records = list(SQLiteExtractor(cfg)())
        assert len(records) == 300

    def test_preserves_order(self, tmp_path):
        from etl.extractors.sql_extractor import SQLiteExtractor, SQLiteExtractorConfig
        db = str(tmp_path / "order.db")
        self._create_db(db, n=100)

        cfg = SQLiteExtractorConfig(
            db_path=db,
            query="SELECT * FROM items ORDER BY id",
            chunk_size=25,
            pool_size=4,
            parallel_chunks=4,
        )
        records = list(SQLiteExtractor(cfg)())
        ids = [r["id"] for r in records]
        assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# Transformers
# ---------------------------------------------------------------------------

class TestTransformers:
    def test_required_fields_drops_missing(self):
        from etl.transformers.transformers import RequiredFieldsValidator
        t = RequiredFieldsValidator(["id", "email"])
        assert t({"id": "1", "email": "a@b.com"}) is not None
        assert t({"id": "1", "email": ""}) is None
        assert t({"id": "1"}) is None

    def test_type_caster(self):
        from etl.transformers.transformers import TypeCaster
        t = TypeCaster({"age": int, "score": float})
        result = t({"age": "25", "score": "9.5"})
        assert result["age"] == 25
        assert result["score"] == 9.5

    def test_field_normalizer_strips_whitespace(self):
        from etl.transformers.transformers import FieldNormalizer
        t = FieldNormalizer(fields=["name"])
        result = t({"name": "  Alice  ", "age": 30})
        assert result["name"] == "Alice"

    def test_duplicate_filter_thread_safe(self):
        from etl.transformers.transformers import DuplicateFilter
        df = DuplicateFilter(key_fields=["id"])
        records = [{"id": i % 10} for i in range(100)]

        results = []
        lock = threading.Lock()

        def process(r):
            out = df(r)
            if out:
                with lock:
                    results.append(out)

        threads = [threading.Thread(target=process, args=(r,)) for r in records]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(results) == 10  # Only 10 unique IDs

    def test_record_hasher(self):
        from etl.transformers.transformers import RecordHasher
        t = RecordHasher(key_fields=["id", "email"])
        r1 = t({"id": 1, "email": "a@b.com"})
        r2 = t({"id": 1, "email": "a@b.com"})
        r3 = t({"id": 2, "email": "a@b.com"})
        assert r1["_hash"] == r2["_hash"]
        assert r1["_hash"] != r3["_hash"]

    def test_chain(self):
        from etl.transformers.transformers import chain, RequiredFieldsValidator, TypeCaster
        t = chain(
            RequiredFieldsValidator(["id"]),
            TypeCaster({"id": int}),
        )
        assert t({"id": "5"})["id"] == 5
        assert t({"name": "no_id"}) is None


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

class TestLoaders:
    def test_csv_loader_thread_safe(self, tmp_path):
        from etl.loaders.loaders import CSVLoader
        out = str(tmp_path / "out.csv")
        loader = CSVLoader(out)

        batches = [[{"id": i, "val": f"v{i}"} for i in range(j, j + 100)] for j in range(0, 1000, 100)]
        threads = [threading.Thread(target=loader, args=(b,)) for b in batches]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with open(out) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1000

    def test_sqlite_loader_inserts(self, tmp_path):
        from etl.loaders.loaders import SQLiteLoader
        db = str(tmp_path / "out.db")
        loader = SQLiteLoader(db, table="results")
        batch = [{"id": i, "name": f"user_{i}"} for i in range(50)]
        loader(batch)

        conn = sqlite3.connect(db)
        count = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
        conn.close()
        assert count == 50

    def test_json_loader_writes_jsonl(self, tmp_path):
        from etl.loaders.loaders import JSONLoader
        out = str(tmp_path / "out.jsonl")
        loader = JSONLoader(out)
        loader([{"id": i} for i in range(20)])

        lines = Path(out).read_text().strip().split("\n")
        assert len(lines) == 20
        assert json.loads(lines[0])["id"] == 0


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

class TestPipelineIntegration:
    def test_end_to_end(self, tmp_path):
        """Full CSV → transform → SQLite pipeline integration test."""
        # Write input CSV
        input_csv = tmp_path / "input.csv"
        rows = [{"id": str(i), "name": f"user {i} ", "email": f"u{i}@x.com", "score": str(i * 1.5)}
                for i in range(200)]
        with open(str(input_csv), "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "email", "score"])
            writer.writeheader()
            writer.writerows(rows)

        from etl.core.pipeline import Pipeline, PipelineConfig
        from etl.extractors.file_extractors import CSVExtractor
        from etl.transformers.transformers import RequiredFieldsValidator, TypeCaster, FieldNormalizer, chain
        from etl.loaders.loaders import SQLiteLoader

        output_db = str(tmp_path / "out.db")

        pipeline = Pipeline(PipelineConfig(
            name="integration-test",
            extract_workers=2,
            transform_workers=4,
            load_workers=2,
            batch_size=50,
        ))
        pipeline.add_extractor(CSVExtractor(str(input_csv)))
        pipeline.add_transformer(chain(
            RequiredFieldsValidator(["id", "email"]),
            TypeCaster({"id": int, "score": float}),
            FieldNormalizer(fields=["name"]),
        ))
        pipeline.add_loader(SQLiteLoader(output_db, table="users"))

        report = pipeline.run()

        assert report["status"] == "completed"
        assert report["metrics"]["extracted"] == 200

        conn = sqlite3.connect(output_db)
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        conn.close()
        assert count == 200
