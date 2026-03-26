"""
Loaders — Thread-safe batch writers for CSV, JSON, and SQLite targets.
"""

import csv
import json
import logging
import sqlite3
import threading
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


# ---------------------------------------------------------------------------
# CSV Loader
# ---------------------------------------------------------------------------

class CSVLoader:
    """
    Write records to a CSV file in a thread-safe manner.

    All loader threads share one CSVLoader instance. A lock ensures
    that batches are written atomically without interleaving.
    """

    def __init__(
        self,
        output_path: PathLike,
        fieldnames: Optional[List[str]] = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
        write_header: bool = True,
    ):
        self.output_path = Path(output_path)
        self.fieldnames = fieldnames
        self.delimiter = delimiter
        self.encoding = encoding
        self.write_header = write_header

        self._lock = threading.Lock()
        self._header_written = False
        self._rows_written = 0

        # Create parent directories if needed
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        # Truncate the file on init
        self.output_path.write_text("", encoding=encoding)

    def __call__(self, batch: List[Dict[str, Any]]):
        if not batch:
            return

        with self._lock:
            fields = self.fieldnames or list(batch[0].keys())

            with open(self.output_path, "a", newline="", encoding=self.encoding) as f:
                writer = csv.DictWriter(f, fieldnames=fields, delimiter=self.delimiter, extrasaction="ignore")

                if self.write_header and not self._header_written:
                    writer.writeheader()
                    self._header_written = True

                writer.writerows(batch)
                self._rows_written += len(batch)

        logger.debug(f"[CSVLoader] Wrote {len(batch)} rows → {self.output_path} (total: {self._rows_written})")

    @property
    def rows_written(self) -> int:
        return self._rows_written


# ---------------------------------------------------------------------------
# JSON Loader (JSON Lines)
# ---------------------------------------------------------------------------

class JSONLoader:
    """
    Append records to a JSON Lines file (.jsonl), one record per line.
    Thread-safe via a write lock.
    """

    def __init__(
        self,
        output_path: PathLike,
        encoding: str = "utf-8",
        ensure_ascii: bool = False,
    ):
        self.output_path = Path(output_path)
        self.encoding = encoding
        self.ensure_ascii = ensure_ascii

        self._lock = threading.Lock()
        self._rows_written = 0

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.write_text("", encoding=encoding)

    def __call__(self, batch: List[Dict[str, Any]]):
        if not batch:
            return

        lines = "\n".join(
            json.dumps(record, ensure_ascii=self.ensure_ascii, default=str)
            for record in batch
        ) + "\n"

        with self._lock:
            with open(self.output_path, "a", encoding=self.encoding) as f:
                f.write(lines)
            self._rows_written += len(batch)

        logger.debug(f"[JSONLoader] Wrote {len(batch)} records → {self.output_path}")

    @property
    def rows_written(self) -> int:
        return self._rows_written


# ---------------------------------------------------------------------------
# SQLite Loader
# ---------------------------------------------------------------------------

class SQLiteLoader:
    """
    Insert transformed records into a SQLite table.

    Features:
        - Auto-creates the table from the first batch's keys (if not exists)
        - Uses executemany for efficient bulk inserts
        - Thread-safe: serializes writes with a lock (WAL mode for reads)
        - Supports INSERT OR REPLACE for upsert behaviour
    """

    def __init__(
        self,
        db_path: PathLike,
        table: str,
        upsert: bool = False,
        batch_commit: bool = True,
    ):
        self.db_path = Path(db_path)
        self.table = table
        self.upsert = upsert
        self.batch_commit = batch_commit

        self._lock = threading.Lock()
        self._rows_written = 0
        self._table_created = False

        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Enable WAL for concurrent reads during writes
        conn = sqlite3.connect(str(self.db_path))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.close()

    def __call__(self, batch: List[Dict[str, Any]]):
        if not batch:
            return

        with self._lock:
            conn = sqlite3.connect(str(self.db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            try:
                columns = list(batch[0].keys())

                if not self._table_created:
                    self._create_table(conn, columns)
                    self._table_created = True

                verb = "INSERT OR REPLACE" if self.upsert else "INSERT OR IGNORE"
                placeholders = ", ".join(["?"] * len(columns))
                col_names = ", ".join(f'"{c}"' for c in columns)
                sql = f'{verb} INTO "{self.table}" ({col_names}) VALUES ({placeholders})'

                rows = [tuple(record.get(c) for c in columns) for record in batch]
                conn.executemany(sql, rows)

                if self.batch_commit:
                    conn.commit()

                self._rows_written += len(batch)
                logger.debug(f"[SQLiteLoader] Inserted {len(batch)} rows → {self.table}")
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def _create_table(self, conn: sqlite3.Connection, columns: List[str]):
        col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
        conn.execute(f'CREATE TABLE IF NOT EXISTS "{self.table}" ({col_defs})')
        conn.commit()
        logger.info(f"[SQLiteLoader] Table '{self.table}' ready in {self.db_path}")

    @property
    def rows_written(self) -> int:
        return self._rows_written


# ---------------------------------------------------------------------------
# Multi-Target Loader — fan-out to multiple loaders simultaneously
# ---------------------------------------------------------------------------

class MultiLoader:
    """
    Fan out each batch to multiple loaders concurrently.
    Useful for writing to both CSV and SQLite in a single pass.
    """

    def __init__(self, *loaders, max_workers: int = 4):
        self.loaders = list(loaders)
        self.max_workers = max_workers

    def __call__(self, batch: List[Dict[str, Any]]):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(self.loaders))) as pool:
            futures = {pool.submit(loader, batch): loader for loader in self.loaders}
            for future in as_completed(futures):
                if exc := future.exception():
                    logger.error(f"Loader {futures[future].__class__.__name__} failed: {exc}")
