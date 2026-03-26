"""
SQL Database Extractor — Thread-safe extraction with connection pooling.
Supports SQLite (zero-config) and any SQLAlchemy-compatible database.
"""

import logging
import queue
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection Pool
# ---------------------------------------------------------------------------

class SQLiteConnectionPool:
    """
    Thread-safe SQLite connection pool.

    SQLite connections cannot be shared across threads, so each thread
    gets its own connection from the pool. Connections are returned after use.
    """

    def __init__(self, db_path: str, pool_size: int = 8):
        self.db_path = db_path
        self._pool: queue.Queue = queue.Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._created = 0
        self._pool_size = pool_size
        self._local = threading.local()

        # Pre-warm the pool
        for _ in range(pool_size):
            conn = self._create_connection()
            self._pool.put(conn)

        logger.info(f"[SQLiteConnectionPool] Pool ready: {pool_size} connections to '{db_path}'")

    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")   # Concurrent reads
        conn.execute("PRAGMA cache_size=-64000")  # 64 MB cache
        conn.execute("PRAGMA synchronous=NORMAL")
        with self._lock:
            self._created += 1
        return conn

    @contextmanager
    def get_connection(self, timeout: float = 5.0):
        conn = None
        try:
            conn = self._pool.get(timeout=timeout)
            yield conn
        finally:
            if conn is not None:
                try:
                    conn.rollback()  # Ensure clean state
                    self._pool.put_nowait(conn)
                except queue.Full:
                    conn.close()

    def close_all(self):
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except queue.Empty:
                break
        logger.info("[SQLiteConnectionPool] All connections closed.")

    @property
    def stats(self) -> Dict[str, int]:
        return {
            "pool_size": self._pool_size,
            "available": self._pool.qsize(),
            "in_use": self._pool_size - self._pool.qsize(),
        }


# ---------------------------------------------------------------------------
# SQLite Extractor
# ---------------------------------------------------------------------------

@dataclass
class SQLiteExtractorConfig:
    db_path: str
    query: str
    params: tuple = ()
    chunk_size: int = 1000
    pool_size: int = 8
    parallel_chunks: int = 4


class SQLiteExtractor:
    """
    Extract rows from SQLite using chunked, parallel reads.

    For large tables, the extractor splits the query into LIMIT/OFFSET chunks
    and reads them concurrently using multiple connections from the pool.
    This dramatically speeds up extraction on read-heavy workloads.
    """

    def __init__(self, config: SQLiteExtractorConfig):
        self.config = config
        self._pool = SQLiteConnectionPool(config.db_path, pool_size=config.pool_size)

    def __call__(self) -> Generator[Dict[str, Any], None, None]:
        total_rows = self._count_rows()
        logger.info(f"[SQLiteExtractor] {total_rows} rows to extract (chunk={self.config.chunk_size})")

        if total_rows == 0:
            return

        # Build chunks: list of (offset, limit) tuples
        chunks = [
            (offset, self.config.chunk_size)
            for offset in range(0, total_rows, self.config.chunk_size)
        ]

        yield from self._read_chunks_parallel(chunks)
        self._pool.close_all()

    def _count_rows(self) -> int:
        count_query = f"SELECT COUNT(*) FROM ({self.config.query})"
        with self._pool.get_connection() as conn:
            cursor = conn.execute(count_query, self.config.params)
            return cursor.fetchone()[0]

    def _read_chunk(self, offset: int, limit: int) -> List[Dict[str, Any]]:
        paginated = f"{self.config.query} LIMIT ? OFFSET ?"
        params = (*self.config.params, limit, offset)

        start = time.perf_counter()
        with self._pool.get_connection() as conn:
            cursor = conn.execute(paginated, params)
            rows = [dict(row) for row in cursor.fetchall()]

        elapsed = time.perf_counter() - start
        logger.debug(f"Chunk offset={offset} → {len(rows)} rows in {elapsed:.3f}s")
        return rows

    def _read_chunks_parallel(self, chunks: List[tuple]) -> Generator[Dict[str, Any], None, None]:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        results: Dict[int, List[Dict]] = {}

        with ThreadPoolExecutor(
            max_workers=self.config.parallel_chunks,
            thread_name_prefix="sql-reader",
        ) as pool:
            future_to_chunk = {
                pool.submit(self._read_chunk, offset, limit): (i, offset)
                for i, (offset, limit) in enumerate(chunks)
            }

            for future in as_completed(future_to_chunk):
                chunk_idx, offset = future_to_chunk[future]
                try:
                    results[chunk_idx] = future.result()
                except Exception as exc:
                    logger.error(f"Chunk at offset {offset} failed: {exc}")
                    results[chunk_idx] = []

        # Yield in original order to preserve record sequence
        for idx in sorted(results.keys()):
            yield from results[idx]


# ---------------------------------------------------------------------------
# Table Partitioner — splits a table into N parallel extractors
# ---------------------------------------------------------------------------

class TablePartitioner:
    """
    Splits a large table into N partitions for maximum parallel extraction.
    Each partition is an independent SQLiteExtractor reading a non-overlapping slice.

    Example:
        partitions = TablePartitioner("data.db", "orders", n=4)
        for extractor in partitions.extractors():
            pipeline.add_extractor(extractor)
    """

    def __init__(
        self,
        db_path: str,
        table: str,
        n_partitions: int = 4,
        partition_column: str = "rowid",
        chunk_size: int = 500,
    ):
        self.db_path = db_path
        self.table = table
        self.n_partitions = n_partitions
        self.partition_column = partition_column
        self.chunk_size = chunk_size

    def extractors(self) -> List[SQLiteExtractor]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            f"SELECT MIN({self.partition_column}), MAX({self.partition_column}) FROM {self.table}"
        )
        row = cur.fetchone()
        conn.close()

        if not row or row[0] is None:
            return []

        min_id, max_id = row[0], row[1]
        range_size = (max_id - min_id) // self.n_partitions
        parts = []

        for i in range(self.n_partitions):
            lo = min_id + i * range_size
            hi = lo + range_size if i < self.n_partitions - 1 else max_id + 1
            query = (
                f"SELECT * FROM {self.table} "
                f"WHERE {self.partition_column} >= {lo} AND {self.partition_column} < {hi}"
            )
            cfg = SQLiteExtractorConfig(
                db_path=self.db_path,
                query=query,
                chunk_size=self.chunk_size,
                pool_size=2,
                parallel_chunks=2,
            )
            parts.append(SQLiteExtractor(cfg))

        logger.info(
            f"[TablePartitioner] '{self.table}' split into {len(parts)} partitions "
            f"over {self.partition_column} [{min_id}, {max_id}]"
        )
        return parts
