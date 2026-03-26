"""
ETL Pipeline Core Orchestrator
Manages the full Extract → Transform → Load lifecycle using multithreading.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass, field
from enum import Enum
from queue import Queue, Empty
from threading import Event, Lock
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class PipelineStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class PipelineResult:
    stage: str
    success: bool
    records_processed: int = 0
    duration_seconds: float = 0.0
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineConfig:
    name: str
    extract_workers: int = 4
    transform_workers: int = 8
    load_workers: int = 4
    batch_size: int = 500
    queue_maxsize: int = 1000
    timeout_seconds: int = 300
    retry_attempts: int = 3
    retry_delay_seconds: float = 1.0


class Pipeline:
    """
    Thread-safe ETL Pipeline with producer-consumer architecture.

    Architecture:
        [Extractor Threads] → extract_queue → [Transformer Threads] → load_queue → [Loader Threads]

    Each stage runs in its own thread pool. Data flows through bounded queues,
    providing natural backpressure between stages.
    """

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.status = PipelineStatus.IDLE
        self.results: List[PipelineResult] = []

        # Inter-stage queues with bounded capacity (backpressure)
        self._extract_queue: Queue = Queue(maxsize=config.queue_maxsize)
        self._load_queue: Queue = Queue(maxsize=config.queue_maxsize)

        # Coordination signals
        self._extract_done = Event()
        self._transform_done = Event()
        self._stop_event = Event()

        # Thread-safe metrics
        self._metrics_lock = Lock()
        self._metrics: Dict[str, int] = {
            "extracted": 0,
            "transformed": 0,
            "loaded": 0,
            "failed": 0,
            "skipped": 0,
        }

        self._extractors: List[Callable] = []
        self._transformers: List[Callable] = []
        self._loaders: List[Callable] = []
        self._hooks: Dict[str, List[Callable]] = {
            "pre_extract": [],
            "post_extract": [],
            "pre_transform": [],
            "post_transform": [],
            "pre_load": [],
            "post_load": [],
        }

    # ------------------------------------------------------------------
    # Registration API
    # ------------------------------------------------------------------

    def add_extractor(self, fn: Callable) -> "Pipeline":
        self._extractors.append(fn)
        return self

    def add_transformer(self, fn: Callable) -> "Pipeline":
        self._transformers.append(fn)
        return self

    def add_loader(self, fn: Callable) -> "Pipeline":
        self._loaders.append(fn)
        return self

    def add_hook(self, event: str, fn: Callable) -> "Pipeline":
        if event not in self._hooks:
            raise ValueError(f"Unknown hook event: {event}")
        self._hooks[event].append(fn)
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        """Execute the full ETL pipeline. Returns a summary report."""
        if self.status == PipelineStatus.RUNNING:
            raise RuntimeError("Pipeline is already running.")

        self.status = PipelineStatus.RUNNING
        start_time = time.perf_counter()
        logger.info(f"[{self.config.name}] Pipeline starting...")

        try:
            self._run_hooks("pre_extract")
            extract_result = self._run_extract_stage()
            self._run_hooks("post_extract")

            self._run_hooks("pre_transform")
            transform_result = self._run_transform_stage()
            self._run_hooks("post_transform")

            self._run_hooks("pre_load")
            load_result = self._run_load_stage()
            self._run_hooks("post_load")

            self.results.extend([extract_result, transform_result, load_result])
            self.status = PipelineStatus.COMPLETED

        except Exception as exc:
            logger.exception(f"[{self.config.name}] Pipeline failed: {exc}")
            self.status = PipelineStatus.FAILED
            raise

        elapsed = time.perf_counter() - start_time
        report = self._build_report(elapsed)
        logger.info(f"[{self.config.name}] Pipeline completed in {elapsed:.2f}s → {report}")
        return report

    def cancel(self):
        """Signal all worker threads to stop gracefully."""
        logger.warning(f"[{self.config.name}] Cancellation requested.")
        self._stop_event.set()
        self.status = PipelineStatus.CANCELLED

    # ------------------------------------------------------------------
    # Stage runners
    # ------------------------------------------------------------------

    def _run_extract_stage(self) -> PipelineResult:
        start = time.perf_counter()
        logger.info(f"EXTRACT stage starting with {self.config.extract_workers} workers")

        futures: List[Future] = []
        with ThreadPoolExecutor(
            max_workers=self.config.extract_workers,
            thread_name_prefix="extractor",
        ) as pool:
            for extractor in self._extractors:
                futures.append(pool.submit(self._extraction_worker, extractor))

            errors = []
            for fut in as_completed(futures):
                exc = fut.exception()
                if exc:
                    errors.append(str(exc))
                    logger.error(f"Extractor failed: {exc}")

        self._extract_done.set()

        if errors and not self._metrics["extracted"]:
            raise RuntimeError(f"All extractors failed: {errors}")

        duration = time.perf_counter() - start
        records = self._metrics["extracted"]
        logger.info(f"EXTRACT done — {records} records in {duration:.2f}s")
        return PipelineResult("extract", True, records, duration)

    def _run_transform_stage(self) -> PipelineResult:
        start = time.perf_counter()
        logger.info(f"TRANSFORM stage starting with {self.config.transform_workers} workers")

        with ThreadPoolExecutor(
            max_workers=self.config.transform_workers,
            thread_name_prefix="transformer",
        ) as pool:
            futures = [
                pool.submit(self._transform_worker)
                for _ in range(self.config.transform_workers)
            ]
            for fut in as_completed(futures):
                if exc := fut.exception():
                    logger.error(f"Transform worker error: {exc}")

        self._transform_done.set()

        duration = time.perf_counter() - start
        records = self._metrics["transformed"]
        logger.info(f"TRANSFORM done — {records} records in {duration:.2f}s")
        return PipelineResult("transform", True, records, duration)

    def _run_load_stage(self) -> PipelineResult:
        start = time.perf_counter()
        logger.info(f"LOAD stage starting with {self.config.load_workers} workers")

        with ThreadPoolExecutor(
            max_workers=self.config.load_workers,
            thread_name_prefix="loader",
        ) as pool:
            futures = [
                pool.submit(self._load_worker)
                for _ in range(self.config.load_workers)
            ]
            for fut in as_completed(futures):
                if exc := fut.exception():
                    logger.error(f"Load worker error: {exc}")

        duration = time.perf_counter() - start
        records = self._metrics["loaded"]
        logger.info(f"LOAD done — {records} records in {duration:.2f}s")
        return PipelineResult("load", True, records, duration)

    # ------------------------------------------------------------------
    # Worker functions (run inside threads)
    # ------------------------------------------------------------------

    def _extraction_worker(self, extractor: Callable):
        """Pull records from a source and push batches onto the extract queue."""
        batch = []
        for record in extractor():
            if self._stop_event.is_set():
                break
            batch.append(record)
            if len(batch) >= self.config.batch_size:
                self._extract_queue.put(batch, timeout=self.config.timeout_seconds)
                self._increment("extracted", len(batch))
                batch = []

        if batch:
            self._extract_queue.put(batch, timeout=self.config.timeout_seconds)
            self._increment("extracted", len(batch))

    def _transform_worker(self):
        """Consume raw batches, apply all transformers, push to load queue."""
        while not (self._extract_done.is_set() and self._extract_queue.empty()):
            if self._stop_event.is_set():
                break
            try:
                batch = self._extract_queue.get(timeout=0.5)
            except Empty:
                continue

            transformed = []
            for record in batch:
                try:
                    result = record
                    for transformer in self._transformers:
                        result = transformer(result)
                        if result is None:
                            self._increment("skipped")
                            break
                    else:
                        transformed.append(result)
                        self._increment("transformed")
                except Exception as exc:
                    self._increment("failed")
                    logger.warning(f"Transform error on record: {exc}")

            if transformed:
                self._load_queue.put(transformed, timeout=self.config.timeout_seconds)

            self._extract_queue.task_done()

    def _load_worker(self):
        """Consume transformed batches and write via all registered loaders."""
        while not (self._transform_done.is_set() and self._load_queue.empty()):
            if self._stop_event.is_set():
                break
            try:
                batch = self._load_queue.get(timeout=0.5)
            except Empty:
                continue

            for loader in self._loaders:
                attempt = 0
                while attempt < self.config.retry_attempts:
                    try:
                        loader(batch)
                        self._increment("loaded", len(batch))
                        break
                    except Exception as exc:
                        attempt += 1
                        logger.warning(f"Loader attempt {attempt} failed: {exc}")
                        if attempt < self.config.retry_attempts:
                            time.sleep(self.config.retry_delay_seconds * attempt)
                        else:
                            self._increment("failed", len(batch))
                            logger.error(f"Loader gave up after {attempt} attempts.")

            self._load_queue.task_done()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _run_hooks(self, event: str):
        for hook in self._hooks.get(event, []):
            hook(self)

    def _increment(self, key: str, amount: int = 1):
        with self._metrics_lock:
            self._metrics[key] += amount

    def _build_report(self, elapsed: float) -> Dict[str, Any]:
        return {
            "pipeline": self.config.name,
            "status": self.status.value,
            "duration_seconds": round(elapsed, 3),
            "metrics": dict(self._metrics),
            "stages": [
                {
                    "name": r.stage,
                    "records": r.records_processed,
                    "duration_seconds": round(r.duration_seconds, 3),
                }
                for r in self.results
            ],
        }

    @property
    def metrics(self) -> Dict[str, int]:
        with self._metrics_lock:
            return dict(self._metrics)
