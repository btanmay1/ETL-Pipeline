"""
Thread Pool Manager — Dynamic worker pool with health monitoring and adaptive scaling.
"""

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class WorkerStats:
    worker_id: str
    tasks_completed: int = 0
    tasks_failed: int = 0
    total_duration: float = 0.0
    last_active: float = field(default_factory=time.time)

    @property
    def avg_duration(self) -> float:
        if self.tasks_completed == 0:
            return 0.0
        return self.total_duration / self.tasks_completed


class ThreadPoolManager:
    """
    Manages a named collection of ThreadPoolExecutors.
    Supports dynamic resizing, per-pool stats, and graceful shutdown.
    """

    def __init__(self):
        self._pools: Dict[str, ThreadPoolExecutor] = {}
        self._pool_configs: Dict[str, Dict[str, Any]] = {}
        self._stats: Dict[str, List[WorkerStats]] = {}
        self._lock = threading.RLock()
        self._shutdown = threading.Event()

    def create_pool(
        self,
        name: str,
        max_workers: int,
        thread_name_prefix: Optional[str] = None,
    ) -> ThreadPoolExecutor:
        with self._lock:
            if name in self._pools:
                logger.warning(f"Pool '{name}' already exists. Returning existing pool.")
                return self._pools[name]

            prefix = thread_name_prefix or name
            pool = ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix=prefix,
            )
            self._pools[name] = pool
            self._pool_configs[name] = {"max_workers": max_workers, "created_at": time.time()}
            self._stats[name] = []
            logger.info(f"Pool '{name}' created with {max_workers} workers.")
            return pool

    def submit(self, pool_name: str, fn: Callable, *args, **kwargs) -> Future:
        """Submit a task to a named pool, wrapping it with timing and stats."""
        with self._lock:
            pool = self._pools.get(pool_name)
            if pool is None:
                raise KeyError(f"Pool '{pool_name}' not found.")

        def tracked(*a, **kw):
            start = time.perf_counter()
            worker_id = threading.current_thread().name
            try:
                result = fn(*a, **kw)
                elapsed = time.perf_counter() - start
                self._record_success(pool_name, worker_id, elapsed)
                return result
            except Exception as exc:
                self._record_failure(pool_name, worker_id)
                raise

        return pool.submit(tracked, *args, **kwargs)

    def resize_pool(self, name: str, new_max_workers: int):
        """Shut down old pool and replace with a resized one (graceful)."""
        with self._lock:
            if name not in self._pools:
                raise KeyError(f"Pool '{name}' not found.")
            old_pool = self._pools.pop(name)

        logger.info(f"Resizing pool '{name}' → {new_max_workers} workers...")
        old_pool.shutdown(wait=True)

        with self._lock:
            config = self._pool_configs[name]
            config["max_workers"] = new_max_workers
            self._pools[name] = ThreadPoolExecutor(
                max_workers=new_max_workers,
                thread_name_prefix=name,
            )
        logger.info(f"Pool '{name}' resized successfully.")

    def shutdown_pool(self, name: str, wait: bool = True):
        with self._lock:
            pool = self._pools.pop(name, None)
        if pool:
            pool.shutdown(wait=wait)
            logger.info(f"Pool '{name}' shut down.")

    def shutdown_all(self, wait: bool = True):
        with self._lock:
            pools = list(self._pools.items())
            self._pools.clear()

        for name, pool in pools:
            pool.shutdown(wait=wait)
            logger.info(f"Pool '{name}' shut down.")

        self._shutdown.set()

    def get_pool_stats(self, name: str) -> Dict[str, Any]:
        with self._lock:
            config = self._pool_configs.get(name, {})
            worker_stats = self._stats.get(name, [])

        total_completed = sum(w.tasks_completed for w in worker_stats)
        total_failed = sum(w.tasks_failed for w in worker_stats)
        avg_duration = (
            sum(w.total_duration for w in worker_stats) / total_completed
            if total_completed
            else 0.0
        )

        return {
            "pool": name,
            "max_workers": config.get("max_workers"),
            "active_workers": len(worker_stats),
            "tasks_completed": total_completed,
            "tasks_failed": total_failed,
            "avg_task_duration_seconds": round(avg_duration, 4),
        }

    def get_all_stats(self) -> List[Dict[str, Any]]:
        with self._lock:
            names = list(self._pools.keys())
        return [self.get_pool_stats(n) for n in names]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_or_create_worker_stat(self, pool_name: str, worker_id: str) -> WorkerStats:
        stats_list = self._stats[pool_name]
        for s in stats_list:
            if s.worker_id == worker_id:
                return s
        stat = WorkerStats(worker_id=worker_id)
        stats_list.append(stat)
        return stat

    def _record_success(self, pool_name: str, worker_id: str, duration: float):
        with self._lock:
            s = self._get_or_create_worker_stat(pool_name, worker_id)
            s.tasks_completed += 1
            s.total_duration += duration
            s.last_active = time.time()

    def _record_failure(self, pool_name: str, worker_id: str):
        with self._lock:
            s = self._get_or_create_worker_stat(pool_name, worker_id)
            s.tasks_failed += 1
            s.last_active = time.time()


# Singleton instance for convenient shared access
pool_manager = ThreadPoolManager()
