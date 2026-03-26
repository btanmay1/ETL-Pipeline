"""
Extractors — CSV and JSON file sources with concurrent multi-file support.
"""

import csv
import glob
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Union

logger = logging.getLogger(__name__)

PathLike = Union[str, Path]


# ---------------------------------------------------------------------------
# CSV Extractor
# ---------------------------------------------------------------------------

class CSVExtractor:
    """
    Extract records from one or many CSV files.

    When multiple files are given, they are read concurrently using a thread pool,
    and their records are merged into a single stream.
    """

    def __init__(
        self,
        paths: Union[PathLike, List[PathLike], str],
        encoding: str = "utf-8",
        delimiter: str = ",",
        max_workers: int = 4,
        glob_pattern: Optional[str] = None,
    ):
        if glob_pattern:
            self._paths = [Path(p) for p in glob.glob(glob_pattern, recursive=True)]
        elif isinstance(paths, (str, Path)):
            self._paths = [Path(paths)]
        else:
            self._paths = [Path(p) for p in paths]

        self.encoding = encoding
        self.delimiter = delimiter
        self.max_workers = max_workers

        if not self._paths:
            raise FileNotFoundError("No CSV files found for the given paths/pattern.")

    def __call__(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all records across all files, reading concurrently."""
        if len(self._paths) == 1:
            yield from self._read_file(self._paths[0])
            return

        # Multi-file: read in parallel, stream results
        results: List[List[Dict]] = [None] * len(self._paths)
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="csv-reader") as pool:
            future_to_idx = {
                pool.submit(list, self._read_file(path)): i
                for i, path in enumerate(self._paths)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                    logger.debug(f"Read {len(results[idx])} rows from {self._paths[idx]}")
                except Exception as exc:
                    logger.error(f"Failed to read {self._paths[idx]}: {exc}")
                    results[idx] = []

        for batch in results:
            yield from batch

    def _read_file(self, path: Path) -> Generator[Dict[str, Any], None, None]:
        logger.info(f"[CSVExtractor] Reading {path}")
        with open(path, newline="", encoding=self.encoding) as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            for row in reader:
                yield dict(row)


# ---------------------------------------------------------------------------
# JSON Extractor
# ---------------------------------------------------------------------------

class JSONExtractor:
    """
    Extract records from JSON files.

    Supports:
        - A JSON array at the top level: [{...}, {...}]
        - A JSON Lines file (.jsonl): one JSON object per line
        - A JSON object with a nested array (specify `record_path`)
    """

    def __init__(
        self,
        paths: Union[PathLike, List[PathLike]],
        record_path: Optional[str] = None,
        encoding: str = "utf-8",
        max_workers: int = 4,
        jsonlines: bool = False,
    ):
        if isinstance(paths, (str, Path)):
            self._paths = [Path(paths)]
        else:
            self._paths = [Path(p) for p in paths]

        self.record_path = record_path
        self.encoding = encoding
        self.max_workers = max_workers
        self.jsonlines = jsonlines

    def __call__(self) -> Generator[Dict[str, Any], None, None]:
        if len(self._paths) == 1:
            yield from self._read_file(self._paths[0])
            return

        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="json-reader") as pool:
            futures = {pool.submit(list, self._read_file(p)): p for p in self._paths}
            for future in as_completed(futures):
                try:
                    yield from future.result()
                except Exception as exc:
                    logger.error(f"Failed reading {futures[future]}: {exc}")

    def _read_file(self, path: Path) -> Generator[Dict[str, Any], None, None]:
        logger.info(f"[JSONExtractor] Reading {path}")
        with open(path, encoding=self.encoding) as f:
            if self.jsonlines:
                for line in f:
                    line = line.strip()
                    if line:
                        yield json.loads(line)
            else:
                data = json.load(f)
                if self.record_path:
                    for key in self.record_path.split("."):
                        data = data[key]
                if isinstance(data, list):
                    yield from data
                elif isinstance(data, dict):
                    yield data
                else:
                    raise ValueError(f"Unexpected JSON structure in {path}")


# ---------------------------------------------------------------------------
# Directory Extractor (auto-detect format)
# ---------------------------------------------------------------------------

class DirectoryExtractor:
    """
    Scan a directory and extract all CSV and JSON files concurrently.
    Useful for processing data lake drops where mixed formats coexist.
    """

    def __init__(self, directory: PathLike, recursive: bool = False, max_workers: int = 8):
        self.directory = Path(directory)
        self.recursive = recursive
        self.max_workers = max_workers

        if not self.directory.is_dir():
            raise NotADirectoryError(f"{directory} is not a valid directory.")

    def __call__(self) -> Generator[Dict[str, Any], None, None]:
        pattern = "**/*" if self.recursive else "*"
        files = list(self.directory.glob(pattern))
        csv_files = [f for f in files if f.suffix.lower() == ".csv"]
        json_files = [f for f in files if f.suffix.lower() in (".json", ".jsonl")]

        logger.info(
            f"[DirectoryExtractor] Found {len(csv_files)} CSV, {len(json_files)} JSON files in {self.directory}"
        )

        extractors = []
        if csv_files:
            extractors.append(CSVExtractor(csv_files, max_workers=self.max_workers // 2 or 1))
        if json_files:
            extractors.append(JSONExtractor(json_files, max_workers=self.max_workers // 2 or 1))

        with ThreadPoolExecutor(max_workers=len(extractors) or 1, thread_name_prefix="dir-reader") as pool:
            futures = {pool.submit(list, e()): e for e in extractors}
            for future in as_completed(futures):
                try:
                    yield from future.result()
                except Exception as exc:
                    logger.error(f"Extractor failed: {exc}")
