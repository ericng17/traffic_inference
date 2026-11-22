import fcntl
import os
import time
from contextlib import contextmanager
from pathlib import Path

from traffic_pipeline.core.common import CacheFull, ConfigurationError, digest, sha256_file
from traffic_pipeline.data.storage import source_stream


class Cache:
    """Node-local byte limit and process-safe pins; downloads serialize under the budget lock."""

    def __init__(self, directory, max_bytes, attempts=3):
        self.root = Path(directory)
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_bytes, self.attempts = max_bytes, attempts
        self.metrics = {"hit": 0, "download_bytes": 0, "download_seconds": 0.0}

    def _usage(self):
        return int((self.root / "used_bytes").read_text())

    def _set_usage(self, size):
        (self.root / "used_bytes").write_text(str(size))

    def _recover(self):
        # O(1) quota checks normally; scan only after an interrupted cache mutation.
        if not (self.root / "used_bytes").exists() or (self.root / "dirty").exists():
            for partial in self.root.glob("*.partial"):
                partial.unlink(missing_ok=True)
            self._set_usage(sum(p.stat().st_size for p in self.root.glob("*.blob")))
            (self.root / "dirty").unlink(missing_ok=True)

    @contextmanager
    def get(self, uri, checksum=None, expected_bytes=None, refresh=False):
        key = digest([uri, checksum])
        path = self.root / (key + ".blob")
        with open(self.root / (key + ".lock"), "a+b") as pin:
            fcntl.flock(pin, fcntl.LOCK_EX)
            with open(self.root / "budget.lock", "a+b") as budget:
                fcntl.flock(budget, fcntl.LOCK_EX)
                self._recover()
                if refresh:
                    if path.exists():
                        (self.root / "dirty").touch()
                        self._set_usage(self._usage() - path.stat().st_size)
                        path.unlink()
                if path.exists() and (not checksum or sha256_file(path) == checksum):
                    self.metrics["hit"] += 1
                else:
                    (self.root / "dirty").touch()
                    if path.exists():
                        self._set_usage(self._usage() - path.stat().st_size)
                        path.unlink()
                    self._download(uri, path, checksum, expected_bytes)
                (self.root / "dirty").unlink(missing_ok=True)
                os.utime(path, None)
            try:
                yield path
            finally:
                os.utime(path, None)

    def _room(self, needed, active_key):
        used = self._usage()
        if used + needed <= self.max_bytes:
            return
        files = sorted(self.root.glob("*.blob"), key=lambda p: p.stat().st_mtime)
        # Evict a little ahead to amortize the full LRU scan across many small images.
        target = max(needed, min(self.max_bytes, needed + self.max_bytes // 10))
        for candidate in files:
            if used + target <= self.max_bytes:
                break
            if candidate.stem == active_key:
                continue
            with open(candidate.with_suffix(".lock"), "a+b") as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    continue
                used -= candidate.stat().st_size
                candidate.unlink()
                self._set_usage(used)
        if used + needed > self.max_bytes:
            raise CacheFull("CACHE_FULL: active files pinned or object exceeds cache budget")

    def _download(self, uri, target, checksum, expected_bytes):
        started = time.monotonic()
        temp = target.with_suffix(".partial")
        for attempt in range(self.attempts):
            try:
                size = 0
                self._room(expected_bytes or 0, target.stem)
                with source_stream(uri) as source, open(temp, "wb") as dest:
                    for block in iter(lambda: source.read(1024 * 1024), b""):
                        size += len(block)
                        self._room(size, target.stem)
                        dest.write(block)
                    dest.flush()
                    os.fsync(dest.fileno())
                if expected_bytes is not None and size != expected_bytes:
                    raise IOError("SOURCE_SIZE_MISMATCH")
                if checksum and sha256_file(temp) != checksum:
                    raise ConfigurationError("SOURCE_CHECKSUM_MISMATCH")
                os.replace(temp, target)
                self._set_usage(self._usage() + size)
                self.metrics["download_bytes"] += size
                self.metrics["download_seconds"] += time.monotonic() - started
                return
            except (ConfigurationError, CacheFull):
                raise
            except Exception:
                if attempt + 1 == self.attempts:
                    # Avoid persisting exception strings containing authorization URLs.
                    raise IOError("SOURCE_UNAVAILABLE") from None
                time.sleep(min(2**attempt, 4))
            finally:
                temp.unlink(missing_ok=True)
