import hashlib
import json
from datetime import datetime, timezone


class ConfigurationError(ValueError):
    pass


class LeaseLost(RuntimeError):
    pass


class CacheFull(RuntimeError):
    pass


def utcnow():
    return datetime.now(timezone.utc)


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=lambda x: x.isoformat(),
    )


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()
