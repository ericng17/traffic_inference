import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

import fsspec

from traffic_pipeline.core.common import ConfigurationError, canonical, sha256_file
from traffic_pipeline.core.config import safe_uri


def join_uri(base, *parts):
    return base.rstrip("/") + "/" + "/".join(str(p).strip("/") for p in parts)


def filesystem(uri):
    safe_uri(uri)
    options = {}
    if uri.startswith("s3://"):
        options = {
            "client_kwargs": {"endpoint_url": os.getenv("OBJECT_STORE_ENDPOINT") or None},
            "config_kwargs": {"connect_timeout": 15, "read_timeout": 60, "retries": {"max_attempts": 2}},
        }
    return fsspec.core.url_to_fs(uri, **options)


@contextmanager
def source_stream(uri):
    safe_uri(uri)
    if uri.startswith("hf://datasets/"):
        import requests
        from huggingface_hub import hf_hub_url, get_token

        # owner/name@commit/path -- tokens are resolved only in this process.
        owner, rest = uri[len("hf://datasets/") :].split("/", 1)
        name_rev, path = rest.split("/", 1)
        name, revision = name_rev.split("@", 1)
        token = os.getenv("HF_TOKEN") or get_token()
        headers = {"Authorization": "Bearer " + token} if token else {}
        url = hf_hub_url(f"{owner}/{name}", path, repo_type="dataset", revision=revision)
        with requests.get(url, headers=headers, stream=True, timeout=(15, 60)) as response:
            response.raise_for_status()
            response.raw.decode_content = True
            yield response.raw
    else:
        fs, path = filesystem(uri)
        with fs.open(path, "rb") as stream:
            yield stream


def put_file(local, uri, immutable=False):
    fs, path = filesystem(uri)
    fs.makedirs(str(Path(path).parent), exist_ok=True)
    if immutable and fs.exists(path):
        with fs.open(path, "rb") as f:
            import hashlib

            h = hashlib.sha256()
            for block in iter(lambda: f.read(1024 * 1024), b""):
                h.update(block)
        if h.hexdigest() != sha256_file(local):
            raise ConfigurationError("immutable object already exists with different content")
        return
    if urlsplit(uri).scheme in ("", "file"):
        target = Path(path)
        with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as f:
            temp = Path(f.name)
            with open(local, "rb") as src:
                import shutil

                shutil.copyfileobj(src, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, target)
        directory_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    else:
        fs.put_file(str(local), path)
    if fs.size(path) != Path(local).stat().st_size:
        raise IOError("upload size mismatch")


def write_json(uri, value, immutable=False):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as f:
        f.write(canonical(value))
        f.flush()
        put_file(f.name, uri, immutable=immutable)


def read_json(uri):
    with source_stream(uri) as f:
        return json.load(f)
