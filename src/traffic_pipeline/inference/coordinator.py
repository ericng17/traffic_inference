import logging
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor

from traffic_pipeline.core.common import ConfigurationError, LeaseLost, digest
from traffic_pipeline.core.config import Config
from traffic_pipeline.inference.detector import is_oom
from traffic_pipeline.data.discovery import verify_artifacts
from traffic_pipeline.data.cache import Cache
from traffic_pipeline.inference.gpu_actor import GPUActor

log = logging.getLogger(__name__)


def next_batch(current, policy):
    return max((n for n in policy if n < current), default=None)


def resume_config(state, run_id):
    run = state.get_run(run_id)
    config = Config.model_validate(run["config"])
    verify_artifacts(config, Cache(config.execution.cache_dir, config.execution.cache_max_bytes))
    if digest(config.spec(run["discovery"])) != run["spec_hash"]:
        raise ConfigurationError("RESUME_SPEC_OR_DEPENDENCIES_MISMATCH")
    return config


def work(state, run_id, actors=None, executor="ray"):
    cfg = resume_config(state, run_id)
    count = actors if actors is not None else cfg.execution.gpu_actors
    if count < 1:
        raise ConfigurationError("gpu actors must be positive")
    if executor == "local" and cfg.detector.backend != "fake":
        raise ConfigurationError("local executor is only for fake-detector tests")
    if executor == "ray":
        import ray

        address = os.getenv("RAY_ADDRESS") or None
        options = (
            {} if address else {"object_store_memory": int(os.getenv("RAY_OBJECT_STORE_BYTES", "134217728"))}
        )
        ray.init(address=address, ignore_reinit_error=True, include_dashboard=False, **options)
        gpu = 0 if cfg.detector.backend == "fake" else 1
        if ray.cluster_resources().get("GPU", 0) < count * gpu:
            raise ConfigurationError("INSUFFICIENT_RAY_GPUS")
        Actor = ray.remote(num_gpus=gpu, num_cpus=1, max_restarts=0, max_task_retries=0)(GPUActor)
        pool = None
    else:
        pool = ThreadPoolExecutor(max_workers=count)
    slots = []

    def create(batch):
        if executor == "ray":
            actor = Actor.remote(cfg.model_dump(), batch)
            try:
                ray.get(actor.ready.remote(), timeout=180)
            except Exception:
                ray.kill(actor, no_restart=True)
                raise
            return actor
        return GPUActor(cfg.model_dump(), batch)

    def discard(slot):
        if executor == "ray":
            ray.kill(slot["actor"], no_restart=True)
        slot["actor"] = None

    try:
        for i in range(count):
            slots.append(
                {
                    "actor": create(cfg.detector.batch_size),
                    "task": None,
                    "future": None,
                    "batch": cfg.detector.batch_size,
                    "index": i,
                    "heartbeat": 0,
                }
            )
        while True:
            state.reap(run_id, cfg.execution)
            for slot in slots:
                task = slot["task"]
                if task:
                    future = slot["future"]
                    ready = bool(ray.wait([future], timeout=0)[0]) if executor == "ray" else future.done()
                    if time.monotonic() - slot["heartbeat"] >= cfg.execution.heartbeat_seconds:
                        if not state.heartbeat(task, cfg.execution.lease_ttl_seconds):
                            if executor == "local":
                                # Cannot cancel a running thread: wait for its orphan, then discard.
                                if not ready:
                                    continue
                            discard(slot)
                            slot["task"] = None
                            continue
                        slot["heartbeat"] = time.monotonic()
                    if not ready:
                        continue
                    try:
                        result = ray.get(future) if executor == "ray" else future.result()
                        state.finalize(task, result["candidate"], result["metrics"], result["environment"])
                    except LeaseLost:
                        pass
                    except Exception as error:
                        cause = (
                            error.as_instanceof_cause() if hasattr(error, "as_instanceof_cause") else error
                        )
                        oom = is_oom(cause)
                        smaller = next_batch(slot["batch"], cfg.execution.oom_batch_sizes) if oom else None
                        code = (
                            "CUDA_OOM"
                            if oom
                            else (
                                "CONFIGURATION_ERROR"
                                if isinstance(cause, ConfigurationError)
                                else "WORKER_FAILURE"
                            )
                        )
                        try:
                            state.fail(
                                task,
                                cfg.execution,
                                code,
                                permanent=isinstance(cause, ConfigurationError) or (oom and smaller is None),
                            )
                        except LeaseLost:
                            pass
                        if smaller:
                            log.warning(
                                "task=%s CUDA OOM: batch %d -> %d, restarting actor",
                                task["task_id"],
                                slot["batch"],
                                smaller,
                            )
                            slot["batch"] = smaller
                        discard(slot)
                        if isinstance(cause, ConfigurationError):
                            raise cause
                    slot["task"] = None
                if slot["actor"] is None:
                    slot["actor"] = create(slot["batch"])
                if slot["task"] is None:
                    task = state.claim(
                        run_id, f"{socket.gethostname()}:{os.getpid()}:{slot['index']}", cfg.execution
                    )
                    if task:
                        slot["task"] = task
                        slot["heartbeat"] = time.monotonic()
                        environment = (
                            ray.get(slot["actor"].ready.remote(), timeout=30)
                            if executor == "ray"
                            else (slot["actor"].ready())
                        )
                        state.note_attempt(task, environment, slot["batch"])
                        slot["future"] = (
                            slot["actor"].process.remote(task)
                            if executor == "ray"
                            else pool.submit(slot["actor"].process, task)
                        )
            status = state.status(run_id)
            if not any(slot["task"] for slot in slots):
                if status["tasks"]["PLANNED"]:
                    raise ConfigurationError("tasks still PLANNED: run publish and ingest first")
                if not any(status["tasks"][s] for s in ("QUEUED", "RUNNING", "RETRY_WAIT")):
                    return status
            time.sleep(0.2)
    finally:
        for slot in slots:
            if slot["actor"] is not None:
                discard(slot)
        if pool:
            pool.shutdown(wait=True)
