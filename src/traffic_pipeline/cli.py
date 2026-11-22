import argparse
import logging
import os
import sys
import uuid

from dotenv import load_dotenv
from pydantic import ValidationError

from traffic_pipeline.core.common import ConfigurationError, canonical
from traffic_pipeline.core.config import load_config
from traffic_pipeline.control.state import State


def main(argv=None):
    load_dotenv(os.getenv("ENV_FILE", ".env"), override=False)
    parser = argparse.ArgumentParser(prog="traffic-pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("discover", "plan", "benchmark"):
        sub = commands.add_parser(name)
        sub.add_argument("--config")
        if name == "discover":
            sub.add_argument("--output")
        if name == "benchmark":
            sub.add_argument(
                "--sample-manifest",
                default=os.getenv("SAMPLE_MANIFEST_URI"),
                required=not bool(os.getenv("SAMPLE_MANIFEST_URI")),
            )
            sub.add_argument("--executor", choices=["ray", "local"], default="ray")
    for name in ("publish", "work", "status", "retry", "reconcile", "verify", "export", "aggregate"):
        sub = commands.add_parser(name)
        default_run = os.getenv("RUN_ID") if name != "publish" else None
        sub.add_argument("--run-id", default=default_run, required=name != "publish" and not default_run)
        if name == "publish":
            sub.add_argument("--watch", action="store_true")
        if name == "work":
            sub.add_argument("--gpu-actors", type=int)
            sub.add_argument("--executor", choices=["ray", "local"], default="ray")
        if name == "retry":
            sub.add_argument("--failed-only", action="store_true", required=True)
        if name == "reconcile":
            sub.add_argument("--republish-planned", action="store_true", required=True)
        if name == "verify":
            sub.add_argument("--repair", action="store_true")
        if name == "aggregate":
            sub.add_argument("--window", default="1min")
    commands.add_parser("migrate")
    commands.add_parser("init-topics")
    sub = commands.add_parser("backup")
    sub.add_argument("--watch", action="store_true")
    sub = commands.add_parser("ingest")
    sub.add_argument("--once", action="store_true")
    sub = commands.add_parser("smoke")
    sub.add_argument("--executor", choices=["ray", "local"], default="local")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        result = dispatch(args)
        print(canonical(result))
        if isinstance(result, dict) and (
            result.get("state") == "INCOMPLETE"
            or result.get("pending_retry")
            or result.get("final") is False
            or (args.command == "work" and result.get("tasks", {}).get("FAILED"))
        ):
            return 2
        return 0
    except KeyboardInterrupt:
        return 130
    except ValidationError as error:
        print(
            canonical(
                {
                    "error": "INVALID_CONFIG",
                    "fields": [
                        {"path": list(e["loc"]), "message": e["msg"]}
                        for e in error.errors(include_input=False)
                    ],
                }
            ),
            file=sys.stderr,
        )
        return 1
    except Exception as error:
        # Third-party exception strings can contain DSNs or signed download URLs.
        message = str(error) if isinstance(error, ConfigurationError) else type(error).__name__
        print(canonical({"error": message}), file=sys.stderr)
        return 1


def dispatch(args):
    from traffic_pipeline.outputs.aggregate import aggregate
    from traffic_pipeline.diagnostics.benchmark import benchmark
    from traffic_pipeline.inference.coordinator import work
    from traffic_pipeline.data.discovery import discover
    from traffic_pipeline.diagnostics.fixtures import make_fixture
    from traffic_pipeline.control.kafka_ingestor import ingest
    from traffic_pipeline.control.outbox import create_topics, publish
    from traffic_pipeline.data.planner import plan
    from traffic_pipeline.outputs.verify import verify, export

    name = args.command
    if name == "discover":
        return discover(load_config(args.config), args.output)
    if name == "init-topics":
        create_topics()
        return {"topics_initialized": True}
    state = State()
    if name == "migrate":
        state.migrate()
        return {"migrated": True}
    if name == "plan":
        return plan(load_config(args.config), state)
    if name == "benchmark":
        return benchmark(load_config(args.config), args.sample_manifest, state, args.executor)
    if name == "publish":
        if args.watch:
            import time

            while True:
                publish(state, args.run_id)
                time.sleep(3)
        return publish(state, args.run_id)
    if name == "backup":
        from traffic_pipeline.control.backup import backup, watch_backups

        return watch_backups(state) if args.watch else backup(state)
    if name == "ingest":
        return ingest(state, args.once)
    if name == "status":
        return state.status(args.run_id)
    if name == "work":
        return work(state, args.run_id, args.gpu_actors, args.executor)
    if name == "retry":
        return {"retried_tasks": state.retry(args.run_id)}
    if name == "reconcile":
        return {"republished_planned": state.reconcile(args.run_id)}
    if name == "verify":
        return verify(state, args.run_id, args.repair)
    if name == "export":
        return export(state, args.run_id)
    if name == "aggregate":
        return aggregate(state, args.run_id, args.window)
    if name == "smoke":
        state.migrate()
        create_topics()
        run_id = "smoke-" + uuid.uuid4().hex[:12]
        config, file = make_fixture(os.path.join(os.getenv("SMOKE_DIR", "var/smoke"), run_id), run_id)
        discover(config)
        plan(config, state)
        publish(state, run_id)
        ingest(state, once=True, idle_seconds=5)
        work(state, run_id, actors=2, executor=args.executor)
        result = export(state, run_id)
        result["timeseries"] = aggregate(state, run_id)
        result["fixture_config"] = str(file)
        return result
    raise AssertionError(name)


if __name__ == "__main__":
    sys.exit(main())
