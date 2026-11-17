"""Command-line interface: start, resume, inspect, export, explain, serve."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from . import client, db
from .config import Config
from .server import build_server
from .warehouse.cli import build_warehouse_parser
from .warehouse.hdb import WarehouseError


def _load_config(path: str | None) -> Config:
    return Config.load(path).env_override()


def _print(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def _run_id_for(db_path: str) -> str:
    # Stable per database: idempotency keys must not change across processes.
    return "run:" + os.path.abspath(db_path)


def cmd_start(args: argparse.Namespace) -> int:
    config = _load_config(args.config)
    os.environ["BARCODE_RUN_ID"] = _run_id_for(args.db)
    conn = db.open_db(args.db)
    try:
        readiness = client.run_start(conn, config, args.source, os.environ["BARCODE_RUN_ID"])
        _print({"event": "start", **readiness})
        return 0 if readiness["deliverable"] else 2
    finally:
        conn.close()


def cmd_resume(args: argparse.Namespace) -> int:
    config = _load_config(args.config)
    os.environ.setdefault("BARCODE_RUN_ID", _run_id_for(args.db))
    conn = db.open_db(args.db)
    try:
        readiness = client.run_resume(conn, config)
        _print({"event": "resume", **readiness})
        return 0 if readiness["deliverable"] else 2
    finally:
        conn.close()


def cmd_inspect(args: argparse.Namespace) -> int:
    conn = db.open_db(args.db)
    try:
        readiness = db.delivery_readiness(conn)
        jobs = []
        for j in db.all_jobs(conn):
            report = None
            if j["server_job_id"]:
                report = db.coverage(conn, j["id"])
            jobs.append(
                {
                    "job_pk": j["id"],
                    "server_job_id": j["server_job_id"],
                    "idempotency_key": j["idempotency_key"],
                    "status": j["status"],
                    "status_detail": j["status_detail"],
                    "expected_slots": j["expected_slots"],
                    "attempts": j["attempts"],
                    "last_error": j["last_error"],
                    "coverage": None
                    if report is None
                    else {
                        "observed_slots": report["observed_slots"],
                        "missing": report["missing"],
                        "pending": report["pending"],
                        "multiple_finals": report["multiple_finals"],
                        "complete_seen": report["complete_seen"],
                        "ok": report["ok"],
                    },
                }
            )
        counts = {
            "source_lines": db.source_line_count(conn),
            "pages": conn.execute("SELECT COUNT(*) FROM page_log").fetchone()[0],
            "observations": conn.execute(
                "SELECT COUNT(*) FROM slot_observations"
            ).fetchone()[0],
            "exports": [
                dict(r) for r in conn.execute("SELECT * FROM exports").fetchall()
            ],
        }
        report = {"readiness": readiness, "counts": counts, "jobs": jobs}
        if args.json:
            _print(report)
        else:
            _human_inspect(report)
        return 0 if readiness["deliverable"] else 2
    finally:
        conn.close()


def _human_inspect(report: dict[str, Any]) -> None:
    r = report["readiness"]
    print(f"source lines : {r['source_lines']}")
    print(f"input states : {r['input_states']}")
    print(f"job states   : {r['job_states']}")
    print(f"deliverable  : {r['deliverable']}")
    for j in report["jobs"]:
        line = (
            f"  job#{j['job_pk']} {j['server_job_id']} status={j['status']}"
            f" slots={j['expected_slots']} attempts={j['attempts']}"
        )
        print(line)
        if j["coverage"]:
            c = j["coverage"]
            print(
                f"      coverage ok={c['ok']} observed={c['observed_slots']}"
                f" missing={c['missing']} pending={c['pending']}"
                f" multiple={c['multiple_finals']} complete_seen={c['complete_seen']}"
            )
        if j["status_detail"]:
            print(f"      detail: {j['status_detail']}")


def cmd_export(args: argparse.Namespace) -> int:
    conn = db.open_db(args.db)
    try:
        if db.export_registered(conn, os.path.abspath(args.output)) and not args.force:
            print(f"output already registered: {args.output}", file=sys.stderr)
            return 3
        manifest = client.run_export(conn, args.output, force=args.force)
        _print({"event": "export", **manifest})
        return 0
    except (client.HardProtocolError, FileExistsError) as exc:
        print(f"export refused: {exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()


def cmd_explain(args: argparse.Namespace) -> int:
    conn = db.open_db(args.db)
    try:
        _print(db.explain_line(conn, args.line))
        return 0
    except KeyError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        conn.close()


def cmd_serve(args: argparse.Namespace) -> int:  # pragma: no cover
    httpd = build_server(
        args.host,
        args.port,
        snapshot_path=args.snapshot,
        log_path=args.log,
        verbose=args.verbose,
    )
    print(f"mock barcode service listening on http://{args.host}:{args.port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="barcode-pipeline")
    p.add_argument("--config", help="path to JSON config")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("start", help="import a JSONL source and run the pipeline")
    s.add_argument("--db", required=True)
    s.add_argument("--source", required=True)
    s.set_defaults(func=cmd_start)

    r = sub.add_parser("resume", help="resume all unfinished jobs in an existing db")
    r.add_argument("--db", required=True)
    r.set_defaults(func=cmd_resume)

    i = sub.add_parser("inspect", help="show persisted pipeline state")
    i.add_argument("--db", required=True)
    i.add_argument("--json", action="store_true")
    i.set_defaults(func=cmd_inspect)

    e = sub.add_parser("export", help="stream the delivery JSONL")
    e.add_argument("--db", required=True)
    e.add_argument("--output", required=True)
    e.add_argument("--force", action="store_true")
    e.set_defaults(func=cmd_export)

    x = sub.add_parser("explain", help="explain the provenance of one source line")
    x.add_argument("--db", required=True)
    x.add_argument("--line", required=True, type=int)
    x.set_defaults(func=cmd_explain)

    v = sub.add_parser("serve", help="run the controllable mock service")
    v.add_argument("--host", default="127.0.0.1")
    v.add_argument("--port", type=int, default=8080)
    v.add_argument("--snapshot")
    v.add_argument("--log")
    v.add_argument("--verbose", action="store_true")
    v.set_defaults(func=cmd_serve)

    build_warehouse_parser(sub)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except (client.HardProtocolError, WarehouseError, ValueError, KeyError,
            FileNotFoundError) as exc:
        # deterministic, user-correctable failures: no Python traceback
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except Exception:
        # unexpected defects keep their traceback for diagnosis
        raise


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
