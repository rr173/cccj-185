"""Command-line surface for the historical warehouse.

Subcommands (all offline — the source enrichment DB and warehouse are local
SQLite files; no network service is called):

  import         admit one change JSONL batch against a read-only enrichment DB
  lookup         point-in-time answer for one (supplier, item_id, market, time)
  timeline       maximal constant segments over a finite half-open range
  enrich         stream a fact JSONL through the warehouse at one pinned seq
  impact         compare two seqs over one fact file, emit recomputable rows
  inspect        batch/claim ledger and revision history
  origin init    bind the durable source-origin identity (idempotent)
  replica init   create an empty managed replica bound to one origin
  package export stream an offline (after, through] package from a source
  package inspect read-only verdict of a package against a target warehouse
  package apply  verify and atomically accept a whole package on a replica
  replica status report role, origin, committed seq and prefix digest
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from . import facts, hdb
from .hdb import WarehouseError
from .importer import import_batch
from .query import lookup, timeline
from .replication import origin as origin_mod
from .replication import replica as replica_mod
from .replication.exporter import export_package
from .replication.package import PackageError
from .replication.replica import apply_package, inspect_package


def _print(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True))


def cmd_import(args: argparse.Namespace) -> int:
    conn = hdb.open_warehouse(args.warehouse)
    try:
        summary = import_batch(conn, args.source, args.changes, args.batch_id)
        _print({"event": "warehouse_import", **summary})
        return 0
    finally:
        conn.close()


def cmd_lookup(args: argparse.Namespace) -> int:
    conn = hdb.open_warehouse(args.warehouse)
    try:
        _print(
            lookup(
                conn,
                args.supplier,
                args.item_id,
                args.market,
                args.event_time,
                seq=args.seq,
            )
        )
        return 0
    finally:
        conn.close()


def cmd_timeline(args: argparse.Namespace) -> int:
    conn = hdb.open_warehouse(args.warehouse)
    try:
        _print(
            timeline(
                conn,
                args.supplier,
                args.item_id,
                args.market,
                args.time_from,
                args.time_to,
                seq=args.seq,
            )
        )
        return 0
    finally:
        conn.close()


def cmd_enrich(args: argparse.Namespace) -> int:
    # Test hooks (dormant in normal use): an optional delay, or a FIFO
    # rendezvous that blocks after the seq has been pinned but before fact
    # streaming starts, so a concurrent import can be admitted deterministically.
    delay = os.environ.get("BARCODE_WH_FACT_DELAY")
    gate = os.environ.get("BARCODE_WH_FACT_GATE")
    conn = hdb.open_warehouse(args.warehouse)
    try:
        if delay:
            import time

            time.sleep(float(delay))
        # Pin the knowledge seq before the rendezvous: the concurrent import
        # admitted while we wait must not change this run's version.
        from .query import resolve_seq

        pinned = resolve_seq(conn, args.seq)
        if gate:  # pragma: no cover - exercised by tests only
            with open(gate, "r", encoding="utf-8") as gate_fh:
                gate_fh.read(1)
        try:
            summary = facts.enrich_file(
                conn, args.facts, args.output, seq=pinned
            )
        except FileExistsError as exc:
            print(f"enrich refused: {exc}", file=sys.stderr)
            return 3
        _print({"event": "warehouse_enrich", **summary})
        return 0
    finally:
        conn.close()


def cmd_impact(args: argparse.Namespace) -> int:
    conn = hdb.open_warehouse(args.warehouse)
    try:
        try:
            summary = facts.impact_file(
                conn, args.facts, args.output, args.old_seq, args.new_seq
            )
        except FileExistsError as exc:
            print(f"impact refused: {exc}", file=sys.stderr)
            return 3
        _print({"event": "warehouse_impact", **summary})
        return 0
    finally:
        conn.close()


def cmd_inspect(args: argparse.Namespace) -> int:
    conn = hdb.open_warehouse(args.warehouse)
    try:
        report: dict[str, Any]
        if args.batch_id is not None:
            batch = conn.execute(
                "SELECT * FROM wh_batches WHERE batch_id=?", (args.batch_id,)
            ).fetchone()
            if batch is None:
                print(f"unknown batch_id {args.batch_id!r}", file=sys.stderr)
                return 1
            revisions = [
                dict(r)
                for r in conn.execute(
                    "SELECT claim_id, revision, kind, knowledge_seq, batch_id,"
                    " supplier, item_id, market, valid_from, valid_to,"
                    " source_line, business_status, normalized_value,"
                    " error_code FROM wh_claim_revisions WHERE batch_id=?"
                    " ORDER BY id",
                    (args.batch_id,),
                )
            ]
            report = {"batch": dict(batch), "revisions": revisions}
        elif args.claim is not None:
            claim = conn.execute(
                "SELECT * FROM wh_claims WHERE claim_id=?", (args.claim,)
            ).fetchone()
            if claim is None:
                print(f"unknown claim_id {args.claim!r}", file=sys.stderr)
                return 1
            revisions = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM wh_claim_revisions WHERE claim_id=?"
                    " ORDER BY revision, knowledge_seq",
                    (args.claim,),
                )
            ]
            report = {"claim": dict(claim), "revisions": revisions}
        else:
            report = hdb.inspect_warehouse(conn)
        if args.json or args.batch_id is not None or args.claim is not None:
            _print(report)
        else:
            _human_inspect(report)
        return 0
    finally:
        conn.close()


def _human_inspect(report: dict[str, Any]) -> None:
    print(f"schema        : {report['schema_version']}")
    print(f"latest seq    : {report['latest_seq']}")
    print(f"batches       : {report['batch_count']}")
    print(f"claims        : {report['claim_count']}")
    print(f"revisions     : {report['revision_count']}")
    for b in report["batches"]:
        print(
            f"  seq={b['knowledge_seq']:>3} {b['status']:<8} {b['batch_id']}"
            f" changes={b['change_count']} new={b['new_revisions']}"
            f" dup={b['duplicate_rows']} retract={b['retractions']}"
        )
    for c in report["claims"]:
        print(
            f"  claim {c['claim_id']} ({c['supplier']},{c['item_id']},"
            f"{c['market']}) head=rev{c['head_revision']}/{c['head_kind']}"
            f" @seq{c['head_seq']}"
        )


# --------------------------------------------------------------------------
# Offline replication commands
# --------------------------------------------------------------------------

def cmd_origin_init(args: argparse.Namespace) -> int:
    conn = hdb.open_warehouse(args.warehouse)
    try:
        _print(origin_mod.initialize_origin(
            conn, origin_id=args.origin_id
        ))
        return 0
    finally:
        conn.close()


def cmd_replica_init(args: argparse.Namespace) -> int:
    conn = hdb.open_warehouse(args.warehouse)
    try:
        _print(origin_mod.initialize_replica(conn, args.origin_id))
        return 0
    finally:
        conn.close()


def cmd_replica_status(args: argparse.Namespace) -> int:
    conn = hdb.open_warehouse(args.warehouse)
    try:
        _print(origin_mod.replica_status(conn))
        return 0
    finally:
        conn.close()


def cmd_package_export(args: argparse.Namespace) -> int:
    through = "latest" if args.through == "latest" else int(args.through)
    report = export_package(
        args.warehouse, int(args.after), through, args.output
    )
    _print(report)
    return 0


def cmd_package_inspect(args: argparse.Namespace) -> int:
    conn = hdb.open_warehouse(args.warehouse)
    try:
        report = inspect_package(args.package, conn)
    finally:
        conn.close()
    _print(report)
    # Non-zero exit for every rejection verdict so scripts can branch.
    return 0 if report["verdict"] in (
        replica_mod.VERDICT_APPENDABLE, replica_mod.VERDICT_REPLAY
    ) else 2


def cmd_package_apply(args: argparse.Namespace) -> int:
    conn = hdb.open_warehouse(args.warehouse)
    try:
        hook = _replica_gate_hook()
        report = apply_package(args.package, conn, pre_commit_hook=hook)
    finally:
        conn.close()
    _print(report)
    return 0


def _replica_gate_hook():
    """Build the deterministic pre-commit rendezvous hook, or None.

    Dormant unless ``BARCODE_REPLICA_GATE`` names a FIFO.  When set, the hook
    touches ``BARCODE_REPLICA_GATE_SENTINEL`` (if given) and then blocks on
    one byte from the FIFO, all inside the apply write transaction after the
    new prefix is fully staged.
    """
    gate = os.environ.get("BARCODE_REPLICA_GATE")
    if not gate:
        return None
    sentinel = os.environ.get("BARCODE_REPLICA_GATE_SENTINEL")

    def hook() -> None:
        if sentinel:
            with open(sentinel, "w", encoding="utf-8") as fh:
                fh.write("at-gate")
        if os.path.exists(gate):
            with open(gate, "r", encoding="utf-8") as fh:
                fh.read(1)

    return hook


def build_warehouse_parser(sub: argparse._SubParsersAction) -> None:
    wh = sub.add_parser("warehouse", help="bi-temporal historical warehouse")
    wsub = wh.add_subparsers(dest="wh_command", required=True)

    imp = wsub.add_parser("import", help="admit one change batch")
    imp.add_argument("--warehouse", required=True)
    imp.add_argument("--source", required=True, help="read-only enrichment DB")
    imp.add_argument("--changes", required=True, help="change JSONL file")
    imp.add_argument("--batch-id", required=True)
    imp.set_defaults(func=cmd_import)
    lk = wsub.add_parser("lookup", help="point lookup at one business time")
    lk.add_argument("--warehouse", required=True)
    lk.add_argument("--supplier", required=True)
    lk.add_argument("--item-id", required=True)
    lk.add_argument("--market", required=True)
    lk.add_argument("--event-time", required=True, type=int)
    lk.add_argument("--seq", default="latest", help="knowledge seq or 'latest'")
    lk.set_defaults(func=cmd_lookup)

    tl = wsub.add_parser("timeline", help="segments over [--from, --to)")
    tl.add_argument("--warehouse", required=True)
    tl.add_argument("--supplier", required=True)
    tl.add_argument("--item-id", required=True)
    tl.add_argument("--market", required=True)
    tl.add_argument("--from", dest="time_from", required=True, type=int)
    tl.add_argument("--to", dest="time_to", required=True, type=int)
    tl.add_argument("--seq", default="latest")
    tl.set_defaults(func=cmd_timeline)

    en = wsub.add_parser("enrich", help="enrich a fact JSONL at a pinned seq")
    en.add_argument("--warehouse", required=True)
    en.add_argument("--facts", required=True)
    en.add_argument("--output", required=True)
    en.add_argument("--seq", default="latest")
    en.set_defaults(func=cmd_enrich)

    im = wsub.add_parser("impact", help="changed answers between two seqs")
    im.add_argument("--warehouse", required=True)
    im.add_argument("--facts", required=True)
    im.add_argument("--output", required=True)
    im.add_argument("--old-seq", required=True)
    im.add_argument("--new-seq", required=True)
    im.set_defaults(func=cmd_impact)

    ins = wsub.add_parser("inspect", help="batch/claim ledger")
    ins.add_argument("--warehouse", required=True)
    ins.add_argument("--batch-id")
    ins.add_argument("--claim")
    ins.add_argument("--json", action="store_true")
    ins.set_defaults(func=cmd_inspect)

    # --- offline replication ---------------------------------------------
    origin = wsub.add_parser(
        "origin", help="source-origin identity management"
    )
    osub = origin.add_subparsers(dest="origin_command", required=True)
    oi = osub.add_parser(
        "init", help="bind (or re-assert) the durable source origin identity"
    )
    oi.add_argument("--warehouse", required=True)
    oi.add_argument(
        "--origin-id",
        help="explicit identity (default: generated); cannot be changed later",
    )
    oi.set_defaults(func=cmd_origin_init)

    replica = wsub.add_parser(
        "replica", help="managed replica lifecycle and status"
    )
    rsub = replica.add_subparsers(dest="replica_command", required=True)
    ri = rsub.add_parser(
        "init", help="turn an empty warehouse into a managed replica"
    )
    ri.add_argument("--warehouse", required=True)
    ri.add_argument("--origin-id", required=True)
    ri.set_defaults(func=cmd_replica_init)
    rs = rsub.add_parser("status", help="role, origin, prefix and seq status")
    rs.add_argument("--warehouse", required=True)
    rs.set_defaults(func=cmd_replica_status)

    pkg = wsub.add_parser(
        "package", help="offline replication package export/inspection/apply"
    )
    psub = pkg.add_subparsers(dest="package_command", required=True)

    pe = psub.add_parser("export", help="export range (after, through]")
    pe.add_argument("--warehouse", required=True)
    pe.add_argument("--after", required=True, type=int)
    pe.add_argument("--through", required=True, help="seq integer or 'latest'")
    pe.add_argument("--output", required=True)
    pe.set_defaults(func=cmd_package_export)

    pi = psub.add_parser(
        "inspect", help="read-only verdict of a package against a warehouse"
    )
    pi.add_argument("--warehouse", required=True)
    pi.add_argument("--package", required=True)
    pi.set_defaults(func=cmd_package_inspect)

    pa = psub.add_parser("apply", help="verify and accept a whole package")
    pa.add_argument("--warehouse", required=True)
    pa.add_argument("--package", required=True)
    pa.set_defaults(func=cmd_package_apply)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="barcode-pipeline warehouse")
    build_warehouse_parser(
        parser.add_subparsers(dest="wh_command", required=True)
    )
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except WarehouseError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except (ValueError, KeyError, FileNotFoundError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
