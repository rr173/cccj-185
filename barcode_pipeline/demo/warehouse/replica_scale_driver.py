#!/usr/bin/env python3
"""Large-package streaming driver: 70,000 revisions, ~4 KiB notes, 256 MiB.

The scale *source* preparation (real pipeline DB plus 70k imported
revisions, each carrying a distinct ~4 KiB business note) runs without a
memory limit, matching the task rule.  The two operations that must stream --
offline package export and package acceptance -- run in separate processes
under a hard 256 MiB ``RLIMIT_AS`` cap.  Neither the full history nor the
whole package is ever resident: frames are read and written one batch at a
time.

Generated data and the package live only in the chosen workdir and are never
committed to the repository.

Usage:
  python3 -I demo/warehouse/replica_scale_driver.py build  --workdir DIR
  python3 -I demo/warehouse/replica_scale_driver.py export --workdir DIR
  python3 -I demo/warehouse/replica_scale_driver.py apply  --workdir DIR
  python3 -I demo/warehouse/replica_scale_driver.py verify --workdir DIR
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "vendor"))
sys.path.insert(0, str(ROOT))

from tests.wh_helpers import build_enrichment_db, write_changes  # noqa: E402
from barcode_pipeline.warehouse import (  # noqa: E402
    apply_package,
    export_package,
    hdb,
    importer,
    initialize_origin,
    replica_status,
)
from barcode_pipeline.warehouse.replication import identity  # noqa: E402

# Task-fixed scale.
N_REVISIONS = 70_000
NOTE_BYTES = 4 * 1024
BATCH_SIZE = 2_000                 # 70,000 / 2,000 = 35 source batches
MEM_CAP_BYTES = 256 * 1024 * 1024

# One confirmed normalized input; every source line reuses it but carries its
# own distinct ~4 KiB business note.
RULES = {"X|CN|v1": "001"}
BASE_SOURCE = [{
    "barcode": "X", "market": "CN", "rule_version": "v1",
    "supplier": "S", "item_id": "K", "fact_ref": "base",
    "note": "x" * NOTE_BYTES,
}]


def _note(index: int) -> str:
    # Deterministic, distinct ~4 KiB note per revision; padded so every note
    # is at least NOTE_BYTES of business text.
    head = f"note-{index:06d}-"
    return head + ("n" * (NOTE_BYTES - len(head)))


def do_build(workdir: Path) -> None:
    """Prepare the large source warehouse (outside the memory cap)."""
    workdir.mkdir(parents=True, exist_ok=True)
    src_dir = workdir / "src"
    source_db = build_enrichment_db(
        src_dir, BASE_SOURCE, RULES, name="scale", batch_size=10
    )

    # Extend the real pipeline DB with N_REVISIONS confirmed source rows.
    # Each row reuses the confirmed service input but carries its own note;
    # the rows are appended through the pipeline's own importer (never
    # hand-edited).  Streamed from disk, not held in a list.
    extra_path = workdir / "extra-sources.jsonl"
    with open(extra_path, "w", encoding="utf-8") as fh:
        # lead with a duplicate base row so line numbering starts cleanly
        fh.write(json.dumps(BASE_SOURCE[0], separators=(",", ":")) + "\n")
        for i in range(N_REVISIONS):
            row = {
                "barcode": "X", "market": "CN", "rule_version": "v1",
                "supplier": "S", "item_id": "K",
                "fact_ref": f"rev-{i}", "note": _note(i),
            }
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")

    from barcode_pipeline import client as pipe_client
    from barcode_pipeline import db as pipe_db
    from barcode_pipeline.config import Config

    conn = pipe_db.open_db(source_db)
    try:
        readiness = pipe_client.run_start(
            conn, Config(base_url="http://127.0.0.1:1", batch_size=1000),
            str(extra_path), run_id="scale-replica-extension",
        )
        assert readiness["deliverable"], readiness
    finally:
        conn.close()
    # source line for revision i: base line 1, extras at 2..N+1
    wh = str(workdir / "source.db")
    wc = hdb.open_warehouse(wh)
    try:
        initialize_origin(wc)
        chunk_path = workdir / "chunk.jsonl"
        n_batches = 0
        for start in range(0, N_REVISIONS, BATCH_SIZE):
            end = min(start + BATCH_SIZE, N_REVISIONS)
            with open(chunk_path, "w", encoding="utf-8") as out:
                for i in range(start, end):
                    out.write(json.dumps({
                        "claim_id": f"C{i:06d}",
                        "revision": 1,
                        "source_line": 2 + i,
                        "valid_from": i,
                        "valid_to": i + 1,
                    }, separators=(",", ":")) + "\n")
            importer.import_batch(
                wc, source_db, str(chunk_path), f"scale-{n_batches + 1}"
            )
            n_batches += 1
        assert hdb.latest_seq(wc) == n_batches
        rev_count = wc.execute(
            "SELECT COUNT(*) n FROM wh_claim_revisions"
        ).fetchone()["n"]
        assert rev_count == N_REVISIONS, rev_count
    finally:
        wc.close()
    print(json.dumps({
        "event": "scale_build_done",
        "revisions": N_REVISIONS,
        "batches": n_batches,
        "note_bytes_each": NOTE_BYTES,
        "workdir": str(workdir),
    }, indent=2))


def _install_cap() -> None:
    resource.setrlimit(
        resource.RLIMIT_AS, (MEM_CAP_BYTES, MEM_CAP_BYTES)
    )


def do_export(workdir: Path) -> None:
    _install_cap()
    wh = str(workdir / "source.db")
    package = str(workdir / "large.pkg")
    top = hdb.latest_seq(hdb.open_warehouse(wh))
    report = export_package(wh, 0, top, package)
    peak_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Per-batch count and prefix checks streamed from the resulting package.
    from barcode_pipeline.warehouse.replication.package import iter_frames
    from barcode_pipeline.warehouse.replication.package import TAG_BATCH
    batch_seen = 0
    rev_seen = 0
    for tag, obj in iter_frames(package):
        if tag == TAG_BATCH:
            batch_seen += 1
            rev_seen += len(obj["revisions"])
    assert batch_seen == top
    assert rev_seen == N_REVISIONS
    size_mib = os.path.getsize(package) / (1024 * 1024)
    print(json.dumps({
        "event": "scale_export_done",
        "batches": batch_seen,
        "revisions": rev_seen,
        "package_mib": round(size_mib, 1),
        "memory_cap_bytes": MEM_CAP_BYTES,
        "peak_rss_kib": peak_kib,
        "peak_rss_mib": round(peak_kib / 1024, 1),
        "under_cap": peak_kib * 1024 < MEM_CAP_BYTES,
        "through_digest": report["through_digest"],
    }, indent=2))


def do_apply(workdir: Path) -> None:
    _install_cap()
    package = str(workdir / "large.pkg")
    replica_db = str(workdir / "replica.db")
    conn = hdb.open_warehouse(replica_db)
    try:
        report = apply_package(package, conn)
    finally:
        conn.close()
    peak_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    conn = hdb.open_warehouse(replica_db)
    try:
        assert hdb.latest_seq(conn) == report["committed_seq"]
        rev_count = conn.execute(
            "SELECT COUNT(*) n FROM wh_claim_revisions"
        ).fetchone()["n"]
        assert rev_count == N_REVISIONS, rev_count
    finally:
        conn.close()
    print(json.dumps({
        "event": "scale_apply_done",
        "committed_seq": report["committed_seq"],
        "revisions": N_REVISIONS,
        "memory_cap_bytes": MEM_CAP_BYTES,
        "peak_rss_kib": peak_kib,
        "peak_rss_mib": round(peak_kib / 1024, 1),
        "under_cap": peak_kib * 1024 < MEM_CAP_BYTES,
        "prefix_digest": report["prefix_digest"],
    }, indent=2))


def do_verify(workdir: Path) -> None:
    """Hand-written anchor queries and full source/replica prefix equality."""
    _install_cap()
    source_db = str(workdir / "source.db")
    replica_db = str(workdir / "replica.db")
    sc = hdb.open_warehouse(source_db)
    rc = hdb.open_warehouse(replica_db)
    try:
        top = hdb.latest_seq(sc)
        assert hdb.latest_seq(rc) == top
        # prefix digests agree at the final and three hand-picked seqs
        for seq in (1, top // 2, top):
            s_digest = sc.execute(
                "SELECT prefix_digest FROM wh_replication_log"
                " WHERE knowledge_seq=?", (seq,)
            ).fetchone()["prefix_digest"]
            r_digest = rc.execute(
                "SELECT prefix_digest FROM wh_replication_log"
                " WHERE knowledge_seq=?", (seq,)
            ).fetchone()["prefix_digest"]
            assert s_digest == r_digest, seq
        # hand-written anchors.  A full-product as-of lookup at seq=top would
        # materialize all 70k revisions of one product, so anchor rows are read
        # by their unique claim id (indexed); the prefix-digest checks above
        # already prove every revision copied correctly.
        anchors = [0, 1, N_REVISIONS // 2, N_REVISIONS - 1]
        anchor_results = {}
        for i in anchors:
            row = rc.execute(
                "SELECT claim_id, kind, valid_from, valid_to, source_raw,"
                " normalized_value, business_status FROM wh_claim_revisions"
                " WHERE claim_id=? AND kind='upsert'",
                (f"C{i:06d}",),
            ).fetchone()
            assert row is not None, i
            assert (int(row["valid_from"]), row["valid_to"]) == (i, i + 1)
            assert row["normalized_value"] == "001"
            note = json.loads(row["source_raw"])["note"]
            assert note == _note(i), i
            srow = sc.execute(
                "SELECT content_digest FROM wh_claim_revisions"
                " WHERE claim_id=?", (f"C{i:06d}",),
            ).fetchone()
            rrow = rc.execute(
                "SELECT content_digest FROM wh_claim_revisions"
                " WHERE claim_id=?", (f"C{i:06d}",),
            ).fetchone()
            assert srow["content_digest"] == rrow["content_digest"], i
            anchor_results[f"C{i:06d}@[{i},{i+1})"] = (
                row["business_status"], row["normalized_value"], len(note)
            )
        # status API on the managed copy
        status = replica_status(rc)
        assert status["role"] == "replica"
    finally:
        sc.close()
        rc.close()
    peak_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({
        "event": "scale_verify_done",
        "anchors": anchor_results,
        "memory_cap_bytes": MEM_CAP_BYTES,
        "peak_rss_kib": peak_kib,
        "peak_rss_mib": round(peak_kib / 1024, 1),
        "under_cap": peak_kib * 1024 < MEM_CAP_BYTES,
    }, indent=2))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("build", "export", "apply", "verify"):
        p = sub.add_parser(name)
        p.add_argument("--workdir", required=True)
    args = ap.parse_args(argv)
    wd = Path(args.workdir)
    {"build": do_build, "export": do_export,
     "apply": do_apply, "verify": do_verify}[args.cmd](wd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
