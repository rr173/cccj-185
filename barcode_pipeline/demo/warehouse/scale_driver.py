#!/usr/bin/env python3
"""Large-scale streaming driver: 20,000 claims / 200,000 facts, 256 MiB cap.

Everything is generated at runtime into a caller-provided working directory;
nothing here is committed to the repository or kept resident as a whole:

  * a small *real* enrichment DB is produced by the local pipeline (the mock
    service answers a handful of distinct inputs);
  * 20,000 change rows are generated and imported in streamed batches;
  * 200,000 fact rows are generated and enriched with a 256 MiB address-space
    rlimit on the *whole* process, which then prints counts, hand-written
    anchor answers and peak RSS (KiB).

Usage:
  python3 -I demo/warehouse/scale_driver.py build  --workdir DIR
  python3 -I demo/warehouse/scale_driver.py check  --workdir DIR
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "vendor"))
sys.path.insert(0, str(ROOT))

from barcode_pipeline.warehouse import hdb, importer, query  # noqa: E402
from barcode_pipeline import client as pipe_client  # noqa: E402
from barcode_pipeline import db as pipe_db  # noqa: E402
from barcode_pipeline.config import Config  # noqa: E402
from tests.wh_helpers import build_enrichment_db  # noqa: E402

# Scale fixed by the task statement.
N_PRODUCTS = 100
CLAIMS_PER_PRODUCT = 200          # 100 * 200 = 20,000 claims
TOTAL_CLAIMS = N_PRODUCTS * CLAIMS_PER_PRODUCT
N_FACTS = 200_000
MEM_CAP_BYTES = 256 * 1024 * 1024

# One confirmed normalized service input, re-used by every product's rows.
RULES = {
    "X|CN|v1": "001",
}
SOURCES = [
    {"barcode": "X", "market": "CN", "rule_version": "v1",
     "supplier": "S", "item_id": "base", "fact_ref": "norm"},
]
# Additional source rows added after the real enrichment run: they reuse the
# same confirmed service input (X|CN|v1 -> 001) but carry each product's own
# key, so claims built on them belong to that product.  They are appended to
# the enrichment DB through the pipeline's own idempotent importer and never
# hand-edited.
EXTRA_ROWS_PER_PRODUCT = 1


def product_id(index: int) -> str:
    return f"P{index:04d}"


def extend_source_db_with_products(workdir: Path, source_db: str) -> str:
    """Add one confirmed row per product through the pipeline itself.

    The extra rows reuse the already-confirmed service input
    (X|CN|v1 -> 001); the pipeline's late-mapping logic joins them onto the
    existing confirmed slot without any new service call, so the DB stays a
    genuine, fully deliverable pipeline product.
    """
    extra = workdir / "extra-sources.jsonl"
    with open(extra, "w", encoding="utf-8") as fh:
        # Pipeline line numbers follow file position and line 1 already
        # exists, so lead with one duplicate base row: the real per-product
        # rows then occupy source lines 2..N_PRODUCTS+1 and no product key is
        # lost to an idempotent no-op.
        fh.write(json.dumps(SOURCES[0], separators=(",", ":")) + "\n")
        for p in range(N_PRODUCTS):
            fh.write(json.dumps({
                "barcode": "X", "market": "CN", "rule_version": "v1",
                "supplier": "S", "item_id": product_id(p),
                "fact_ref": f"row-{p}",
            }, separators=(",", ":")) + "\n")
    cfg = Config(base_url="http://127.0.0.1:1", batch_size=500)
    conn = pipe_db.open_db(source_db)
    try:
        readiness = pipe_client.run_start(
            conn, cfg, str(extra), run_id="scale-extension"
        )
        assert readiness["deliverable"], readiness
    finally:
        conn.close()
    # source_line of product p's confirmed row: base row is line 1, extras
    # follow at 2..N_PRODUCTS+1
    return source_db


def source_line_for_product(p: int) -> int:
    return 2 + p


def write_changes_stream(path: Path) -> None:
    """Emit 20k upsert rows; each product gets 200 staggered [k,k+2) claims."""
    with open(path, "w", encoding="utf-8") as fh:
        n = 0
        for p in range(N_PRODUCTS):
            for k in range(CLAIMS_PER_PRODUCT):
                row = {
                    "claim_id": f"C-{p}-{k}",
                    "revision": 1,
                    "source_line": source_line_for_product(p),
                    "valid_from": k * 2,
                    "valid_to": k * 2 + 2,
                }
                fh.write(json.dumps(row, separators=(",", ":")) + "\n")
                n += 1
    assert n == TOTAL_CLAIMS


def import_all(workdir: Path, source_db: str, changes: Path) -> None:
    """Import the 20k rows in several batches, each its own knowledge seq."""
    conn = hdb.open_warehouse(str(workdir / "wh.db"))
    try:
        batch_no = 0
        buffer: list[dict] = []
        chunk_size = 2_500
        tmp = workdir / "batch-chunk.jsonl"
        with open(changes, "r", encoding="utf-8") as fh:
            for line in fh:
                buffer.append(json.loads(line))
                if len(buffer) == chunk_size:
                    batch_no += 1
                    with open(tmp, "w", encoding="utf-8") as out:
                        for row in buffer:
                            out.write(json.dumps(row) + "\n")
                    importer.import_batch(
                        conn, source_db, str(tmp), f"scale-{batch_no}"
                    )
                    buffer.clear()
        assert not buffer  # 20,000 divides evenly into 2,500-row chunks
        assert batch_no == TOTAL_CLAIMS // chunk_size
    finally:
        conn.close()


def write_facts_stream(path: Path) -> None:
    """200k facts: deterministic product/time spread, shuffled-ish by hash."""
    with open(path, "w", encoding="utf-8") as fh:
        for i in range(N_FACTS):
            p = (i * 7 + (i // N_PRODUCTS)) % N_PRODUCTS
            # times span well beyond the claimed domain so missing occurs too
            t = (i * 3) % (2 * CLAIMS_PER_PRODUCT + 20) - 10
            row = {
                "fact_id": f"F{i:07d}",
                "supplier": "S",
                "item_id": product_id(p),
                "market": "CN",
                "event_time": t,
                "amount": i % 1000,
            }
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")


def expected_anchor(item_id: str, t: int) -> tuple[str, str | None]:
    """Hand-written expectation for the staggered [k,k+2) layout.

    Width-2 claims tile [0,400) with no gaps ([0,2),[2,4),...,[398,400)),
    so every integer in that range is normalized 001; only times outside are
    missing.
    """
    del item_id  # every product uses the same timeline shape
    if 0 <= t < 2 * CLAIMS_PER_PRODUCT:
        return "normalized", "001"
    return "missing", None


def do_build(workdir: Path) -> None:
    workdir.mkdir(parents=True, exist_ok=True)
    source_db = build_enrichment_db(
        workdir / "src", SOURCES, RULES, name="scale-src", batch_size=10
    )
    extend_source_db_with_products(workdir, source_db)
    changes = workdir / "changes-20k.jsonl"
    write_changes_stream(changes)
    import_all(workdir, source_db, changes)
    write_facts_stream(workdir / "facts-200k.jsonl")
    print(json.dumps({
        "event": "build_done",
        "claims": TOTAL_CLAIMS,
        "facts": N_FACTS,
        "workdir": str(workdir),
    }, indent=2))


def do_check(workdir: Path, output: Path) -> None:
    # Hard cap on the whole process: malloc beyond this raises MemoryError.
    resource.setrlimit(
        resource.RLIMIT_AS, (MEM_CAP_BYTES, MEM_CAP_BYTES)
    )
    from barcode_pipeline.warehouse import facts

    conn = hdb.open_warehouse(str(workdir / "wh.db"))
    try:
        assert hdb.latest_seq(conn) == TOTAL_CLAIMS // 2_500
        # a few hand-written anchors before streaming
        anchors = [("P0000", 0), ("P0000", 398), ("P0042", 100),
                   ("P0099", 398), ("P0099", 500)]
        anchor_results = {}
        for item_id, t in anchors:
            ans = query.lookup(conn, "S", item_id, "CN", t, seq="latest")
            exp_state, exp_val = expected_anchor(item_id, t)
            assert ans["state"] == exp_state, (item_id, t, ans["state"])
            assert ans["normalized_value"] == exp_val
            anchor_results[f"{item_id}@{t}"] = (ans["state"], exp_val)

        summary = facts.enrich_file(
            conn,
            str(workdir / "facts-200k.jsonl"),
            str(output),
            seq="latest",
        )
        assert summary["row_count"] == N_FACTS, summary["row_count"]
        # every generated fact is either normalized (even t in range) or
        # missing; no conflicts because each product's claims are disjoint
        assert sum(summary["state_counts"].values()) == N_FACTS
        assert summary["state_counts"]["conflict"] == 0
        assert summary["state_counts"]["invalid"] == 0
    finally:
        conn.close()

    # verify a sample of output rows independently, again under the cap
    checked = 0
    with open(output, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            if line_no % 997 != 0:
                continue
            rec = json.loads(line)
            exp_state, exp_val = expected_anchor(
                rec["item_id"], rec["event_time"]
            )
            assert rec["state"] == exp_state, (line_no, rec)
            assert rec["normalized_value"] == exp_val
            checked += 1
    peak_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(json.dumps({
        "event": "check_done",
        "row_count": summary["row_count"],
        "state_counts": summary["state_counts"],
        "anchor_results": anchor_results,
        "sampled_rows": checked,
        "memory_cap_bytes": MEM_CAP_BYTES,
        "peak_rss_kib": peak_kib,
        "peak_rss_mib": round(peak_kib / 1024, 1),
        "under_cap": peak_kib * 1024 < MEM_CAP_BYTES,
    }, indent=2))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--workdir", required=True)
    c = sub.add_parser("check")
    c.add_argument("--workdir", required=True)
    c.add_argument("--output", required=True)
    args = ap.parse_args(argv)
    wd = Path(args.workdir)
    if args.cmd == "build":
        do_build(wd)
    else:
        do_check(wd, Path(args.output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
