#!/usr/bin/env python3
"""End-to-end historical-warehouse demonstration with hand-written checks.

The script:

  1. runs the *real* local pipeline against the mock service to produce a
     confirmed enrichment DB carrying supplier/item_id columns (the same
     entry points as start/resume/export — nothing is hand-edited);
  2. admits the canonical s1..s4 change batches and checks every required
     point answer against a hand-written list;
  3. demonstrates late low revisions, retract-then-recover, same-error-code
     invalid merging, market context isolation and half-open endpoints;
  4. runs enrich + impact and prints the recomputation list.

All artifacts land in the given workdir; re-running is self-contained.

Usage: python3 -I demo/warehouse/walkthrough_demo.py --workdir DIR
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "vendor"))
sys.path.insert(0, str(ROOT))

from barcode_pipeline.warehouse import hdb, importer, query, facts  # noqa: E402
from tests.wh_helpers import (  # noqa: E402
    WALKTHROUGH_RULES,
    WALKTHROUGH_SOURCES,
    build_enrichment_db,
    write_changes,
    write_jsonl,
)

P = ("sup1", "P", "CN")


def up(claim, rev, line, vf, vt):
    return {"claim_id": claim, "revision": rev, "source_line": line,
            "valid_from": vf, "valid_to": vt}


def ret(claim, rev):
    return {"claim_id": claim, "revision": rev}


def check(label, got, expected):
    ok = got == expected
    mark = "ok " if ok else "FAIL"
    print(f"  [{mark}] {label}: got={got!r} want={expected!r}")
    if not ok:
        raise AssertionError(label)


def main(workdir: Path) -> int:
    workdir.mkdir(parents=True, exist_ok=True)
    print("== 1. real enrichment DB from the local pipeline ==")
    source_db = build_enrichment_db(
        workdir / "src", WALKTHROUGH_SOURCES, WALKTHROUGH_RULES, name="demo"
    )
    print(f"   source enrichment db: {source_db} (all inputs confirmed)")

    wh = str(workdir / "warehouse.db")
    ch = workdir / "changes"
    ch.mkdir(exist_ok=True)

    def admit(batch_id, rows):
        path = write_changes(ch / f"{batch_id}.jsonl", rows)
        conn = hdb.open_warehouse(wh)
        try:
            summary = importer.import_batch(conn, source_db, path, batch_id)
        finally:
            conn.close()
        print(f"   admitted {batch_id}: knowledge_seq={summary['knowledge_seq']}"
              f" new={summary['new_revisions']} dup={summary['duplicate_rows']}")
        return summary["knowledge_seq"]

    print("== 2. canonical s1..s4 batches ==")
    s1 = admit("s1", [up("A", 1, 1, 10, 40)])                 # A [10,40)->001
    s2 = admit("s2", [up("B", 1, 3, 20, 30)])                # B [20,30)->999
    s3 = admit("s3", [up("B", 2, 1, 25, 35)])                # B rev2 [25,35)->001
    s4 = admit("s4", [ret("A", 2)])                          # A retracted
    assert (s1, s2, s3, s4) == (1, 2, 3, 4)

    conn = hdb.open_warehouse(wh)
    try:
        def state(seq, t):
            a = query.lookup(conn, *P, t, seq=seq)
            return (a["state"], a["normalized_value"], a["error_code"])

        print("   -- hand-written point checklist --")
        # s2: conflicts at 22/27, 32 still 001
        check("s2 @22", state(s2, 22)[0], "conflict")
        check("s2 @27", state(s2, 27)[0], "conflict")
        check("s2 @32", (state(s2, 32)[0], state(s2, 32)[1]),
              ("normalized", "001"))
        # s3: all 001; 27 has A and B support
        for t in (22, 27, 32):
            check(f"s3 @{t}", (state(s3, t)[0], state(s3, t)[1]),
                  ("normalized", "001"))
        a27 = query.lookup(conn, *P, 27, seq=s3)
        check("s3 @27 support claims",
              sorted(x["claim_id"] for x in a27["supports"]), ["A", "B"])
        # s4: 22 missing, 27/32 001 (B only), 40 missing
        check("s4 @22", state(s4, 22)[0], "missing")
        check("s4 @27", (state(s4, 27)[0], state(s4, 27)[1]),
              ("normalized", "001"))
        check("s4 @32", (state(s4, 32)[0], state(s4, 32)[1]),
              ("normalized", "001"))
        check("s4 @40", state(s4, 40)[0], "missing")
        check("s4 @27 only B",
              [x["claim_id"] for x in
               query.lookup(conn, *P, 27, seq=s4)["supports"]],
              ["B"])
        # half-open endpoints
        check("@25 included", state(s4, 25)[0], "normalized")
        check("@35 excluded", state(s4, 35)[0], "missing")
        # history immutability
        check("s1 answer still 001", state(s1, 22)[1], "001")
        check("s2 answer still conflict", state(s2, 22)[0], "conflict")

        print("== 3. late low revision never overrides higher ==")
        # B rev1 arrives again now (already seen) — duplicate, no new revision;
        # then a genuinely late, lower rev on a fresh claim C to prove ordering
        admit("late-b1", [up("B", 1, 3, 20, 30)])  # identical duplicate batch
        admit("c2", [up("C", 2, 1, 0, 100)])
        admit("c1-late", [up("C", 1, 1, 0, 1000)])
        ans = query.lookup(conn, *P, 50, seq="latest")
        check("C head stays rev2 interval",
              (ans["supports"][0]["claim_id"],
               ans["supports"][0]["revision"],
               (ans["supports"][0]["valid_from"],
                ans["supports"][0]["valid_to"])),
              ("C", 2, (0, 100)))

        print("== 4. retract then higher revision recovers ==")
        admit("d1", [up("D", 1, 1, 0, 50)])
        admit("d2-retract", [ret("D", 2)])
        # Lookup is product-wide; observe D's own lifecycle through its
        # ledger head and through its support entry in the merged answer.
        head = conn.execute(
            "SELECT head_revision, head_kind FROM wh_claims WHERE claim_id='D'"
        ).fetchone()
        check("D head after retract",
              (head["head_revision"], head["head_kind"]), (2, "retract"))
        admit("d3-recover", [up("D", 3, 1, 5, 15)])
        head = conn.execute(
            "SELECT head_revision, head_kind FROM wh_claims WHERE claim_id='D'"
        ).fetchone()
        check("D head after recovery",
              (head["head_revision"], head["head_kind"]),
              (3, "upsert"))
        merged = query.lookup(conn, *P, 10, seq="latest")
        d_support = next(s for s in merged["supports"] if s["claim_id"] == "D")
        check("D recovered at rev3 (merged 001 answer)",
              (merged["state"], merged["normalized_value"], d_support["revision"]),
              ("normalized", "001", 3))

        print("== 5. same error code invalid claims merge ==")
        # source line 5 is product E -> invalid BAD_BARCODE
        admit("e1", [up("E1", 1, 5, 0, 100)])
        admit("e2", [up("E2", 1, 5, 20, 80)])
        ea = query.lookup(conn, "sup1", "E", "CN", 50, seq="latest")
        check("merged invalid",
              (ea["state"], ea["error_code"],
               sorted(s["claim_id"] for s in ea["supports"])),
              ("invalid", "BAD_BARCODE", ["E1", "E2"]))

        print("== 6. market context isolation ==")
        # line 7 is P/EU -> 555, never mixed with P/CN
        admit("eu1", [up("EU1", 1, 7, 0, 100)])
        cn = query.lookup(conn, "sup1", "P", "CN", 50, seq="latest")
        eu = query.lookup(conn, "sup1", "P", "EU", 50, seq="latest")
        check("CN answer", cn["normalized_value"], "001")
        check("EU answer", eu["normalized_value"], "555")
        check("unknown market missing",
              query.lookup(conn, "sup1", "P", "US", 50, seq="latest")["state"],
              "missing")

    finally:
        conn.close()

    print("== 7. enrich + impact recomputation list ==")
    facts_path = write_jsonl(workdir / "facts.jsonl", [
        {"fact_id": "f1", **dict(zip(("supplier", "item_id", "market"), P)),
         "event_time": 32},
        {"fact_id": "f2", **dict(zip(("supplier", "item_id", "market"), P)),
         "event_time": 22},
        {"fact_id": "f3", **dict(zip(("supplier", "item_id", "market"), P)),
         "event_time": 27},
        {"fact_id": "f4", **dict(zip(("supplier", "item_id", "market"), P)),
         "event_time": 40},
    ])
    conn = hdb.open_warehouse(wh)
    try:
        enrich_summary = facts.enrich_file(
            conn, facts_path, str(workdir / "enriched.jsonl"), seq="latest"
        )
        impact_summary = facts.impact_file(
            conn, facts_path, str(workdir / "impact-s2-s3.jsonl"), 2, 3
        )
    finally:
        conn.close()
    changed = [json.loads(line)["line_no"]
               for line in open(workdir / "impact-s2-s3.jsonl", encoding="utf-8")
               if line.strip()]
    # 32 changes evidence only -> excluded; 22 and 27 conflict->001.
    check("impact lines (22,27 only)", changed, [2, 3])
    print(f"   enriched rows: {enrich_summary['row_count']} "
          f"counts={enrich_summary['state_counts']}")
    print(f"   impact s2->s3 changed lines: {changed}")
    print("ALL HAND-WRITTEN CHECKS PASSED")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", required=True)
    raise SystemExit(main(Path(ap.parse_args().workdir)))
