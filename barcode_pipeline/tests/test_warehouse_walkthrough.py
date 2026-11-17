"""Canonical bi-temporal walkthrough, checked against a hand-written list.

Timeline (one product, supplier=sup1, item_id=P, market=CN):

  s1: A rev1  [10,40) -> 001
  s2: B rev1  [20,30) -> 999   (conflicts with A inside the overlap)
  s3: B rev2  [25,35) -> 001   (replaces B rev1 wholesale)
  s4: A rev2  retract          (A disappears entirely)

The expected point answers are fixed by hand, never derived from the code
under test.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.helpers import BaseCase, read_jsonl  # noqa: E402
from tests.wh_helpers import (  # noqa: E402
    WALKTHROUGH_RULES,
    WALKTHROUGH_SOURCES,
    build_enrichment_db,
    wh_cli,
    write_changes,
    write_jsonl,
)
from barcode_pipeline.warehouse import hdb, query  # noqa: E402


def _upsert(claim_id, revision, source_line, valid_from, valid_to):
    return {
        "claim_id": claim_id,
        "revision": revision,
        "source_line": source_line,
        "valid_from": valid_from,
        "valid_to": valid_to,
    }


def _retract(claim_id, revision):
    return {"claim_id": claim_id, "revision": revision}


# Hand-written expectation table: (seq, event_time) -> (state, value/code)
HAND_WRITTEN = {
    # s1: only A covers [10,40)
    (1, 9): ("missing", None),
    (1, 10): ("normalized", "001"),
    (1, 39): ("normalized", "001"),
    (1, 40): ("missing", None),
    # s2: B rev1 [20,30)->999 overlaps A [10,40)->001
    (2, 19): ("normalized", "001"),
    (2, 22): ("conflict", None),
    (2, 27): ("conflict", None),
    (2, 29): ("conflict", None),
    (2, 30): ("normalized", "001"),
    (2, 32): ("normalized", "001"),
    # s3: B rev2 [25,35)->001; A and B now agree everywhere both exist
    (3, 19): ("normalized", "001"),
    (3, 22): ("normalized", "001"),
    (3, 27): ("normalized", "001"),
    (3, 32): ("normalized", "001"),
    (3, 35): ("normalized", "001"),
    (3, 39): ("normalized", "001"),
    (3, 40): ("missing", None),
    # s4: A retracted; B rev2 [25,35) is the only remaining claim
    (4, 22): ("missing", None),
    (4, 25): ("normalized", "001"),
    (4, 27): ("normalized", "001"),
    (4, 32): ("normalized", "001"),
    (4, 34): ("normalized", "001"),
    (4, 35): ("missing", None),
    (4, 40): ("missing", None),
}


class WalkthroughTest(BaseCase):
    def setUp(self) -> None:
        super().setUp()
        self.source_db = build_enrichment_db(
            self.workdir / "src", WALKTHROUGH_SOURCES, WALKTHROUGH_RULES,
            name="walk",
        )
        self.wh_db = str(self.workdir / "wh.db")
        self.changes = self.workdir / "changes"
        self.changes.mkdir()
        # Source line 1 is product P via barcode X -> 001; line 3 is the
        # same product P via alternative barcode Y -> 999.
        write_changes(self.changes / "s1.jsonl", [
            _upsert("A", 1, 1, 10, 40),
        ])
        write_changes(self.changes / "s2.jsonl", [
            _upsert("B", 1, 3, 20, 30),
        ])
        write_changes(self.changes / "s3.jsonl", [
            _upsert("B", 2, 1, 25, 35),
        ])
        write_changes(self.changes / "s4.jsonl", [
            _retract("A", 2),
        ])

    def _import(self, seq: int) -> None:
        proc = wh_cli([
            "warehouse", "import", "--warehouse", self.wh_db,
            "--source", self.source_db,
            "--changes", str(self.changes / f"s{seq}.jsonl"),
            "--batch-id", f"batch-s{seq}",
        ])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        got = json.loads(proc.stdout)
        self.assertEqual(got["knowledge_seq"], seq)
        self.assertFalse(got["replayed"])

    def test_four_batches_hand_written_points(self) -> None:
        for seq in (1, 2, 3, 4):
            self._import(seq)
        conn = hdb.open_warehouse(self.wh_db)
        try:
            for (seq, t), (state, value) in HAND_WRITTEN.items():
                ans = query.lookup(
                    conn, "sup1", "P", "CN", t, seq=seq
                )
                self.assertEqual(
                    ans["state"], state,
                    f"seq={seq} t={t}: state {ans['state']!r} != {state!r}",
                )
                if state == "normalized":
                    self.assertEqual(ans["normalized_value"], value)
                else:
                    self.assertIsNone(ans["normalized_value"])
        finally:
            conn.close()

    def test_conflict_lists_both_sides_and_never_picks_winner(self) -> None:
        for seq in (1, 2):
            self._import(seq)
        conn = hdb.open_warehouse(self.wh_db)
        try:
            ans = query.lookup(conn, "sup1", "P", "CN", 22, seq=2)
            self.assertEqual(ans["state"], "conflict")
            self.assertEqual(len(ans["disagreements"]), 2)
            by_status = {
                d["normalized_value"] or d["error_code"]: d
                for d in ans["disagreements"]
            }
            self.assertEqual(by_status["001"]["supports"][0]["claim_id"], "A")
            self.assertEqual(by_status["999"]["supports"][0]["claim_id"], "B")
            claims = sorted(s["claim_id"] for s in ans["supports"])
            self.assertEqual(claims, ["A", "B"])
            # conflict carries no chosen answer
            self.assertIsNone(ans["normalized_value"])
            self.assertIsNone(ans["error_code"])
        finally:
            conn.close()

    def test_s3_dual_support_and_s4_single_support(self) -> None:
        for seq in (1, 2, 3, 4):
            self._import(seq)
        conn = hdb.open_warehouse(self.wh_db)
        try:
            s3_27 = query.lookup(conn, "sup1", "P", "CN", 27, seq=3)
            self.assertEqual(s3_27["state"], "normalized")
            self.assertEqual(s3_27["normalized_value"], "001")
            self.assertEqual(
                sorted(s["claim_id"] for s in s3_27["supports"]), ["A", "B"]
            )
            self.assertEqual(
                {s["revision"] for s in s3_27["supports"]}, {1, 2}
            )
            s3_32 = query.lookup(conn, "sup1", "P", "CN", 32, seq=3)
            self.assertEqual(
                sorted(s["claim_id"] for s in s3_32["supports"]), ["A", "B"]
            )
            s4_27 = query.lookup(conn, "sup1", "P", "CN", 27, seq=4)
            self.assertEqual(
                [s["claim_id"] for s in s4_27["supports"]], ["B"]
            )
            self.assertEqual(s4_27["supports"][0]["revision"], 2)
        finally:
            conn.close()

    def test_half_open_endpoints(self) -> None:
        for seq in (1, 2, 3, 4):
            self._import(seq)
        conn = hdb.open_warehouse(self.wh_db)
        try:
            # [25,35): 25 inclusive, 35 exclusive
            inside = query.lookup(conn, "sup1", "P", "CN", 25, seq=4)
            boundary = query.lookup(conn, "sup1", "P", "CN", 35, seq=4)
            self.assertEqual(inside["state"], "normalized")
            self.assertEqual(boundary["state"], "missing")
        finally:
            conn.close()

    def test_timeline_segments_and_evidence_change_splits(self) -> None:
        for seq in (1, 2, 3, 4):
            self._import(seq)
        conn = hdb.open_warehouse(self.wh_db)
        try:
            # s2 over [10,40): A-only, conflict overlap, A-only, gap
            tl2 = query.timeline(conn, "sup1", "P", "CN", 10, 40, seq=2)
            states2 = [(s["start"], s["end"], s["state"]) for s in tl2["segments"]]
            self.assertEqual(states2, [
                (10, 20, "normalized"),
                (20, 30, "conflict"),
                (30, 40, "normalized"),
            ])
            # s3: same value 001 throughout [10,40), but the support set
            # changes at 25 (B joins) and 35 (B leaves) -> three segments.
            tl3 = query.timeline(conn, "sup1", "P", "CN", 10, 40, seq=3)
            self.assertEqual([s["state"] for s in tl3["segments"]],
                             ["normalized"] * 3)
            self.assertEqual(
                [(s["start"], s["end"]) for s in tl3["segments"]],
                [(10, 25), (25, 35), (35, 40)],
            )
            mid = tl3["segments"][1]
            self.assertEqual(
                sorted(s["claim_id"] for s in mid["supports"]), ["A", "B"]
            )
            # segments are contiguous and fully cover the query range
            self.assertEqual(tl3["segments"][0]["start"], 10)
            self.assertEqual(tl3["segments"][-1]["end"], 40)
            for a, b in zip(tl3["segments"], tl3["segments"][1:]):
                self.assertEqual(a["end"], b["start"])
            # s4: only B's [25,35) remains, flanked by missing segments
            tl4 = query.timeline(conn, "sup1", "P", "CN", 10, 40, seq=4)
            self.assertEqual(
                [(s["start"], s["end"], s["state"]) for s in tl4["segments"]],
                [(10, 25, "missing"), (25, 35, "normalized"),
                 (35, 40, "missing")],
            )
        finally:
            conn.close()

    def test_enrich_and_impact_from_fact_file(self) -> None:
        for seq in (1, 2, 3, 4):
            self._import(seq)
        # Deliberately shuffled, repeated and carrying extra columns; rows
        # must come back in input order including exact duplicates.
        facts = [
            {"fact_id": "f1", "supplier": "sup1", "item_id": "P",
             "market": "CN", "event_time": 32, "note": "late fact"},
            {"fact_id": "f2", "supplier": "sup1", "item_id": "P",
             "market": "CN", "event_time": 22},
            {"fact_id": "f2", "supplier": "sup1", "item_id": "P",
             "market": "CN", "event_time": 22},  # exact duplicate retained
            {"fact_id": "f3", "supplier": "sup1", "item_id": "P",
             "market": "CN", "event_time": 27},
            {"fact_id": "f4", "supplier": "sup1", "item_id": "P",
             "market": "CN", "event_time": 40},
        ]
        fact_path = write_jsonl(self.workdir / "facts.jsonl", facts)
        out_s2 = str(self.workdir / "enrich-s2.jsonl")
        proc = wh_cli([
            "warehouse", "enrich", "--warehouse", self.wh_db,
            "--facts", fact_path, "--output", out_s2, "--seq", "2",
        ])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        rows = read_jsonl(out_s2)
        self.assertEqual(len(rows), 5)
        self.assertEqual([r["line_no"] for r in rows], [1, 2, 3, 4, 5])
        self.assertEqual(
            [r["state"] for r in rows],
            ["normalized", "conflict", "conflict", "conflict", "missing"],
        )
        self.assertEqual(rows[0]["note"], "late fact")  # extra column kept
        self.assertEqual(rows[1]["fact_id"], rows[2]["fact_id"])  # dup kept
        self.assertTrue(all(r["knowledge_seq"] == 2 for r in rows))

        impact_path = str(self.workdir / "impact.jsonl")
        proc = wh_cli([
            "warehouse", "impact", "--warehouse", self.wh_db,
            "--facts", fact_path, "--output", impact_path,
            "--old-seq", "2", "--new-seq", "3",
        ])
        self.assertEqual(proc.returncode, 0, proc.stderr)
        changed = read_jsonl(impact_path)
        # 22 (conflict->001, lines 2 and its exact duplicate 3) and 27
        # (conflict->001, line 4); 32 keeps 001 even though it gains a
        # second supporting claim; 40 stays missing.
        self.assertEqual([c["line_no"] for c in changed], [2, 3, 4])
        for c in changed:
            self.assertEqual(c["old"]["state"], "conflict")
            self.assertEqual(c["new"]["state"], "normalized")
            self.assertEqual(c["new"]["normalized_value"], "001")
        # The two duplicate input lines produce two impact rows.
        self.assertEqual(changed[0]["fact"], changed[1]["fact"])

        # Re-running enrich onto the existing output must be refused.
        again = wh_cli([
            "warehouse", "enrich", "--warehouse", self.wh_db,
            "--facts", fact_path, "--output", out_s2,
        ])
        self.assertEqual(again.returncode, 3)

    def test_history_s1_s2_immutable_after_later_imports(self) -> None:
        for seq in (1, 2, 3, 4):
            self._import(seq)
        conn = hdb.open_warehouse(self.wh_db)
        try:
            # old seq answers must not have been rewritten by later batches
            self.assertEqual(
                query.lookup(conn, "sup1", "P", "CN", 22, seq=1)["state"],
                "normalized",
            )
            self.assertEqual(
                query.lookup(conn, "sup1", "P", "CN", 22, seq=2)["state"],
                "conflict",
            )
            ledger = hdb.inspect_warehouse(conn)
            self.assertEqual([b["knowledge_seq"] for b in ledger["batches"]],
                             [1, 2, 3, 4])
            self.assertTrue(all(b["status"] == "admitted"
                                for b in ledger["batches"]))
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
