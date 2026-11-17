"""Revision semantics, batch rollback, idempotency, crash and isolation."""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
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
from barcode_pipeline.warehouse import hdb, importer, query  # noqa: E402


def _up(claim_id, revision, source_line, vf, vt):
    return {"claim_id": claim_id, "revision": revision,
            "source_line": source_line, "valid_from": vf, "valid_to": vt}


def _ret(claim_id, revision):
    return {"claim_id": claim_id, "revision": revision}


class SemanticsTest(BaseCase):
    def setUp(self) -> None:
        super().setUp()
        self.source_db = build_enrichment_db(
            self.workdir / "src", WALKTHROUGH_SOURCES, WALKTHROUGH_RULES,
            name="sem",
        )
        # Copy the source DB so the "source moved/deleted" tests can delete
        # the original without breaking later tests.
        self.source_copy = str(self.workdir / "source-copy.db")
        with open(self.source_db, "rb") as a, open(self.source_copy, "wb") as b:
            b.write(a.read())
        self.wh = str(self.workdir / "wh.db")
        self.ch = self.workdir / "ch"
        self.ch.mkdir()

    def _import(self, fname, rows, batch_id, source=None):
        path = write_changes(self.ch / fname, rows)
        conn = hdb.open_warehouse(self.wh)
        try:
            return importer.import_batch(
                conn, source or self.source_db, path, batch_id
            )
        finally:
            conn.close()

    def _open(self):
        return hdb.open_warehouse(self.wh)

    # ------------------------------------------------------------------
    # Late low revisions and retract/recover
    # ------------------------------------------------------------------
    def test_late_low_revision_never_overrides_higher(self) -> None:
        self._import("b1.jsonl", [_up("A", 2, 1, 10, 40)], "b1")
        # rev 1 arrives later: admitted but never selected while rev2 exists
        self._import("b2.jsonl", [_up("A", 1, 1, 0, 100)], "b2")
        conn = self._open()
        try:
            self.assertEqual(hdb.latest_seq(conn), 2)
            ans = query.lookup(conn, "sup1", "P", "CN", 15, seq=2)
            self.assertEqual(ans["state"], "normalized")
            self.assertEqual(ans["normalized_value"], "001")
            # evidence must be rev2's interval [10,40), not rev1's [0,100)
            self.assertEqual(ans["supports"][0]["revision"], 2)
            self.assertEqual(
                (ans["supports"][0]["valid_from"],
                 ans["supports"][0]["valid_to"]),
                (10, 40),
            )
            # at seq 1 only rev2 existed anyway
            ans1 = query.lookup(conn, "sup1", "P", "CN", 5, seq=1)
            self.assertEqual(ans1["state"], "missing")
        finally:
            conn.close()

    def test_retract_then_higher_revision_recovers(self) -> None:
        self._import("b1.jsonl", [_up("C", 1, 1, 10, 40)], "b1")
        self._import("b2.jsonl", [_ret("C", 2)], "b2")
        conn = self._open()
        try:
            self.assertEqual(
                query.lookup(conn, "sup1", "P", "CN", 20, seq=2)["state"],
                "missing",
            )
        finally:
            conn.close()
        # a higher revision re-establishes the claim
        self._import("b3.jsonl", [_up("C", 3, 1, 25, 35)], "b3")
        conn = self._open()
        try:
            ans = query.lookup(conn, "sup1", "P", "CN", 30, seq=3)
            self.assertEqual(ans["state"], "normalized")
            self.assertEqual(ans["supports"][0]["revision"], 3)
            # history of the retracted period stays reproducible
            self.assertEqual(
                query.lookup(conn, "sup1", "P", "CN", 20, seq=2)["state"],
                "missing",
            )
            # rev 2 (the retraction) cannot be re-upserted at a lower number
            with self.assertRaises(hdb.WarehouseError):
                self._import("b4.jsonl", [_up("C", 2, 1, 0, 5)], "b4")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Invalid merging and normalized/invalid mixing
    # ------------------------------------------------------------------
    def test_same_error_code_invalid_claims_merge(self) -> None:
        # product E has one invalid source row (line 5); two claims on it
        self._import("b1.jsonl", [_up("E1", 1, 5, 0, 100)], "b1")
        self._import("b2.jsonl", [_up("E2", 1, 5, 20, 80)], "b2")
        conn = self._open()
        try:
            ans = query.lookup(conn, "sup1", "E", "CN", 50, seq=2)
            self.assertEqual(ans["state"], "invalid")
            self.assertEqual(ans["error_code"], "BAD_BARCODE")
            self.assertEqual(
                sorted(s["claim_id"] for s in ans["supports"]), ["E1", "E2"]
            )
        finally:
            conn.close()

    def test_different_error_codes_and_normalized_invalid_conflict(self) -> None:
        # E -> BAD_BARCODE (line 5), D -> CHECKSUM_FAIL (line 6): different
        # codes on the same claim key can only be built by pointing two
        # claims at different source rows of the *same product*. Repoint D's
        # product key by constructing an extra source row manually through a
        # second enrichment DB sharing product E.
        sources = WALKTHROUGH_SOURCES + [
            {"barcode": "DUD", "market": "CN", "rule_version": "v1",
             "supplier": "sup1", "item_id": "E", "fact_ref": "e-dud"},
            # same service input X|CN|v1 (answer 001) but product key E
            {"barcode": "X", "market": "CN", "rule_version": "v1",
             "supplier": "sup1", "item_id": "E", "fact_ref": "e-norm"},
        ]
        db2 = build_enrichment_db(
            self.workdir / "src2", sources, WALKTHROUGH_RULES, name="sem2"
        )
        # line 8 is the DUD/E row -> CHECKSUM_FAIL on product E
        self._import("b1.jsonl", [_up("E1", 1, 5, 0, 100)], "b1")
        self._import("b2.jsonl", [_up("E2", 1, 8, 20, 80)], "b2", source=db2)
        conn = self._open()
        try:
            ans = query.lookup(conn, "sup1", "E", "CN", 50, seq=2)
            self.assertEqual(ans["state"], "conflict")
            codes = {d["error_code"] for d in ans["disagreements"]}
            self.assertEqual(codes, {"BAD_BARCODE", "CHECKSUM_FAIL"})

            # normalized vs invalid mix: a normalized source row carrying
            # product key E (same barcode triple X|CN -> 001, different
            # product key), line 9
            self._import("b3.jsonl", [_up("E3", 1, 9, 40, 60)], "b3",
                         source=db2)
            ans3 = query.lookup(conn, "sup1", "E", "CN", 50, seq=3)
            self.assertEqual(ans3["state"], "conflict")
            statuses = {d["business_status"] for d in ans3["disagreements"]}
            self.assertEqual(statuses, {"invalid", "normalized"})
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Context isolation: (supplier, item_id, market) is the full key
    # ------------------------------------------------------------------
    def test_market_isolates_claims(self) -> None:
        # line 1: P/CN -> 001; line 7: P/EU -> 555
        self._import("b1.jsonl", [_up("CN1", 1, 1, 0, 100)], "b1")
        self._import("b2.jsonl", [_up("EU1", 1, 7, 0, 100)], "b2")
        conn = self._open()
        try:
            cn = query.lookup(conn, "sup1", "P", "CN", 50, seq=2)
            eu = query.lookup(conn, "sup1", "P", "EU", 50, seq=2)
            self.assertEqual(cn["normalized_value"], "001")
            self.assertEqual(eu["normalized_value"], "555")
            self.assertEqual(cn["supports"][0]["claim_id"], "CN1")
            self.assertEqual(eu["supports"][0]["claim_id"], "EU1")
            # an unknown market is missing, not invalid
            self.assertEqual(
                query.lookup(conn, "sup1", "P", "US", 50, seq=2)["state"],
                "missing",
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Idempotency and batch identity
    # ------------------------------------------------------------------
    def test_retry_same_batch_returns_original_seq(self) -> None:
        path = write_changes(self.ch / "b1.jsonl", [_up("A", 1, 1, 10, 40)])
        conn = self._open()
        try:
            first = importer.import_batch(conn, self.source_db, path, "b1")
            second = importer.import_batch(conn, self.source_db, path, "b1")
            third = importer.import_batch(conn, self.source_copy, path, "b1")
        finally:
            conn.close()
        self.assertEqual(first["knowledge_seq"], 1)
        self.assertFalse(first["replayed"])
        self.assertEqual(second["knowledge_seq"], 1)
        self.assertTrue(second["replayed"])
        self.assertEqual(third["knowledge_seq"], 1)
        conn = self._open()
        try:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM wh_batches").fetchone()["n"],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM wh_claim_revisions"
                ).fetchone()["n"],
                1,
            )
        finally:
            conn.close()

    def test_duplicate_only_batch_still_gets_a_seq(self) -> None:
        path1 = write_changes(self.ch / "b1.jsonl", [_up("A", 1, 1, 10, 40)])
        path2 = write_changes(self.ch / "b2.jsonl", [_up("A", 1, 1, 10, 40)])
        conn = self._open()
        try:
            importer.import_batch(conn, self.source_db, path1, "b1")
            summary = importer.import_batch(conn, self.source_db, path2, "b2")
        finally:
            conn.close()
        self.assertEqual(summary["knowledge_seq"], 2)
        self.assertEqual(summary["new_revisions"], 0)
        self.assertEqual(summary["duplicate_rows"], 1)
        conn = self._open()
        try:
            ledger = hdb.inspect_warehouse(conn)
            self.assertEqual([b["knowledge_seq"] for b in ledger["batches"]], [1, 2])
            self.assertEqual(ledger["batches"][1]["duplicate_rows"], 1)
        finally:
            conn.close()

    def test_old_batch_replays_after_retract_and_recovery(self) -> None:
        # A batch admitted before a retraction must still replay afterwards:
        # duplicate content is accepted on identity alone, never
        # re-validated against the current head.
        path1 = write_changes(self.ch / "b1.jsonl", [_up("A", 1, 1, 10, 40)])
        path_ret = write_changes(self.ch / "b2.jsonl", [_ret("A", 2)])
        path_rec = write_changes(self.ch / "b3.jsonl", [_up("A", 3, 1, 5, 15)])
        conn = self._open()
        try:
            first = importer.import_batch(conn, self.source_db, path1, "b1")
            importer.import_batch(conn, self.source_db, path_ret, "b2")
            importer.import_batch(conn, self.source_db, path_rec, "b3")
            # replay the original upsert batch, twice; same seq, no growth
            replay1 = importer.import_batch(conn, self.source_db, path1, "b1")
            replay2 = importer.import_batch(conn, self.source_db, path1, "b1")
            # the retraction batch replays too
            replay_ret = importer.import_batch(
                conn, self.source_db, path_ret, "b2"
            )
        finally:
            conn.close()
        self.assertEqual(replay1["knowledge_seq"], first["knowledge_seq"])
        self.assertTrue(replay1["replayed"] and replay2["replayed"])
        self.assertTrue(replay_ret["replayed"])
        conn = self._open()
        try:
            # only the three original material batches exist; A still head rev3
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM wh_batches").fetchone()["n"],
                3,
            )
            head = conn.execute(
                "SELECT head_revision, head_kind FROM wh_claims WHERE claim_id='A'"
            ).fetchone()
            self.assertEqual((head["head_revision"], head["head_kind"]),
                             (3, "upsert"))
            # retract then re-upsert the SAME batch_id with different content
            with self.assertRaises(hdb.WarehouseError):
                importer.import_batch(
                    conn, self.source_db,
                    write_changes(self.ch / "b3b.jsonl",
                                  [_up("A", 3, 1, 5, 99)]),
                    "b3",
                )
        finally:
            conn.close()

    def test_same_batch_id_different_material_rejected(self) -> None:
        path1 = write_changes(self.ch / "b1.jsonl", [_up("A", 1, 1, 10, 40)])
        path2 = write_changes(self.ch / "b1b.jsonl", [_up("A", 1, 1, 10, 50)])
        conn = self._open()
        try:
            importer.import_batch(conn, self.source_db, path1, "shared")
            with self.assertRaises(hdb.WarehouseError):
                importer.import_batch(conn, self.source_db, path2, "shared")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Whole-batch rollback on bad tail records / content conflicts
    # ------------------------------------------------------------------
    def test_tail_bad_record_rolls_back_whole_batch(self) -> None:
        # valid first row, malformed tail: nothing may be visible.  The tail
        # has source_line but no interval, so it matches neither the upsert
        # nor the retract row shape.
        bad = self.ch / "bad-tail.jsonl"
        with open(bad, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(_up("A", 1, 1, 10, 40)) + "\n")
            fh.write('{"claim_id": "A", "revision": 2, "source_line": 1}\n')
        conn = self._open()
        try:
            with self.assertRaises(hdb.WarehouseError):
                importer.import_batch(conn, self.source_db, bad, "bad1")
            self.assertEqual(hdb.latest_seq(conn), 0)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM wh_claims").fetchone()["n"],
                0,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM wh_batches").fetchone()["n"],
                0,
            )
        finally:
            conn.close()
        # fixing the tail allows the same batch_id to run
        fixed = write_changes(self.ch / "fixed.jsonl", [_up("A", 1, 1, 10, 40)])
        conn = self._open()
        try:
            summary = importer.import_batch(conn, self.source_db, fixed, "bad1")
            self.assertEqual(summary["knowledge_seq"], 1)
        finally:
            conn.close()

    def test_same_revision_different_content_rejects_and_rolls_back(self) -> None:
        path1 = write_changes(self.ch / "b1.jsonl", [_up("A", 1, 1, 10, 40)])
        # same claim/revision, different interval — content comparison must
        # catch it even though the normalized value is identical
        path2 = write_changes(self.ch / "b2.jsonl", [_up("A", 1, 1, 10, 50)])
        conn = self._open()
        try:
            importer.import_batch(conn, self.source_db, path1, "b1")
            before = conn.execute(
                "SELECT COUNT(*) AS n FROM wh_claim_revisions"
            ).fetchone()["n"]
            with self.assertRaises(hdb.WarehouseError):
                importer.import_batch(conn, self.source_db, path2, "b2")
            after = conn.execute(
                "SELECT COUNT(*) AS n FROM wh_claim_revisions"
            ).fetchone()["n"]
            self.assertEqual(before, after)
            self.assertEqual(hdb.latest_seq(conn), 1)
        finally:
            conn.close()

    def test_in_batch_same_revision_different_content_rejected(self) -> None:
        path = write_changes(self.ch / "mix.jsonl", [
            _up("A", 1, 1, 10, 40),
            _up("A", 1, 1, 10, 50),
        ])
        conn = self._open()
        try:
            with self.assertRaises(hdb.WarehouseError):
                importer.import_batch(conn, self.source_db, path, "mix")
            self.assertEqual(hdb.latest_seq(conn), 0)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Input validation
    # ------------------------------------------------------------------
    def test_bad_intervals_and_fields_rejected(self) -> None:
        conn = self._open()
        try:
            # valid_from >= valid_to
            with self.assertRaises(hdb.WarehouseError):
                importer.import_batch(
                    conn, self.source_db,
                    write_changes(self.ch / "i1.jsonl",
                                  [_up("A", 1, 1, 40, 40)]),
                    "i1",
                )
            # out of int64 range
            with self.assertRaises(hdb.WarehouseError):
                importer.import_batch(
                    conn, self.source_db,
                    write_changes(self.ch / "i2.jsonl",
                                  [_up("A", 1, 1, 0, 2 ** 63)]),
                    "i2",
                )
            # non-positive revision / booleans rejected
            with self.assertRaises(hdb.WarehouseError):
                importer.import_batch(
                    conn, self.source_db,
                    write_changes(self.ch / "i3.jsonl",
                                  [_up("A", 0, 1, 0, 10)]),
                    "i3",
                )
            # unknown source line
            with self.assertRaises(hdb.WarehouseError):
                importer.import_batch(
                    conn, self.source_db,
                    write_changes(self.ch / "i4.jsonl",
                                  [_up("A", 1, 999, 0, 10)]),
                    "i4",
                )
            self.assertEqual(hdb.latest_seq(conn), 0)
        finally:
            conn.close()

    def test_undeliverable_source_db_rejected(self) -> None:
        # A pipeline DB whose input is still pending (service never ran) must
        # fail the delivery gate rather than expose unconfirmed evidence.
        from barcode_pipeline import db as pipedb
        pending_db = str(self.workdir / "pending-pipe.db")
        c = pipedb.open_db(pending_db)
        pipedb.insert_source_row(c, 1, {
            "barcode": "X", "market": "CN", "rule_version": "v1",
            "supplier": "sup1", "item_id": "P",
        })
        c.close()
        conn = self._open()
        try:
            with self.assertRaises(hdb.WarehouseError) as ctx:
                importer.import_batch(
                    conn, pending_db,
                    write_changes(self.ch / "e.jsonl",
                                  [_up("A", 1, 1, 0, 10)]),
                    "e",
                )
            self.assertIn("not deliverable", str(ctx.exception))
            self.assertEqual(hdb.latest_seq(conn), 0)
        finally:
            conn.close()

    def test_product_key_immutable_across_batches(self) -> None:
        # first establish A on P/CN via line 1
        self._import("b1.jsonl", [_up("A", 1, 1, 0, 100)], "b1")
        # line 4 is product Q — A cannot move product
        with self.assertRaises(hdb.WarehouseError):
            self._import("b2.jsonl", [_up("A", 2, 4, 0, 100)], "b2")

    # ------------------------------------------------------------------
    # Source independence: deleting the source DB after import
    # ------------------------------------------------------------------
    def test_source_db_can_be_deleted_after_import(self) -> None:
        path = write_changes(self.ch / "b1.jsonl", [_up("A", 1, 1, 10, 40)])
        conn = self._open()
        try:
            importer.import_batch(conn, self.source_db, path, "b1")
        finally:
            conn.close()
        os.remove(self.source_db)
        for suffix in ("-wal", "-shm"):
            p = self.source_db + suffix
            if os.path.exists(p):
                os.remove(p)
        # old seq still fully reproducible, provenance included
        conn = self._open()
        try:
            ans = query.lookup(conn, "sup1", "P", "CN", 20, seq=1)
            self.assertEqual(ans["normalized_value"], "001")
            src = ans["supports"][0]["result_source"]
            self.assertEqual(set(src),
                             {"job_id", "slot", "page_index",
                              "payload_hash", "input_digest"})
            self.assertEqual(ans["supports"][0]["source"]["fact_ref"], "p-1")
            # replaying the same batch needs no source DB either
            replay = importer.import_batch(
                conn, self.source_db, path, "b1"
            )
            self.assertTrue(replay["replayed"])
            self.assertEqual(replay["knowledge_seq"], 1)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Real SIGKILL mid-import, then new-process retry
    # ------------------------------------------------------------------
    def test_sigkill_then_new_process_retry_and_old_seq(self) -> None:
        path = write_changes(self.ch / "k1.jsonl", [_up("A", 1, 1, 10, 40)])
        entry = str(Path(__file__).resolve().parents[1]
                    / "barcode_pipeline" / "__main__.py")
        env = dict(os.environ, BARCODE_KILL_AFTER="wh_batch_staged")
        proc = subprocess.Popen(
            [sys.executable, "-I", entry, "warehouse", "import",
             "--warehouse", self.wh, "--source", self.source_db,
             "--changes", path, "--batch-id", "kill-batch"],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover
            proc.kill()
            proc.wait()
        self.assertEqual(proc.returncode, -signal.SIGKILL, proc.stderr.read())

        # nothing visible: no seq consumed, no claim
        conn = self._open()
        try:
            self.assertEqual(hdb.latest_seq(conn), 0)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM wh_claims").fetchone()["n"],
                0,
            )
        finally:
            conn.close()

        # new process retries the same batch_id -> admitted once, seq 1
        retry = wh_cli([
            "warehouse", "import", "--warehouse", self.wh,
            "--source", self.source_copy, "--changes", path,
            "--batch-id", "kill-batch",
        ])
        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assertEqual(json.loads(retry.stdout)["knowledge_seq"], 1)

        # a subsequent batch takes seq 2 (the killed attempt consumed nothing)
        path2 = write_changes(self.ch / "k2.jsonl", [_up("B", 1, 3, 20, 30)])
        again = wh_cli([
            "warehouse", "import", "--warehouse", self.wh,
            "--source", self.source_copy, "--changes", path2,
            "--batch-id", "after-kill",
        ])
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(json.loads(again.stdout)["knowledge_seq"], 2)
        conn = self._open()
        try:
            # batch ledger explains the admission and old seq 1 still works
            self.assertEqual(
                query.lookup(conn, "sup1", "P", "CN", 15, seq=1)["state"],
                "normalized",
            )
            batch = conn.execute(
                "SELECT * FROM wh_batches WHERE batch_id=?", ("kill-batch",)
            ).fetchone()
            self.assertEqual(batch["status"], "admitted")
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Concurrent writers get distinct seqs; readers see whole batches only
    # ------------------------------------------------------------------
    def test_concurrent_imports_get_distinct_seqs(self) -> None:
        n = 6
        paths = []
        for i in range(n):
            paths.append(write_changes(
                self.ch / f"c{i}.jsonl",
                [_up(f"CLAIM{i}", 1, 1, 0, 100)],
            ))
        procs = []
        for i, p in enumerate(paths):
            procs.append(subprocess.Popen(
                [sys.executable, "-I",
                 str(Path(__file__).resolve().parents[1]
                     / "barcode_pipeline" / "__main__.py"),
                 "warehouse", "import", "--warehouse", self.wh,
                 "--source", self.source_db, "--changes", p,
                 "--batch-id", f"conc-{i}"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            ))
        seqs = []
        for proc in procs:
            out, err = proc.communicate(timeout=60)
            self.assertEqual(proc.returncode, 0, err)
            seqs.append(json.loads(out)["knowledge_seq"])
        self.assertEqual(sorted(seqs), list(range(1, n + 1)))
        conn = self._open()
        try:
            rows = conn.execute(
                "SELECT knowledge_seq, batch_id FROM wh_batches ORDER BY"
                " knowledge_seq"
            ).fetchall()
            self.assertEqual(len(rows), n)
            self.assertEqual(len({r["knowledge_seq"] for r in rows}), n)
        finally:
            conn.close()

    def test_reader_never_sees_half_batch(self) -> None:
        # Every import is one transaction: a concurrent reader must either
        # see seq k-1 or seq k, never revisions without their batch row.
        path = write_changes(self.ch / "one.jsonl", [_up("A", 1, 1, 0, 100)])
        conn = self._open()
        try:
            importer.import_batch(conn, self.source_db, path, "b1")
            bad = conn.execute(
                "SELECT COUNT(*) AS n FROM wh_claim_revisions r"
                " LEFT JOIN wh_batches b ON b.knowledge_seq=r.knowledge_seq"
                " WHERE b.knowledge_seq IS NULL"
            ).fetchone()["n"]
            self.assertEqual(bad, 0)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
