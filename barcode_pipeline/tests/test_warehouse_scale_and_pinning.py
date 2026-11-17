"""Large-scale streaming (256 MiB cap) and long-read version pinning."""

from __future__ import annotations

import json
import os
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
from barcode_pipeline.warehouse import hdb, importer  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
ENTRY = ROOT / "barcode_pipeline" / "__main__.py"


class ScaleStreamingTest(BaseCase):
    def test_20k_claims_200k_facts_under_256mib(self) -> None:
        driver = ROOT / "demo" / "warehouse" / "scale_driver.py"
        workdir = self.workdir / "scale"
        output = workdir / "enriched-200k.jsonl"
        build = subprocess.run(
            [sys.executable, "-I", str(driver), "build",
             "--workdir", str(workdir)],
            capture_output=True, text=True, timeout=600,
        )
        self.assertEqual(build.returncode, 0, build.stderr)
        check = subprocess.run(
            [sys.executable, "-I", str(driver), "check",
             "--workdir", str(workdir), "--output", str(output)],
            capture_output=True, text=True, timeout=600,
        )
        self.assertEqual(check.returncode, 0, check.stderr)
        report = json.loads(check.stdout)
        self.assertEqual(report["row_count"], 200_000)
        self.assertEqual(
            report["state_counts"]["normalized"]
            + report["state_counts"]["missing"],
            200_000,
        )
        self.assertEqual(report["state_counts"]["conflict"], 0)
        self.assertGreater(report["sampled_rows"], 100)
        self.assertTrue(report["under_cap"], report)
        # Persisted scale: 20k revisions, 8 import batches, 20k claims.
        conn = hdb.open_warehouse(str(workdir / "wh.db"))
        try:
            counts = {
                t: conn.execute(
                    f"SELECT COUNT(*) AS n FROM {t}"
                ).fetchone()["n"]
                for t in ("wh_claims", "wh_claim_revisions", "wh_batches")
            }
        finally:
            conn.close()
        self.assertEqual(counts["wh_claims"], 20_000)
        self.assertEqual(counts["wh_claim_revisions"], 20_000)
        self.assertEqual(counts["wh_batches"], 8)
        # output file really has one physical line per fact (streamed)
        with open(output, "r", encoding="utf-8") as fh:
            physical = sum(1 for line in fh if line.strip())
        self.assertEqual(physical, 200_000)


class VersionPinningTest(BaseCase):
    def setUp(self) -> None:
        super().setUp()
        self.source_db = build_enrichment_db(
            self.workdir / "src", WALKTHROUGH_SOURCES, WALKTHROUGH_RULES,
            name="pin",
        )
        self.wh = str(self.workdir / "wh.db")
        self.ch = self.workdir / "ch"
        self.ch.mkdir()

    def _import(self, rows, batch_id):
        path = write_changes(self.ch / f"{batch_id}.jsonl", rows)
        conn = hdb.open_warehouse(self.wh)
        try:
            importer.import_batch(conn, self.source_db, path, batch_id)
        finally:
            conn.close()

    def test_long_enrich_pins_seq_even_when_batch_lands_midrun(self) -> None:
        # seq 1: A [0,100) -> 001 on product P
        self._import([
            {"claim_id": "A", "revision": 1, "source_line": 1,
             "valid_from": 0, "valid_to": 100},
        ], "b1")
        # seq 2 (imported *during* the enrich run): B conflicts 20..80 -> 999
        later = write_changes(self.ch / "b2.jsonl", [
            {"claim_id": "B", "revision": 1, "source_line": 3,
             "valid_from": 20, "valid_to": 80},
        ])
        facts = []
        for i in range(2_000):
            t = (i * 7) % 100
            facts.append({
                "fact_id": f"F{i}", "supplier": "sup1", "item_id": "P",
                "market": "CN", "event_time": t,
            })
        fact_path = write_jsonl(self.workdir / "facts.jsonl", facts)
        out_path = str(self.workdir / "out.jsonl")

        # Deterministic FIFO rendezvous: the child pins seq 1, then blocks on
        # the gate; this process admits seq 2 and releases the gate.
        gate = str(self.workdir / "gate.fifo")
        os.mkfifo(gate)
        env = dict(os.environ, BARCODE_WH_FACT_GATE=gate)
        proc = subprocess.Popen(
            [sys.executable, "-I", str(ENTRY), "warehouse", "enrich",
             "--warehouse", self.wh, "--facts", fact_path,
             "--output", out_path],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        # Opening the FIFO for writing unblocks the child's read open; then
        # hold it open until the mid-run import has been admitted.
        gate_writer = open(gate, "w", encoding="utf-8")
        time.sleep(0.3)  # let the child reach and block on the gate
        conn = hdb.open_warehouse(self.wh)
        try:
            importer.import_batch(conn, self.source_db, later, "b2")
            self.assertEqual(hdb.latest_seq(conn), 2)
        finally:
            conn.close()
        gate_writer.write("x")
        gate_writer.flush()
        gate_writer.close()
        stdout, stderr = proc.communicate(timeout=60)
        self.assertEqual(proc.returncode, 0, stderr)
        summary = json.loads(stdout)
        # The whole file must answer at the seq pinned when enrich started.
        self.assertEqual(summary["knowledge_seq"], 1)
        rows = read_jsonl(out_path)
        self.assertEqual(len(rows), 2_000)
        self.assertTrue(all(r["knowledge_seq"] == 1 for r in rows))
        # pinned to seq 1: B's conflict is invisible, every fact is 001
        self.assertTrue(all(r["state"] == "normalized" for r in rows))
        self.assertTrue(all(r["normalized_value"] == "001" for r in rows))

        # A second, unpinned run now sees seq 2 and the conflict window.
        out2 = str(self.workdir / "out2.jsonl")
        again = wh_cli([
            "warehouse", "enrich", "--warehouse", self.wh,
            "--facts", fact_path, "--output", out2,
        ])
        self.assertEqual(again.returncode, 0, again.stderr)
        rows2 = read_jsonl(out2)
        self.assertTrue(all(r["knowledge_seq"] == 2 for r in rows2))
        self.assertIn("conflict", {r["state"] for r in rows2})


if __name__ == "__main__":
    unittest.main()
