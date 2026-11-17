"""Main walkthrough: EU page first, CN/BAD next, duplicate input mapping."""

from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.helpers import (  # noqa: E402
    FIXTURES,
    MAIN_EXPECTED,
    MAIN_RULES,
    BaseCase,
    ServerHarness,
    read_jsonl,
    run_cli,
    write_config,
)


class MainWalkthroughTest(BaseCase):
    scenario = {
        "rules": MAIN_RULES,
        "page_size": 1,
        "page_order": "desc",  # EU (highest slot) comes back first
    }

    def _run(self, ingestion: str) -> None:
        wd = self.workdir
        with ServerHarness(self.scenario) as srv:
            cfg = write_config(
                wd / f"cfg-{ingestion}.json", srv.base_url,
                batch_size=2, ingestion=ingestion,
            )
            db_path = str(wd / f"pipe-{ingestion}.db")
            out_path = str(wd / f"out-{ingestion}.jsonl")

            # start is allowed to finish the whole job here
            proc = run_cli(["--config", cfg, "start",
                            "--db", db_path, "--source", str(FIXTURES / "main.jsonl")])
            self.assertEqual(
                proc.returncode, 0,
                f"start failed: {proc.stderr}\n{proc.stdout}",
            )

            # inspect must report deliverable
            insp = run_cli(["--config", cfg, "inspect", "--db", db_path, "--json"])
            self.assertEqual(insp.returncode, 0, insp.stderr)
            report = json.loads(insp.stdout)
            self.assertTrue(report["readiness"]["deliverable"])
            self.assertEqual(report["readiness"]["source_lines"], 4)

            # export
            exp = run_cli(["--config", cfg, "export",
                           "--db", db_path, "--output", out_path])
            self.assertEqual(exp.returncode, 0, exp.stderr)
            rows = read_jsonl(out_path)

            # ---- hand-written expectations, in original order ----
            self.assertEqual(len(rows), 4)
            for got, want in zip(rows, MAIN_EXPECTED):
                for key, value in want.items():
                    self.assertEqual(got[key], value, (key, got))

            # every output line keeps the full original business row
            raw_rows = read_jsonl(FIXTURES / "main.jsonl")
            for got, raw in zip(rows, raw_rows):
                self.assertEqual(got["source"], raw)

            # the two identical CN/X/v1 lines are TWO deliveries sharing one
            # service slot result but keeping distinct occurrence identities
            self.assertEqual(rows[0]["line"], 1)
            self.assertEqual(rows[1]["line"], 2)
            self.assertEqual(rows[0]["input_digest"], rows[1]["input_digest"])
            self.assertEqual(rows[0]["result_source"]["job_id"],
                             rows[1]["result_source"]["job_id"])
            self.assertEqual(rows[0]["result_source"]["slot"],
                             rows[1]["result_source"]["slot"])
            self.assertEqual(rows[0]["result_source"]["payload_hash"],
                             rows[1]["result_source"]["payload_hash"])

            # EU context must not collide with CN despite same barcode X
            self.assertNotEqual(rows[2]["input_digest"], rows[0]["input_digest"])
            self.assertEqual(rows[2]["normalized_value"], "999")

            # real business invalid is delivered, not retried into something else
            self.assertEqual(rows[3]["state"], "invalid")
            self.assertEqual(rows[3]["error_code"], "BAD_BARCODE")
            self.assertIsNone(rows[3]["normalized_value"])

            # explain line 2: full provenance including both mapped lines
            ex = run_cli(["--config", cfg, "explain",
                          "--db", db_path, "--line", "2"])
            self.assertEqual(ex.returncode, 0, ex.stderr)
            provenance = json.loads(ex.stdout)
            self.assertEqual(provenance["input"]["market"], "CN")
            self.assertEqual(provenance["confirmed_result"]["normalized_value"], "001")
            self.assertEqual(provenance["mapped_source_lines"], [1, 2])
            self.assertIn("client_slot", provenance["job"])
            self.assertIn("idempotency_key", provenance["job"])

            # submission log: quota charged once per unique input
            events = srv.submission_log()
            submits = [e for e in events if e["event"] == "submit"]
            registered_slots = sum(e["item_count"] for e in submits)
            # 3 unique inputs (X/CN, X/EU, BAD/CN), batch_size=2 -> 2 jobs
            self.assertEqual(registered_slots, 3)

    def test_streaming_ingestion(self) -> None:
        self._run("streaming")

    def test_prescan_ingestion(self) -> None:
        self._run("prescan")

    def test_both_ingestion_modes_agree(self) -> None:
        # one workdir, one server, two databases: outputs must be identical
        with ServerHarness(self.scenario) as srv:
            produced = {}
            for ingestion in ("streaming", "prescan"):
                wd = self.workdir
                cfg = write_config(
                    wd / f"cfg-{ingestion}.json", srv.base_url,
                    batch_size=2, ingestion=ingestion,
                )
                db_path = str(wd / f"pipe-{ingestion}.db")
                out_path = str(wd / f"out-{ingestion}.jsonl")
                proc = run_cli(["--config", cfg, "start",
                                "--db", db_path,
                                "--source", str(FIXTURES / "main.jsonl")])
                self.assertEqual(proc.returncode, 0, proc.stderr)
                exp = run_cli(["--config", cfg, "export",
                               "--db", db_path, "--output", out_path])
                self.assertEqual(exp.returncode, 0, exp.stderr)
                produced[ingestion] = read_jsonl(out_path)
        a, b = produced["streaming"], produced["prescan"]
        # client_slot may differ because the two modes batch differently;
        # the business content, line order and result payloads must agree
        for key in ("line", "barcode", "market", "rule_version", "state",
                    "normalized_value", "error_code", "input_digest", "source"):
            self.assertEqual(
                [r[key] for r in a], [r[key] for r in b], key
            )
        self.assertEqual(
            [r["result_source"]["payload_hash"] for r in a],
            [r["result_source"]["payload_hash"] for r in b],
        )


if __name__ == "__main__":
    unittest.main()
