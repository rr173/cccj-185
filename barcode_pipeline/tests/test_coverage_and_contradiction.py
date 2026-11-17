"""Coverage gate tests: item totals can never substitute for slot coverage.

Scenario B omits one slot from the last page and duplicates another slot,
so the total item count still equals the expected slot count.  The pipeline
must mark the job INCOMPLETE and refuse delivery, then complete after the
scenario is repaired and a fresh resume.

Scenario C adds a contradictory payload for one slot: two different final
results for the same slot.  The pipeline must block (no arbitrary choice),
persisting both payloads for inspection.
"""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.helpers import (  # noqa: E402
    FIXTURES,
    MAIN_RULES,
    BaseCase,
    ServerHarness,
    read_jsonl,
    run_cli,
    write_config,
)


class CoverageGapTest(BaseCase):
    def test_omitted_slot_duplicated_slot_blocks_delivery_then_resumes(self) -> None:
        wd = self.workdir
        # One page only, 4 items for 3 slots: slot 0 duplicated, slot 2 omitted.
        # Slots under batch_size=2: job#1 slots [0,1], job#2 slot [0].
        # Use a single job (batch_size=10) so the custom page layout is exact:
        # slots are assigned by input_digest order; assert via inspect instead
        # of assuming order — the omission targets the BAD input's slot.
        scenario_bad = {
            "rules": MAIN_RULES,
            "custom_pages": [[0, 1, 0]],  # slot 2 missing, slot 0 twice
        }
        with ServerHarness(scenario_bad) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url, batch_size=10)
            db_path = str(wd / "pipe.db")
            out_path = str(wd / "out.jsonl")

            proc = run_cli(["--config", cfg, "start",
                            "--db", db_path,
                            "--source", str(FIXTURES / "main.jsonl")])
            self.assertNotEqual(proc.returncode, 0)
            report = json.loads(proc.stdout)
            self.assertFalse(report["deliverable"])

            # inspect: exactly one job, incomplete with the missing slot named
            insp = run_cli(["--config", cfg, "inspect",
                            "--db", db_path, "--json"])
            data = json.loads(insp.stdout)
            self.assertEqual(len(data["jobs"]), 1)
            job = data["jobs"][0]
            self.assertEqual(job["status"], "incomplete")
            self.assertEqual(job["coverage"]["missing"], [2])
            # item total looked fine (3 items for 3 slots), coverage did not
            self.assertTrue(job["coverage"]["complete_seen"])
            self.assertFalse(job["coverage"]["ok"])

            # export must be refused while coverage is incomplete
            exp = run_cli(["--config", cfg, "export",
                           "--db", db_path, "--output", out_path])
            self.assertEqual(exp.returncode, 2)
            self.assertNotIn("manifest", exp.stdout)

            # repair the server: normal paged result set, resume in new process
            srv.set_scenario({"rules": MAIN_RULES, "page_size": 2})
            resume = run_cli(["--config", cfg, "resume", "--db", db_path])
            self.assertEqual(resume.returncode, 0, resume.stderr)
            self.assertTrue(json.loads(resume.stdout)["deliverable"])

            exp2 = run_cli(["--config", cfg, "export",
                            "--db", db_path, "--output", out_path])
            self.assertEqual(exp2.returncode, 0, exp2.stderr)
            rows = read_jsonl(out_path)
            self.assertEqual([r["line"] for r in rows], [1, 2, 3, 4])
            self.assertEqual(
                [r["state"] for r in rows],
                ["normalized", "normalized", "normalized", "invalid"],
            )


class ContradictionTest(BaseCase):
    def test_contradictory_final_results_block_and_are_both_retained(self) -> None:
        wd = self.workdir
        # Normal pages plus an extra, different final payload for slot 0.
        scenario = {
            "rules": MAIN_RULES,
            "page_size": 2,
            "contradictions": {
                "0": {"normalized_value": "OTHER", "error_code": None}
            },
        }
        with ServerHarness(scenario) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url, batch_size=10)
            db_path = str(wd / "pipe.db")

            proc = run_cli(["--config", cfg, "start",
                            "--db", db_path,
                            "--source", str(FIXTURES / "main.jsonl")])
            self.assertNotEqual(proc.returncode, 0)

            insp = run_cli(["--config", cfg, "inspect",
                            "--db", db_path, "--json"])
            data = json.loads(insp.stdout)
            job = data["jobs"][0]
            self.assertEqual(job["status"], "blocked")
            self.assertIn("distinct final payloads", job["status_detail"])

            # both payloads remain observable in the audit table; nothing was
            # silently chosen
            import sqlite3

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            payloads = [
                r["normalized_value"]
                for r in conn.execute(
                    "SELECT DISTINCT normalized_value FROM slot_observations"
                    " WHERE slot=0 AND kind='final'"
                )
            ]
            self.assertEqual(sorted(payloads), ["001", "OTHER"])
            conn.close()

            # export remains refused
            exp = run_cli(["--config", cfg, "export",
                           "--db", db_path, "--output", str(wd / "x.jsonl")])
            self.assertEqual(exp.returncode, 2)


if __name__ == "__main__":
    unittest.main()
