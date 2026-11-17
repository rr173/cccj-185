"""Streaming, mapping and persistence tests.

  * two byte-identical source lines still produce two delivery rows
  * large source is handled in small batches; export row count matches and
    original order is preserved
  * a source line that appears only after its input's slot was already
    dispatched (and even confirmed) joins the existing mapping and receives
    the already-confirmed result — no duplicate job, output context unchanged
  * the mock service state (jobs, idempotency index, scenario) survives a
    service restart through its snapshot file
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
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
from barcode_pipeline.server import MockService  # noqa: E402


class IdenticalLinesTest(BaseCase):
    def test_byte_identical_lines_yield_two_deliveries(self) -> None:
        wd = self.workdir
        src = wd / "dup.jsonl"
        line = json.dumps({
            "barcode": "X", "market": "CN", "rule_version": "v1",
            "sku": "SAME", "note": "exact duplicate",
        }, ensure_ascii=False)
        src.write_text(f"{line}\n{line}\n", encoding="utf-8")
        with ServerHarness({"rules": MAIN_RULES}) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url)
            db_path = str(wd / "pipe.db")
            out = str(wd / "o.jsonl")
            proc = run_cli(["--config", cfg, "start",
                            "--db", db_path, "--source", str(src)])
            self.assertEqual(proc.returncode, 0, proc.stderr)
            exp = run_cli(["--config", cfg, "export",
                           "--db", db_path, "--output", out])
            self.assertEqual(exp.returncode, 0, exp.stderr)
            rows = read_jsonl(out)
            self.assertEqual(len(rows), 2)  # occurrence identity preserved
            self.assertEqual([r["line"] for r in rows], [1, 2])
            self.assertEqual(rows[0]["input_digest"], rows[1]["input_digest"])
            self.assertEqual(rows[0]["source"], rows[1]["source"])
            # quota: exactly one unique input registered
            submits = [e for e in srv.submission_log()
                       if e["event"] == "submit"]
            self.assertEqual(sum(e["item_count"] for e in submits), 1)


class LargeFileStreamingTest(BaseCase):
    def test_large_source_small_batches_preserves_every_row(self) -> None:
        wd = self.workdir
        n_lines = 3000
        n_unique = 25
        src = wd / "big.jsonl"
        rules = dict(MAIN_RULES)
        with open(src, "w", encoding="utf-8") as fh:
            for i in range(n_lines):
                k = i % n_unique
                market = "CN" if k % 2 == 0 else "EU"
                barcode = f"BC{k:03d}"
                rules[f"{barcode}|{market}|v1"] = f"{1000 + k:04d}"
                fh.write(json.dumps({
                    "barcode": barcode,
                    "market": market,
                    "rule_version": "v1",
                    "seq": i,  # every line carries distinct business data
                }, ensure_ascii=False) + "\n")

        with ServerHarness({"rules": rules, "page_size": 7}) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url,
                               batch_size=13, max_attempts=5)
            db_path = str(wd / "pipe.db")
            out = str(wd / "o.jsonl")
            proc = run_cli(["--config", cfg, "start",
                            "--db", db_path, "--source", str(src)])
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(
                run_cli(["--config", cfg, "export",
                         "--db", db_path, "--output", out]).returncode,
                0,
            )
            rows = read_jsonl(out)
            self.assertEqual(len(rows), n_lines)
            self.assertEqual([r["line"] for r in rows],
                             list(range(1, n_lines + 1)))
            # every original business column survived, in original order
            for i, r in enumerate(rows):
                self.assertEqual(r["source"]["seq"], i)
                k = i % n_unique
                self.assertEqual(r["normalized_value"], f"{1000 + k:04d}")
                self.assertEqual(r["state"], "normalized")
            # quota charged per unique input only
            submits = [e for e in srv.submission_log()
                       if e["event"] == "submit"]
            self.assertEqual(sum(e["item_count"] for e in submits), n_unique)


class LateMappingTest(BaseCase):
    def test_new_source_line_joins_already_confirmed_slot(self) -> None:
        wd = self.workdir
        part1 = wd / "part1.jsonl"
        part1.write_text(
            json.dumps({"barcode": "X", "market": "CN",
                        "rule_version": "v1", "sku": "P1-1"}) + "\n" +
            json.dumps({"barcode": "X", "market": "CN",
                        "rule_version": "v1", "sku": "P1-2"}) + "\n",
            encoding="utf-8",
        )
        with ServerHarness({"rules": MAIN_RULES, "page_size": 1}) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url)
            db_path = str(wd / "pipe.db")

            proc1 = run_cli(["--config", cfg, "start",
                             "--db", db_path, "--source", str(part1)])
            self.assertEqual(proc1.returncode, 0, proc1.stderr)
            submits_after_first = [
                e for e in srv.submission_log() if e["event"] == "submit"
            ]
            self.assertEqual(len(submits_after_first), 1)

            # grow the source: line 3 repeats the SAME input (now already
            # confirmed), lines 4-5 introduce new contexts, incl. v2 rule
            full = wd / "full.jsonl"
            extra_rules = dict(MAIN_RULES)
            extra_rules["X|CN|v2"] = "777"
            srv.set_scenario({"rules": extra_rules, "page_size": 1})
            full.write_text(
                part1.read_text(encoding="utf-8") +
                json.dumps({"barcode": "X", "market": "CN",
                            "rule_version": "v1", "sku": "LATE-3"}) + "\n" +
                json.dumps({"barcode": "X", "market": "EU",
                            "rule_version": "v1", "sku": "LATE-4"}) + "\n" +
                json.dumps({"barcode": "X", "market": "CN",
                            "rule_version": "v2", "sku": "LATE-5"}) + "\n",
                encoding="utf-8",
            )

            # second start over the same db: only new inputs dispatch jobs
            proc2 = run_cli(["--config", cfg, "start",
                             "--db", db_path, "--source", str(full)])
            self.assertEqual(proc2.returncode, 0, proc2.stderr)
            all_submits = [e for e in srv.submission_log()
                           if e["event"] == "submit"]
            # first run: 1 input; second run: 2 new inputs (EU/v1, CN/v2)
            self.assertEqual(sum(e["item_count"] for e in all_submits), 3)

            out = str(wd / "o.jsonl")
            self.assertEqual(
                run_cli(["--config", cfg, "export",
                         "--db", db_path, "--output", out]).returncode,
                0,
            )
            rows = read_jsonl(out)
            self.assertEqual([r["line"] for r in rows], [1, 2, 3, 4, 5])
            # the late line 3 binds to the already-confirmed CN/v1 answer
            self.assertEqual(rows[2]["normalized_value"], "001")
            self.assertEqual(rows[2]["input_digest"], rows[0]["input_digest"])
            self.assertEqual(
                rows[2]["result_source"]["payload_hash"],
                rows[0]["result_source"]["payload_hash"],
            )
            # rule_version is part of identity: v2 gets its own answer
            self.assertEqual(rows[4]["rule_version"], "v2")
            self.assertEqual(rows[4]["normalized_value"], "777")
            self.assertNotEqual(rows[4]["input_digest"], rows[0]["input_digest"])

            # explain the late line: mapping includes all three CN/v1 lines
            ex = json.loads(run_cli(
                ["--config", cfg, "explain", "--db", db_path, "--line", "3"]
            ).stdout)
            self.assertEqual(ex["mapped_source_lines"], [1, 2, 3])
            self.assertEqual(ex["confirmed_result"]["normalized_value"], "001")


class ServerSnapshotTest(BaseCase):
    def test_service_state_survives_restart_via_snapshot(self) -> None:
        wd = self.workdir
        snapshot = str(wd / "server-state.json")
        log_path = str(wd / "server-log.jsonl")
        svc1 = MockService(snapshot_path=snapshot, log_path=log_path)
        status, payload, reset = svc1.submit_job(
            "idem-persist",
            {"body_digest": None, "items": []},  # invalid, just to exercise
        )
        self.assertEqual(status, 400)
        # a real submission
        from barcode_pipeline.digest import request_body_digest

        items = [{"client_slot": 0, "barcode": "X",
                  "market": "CN", "rule_version": "v1"}]
        digest = request_body_digest(items)
        status, payload, reset = svc1.submit_job(
            "idem-persist", {"body_digest": digest, "items": items}
        )
        self.assertEqual(status, 201)
        job_id = payload["job_id"]

        # brand-new instance, same snapshot file: idempotent replay works
        svc2 = MockService(snapshot_path=snapshot, log_path=log_path)
        status2, payload2, _ = svc2.submit_job(
            "idem-persist", {"body_digest": digest, "items": items}
        )
        self.assertEqual(status2, 200)
        self.assertTrue(payload2["replayed"])
        self.assertEqual(payload2["job_id"], job_id)
        st, meta = svc2.get_job(job_id)
        self.assertEqual(st, 200)
        self.assertEqual(meta["expected_slots"], 1)
        # append log accumulated across both service instances
        disk = read_jsonl(log_path)
        self.assertEqual([e["event"] for e in disk], ["submit", "replay"])


if __name__ == "__main__":
    unittest.main()
