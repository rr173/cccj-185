"""Protocol and edge-case tests.

Covers:
  * same Idempotency-Key + different body -> 409 conflict, job blocked
  * wrong body_digest -> 422, no quota charged
  * transient 404/503 faults are retried and eventually succeed
  * retry budget exhausted -> job incomplete, then a fresh resume recovers it
  * empty source file -> deliverable zero-row package
  * output target already exists -> refused without --force
  * server-side pending slot is never treated as business invalid; once
    resolved, a new-process resume completes the job
  * spoofed results (unknown slot, bad input_digest, wrong job_id) are
    rejected and never back-filled
"""

from __future__ import annotations

import json
import os
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


class IdempotencyConflictTest(BaseCase):
    def test_same_key_different_body_is_conflict_and_blocks(self) -> None:
        wd = self.workdir
        with ServerHarness({"rules": MAIN_RULES}) as srv:
            import httpx

            from barcode_pipeline.digest import (
                idempotency_key,
                request_body_digest,
            )

            # First body: the real deterministic key the client itself derives
            body_a = [{"client_slot": 0, "barcode": "X",
                       "market": "CN", "rule_version": "v1"}]
            digest_a = request_body_digest(body_a)
            key = idempotency_key("run:/tmp/conflict-demo", digest_a)

            r1 = httpx.post(
                f"{srv.base_url}/jobs",
                headers={"Idempotency-Key": key},
                json={"body_digest": digest_a, "items": body_a},
                timeout=5.0,
            )
            self.assertEqual(r1.status_code, 201)
            first_job_id = r1.json()["job_id"]

            # Same key, HONEST but different body/digest -> 409, no new job
            body_b = [{"client_slot": 0, "barcode": "X",
                       "market": "US", "rule_version": "v1"}]
            digest_b = request_body_digest(body_b)
            r2 = httpx.post(
                f"{srv.base_url}/jobs",
                headers={"Idempotency-Key": key},
                json={"body_digest": digest_b, "items": body_b},
                timeout=5.0,
            )
            self.assertEqual(r2.status_code, 409)
            self.assertEqual(r2.json()["existing_job_id"], first_job_id)
            log = srv.submission_log()
            self.assertEqual(
                [e["event"] for e in log], ["submit", "conflict"]
            )

            # and the client maps a real 409 onto the blocked state end to
            # end.  Pre-occupy the deterministic key the client will derive
            # for its single batch with a different body.
            import os

            from barcode_pipeline.digest import input_digest

            db_path2 = str(wd / "pipe2.db")
            # streaming mode numbers slots in first-appearance source order:
            # line1 X/CN -> 0, line3 X/EU -> 1, line4 BAD/CN -> 2
            ordered = [
                ("X", "CN", "v1"),
                ("X", "EU", "v1"),
                ("BAD", "CN", "v1"),
            ]
            real_items = [
                {"client_slot": i, "barcode": b, "market": m,
                 "rule_version": rv}
                for i, (b, m, rv) in enumerate(ordered)
            ]
            real_digest = request_body_digest(real_items)
            run_id = "run:" + os.path.abspath(db_path2)
            derived_key = idempotency_key(run_id, real_digest)

            # occupy derived_key with a *different* body before the client runs
            squatter = [{"client_slot": 7, "barcode": "Y",
                         "market": "JP", "rule_version": "v9"}]
            sq = httpx.post(
                f"{srv.base_url}/jobs",
                headers={"Idempotency-Key": derived_key},
                json={"body_digest": request_body_digest(squatter),
                      "items": squatter},
                timeout=5.0,
            )
            self.assertEqual(sq.status_code, 201)

            cfg2 = write_config(wd / "cfg2.json", srv.base_url, batch_size=10)
            proc2 = run_cli(["--config", cfg2, "start",
                             "--db", db_path2,
                             "--source", str(FIXTURES / "main.jsonl")])
            self.assertEqual(proc2.returncode, 2)
            insp2 = json.loads(run_cli(
                ["--config", cfg2, "inspect", "--db", db_path2, "--json"]
            ).stdout)
            blocked = next(
                j for j in insp2["jobs"] if j["status"] == "blocked"
            )
            self.assertIn("different body", blocked["status_detail"])


class WrongDigestTest(BaseCase):
    def test_wrong_body_digest_is_rejected_without_quota(self) -> None:
        wd = self.workdir
        with ServerHarness({"rules": MAIN_RULES}) as srv:
            # direct protocol probe, bypassing the honest client
            import httpx

            item = {"client_slot": 0, "barcode": "X",
                    "market": "CN", "rule_version": "v1"}
            resp = httpx.post(
                f"{srv.base_url}/jobs",
                headers={"Idempotency-Key": "idem-tampered"},
                json={"body_digest": "sha256:lies", "items": [item]},
                timeout=5.0,
            )
            self.assertEqual(resp.status_code, 422)
            self.assertIn("body_digest mismatch", resp.json()["error"])
            self.assertEqual(srv.submission_log(), [])  # nothing registered


class TransientFaultTest(BaseCase):
    def test_404_then_503_are_retried(self) -> None:
        wd = self.workdir
        scenario = {
            "rules": MAIN_RULES,
            "page_size": 2,
            "faults": {
                "post_jobs": ["503", "404"],
                "get_job": ["503"],
                "get_results": ["404", "503"],
            },
        }
        with ServerHarness(scenario) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url,
                               batch_size=10, max_attempts=6)
            db_path = str(wd / "pipe.db")
            proc = run_cli(["--config", cfg, "start",
                            "--db", db_path,
                            "--source", str(FIXTURES / "main.jsonl")])
            self.assertEqual(proc.returncode, 0, proc.stderr)
            out = str(wd / "o.jsonl")
            self.assertEqual(
                run_cli(["--config", cfg, "export",
                         "--db", db_path, "--output", out]).returncode,
                0,
            )
            rows = read_jsonl(out)
            self.assertEqual([r["normalized_value"] for r in rows],
                             ["001", "001", "999", None])


class RetryExhaustionTest(BaseCase):
    def test_exhausted_retries_mark_incomplete_and_resume_recovers(self) -> None:
        wd = self.workdir
        # persistent 503 on GET results: the budget is spent, job keeps the
        # data already sealed and stays resumable
        scenario = {
            "rules": MAIN_RULES,
            "faults": {"get_results": ["503", "503", "503", "503", "503"]},
        }
        with ServerHarness(scenario) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url,
                               batch_size=10, max_attempts=3)
            db_path = str(wd / "pipe.db")
            proc = run_cli(["--config", cfg, "start",
                            "--db", db_path,
                            "--source", str(FIXTURES / "main.jsonl")])
            self.assertEqual(proc.returncode, 2)
            insp = json.loads(run_cli(
                ["--config", cfg, "inspect", "--db", db_path, "--json"]
            ).stdout)
            job = insp["jobs"][0]
            self.assertEqual(job["status"], "incomplete")
            self.assertIn("network exhausted", job["status_detail"])
            # source rows and the sealed mapping survived
            self.assertEqual(insp["readiness"]["source_lines"], 4)

            # repair the service; a NEW process resume finishes the delivery
            srv.set_scenario({"rules": MAIN_RULES, "page_size": 2})
            resume = run_cli(["--config", cfg, "resume", "--db", db_path])
            self.assertEqual(resume.returncode, 0, resume.stderr)
            out = str(wd / "o.jsonl")
            self.assertEqual(
                run_cli(["--config", cfg, "export",
                         "--db", db_path, "--output", out]).returncode,
                0,
            )
            self.assertEqual([r["line"] for r in read_jsonl(out)],
                             [1, 2, 3, 4])


class EmptyInputTest(BaseCase):
    def test_empty_source_produces_zero_row_deliverable(self) -> None:
        wd = self.workdir
        with ServerHarness({"rules": MAIN_RULES}) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url)
            db_path = str(wd / "pipe.db")
            out = str(wd / "empty.jsonl")
            proc = run_cli(["--config", cfg, "start",
                            "--db", db_path,
                            "--source", str(FIXTURES / "empty.jsonl")])
            self.assertEqual(proc.returncode, 0, proc.stderr)
            readiness = json.loads(proc.stdout)
            self.assertEqual(readiness["source_lines"], 0)
            self.assertTrue(readiness["deliverable"])
            self.assertEqual(
                run_cli(["--config", cfg, "export",
                         "--db", db_path, "--output", out]).returncode,
                0,
            )
            self.assertEqual(read_jsonl(out), [])
            self.assertEqual(os.path.getsize(out), 0)
            # no jobs were ever sent for an empty input domain
            self.assertEqual(srv.submission_log(), [])


class OutputExistsTest(BaseCase):
    def test_existing_output_is_refused_then_force_overwrites(self) -> None:
        wd = self.workdir
        with ServerHarness({"rules": MAIN_RULES}) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url, batch_size=10)
            db_path = str(wd / "pipe.db")
            out = str(wd / "o.jsonl")
            with open(out, "w", encoding="utf-8") as fh:
                fh.write("PRE-EXISTING CONTENT\n")
            proc = run_cli(["--config", cfg, "start",
                            "--db", db_path,
                            "--source", str(FIXTURES / "main.jsonl")])
            self.assertEqual(proc.returncode, 0, proc.stderr)

            refused = run_cli(["--config", cfg, "export",
                               "--db", db_path, "--output", out])
            self.assertEqual(refused.returncode, 2)
            self.assertEqual(
                open(out, encoding="utf-8").read(), "PRE-EXISTING CONTENT\n"
            )

            forced = run_cli(["--config", cfg, "export",
                              "--db", db_path, "--output", out, "--force"])
            self.assertEqual(forced.returncode, 0, forced.stderr)
            rows = read_jsonl(out)
            self.assertEqual(len(rows), 4)


class PendingThenResolvedTest(BaseCase):
    def test_pending_slot_is_not_invalid_and_resumes_after_resolution(self) -> None:
        wd = self.workdir
        scenario = {
            "rules": MAIN_RULES,
            "page_size": 2,
            "pending_slots": [0],  # one slot still processing server-side
        }
        with ServerHarness(scenario) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url, batch_size=10)
            db_path = str(wd / "pipe.db")
            proc = run_cli(["--config", cfg, "start",
                            "--db", db_path,
                            "--source", str(FIXTURES / "main.jsonl")])
            self.assertEqual(proc.returncode, 2)  # not deliverable yet
            insp = json.loads(run_cli(
                ["--config", cfg, "inspect", "--db", db_path, "--json"]
            ).stdout)
            job = insp["jobs"][0]
            self.assertEqual(job["status"], "incomplete")
            self.assertIn("processing", job["status_detail"])

            # the pending observation must not have been turned into invalid
            import sqlite3

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            kinds = {
                (r["slot"], r["business_status"])
                for r in conn.execute(
                    "SELECT slot, business_status FROM slot_observations"
                    " WHERE contradicting=0"
                )
            }
            self.assertIn((0, "pending"), kinds)
            self.assertNotIn((0, "invalid"), kinds)
            conn.close()

            # server finishes the slot; new-process resume completes
            srv.resolve_pending()
            resume = run_cli(["--config", cfg, "resume", "--db", db_path])
            self.assertEqual(resume.returncode, 0, resume.stderr)
            out = str(wd / "o.jsonl")
            self.assertEqual(
                run_cli(["--config", cfg, "export",
                         "--db", db_path, "--output", out]).returncode,
                0,
            )
            rows = read_jsonl(out)
            # slot 0 is one of the three unique inputs; every line is final
            self.assertTrue(
                all(r["state"] in ("normalized", "invalid") for r in rows)
            )


class SpoofedResultTest(BaseCase):
    def test_unknown_slot_bad_digest_wrong_job_id_are_rejected(self) -> None:
        wd = self.workdir
        base_scenario = {"rules": MAIN_RULES, "page_size": 10}
        with ServerHarness(dict(base_scenario, spoof={
            "unknown_slot": 0,
        })) as srv:
            cfg = write_config(wd / "cfg-a.json", srv.base_url, batch_size=10)
            proc = run_cli(["--config", cfg, "start",
                            "--db", str(wd / "a.db"),
                            "--source", str(FIXTURES / "main.jsonl")])
            self.assertEqual(proc.returncode, 2)
            job = json.loads(run_cli(
                ["--config", cfg, "inspect", "--db", str(wd / "a.db"),
                 "--json"]).stdout)["jobs"][0]
            self.assertEqual(job["status"], "blocked")
            self.assertIn("unknown slot", job["status_detail"])

        with ServerHarness(dict(base_scenario, spoof={
            "bad_digest_slot": 1,
        })) as srv:
            cfg = write_config(wd / "cfg-b.json", srv.base_url, batch_size=10)
            run_cli(["--config", cfg, "start",
                     "--db", str(wd / "b.db"),
                     "--source", str(FIXTURES / "main.jsonl")])
            job = json.loads(run_cli(
                ["--config", cfg, "inspect", "--db", str(wd / "b.db"),
                 "--json"]).stdout)["jobs"][0]
            self.assertEqual(job["status"], "blocked")
            self.assertIn("input_digest mismatch", job["status_detail"])

        with ServerHarness(dict(base_scenario, spoof={
            "wrong_job_id_slot": 2,
        })) as srv:
            cfg = write_config(wd / "cfg-c.json", srv.base_url, batch_size=10)
            run_cli(["--config", cfg, "start",
                     "--db", str(wd / "c.db"),
                     "--source", str(FIXTURES / "main.jsonl")])
            job = json.loads(run_cli(
                ["--config", cfg, "inspect", "--db", str(wd / "c.db"),
                 "--json"]).stdout)["jobs"][0]
            self.assertEqual(job["status"], "blocked")
            self.assertIn("job_id", job["status_detail"])


if __name__ == "__main__":
    unittest.main()
