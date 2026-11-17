"""Crash recovery with real new-process resumes.

Two interruption points required by the spec:

  R1. POST accepted by the service but the client never receives the job_id
      (TCP reset after durable server registration). The client must keep its
      SEALED job and the *same* idempotency key; resume replays the POST and
      receives the same server job_id.

  R2. Result rows already landed in SQLite but the fetch progress was not yet
      confirmed externally (we kill the process after the first page is
      durable). Resume continues from the persisted cursor; identical items
      retransmitted by the replayed page are accepted.
"""

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

from tests.helpers import (  # noqa: E402
    FIXTURES,
    MAIN_RULES,
    ROOT,
    BaseCase,
    ServerHarness,
    read_jsonl,
    run_cli,
    write_config,
)
from barcode_pipeline import db as dbmod  # noqa: E402


class RecoveryTest(BaseCase):
    def _run_and_kill(self, command: str, cfg: str, db_path: str,
                      wait_for: str, source: str | None = None) -> None:
        """Run a pipeline command in a child process and SIGKILL at a point."""
        entry = str(ROOT / "barcode_pipeline" / "__main__.py")
        env = dict(os.environ)
        env["BARCODE_KILL_AFTER"] = wait_for
        args = [sys.executable, "-I", entry, "--config", cfg, command,
                "--db", db_path]
        if source is not None:
            args += ["--source", source]
        proc = subprocess.Popen(
            args, cwd=str(ROOT), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        # Give the child a bounded window; it is expected to be killed.
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:  # pragma: no cover
            proc.kill()
            proc.wait()
        err_tail = proc.stderr.read()
        proc.stdout.close()
        proc.stderr.close()
        self.assertEqual(
            proc.returncode, -signal.SIGKILL,
            f"child should have been killed, got {proc.returncode}: {err_tail}",
        )

    def test_recover_lost_post_response_then_lost_progress(self) -> None:
        wd = self.workdir
        log_path = str(wd / "server-submit.jsonl")
        # R1: the very first POST is registered then the socket is dropped.
        scenario = {
            "rules": MAIN_RULES,
            "page_size": 2,
            "faults": {"post_jobs": ["reset"]},
        }
        with ServerHarness(scenario, log_path=log_path) as srv:
            cfg = write_config(wd / "cfg.json", srv.base_url, batch_size=2,
                               max_attempts=5)
            db_path = str(wd / "pipe.db")

            # ---- interruption 1: POST accepted, response lost ----
            self._run_and_kill(
                "start", cfg, db_path, wait_for="post_registered",
                source=str(FIXTURES / "main.jsonl"),
            )

            # local state: both batches were SEALED before any network call,
            # but only the first reached the server.  No server_job_id is
            # persisted and every idempotency key is stable (no random re-key).
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            sealed = conn.execute(
                "SELECT * FROM jobs WHERE status='sealed' ORDER BY id"
            ).fetchall()
            self.assertEqual(len(sealed), 2)
            self.assertTrue(all(j["server_job_id"] is None for j in sealed))
            persisted_keys = [j["idempotency_key"] for j in sealed]
            self.assertTrue(all(k.startswith("idem-") for k in persisted_keys))
            self.assertEqual(len(set(persisted_keys)), 2)
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM source_rows").fetchone()[0], 4)
            # all three unique inputs are mapped, none confirmed yet
            self.assertEqual(conn.execute(
                "SELECT state, COUNT(*) AS n FROM inputs GROUP BY state"
            ).fetchone()["state"], "dispatched")
            conn.close()

            # server side: exactly one job exists, recorded in the submit log
            events = srv.submission_log()
            submit_events = [e for e in events if e["event"] == "submit"]
            self.assertEqual(len(submit_events), 1)
            server_job_id = submit_events[0]["job_id"]

            # ---- resume in a NEW process: same key -> same job_id ----
            resume1 = run_cli(["--config", cfg, "resume", "--db", db_path])
            self.assertEqual(resume1.returncode, 0, resume1.stderr)
            events = srv.submission_log()
            self.assertIn("replay", [e["event"] for e in events])
            replay = next(e for e in events if e["event"] == "replay")
            self.assertEqual(replay["job_id"], server_job_id)
            self.assertEqual(
                replay["idempotency_key"], persisted_keys[0],
                "resume must not mint a new idempotency key",
            )

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM jobs WHERE idempotency_key=?",
                (persisted_keys[0],),
            ).fetchone()
            self.assertEqual(row["server_job_id"], server_job_id)
            # the second batch was submitted normally during resume (new job)
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM jobs WHERE server_job_id IS NOT NULL"
                ).fetchone()["n"],
                2,
            )
            conn.close()

            # ---- interruption 2: kill after first result page is durable ----
            # Rewind every job to mid-paging: status fetching and the cursor
            # back before page 0, so the resumed process really re-enters the
            # paging loop and dies with a durable page on disk.
            conn = sqlite3.connect(db_path)
            conn.execute("UPDATE jobs SET status='fetching'")
            conn.execute(
                "UPDATE page_cursor SET next_token=NULL, complete_seen=0"
            )
            conn.commit()
            conn.close()
            self._run_and_kill(
                "resume", cfg, db_path, wait_for="first_page_durable",
            )

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            pages = conn.execute("SELECT COUNT(*) AS n FROM page_log").fetchone()
            self.assertGreaterEqual(pages["n"], 1)
            # progress beyond the first page must not have been confirmed:
            # no job is confirmed yet
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM jobs WHERE status='confirmed'"
                ).fetchone()["n"],
                0,
            )
            obs_before = conn.execute(
                "SELECT COUNT(*) AS n FROM slot_observations"
            ).fetchone()["n"]
            conn.close()

            # ---- second NEW-process resume completes the delivery ----
            resume2 = run_cli(["--config", cfg, "resume", "--db", db_path])
            self.assertEqual(resume2.returncode, 0, resume2.stderr)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            jobs = conn.execute("SELECT status FROM jobs").fetchall()
            self.assertTrue(all(j["status"] == "confirmed" for j in jobs))
            obs_after = conn.execute(
                "SELECT COUNT(*) AS n FROM slot_observations"
            ).fetchone()["n"]
            # the replayed page's identical items must collapse, not inflate
            # the distinct observation count beyond the unique payloads
            distinct_payloads = conn.execute(
                "SELECT COUNT(DISTINCT payload_hash) AS n FROM slot_observations"
            ).fetchone()["n"]
            self.assertEqual(obs_after, distinct_payloads)
            conn.close()

            # export and verify the full hand-written expectation
            out_path = str(wd / "final.jsonl")
            exp = run_cli(["--config", cfg, "export",
                           "--db", db_path, "--output", out_path])
            self.assertEqual(exp.returncode, 0, exp.stderr)
            rows = read_jsonl(out_path)
            self.assertEqual([r["line"] for r in rows], [1, 2, 3, 4])
            self.assertEqual(
                [r["normalized_value"] for r in rows],
                ["001", "001", "999", None],
            )
            self.assertEqual(rows[3]["state"], "invalid")

            # server-side submit log: two quota-charging submits total
            # (3 unique inputs split into batches of 2); everything after the
            # first ambiguous POST was idempotent replay, never a third submit
            submit_events = [
                e for e in srv.submission_log() if e["event"] == "submit"
            ]
            self.assertEqual(len(submit_events), 2)
            self.assertEqual(
                sum(e["item_count"] for e in submit_events), 3
            )
            replay_events = [
                e for e in srv.submission_log() if e["event"] == "replay"
            ]
            self.assertGreaterEqual(len(replay_events), 1)

        # the on-disk JSONL log mirrors the in-memory submission log
        disk_log = read_jsonl(log_path)
        self.assertTrue(any(e["event"] == "submit" for e in disk_log))
        self.assertTrue(any(e["event"] == "replay" for e in disk_log))


if __name__ == "__main__":
    unittest.main()
