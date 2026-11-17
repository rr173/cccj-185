"""HTTPX client implementing the resumable enrichment protocol.

Recovery guarantees (all state lives in SQLite, nothing recoverable is kept
only in memory):

  * The POST idempotency key is derived from the batch body, never randomly
    generated.  A job is SEALED locally before the request, so a process
    killed after server acceptance but before the job_id is persisted simply
    re-POSTs the same key and receives the same job_id.
  * A result page is persisted together with its observations and its next
    cursor in one transaction.  A kill after rows landed but before the next
    request therefore re-fetches the same page; identical retransmissions
    collapse, contradictions block.
  * Network failures and server-side "pending" are never rewritten into
    business "invalid".  Exhausting retries marks the job INCOMPLETE and
    keeps every result already collected.
  * Delivery requires every expected slot of every job to hold exactly one
    consistent FINAL observation — item totals can never prove coverage.
"""

from __future__ import annotations

import json
import os
import signal
import time
from typing import Any, Callable, Iterator

import httpx

from . import db
from .config import Config
from .digest import idempotency_key, request_body_digest

TRANSIENT_STATUSES = {404, 500, 502, 503, 504}


def _test_kill(point: str) -> None:
    """Deterministic crash injection for recovery tests.

    Dormant unless BARCODE_KILL_AFTER is set, so production runs are
    unaffected.  SIGKILL simulates power loss: no Python cleanup, no
    buffer flush beyond what SQLite already committed.
    """
    if os.environ.get("BARCODE_KILL_AFTER") == point:
        os.kill(os.getpid(), signal.SIGKILL)


class RetryExhausted(RuntimeError):
    pass


class HardProtocolError(RuntimeError):
    """Non-retryable protocol failure (4xx semantics, contradiction, ...)."""


def stream_jsonl(path: str) -> Iterator[tuple[int, dict[str, Any]]]:
    """Yield (line_no, row) without loading the whole file."""
    with open(path, "r", encoding="utf-8") as fh:
        line_no = 0
        for line in fh:
            if not line.strip():
                continue
            line_no += 1
            row = json.loads(line)
            missing = [k for k in ("barcode", "market", "rule_version") if k not in row]
            if missing:
                raise HardProtocolError(
                    f"source line {line_no} missing fields: {missing}"
                )
            yield line_no, row


# --------------------------------------------------------------------------
# HTTP retry helper
# --------------------------------------------------------------------------

class HttpCaller:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._client: httpx.Client | None = None

    def __enter__(self) -> "HttpCaller":
        self._client = httpx.Client(timeout=config_timeout(self.config))
        return self

    def __exit__(self, *exc: Any) -> None:
        assert self._client is not None
        self._client.close()
        self._client = None

    def _backoff(self, attempt: int) -> float:
        r = self.config.retry
        return min(r.backoff_max_s, r.backoff_base_s * (2 ** attempt))

    def request(
        self,
        method: str,
        url: str,
        *,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        on_attempt: Callable[[int, str], None] | None = None,
    ) -> httpx.Response:
        """Call with bounded retries on transient errors.

        Raises RetryExhausted when the attempt budget is spent.  The caller
        decides whether that means INCOMPLETE (network) or a hard block.
        """
        assert self._client is not None
        attempts = self.config.retry.max_attempts
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            if on_attempt:
                on_attempt(attempt, method)
            try:
                resp = self._client.request(
                    method, url, json=json_body, headers=headers, params=params
                )
            except httpx.TransportError as exc:
                last_exc = exc
                if attempt == attempts:
                    break
                time.sleep(self._backoff(attempt - 1))
                continue
            if resp.status_code in TRANSIENT_STATUSES:
                # A 404 carrying an explicit stale_token code is a protocol
                # signal, not a transient outage: return it so the caller can
                # start a fresh epoch.  Plain 404/503 stay retryable.
                if resp.status_code == 404 and _is_stale_token(resp):
                    return resp
                last_exc = RuntimeError(
                    f"HTTP {resp.status_code}: {resp.text[:200]}"
                )
                if attempt == attempts:
                    break
                time.sleep(self._backoff(attempt - 1))
                continue
            return resp
        raise RetryExhausted(
            f"{method} {url} failed after {attempts} attempts: {last_exc}"
        )


def _is_stale_token(resp: httpx.Response) -> bool:
    try:
        return resp.json().get("code") == "stale_token"
    except (json.JSONDecodeError, ValueError):
        return False


def config_timeout(config: Config) -> httpx.Timeout:
    return httpx.Timeout(config.timeout_s)


# --------------------------------------------------------------------------
# Ingestion & dispatch
# --------------------------------------------------------------------------

def import_source(conn, path: str) -> int:
    """Stream-import a JSONL source.  Re-running is idempotent by line_no."""
    n = 0
    for line_no, row in stream_jsonl(path):
        db.insert_source_row(conn, line_no, row)
        n += 1
    return n


def _build_batch_items(rows: list, slot_base: int = 0) -> list[dict[str, Any]]:
    return [
        {
            "client_slot": slot_base + slot,
            "barcode": r["barcode"],
            "market": r["market"],
            "rule_version": r["rule_version"],
        }
        for slot, r in enumerate(rows)
    ]


def dispatch_pending(conn, config: Config, run_id: str) -> list[int]:
    """Seal jobs for all inputs still pending. Deterministic across processes.

    Slots are globally unique per database (never restarted at 0 per batch),
    so two different batches always produce different bodies and therefore
    different idempotency keys.
    """
    sealed: list[int] = []
    pending = db.pending_inputs(conn)
    slot_base = db.next_free_slot(conn)
    for start in range(0, len(pending), config.batch_size):
        chunk = pending[start:start + config.batch_size]
        items = _build_batch_items(chunk, slot_base + start)
        body_digest = request_body_digest(items)
        key = idempotency_key(run_id, body_digest)
        job_pk, created = db.seal_job(conn, key, body_digest, items)
        if created:
            sealed.append(job_pk)
    return sealed


# --------------------------------------------------------------------------
# Job lifecycle against the service
# --------------------------------------------------------------------------

def submit_job(conn, caller: HttpCaller, config: Config, job_pk: int) -> None:
    job = db.get_job(conn, job_pk)
    if job["server_job_id"]:
        return  # previously confirmed accepted by the server

    slot_rows = list(
        conn.execute(
            "SELECT s.slot, i.barcode, i.market, i.rule_version"
            " FROM job_slots s JOIN inputs i ON i.input_digest=s.input_digest"
            " WHERE s.job_pk=? ORDER BY s.slot ASC",
            (job_pk,),
        )
    )
    items = [
        {
            "client_slot": int(r["slot"]),
            "barcode": r["barcode"],
            "market": r["market"],
            "rule_version": r["rule_version"],
        }
        for r in slot_rows
    ]
    body_digest = request_body_digest(items)
    if body_digest != job["body_digest"]:  # defensive: sealed body is immutable
        raise HardProtocolError(f"job {job_pk}: sealed body digest changed")
    body = {"body_digest": body_digest, "items": items}
    url = f"{config.base_url.rstrip('/')}/jobs"

    try:
        resp = caller.request(
            "POST",
            url,
            json_body=body,
            headers={"Idempotency-Key": job["idempotency_key"]},
        )
    except RetryExhausted as exc:
        # Ambiguous: the POST may have been accepted.  We must NOT mint a new
        # idempotency key; the sealed row keeps the old one for resume.
        db.record_attempt_error(conn, job_pk, f"submit retries exhausted: {exc}")
        db.mark_job_incomplete(conn, job_pk, f"network exhausted: {exc}")
        return

    if resp.status_code in (200, 201):
        payload = resp.json()
        # R1 interruption point: the POST was durably accepted server-side;
        # die before the job_id reaches local SQLite.
        _test_kill("post_registered")
        db.mark_job_submitted(conn, job_pk, str(payload["job_id"]))
        return

    if resp.status_code == 409:
        db.record_attempt_error(conn, job_pk, f"409 conflict: {resp.text[:200]}")
        db.mark_job_blocked(
            conn, job_pk, "idempotency key reused with a different body"
        )
        return

    # 400/422 and other non-transient 4xx: the request itself is wrong.
    db.record_attempt_error(conn, job_pk, f"HTTP {resp.status_code}: {resp.text[:200]}")
    db.mark_job_blocked(conn, job_pk, f"rejected by service: HTTP {resp.status_code}")


def fetch_job_results(conn, caller: HttpCaller, config: Config, job_pk: int) -> None:
    job = db.get_job(conn, job_pk)
    if not job["server_job_id"]:
        submit_job(conn, caller, config, job_pk)
        job = db.get_job(conn, job_pk)
        if not job["server_job_id"]:
            return  # incomplete/blocked already recorded

    base = f"{config.base_url.rstrip('/')}/jobs/{job['server_job_id']}"

    # 1) job metadata: expected slot count and processing/complete status
    try:
        meta_resp = caller.request("GET", base)
    except RetryExhausted as exc:
        db.record_attempt_error(conn, job_pk, f"GET job retries exhausted: {exc}")
        db.mark_job_incomplete(conn, job_pk, f"network exhausted: {exc}")
        return
    if meta_resp.status_code != 200:
        db.mark_job_blocked(
            conn, job_pk, f"GET job rejected: HTTP {meta_resp.status_code}"
        )
        return
    meta = meta_resp.json()
    if int(meta["expected_slots"]) != int(job["expected_slots"]):
        db.mark_job_blocked(
            conn,
            job_pk,
            f"expected_slots mismatch: local={job['expected_slots']}"
            f" server={meta['expected_slots']}",
        )
        return

    # 2) result paging with epoch restarts.
    #    A completed cursor carries a generation-bound token; probing it
    #    detects a server-side result-set rebuild even after complete=True.
    restarts_left = 3
    while True:
        cursor = db.get_cursor(conn, job_pk)
        token = cursor["next_token"] if cursor else None
        page_index = db.next_page_index(conn, job_pk)
        pages_seen = bool(cursor and cursor["complete_seen"])

        if pages_seen:
            # We previously reached complete=True in this epoch. Verify the
            # server result set is still the generation we recorded.
            if token is None:
                db.begin_new_epoch(conn, job_pk, "completed cursor had no token")
                restarts_left -= 1
                if restarts_left < 0:
                    db.mark_job_blocked(conn, job_pk, "too many epochs")
                    return
                continue
            try:
                probe = caller.request(
                    "GET", f"{base}/results", params={"token": token}
                )
            except RetryExhausted as exc:
                db.record_attempt_error(
                    conn, job_pk, f"cursor probe retries exhausted: {exc}"
                )
                db.mark_job_incomplete(conn, job_pk, f"network exhausted: {exc}")
                return
            if probe.status_code == 404:
                try:
                    detail = probe.json()
                except json.JSONDecodeError:
                    detail = {}
                if detail.get("code") == "stale_token":
                    if restarts_left <= 0:
                        db.mark_job_blocked(
                            conn, job_pk, "too many result-set generations"
                        )
                        return
                    db.begin_new_epoch(
                        conn, job_pk, "stale token on completed paging"
                    )
                    restarts_left -= 1
                    continue
                db.mark_job_blocked(conn, job_pk, "results: unknown job")
                return
            if probe.status_code != 200:
                db.mark_job_blocked(
                    conn, job_pk,
                    f"cursor probe rejected: HTTP {probe.status_code}",
                )
                return
            # probe 200: result set unchanged in this epoch -> coverage gate
            break

        # Not yet paged to completion: fetch the next page.
        try:
            resp = caller.request(
                "GET", f"{base}/results", params={"token": token}
            )
        except RetryExhausted as exc:
            db.record_attempt_error(
                conn, job_pk, f"GET results retries exhausted: {exc}"
            )
            db.mark_job_incomplete(conn, job_pk, f"network exhausted: {exc}")
            return

        if resp.status_code == 404:
            try:
                detail = resp.json()
            except json.JSONDecodeError:
                detail = {}
            if detail.get("code") == "stale_token":
                if restarts_left <= 0:
                    db.mark_job_blocked(
                        conn, job_pk, "too many result-set generations"
                    )
                    return
                db.begin_new_epoch(conn, job_pk, "stale_token while paging")
                restarts_left -= 1
                continue
            db.mark_job_blocked(conn, job_pk, "results: unknown job")
            return
        if resp.status_code != 200:
            db.mark_job_blocked(
                conn, job_pk, f"GET results rejected: HTTP {resp.status_code}"
            )
            return

        page = resp.json()
        try:
            db.apply_page(conn, job_pk, page_index, token, page)
        except db.ContradictionError as exc:
            db.record_attempt_error(conn, job_pk, str(exc))
            db.mark_job_blocked(conn, job_pk, str(exc))
            return
        except db.ResultRejected as exc:
            db.record_attempt_error(conn, job_pk, str(exc))
            db.mark_job_blocked(conn, job_pk, str(exc))
            return

        # R2 interruption point: page + cursor are committed in SQLite, but
        # the next fetch has not been confirmed.
        _test_kill("first_page_durable")

        if page.get("complete"):
            # loop again: the completed cursor is probed, then coverage runs
            continue

    # 3) coverage gate: complete=True only says paging ended; every expected
    #    slot must hold exactly one consistent FINAL observation.
    report = db.coverage(conn, job_pk)
    if report["ok"]:
        db.confirm_job(conn, job_pk)
        return
    if report["multiple_finals"]:
        detail = f"contradictory finals: multiple={report['multiple_finals']}"
        db.record_attempt_error(conn, job_pk, detail)
        db.mark_job_blocked(conn, job_pk, detail)
        return
    if report["pending"] and meta.get("status") == "processing":
        # Server-side slots not finished: never rewrite to business invalid.
        detail = f"server still processing: pending={report['pending']}"
        db.record_attempt_error(conn, job_pk, detail)
        db.mark_job_incomplete(conn, job_pk, detail)
        return
    detail = (
        f"coverage gap: missing={report['missing']}"
        f" pending={report['pending']} multiple={report['multiple_finals']}"
    )
    db.record_attempt_error(conn, job_pk, detail)
    db.mark_job_incomplete(conn, job_pk, detail)


def drive_jobs(conn, config: Config, only: list[int] | None = None) -> None:
    """One network pass over every job not yet confirmed."""
    with HttpCaller(config) as caller:
        jobs = db.active_jobs(conn)
        for job in jobs:
            if only is not None and job["id"] not in only:
                continue
            if job["status"] in (db.JOB_SEALED, db.JOB_INCOMPLETE) and not job["server_job_id"]:
                submit_job(conn, caller, config, job["id"])
            fetch_job_results(conn, caller, config, job["id"])


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------

def run_start(
    conn, config: Config, source_path: str, run_id: str
) -> dict[str, Any]:
    if config.ingestion == "prescan":
        import_source(conn, source_path)
        dispatch_pending(conn, config, run_id)
    else:
        # streaming: import lines, dedupe against the whole database and seal
        # batches as they fill; slots keep a globally unique numbering and
        # follow first-appearance order, so a later run only seals new inputs
        batch: list = []
        seen_digests: set[str] = set()

        def flush() -> None:
            fresh = [r for r in batch if r["state"] == db.IN_PENDING]
            if not fresh:
                return
            slot_base = db.next_free_slot(conn)
            items = _build_batch_items(fresh, slot_base)
            body_digest = request_body_digest(items)
            key = idempotency_key(run_id, body_digest)
            db.seal_job(conn, key, body_digest, items)
            for r in fresh:
                seen_digests.add(r["input_digest"])

        for line_no, row in stream_jsonl(source_path):
            digest = db.insert_source_row(conn, line_no, row)
            if digest in seen_digests:
                continue
            row2 = conn.execute(
                "SELECT * FROM inputs WHERE input_digest=?", (digest,)
            ).fetchone()
            if row2["state"] != db.IN_PENDING:
                # already sealed/confirmed by an earlier run or batch; its new
                # source-line mapping was persisted by insert_source_row
                seen_digests.add(digest)
                continue
            seen_digests.add(digest)
            batch.append(row2)
            if len(batch) >= config.batch_size:
                flush()
                batch.clear()
        if batch:
            flush()
    drive_jobs(conn, config)
    return db.delivery_readiness(conn)


def run_resume(conn, config: Config) -> dict[str, Any]:
    # Re-dispatch anything that never made it into a job, then drive the rest.
    dispatch_pending(conn, config, run_id=os.environ.get("BARCODE_RUN_ID", "resume"))
    drive_jobs(conn, config)
    return db.delivery_readiness(conn)


def run_export(conn, output_path: str, force: bool = False) -> dict[str, Any]:
    readiness = db.delivery_readiness(conn)
    if not readiness["deliverable"]:
        raise HardProtocolError(
            "delivery not ready: " + json.dumps(readiness, ensure_ascii=False, sort_keys=True)
        )
    if os.path.exists(output_path) and not force:
        raise FileExistsError(f"output already exists: {output_path}")
    parent = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(parent, exist_ok=True)
    tmp = output_path + ".tmp"
    count = 0
    with open(tmp, "w", encoding="utf-8") as fh:
        for record in db.iter_joined_outputs(conn):
            fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            count += 1
    os.replace(tmp, output_path)
    manifest = {
        "row_count": count,
        "source_lines": readiness["source_lines"],
        "job_states": readiness["job_states"],
        "input_states": readiness["input_states"],
        "generated_at": time.time(),
    }
    db.record_export(conn, os.path.abspath(output_path), count, manifest)
    manifest["output_path"] = os.path.abspath(output_path)
    return manifest
