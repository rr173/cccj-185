"""SQLite persistence for the resumable enrichment pipeline.

Everything needed to resume lives in one SQLite database:

  source_rows        every original JSONL line, keyed by its 1-based line
                     number (the occurrence identity; duplicate lines stay
                     duplicate rows)
  inputs             the de-duplicated *complete input* domain
                     (barcode, market, rule_version) -> input_digest
  jobs               service jobs, including a local SEALED state created
                     before the POST so a lost response is recoverable
  job_slots          job -> slot -> input_digest (unique per job)
  slot_observations  every distinct result payload seen for a slot
                     (identical retransmissions collapse, contradictions
                      are detected here)
  page_log           raw immutable result pages, as received
  page_cursor        paging progress per job (persisted only after the
                     page's results are durably applied)
  exports            produced delivery packages

The module never talks to the network; the client orchestrates and calls
these functions inside short transactions.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

from .digest import canonical_json, input_digest, sha256_hex

SCHEMA_VERSION = "1"

# Job lifecycle
JOB_SEALED = "sealed"          # request persisted locally, POST not yet confirmed
JOB_SUBMITTED = "submitted"    # server returned a job_id
JOB_FETCHING = "fetching"      # at least one result page durably applied
JOB_INCOMPLETE = "incomplete"  # retries exhausted / coverage missing: resumable
JOB_BLOCKED = "blocked"        # contradiction or malformed data: needs a human
JOB_CONFIRMED = "confirmed"    # every expected slot has one consistent result

# Input lifecycle
IN_PENDING = "pending"
IN_DISPATCHED = "dispatched"
IN_CONFIRMED = "confirmed"
IN_INCOMPLETE = "incomplete"

# Business result statuses (only these two are *final business* statuses)
BIZ_NORMALIZED = "normalized"
BIZ_INVALID = "invalid"
BIZ_PENDING = "pending"        # slot not finished server-side; NOT a failure


def open_db(path: str) -> sqlite3.Connection:
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=10000")
    _create_schema(conn)
    return conn


@contextmanager
def tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS source_rows (
            line_no      INTEGER PRIMARY KEY,   -- 1-based occurrence identity
            raw_json     TEXT NOT NULL,
            barcode      TEXT NOT NULL,
            market       TEXT NOT NULL,
            rule_version TEXT NOT NULL,
            input_digest TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_src_input ON source_rows(input_digest);

        CREATE TABLE IF NOT EXISTS inputs (
            input_digest TEXT PRIMARY KEY,
            barcode      TEXT NOT NULL,
            market       TEXT NOT NULL,
            rule_version TEXT NOT NULL,
            state        TEXT NOT NULL DEFAULT 'pending',
            job_pk       INTEGER,
            slot         INTEGER,
            FOREIGN KEY(job_pk) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS jobs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            idempotency_key TEXT NOT NULL UNIQUE,
            body_digest     TEXT NOT NULL,
            expected_slots  INTEGER NOT NULL,
            status          TEXT NOT NULL,
            status_detail   TEXT,
            server_job_id   TEXT UNIQUE,
            attempts        INTEGER NOT NULL DEFAULT 0,
            last_error      TEXT,
            created_at      REAL NOT NULL,
            updated_at      REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS job_slots (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            job_pk       INTEGER NOT NULL,
            slot         INTEGER NOT NULL,
            input_digest TEXT NOT NULL UNIQUE,
            UNIQUE(job_pk, slot),
            FOREIGN KEY(job_pk) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS slot_observations (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            job_pk           INTEGER NOT NULL,
            epoch            INTEGER NOT NULL DEFAULT 0,
            slot             INTEGER NOT NULL,
            input_digest     TEXT NOT NULL,
            kind             TEXT NOT NULL,           -- final | pending
            business_status  TEXT,
            normalized_value TEXT,
            error_code       TEXT,
            payload_hash     TEXT NOT NULL,
            contradicting    INTEGER NOT NULL DEFAULT 0,
            page_index       INTEGER NOT NULL,
            observed_at      REAL NOT NULL,
            UNIQUE(job_pk, epoch, slot, payload_hash),
            FOREIGN KEY(job_pk) REFERENCES jobs(id)
        );
        CREATE INDEX IF NOT EXISTS idx_obs_lookup
            ON slot_observations(job_pk, slot);

        CREATE TABLE IF NOT EXISTS page_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_pk          INTEGER NOT NULL,
            epoch           INTEGER NOT NULL DEFAULT 0,
            page_index      INTEGER NOT NULL,
            token_requested TEXT,
            complete        INTEGER NOT NULL,
            item_count      INTEGER NOT NULL,
            raw_json        TEXT NOT NULL,
            received_at     REAL NOT NULL,
            UNIQUE(job_pk, epoch, page_index),
            FOREIGN KEY(job_pk) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS page_cursor (
            job_pk        INTEGER PRIMARY KEY,
            epoch         INTEGER NOT NULL DEFAULT 0,
            next_token    TEXT,
            complete_seen INTEGER NOT NULL DEFAULT 0,
            updated_at    REAL NOT NULL,
            FOREIGN KEY(job_pk) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS exports (
            output_path TEXT PRIMARY KEY,
            created_at  REAL NOT NULL,
            row_count   INTEGER NOT NULL,
            manifest    TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    conn.commit()


# --------------------------------------------------------------------------
# Source ingestion
# --------------------------------------------------------------------------

def insert_source_row(
    conn: sqlite3.Connection,
    line_no: int,
    row: dict[str, Any],
) -> str:
    """Persist one source line and its unique input.

    Returns the input_digest.  Re-importing the same line_no is a no-op so
    resume can re-scan a source file safely.
    """
    barcode = str(row["barcode"])
    market = str(row["market"])
    rule_version = str(row["rule_version"])
    digest = input_digest(barcode, market, rule_version)
    raw = json.dumps(row, ensure_ascii=False, sort_keys=True)
    with tx(conn):
        exists = conn.execute(
            "SELECT 1 FROM source_rows WHERE line_no=?", (line_no,)
        ).fetchone()
        if exists is None:
            conn.execute(
                "INSERT INTO source_rows(line_no, raw_json, barcode, market,"
                " rule_version, input_digest) VALUES(?,?,?,?,?,?)",
                (line_no, raw, barcode, market, rule_version, digest),
            )
        conn.execute(
            "INSERT OR IGNORE INTO inputs(input_digest, barcode, market,"
            " rule_version, state) VALUES(?,?,?,?,?)",
            (digest, barcode, market, rule_version, IN_PENDING),
        )
    return digest


def source_line_count(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0]


def get_source_row(conn: sqlite3.Connection, line_no: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM source_rows WHERE line_no=?", (line_no,)
    ).fetchone()


def iter_source_lines(conn: sqlite3.Connection) -> Iterator[sqlite3.Row]:
    """Stream source rows in original line order (without loading all)."""
    for row in conn.execute(
        "SELECT * FROM source_rows ORDER BY line_no ASC"
    ):
        yield row


# --------------------------------------------------------------------------
# Input domain / dispatch
# --------------------------------------------------------------------------

def pending_inputs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT * FROM inputs WHERE state=? ORDER BY input_digest ASC",
            (IN_PENDING,),
        )
    )


def pending_inputs_by_first_line(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Pending inputs in order of their earliest source line.

    Used by streaming ingestion so slots are deterministic across runs and
    independent of how the source was split into batches.
    """
    return list(
        conn.execute(
            "SELECT i.* FROM inputs i"
            " JOIN (SELECT input_digest, MIN(line_no) AS first_line"
            "       FROM source_rows GROUP BY input_digest) f"
            " ON f.input_digest = i.input_digest"
            " WHERE i.state=? ORDER BY f.first_line ASC",
            (IN_PENDING,),
        )
    )


def next_free_slot(conn: sqlite3.Connection) -> int:
    """Slots are globally unique across every job of one pipeline database."""
    row = conn.execute("SELECT COALESCE(MAX(slot) + 1, 0) AS n FROM job_slots")
    return int(row.fetchone()["n"])


def seal_job(
    conn: sqlite3.Connection,
    idem_key: str,
    body_digest: str,
    items: list[dict[str, Any]],
) -> tuple[int, bool]:
    """Persist a job request *before* it is POSTed.

    Returns (job_pk, created).  If the idempotency key already exists its
    job is returned instead — same key must always mean the same request.
    """
    now = time.time()
    with tx(conn):
        row = conn.execute(
            "SELECT id FROM jobs WHERE idempotency_key=?", (idem_key,)
        ).fetchone()
        if row is not None:
            return int(row["id"]), False
        cur = conn.execute(
            "INSERT INTO jobs(idempotency_key, body_digest, expected_slots,"
            " status, created_at, updated_at) VALUES(?,?,?,?,?,?)",
            (idem_key, body_digest, len(items), JOB_SEALED, now, now),
        )
        job_pk = int(cur.lastrowid)
        for it in items:
            digest = input_digest(
                it["barcode"], it["market"], it["rule_version"]
            )
            conn.execute(
                "INSERT INTO job_slots(job_pk, slot, input_digest)"
                " VALUES(?,?,?)",
                (job_pk, int(it["client_slot"]), digest),
            )
            conn.execute(
                "UPDATE inputs SET state=?, job_pk=?, slot=?"
                " WHERE input_digest=?",
                (IN_DISPATCHED, job_pk, int(it["client_slot"]), digest),
            )
        conn.execute(
            "INSERT INTO page_cursor(job_pk, next_token, complete_seen,"
            " updated_at) VALUES(?,?,0,?)",
            (job_pk, None, now),
        )
    return job_pk, True


def get_job_by_idem(conn: sqlite3.Connection, idem_key: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM jobs WHERE idempotency_key=?", (idem_key,)
    ).fetchone()


def mark_job_submitted(
    conn: sqlite3.Connection, job_pk: int, server_job_id: str
) -> None:
    with tx(conn):
        conn.execute(
            "UPDATE jobs SET status=?, server_job_id=?, updated_at=?"
            " WHERE id=?",
            (JOB_SUBMITTED, server_job_id, time.time(), job_pk),
        )


def record_attempt_error(conn: sqlite3.Connection, job_pk: int, message: str) -> None:
    with tx(conn):
        conn.execute(
            "UPDATE jobs SET attempts=attempts+1, last_error=?, updated_at=?"
            " WHERE id=?",
            (message, time.time(), job_pk),
        )


def mark_job_incomplete(conn: sqlite3.Connection, job_pk: int, detail: str) -> None:
    with tx(conn):
        conn.execute(
            "UPDATE jobs SET status=?, status_detail=?, updated_at=? WHERE id=?",
            (JOB_INCOMPLETE, detail, time.time(), job_pk),
        )
        conn.execute(
            "UPDATE inputs SET state=? WHERE job_pk=? AND state=?",
            (IN_INCOMPLETE, job_pk, IN_DISPATCHED),
        )


def mark_job_blocked(conn: sqlite3.Connection, job_pk: int, detail: str) -> None:
    with tx(conn):
        conn.execute(
            "UPDATE jobs SET status=?, status_detail=?, updated_at=? WHERE id=?",
            (JOB_BLOCKED, detail, time.time(), job_pk),
        )
        conn.execute(
            "UPDATE inputs SET state=? WHERE job_pk=? AND state=?",
            (IN_INCOMPLETE, job_pk, IN_DISPATCHED),
        )


def active_jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Jobs that still need network work on resume."""
    return list(
        conn.execute(
            "SELECT * FROM jobs WHERE status IN (?,?,?,?) ORDER BY id ASC",
            (JOB_SEALED, JOB_SUBMITTED, JOB_FETCHING, JOB_INCOMPLETE),
        )
    )


def all_jobs(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(conn.execute("SELECT * FROM jobs ORDER BY id ASC"))


def get_job(conn: sqlite3.Connection, job_pk: int) -> sqlite3.Row:
    return conn.execute("SELECT * FROM jobs WHERE id=?", (job_pk,)).fetchone()


def job_slot_map(conn: sqlite3.Connection, job_pk: int) -> dict[int, str]:
    rows = conn.execute(
        "SELECT slot, input_digest FROM job_slots WHERE job_pk=?", (job_pk,)
    ).fetchall()
    return {int(r["slot"]): r["input_digest"] for r in rows}


# --------------------------------------------------------------------------
# Result pages and observations
# --------------------------------------------------------------------------

class ResultRejected(Exception):
    """A result item cannot be bound to a local slot (hard block)."""


class ContradictionError(Exception):
    """Two different final payloads arrived for the same slot."""


def current_epoch(conn: sqlite3.Connection, job_pk: int) -> int:
    row = conn.execute(
        "SELECT epoch FROM page_cursor WHERE job_pk=?", (job_pk,)
    ).fetchone()
    return int(row["epoch"]) if row else 0


def begin_new_epoch(conn: sqlite3.Connection, job_pk: int, reason: str) -> int:
    """Start a fresh result-set generation for a job.

    Used when the server signals a stale token (its result set was rebuilt)
    or when a paged-through job still reports "processing" with pending
    slots that have since been resolved.  Old pages/observations stay on
    record for audit; coverage only considers the latest epoch.
    """
    now = time.time()
    with tx(conn):
        row = conn.execute(
            "SELECT epoch FROM page_cursor WHERE job_pk=?", (job_pk,)
        ).fetchone()
        new_epoch = int(row["epoch"]) + 1
        conn.execute(
            "UPDATE page_cursor SET epoch=?, next_token=NULL, complete_seen=0,"
            " updated_at=? WHERE job_pk=?",
            (new_epoch, now, job_pk),
        )
        conn.execute(
            "UPDATE jobs SET last_error=?, updated_at=? WHERE id=?",
            (f"new epoch {new_epoch}: {reason}", now, job_pk),
        )
    return new_epoch


def result_payload_hash(item: dict[str, Any]) -> str:
    payload = {
        "business_status": item.get("business_status"),
        "normalized_value": item.get("normalized_value"),
        "error_code": item.get("error_code"),
    }
    return sha256_hex(canonical_json(payload))


def _record_contradiction_audit(
    conn: sqlite3.Connection,
    job_pk: int,
    epoch: int,
    slot: int,
    input_digest: str,
    item: dict[str, Any],
    page_index: int,
) -> None:
    """Persist a conflicting payload in its own committed transaction.

    The page carrying it will be rejected, but the contradictory evidence
    must survive for inspection — so it is stored separately, flagged
    ``contradicting=1`` and excluded from coverage/export lookups.
    """
    phash = result_payload_hash(item)
    now = time.time()
    with tx(conn):
        conn.execute(
            "INSERT OR IGNORE INTO slot_observations(job_pk, epoch, slot,"
            " input_digest, kind, business_status, normalized_value,"
            " error_code, payload_hash, contradicting, page_index, observed_at)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                job_pk,
                epoch,
                slot,
                input_digest,
                "final",
                item.get("business_status"),
                item.get("normalized_value"),
                item.get("error_code"),
                phash,
                1,
                page_index,
                now,
            ),
        )


def apply_page(
    conn: sqlite3.Connection,
    job_pk: int,
    page_index: int,
    token_requested: str | None,
    page: dict[str, Any],
) -> None:
    """Durably apply one result page inside a single transaction.

    Validation order:
      1. job_id must name this job
      2. slot must belong to this job
      3. input_digest in the result must match the slot's registered input
      4. identical retransmissions are accepted; differing finals contradict
    Only after every item passes are observations, the raw page log and the
    new cursor committed together.  Pages belong to an epoch: re-reading a
    rebuilt result set starts a new epoch rather than overwriting history.

    A contradictory item is first captured in a separate audit transaction
    (flagged ``contradicting``), then the whole page is rejected and the
    job is blocked — no payload is ever silently chosen.
    """
    job = get_job(conn, job_pk)
    server_job_id = job["server_job_id"]
    slot_map = job_slot_map(conn, job_pk)
    epoch = current_epoch(conn, job_pk)
    now = time.time()
    raw_json = json.dumps(page, ensure_ascii=False, sort_keys=True)

    # Fast path: byte-identical replay of a page already stored in this epoch.
    dup = conn.execute(
        "SELECT id, raw_json FROM page_log WHERE job_pk=? AND epoch=?"
        " AND page_index=?",
        (job_pk, epoch, page_index),
    ).fetchone()
    if dup is not None:
        if dup["raw_json"] != raw_json:
            raise ContradictionError(
                f"epoch={epoch} page_index={page_index} retransmitted"
                " with a different body"
            )
        return  # identical replay: nothing new to apply

    items = page.get("items", [])

    # ---- validation pass (no writes): bind every item to a local slot ----
    prepared: list[tuple[int, str, str, str, dict[str, Any]]] = []
    for item in items:
        if item.get("job_id") != server_job_id:
            raise ResultRejected(
                f"result carries foreign/missing job_id: {item.get('job_id')!r}"
                f" (expected {server_job_id!r})"
            )
        slot = item.get("slot")
        if not isinstance(slot, int) or slot not in slot_map:
            raise ResultRejected(
                f"unknown slot {slot!r} for job {server_job_id}"
            )
        want_digest = slot_map[slot]
        if item.get("input_digest") != want_digest:
            raise ResultRejected(
                f"input_digest mismatch on slot {slot}: got"
                f" {item.get('input_digest')!r}, want {want_digest!r}"
            )
        kind = (
            "pending"
            if item.get("business_status") == BIZ_PENDING
            else "final"
        )
        if kind == "final" and item.get("business_status") not in (
            BIZ_NORMALIZED,
            BIZ_INVALID,
        ):
            raise ResultRejected(
                f"slot {slot}: unknown final business_status"
                f" {item.get('business_status')!r}"
            )
        prepared.append((slot, want_digest, kind, result_payload_hash(item), item))

    # ---- contradiction detection -------------------------------------------
    # A slot may legitimately appear several times in one page (page-level
    # retransmission): identical payloads collapse.  Two *different* final
    # payloads for one slot — inside the page or against a payload committed
    # on an earlier page — are a hard contradiction.
    page_finals: dict[int, set[str]] = {}
    page_items_by_slot: dict[int, list[tuple[str, dict[str, Any]]]] = {}
    for slot, want_digest, kind, phash, item in prepared:
        page_items_by_slot.setdefault(slot, []).append((phash, item))
        if kind == "final":
            page_finals.setdefault(slot, set()).add(phash)

    for slot, hashes in page_finals.items():
        committed = conn.execute(
            "SELECT payload_hash FROM slot_observations"
            " WHERE job_pk=? AND epoch=? AND slot=? AND kind='final'"
            " AND contradicting=0",
            (job_pk, epoch, slot),
        ).fetchall()
        committed_hashes = {r["payload_hash"] for r in committed}
        distinct = hashes | committed_hashes
        if len(distinct) > 1:
            # keep the conflicting evidence even though the page is rejected
            for phash, item in page_items_by_slot[slot]:
                if item.get("business_status") == BIZ_PENDING:
                    continue
                if phash not in committed_hashes:
                    _record_contradiction_audit(
                        conn, job_pk, epoch, slot, slot_map[slot],
                        item, page_index,
                    )
            raise ContradictionError(
                f"slot {slot}: {len(distinct)} distinct final payloads"
                f" (committed={sorted(h[:10] for h in committed_hashes)},"
                f" page={sorted(h[:10] for h in hashes)})"
            )

    # ---- commit pass: observations + raw page + cursor, atomically ----
    with tx(conn):
        for slot, want_digest, kind, phash, item in prepared:
            conn.execute(
                "INSERT OR IGNORE INTO slot_observations(job_pk, epoch, slot,"
                " input_digest, kind, business_status, normalized_value,"
                " error_code, payload_hash, contradicting, page_index, observed_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    job_pk,
                    epoch,
                    slot,
                    want_digest,
                    kind,
                    item.get("business_status"),
                    item.get("normalized_value"),
                    item.get("error_code"),
                    phash,
                    0,
                    page_index,
                    now,
                ),
            )

        conn.execute(
            "INSERT INTO page_log(job_pk, epoch, page_index, token_requested,"
            " complete, item_count, raw_json, received_at)"
            " VALUES(?,?,?,?,?,?,?,?)",
            (
                job_pk,
                epoch,
                page_index,
                token_requested,
                1 if page.get("complete") else 0,
                len(items),
                raw_json,
                now,
            ),
        )
        conn.execute(
            "UPDATE page_cursor SET next_token=?, complete_seen=?, updated_at=?"
            " WHERE job_pk=?",
            (
                # On the last page next_token is NULL; keep the generation-bound
                # current_token instead so a later resume can detect a rebuilt
                # server result set.
                page.get("next_token") or page.get("current_token"),
                1 if page.get("complete") else 0,
                now,
                job_pk,
            ),
        )
        if job["status"] in (JOB_SUBMITTED, JOB_INCOMPLETE, JOB_SEALED):
            conn.execute(
                "UPDATE jobs SET status=?, updated_at=? WHERE id=?",
                (JOB_FETCHING, now, job_pk),
            )


def get_cursor(conn: sqlite3.Connection, job_pk: int) -> sqlite3.Row:
    return conn.execute(
        "SELECT * FROM page_cursor WHERE job_pk=?", (job_pk,)
    ).fetchone()


def next_page_index(conn: sqlite3.Connection, job_pk: int) -> int:
    epoch = current_epoch(conn, job_pk)
    row = conn.execute(
        "SELECT COALESCE(MAX(page_index)+1, 0) AS n FROM page_log"
        " WHERE job_pk=? AND epoch=?",
        (job_pk, epoch),
    ).fetchone()
    return int(row["n"])


def coverage(conn: sqlite3.Connection, job_pk: int) -> dict[str, Any]:
    """Coverage report for one job (latest epoch only).

    A job is confirmed only when *every expected slot* has exactly one
    consistent FINAL observation.  Item counts can never substitute for
    this check: pages may retransmit items, omit slots or arrive unordered.
    Pages from superseded epochs remain in page_log for audit.
    """
    job = get_job(conn, job_pk)
    expected = int(job["expected_slots"])
    epoch = current_epoch(conn, job_pk)
    slot_map = job_slot_map(conn, job_pk)
    missing: list[int] = []
    pending: list[int] = []
    multiple_finals: list[int] = []
    finals: dict[int, sqlite3.Row] = {}
    for slot in sorted(slot_map):
        obs = conn.execute(
            "SELECT * FROM slot_observations WHERE job_pk=? AND epoch=? AND slot=?"
            " AND contradicting=0",
            (job_pk, epoch, slot),
        ).fetchall()
        fin = [r for r in obs if r["kind"] == "final"]
        if not fin:
            if obs:
                pending.append(slot)
            else:
                missing.append(slot)
        elif len(fin) > 1:
            multiple_finals.append(slot)
        else:
            finals[slot] = fin[0]
    cursor = get_cursor(conn, job_pk)
    return {
        "expected": expected,
        "observed_slots": len(set(
            r["slot"]
            for r in conn.execute(
                "SELECT slot FROM slot_observations WHERE job_pk=? AND epoch=?"
                " AND contradicting=0",
                (job_pk, epoch),
            )
        )),
        "missing": missing,
        "pending": pending,
        "multiple_finals": multiple_finals,
        "complete_seen": bool(cursor and cursor["complete_seen"]),
        "epoch": epoch,
        "ok": not (missing or pending or multiple_finals)
        and len(finals) == expected,
        "finals": finals,
    }


def confirm_job(conn: sqlite3.Connection, job_pk: int) -> None:
    """Promote a fully covered job and its inputs to confirmed."""
    report = coverage(conn, job_pk)
    if not report["ok"]:
        raise ValueError(
            f"job {job_pk} cannot be confirmed: missing={report['missing']}"
            f" pending={report['pending']} multiple={report['multiple_finals']}"
        )
    now = time.time()
    with tx(conn):
        conn.execute(
            "UPDATE jobs SET status=?, status_detail=NULL, updated_at=? WHERE id=?",
            (JOB_CONFIRMED, now, job_pk),
        )
        conn.execute(
            "UPDATE inputs SET state=? WHERE job_pk=?",
            (IN_CONFIRMED, job_pk),
        )


def slot_final(conn: sqlite3.Connection, job_pk: int, slot: int) -> sqlite3.Row | None:
    epoch = current_epoch(conn, job_pk)
    rows = conn.execute(
        "SELECT * FROM slot_observations WHERE job_pk=? AND epoch=? AND slot=?"
        " AND kind='final' AND contradicting=0",
        (job_pk, epoch, slot),
    ).fetchall()
    if len(rows) == 1:
        return rows[0]
    return None


# --------------------------------------------------------------------------
# Delivery / explain
# --------------------------------------------------------------------------

def delivery_readiness(conn: sqlite3.Connection) -> dict[str, Any]:
    total_src = source_line_count(conn)
    inputs = conn.execute(
        "SELECT state, COUNT(*) AS n FROM inputs GROUP BY state"
    ).fetchall()
    states = {r["state"]: int(r["n"]) for r in inputs}
    jobs = conn.execute(
        "SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"
    ).fetchall()
    job_states = {r["status"]: int(r["n"]) for r in jobs}
    # An empty (but explicitly imported) source is a valid zero-row delivery:
    # there are simply no inputs and therefore no jobs.  The gate only checks
    # that nothing is left in an unfinished state.
    deliverable = (
        states.get(IN_PENDING, 0) == 0
        and states.get(IN_DISPATCHED, 0) == 0
        and states.get(IN_INCOMPLETE, 0) == 0
        and job_states.get(JOB_INCOMPLETE, 0) == 0
        and job_states.get(JOB_BLOCKED, 0) == 0
        and job_states.get(JOB_SEALED, 0) == 0
        and job_states.get(JOB_SUBMITTED, 0) == 0
        and job_states.get(JOB_FETCHING, 0) == 0
    )
    return {
        "source_lines": total_src,
        "input_states": states,
        "job_states": job_states,
        "deliverable": deliverable,
    }


def record_export(
    conn: sqlite3.Connection, path: str, row_count: int, manifest: dict[str, Any]
) -> None:
    with tx(conn):
        conn.execute(
            "INSERT OR REPLACE INTO exports(output_path, created_at, row_count,"
            " manifest) VALUES(?,?,?,?)",
            (path, time.time(), row_count, json.dumps(manifest, ensure_ascii=False, sort_keys=True)),
        )


def export_registered(conn: sqlite3.Connection, path: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM exports WHERE output_path=?", (path,)
    ).fetchone()


def explain_line(conn: sqlite3.Connection, line_no: int) -> dict[str, Any]:
    """Full provenance chain for one original line number."""
    src = get_source_row(conn, line_no)
    if src is None:
        raise KeyError(f"unknown source line {line_no}")
    inp = conn.execute(
        "SELECT * FROM inputs WHERE input_digest=?", (src["input_digest"],)
    ).fetchone()
    out: dict[str, Any] = {
        "line_no": line_no,
        "source_row": json.loads(src["raw_json"]),
        "input": {
            "input_digest": inp["input_digest"],
            "barcode": inp["barcode"],
            "market": inp["market"],
            "rule_version": inp["rule_version"],
            "state": inp["state"],
        },
    }
    if inp["job_pk"] is not None:
        job = get_job(conn, inp["job_pk"])
        out["job"] = {
            "job_pk": job["id"],
            "server_job_id": job["server_job_id"],
            "status": job["status"],
            "status_detail": job["status_detail"],
            "client_slot": inp["slot"],
            "idempotency_key": job["idempotency_key"],
            "body_digest": job["body_digest"],
            "expected_slots": job["expected_slots"],
        }
        fin = slot_final(conn, job["id"], int(inp["slot"]))
        if fin is not None:
            out["confirmed_result"] = {
                "business_status": fin["business_status"],
                "normalized_value": fin["normalized_value"],
                "error_code": fin["error_code"],
                "observed_on_page": fin["page_index"],
                "payload_hash": fin["payload_hash"],
            }
        same_input_lines = [
            int(r["line_no"])
            for r in conn.execute(
                "SELECT line_no FROM source_rows WHERE input_digest=?"
                " ORDER BY line_no ASC",
                (inp["input_digest"],),
            )
        ]
        out["mapped_source_lines"] = same_input_lines
    return out


def iter_joined_outputs(conn: sqlite3.Connection) -> Iterator[dict[str, Any]]:
    """Stream one output record per source line, in original order."""
    for src in iter_source_lines(conn):
        inp = conn.execute(
            "SELECT * FROM inputs WHERE input_digest=?", (src["input_digest"],)
        ).fetchone()
        record: dict[str, Any] = {
            "line": src["line_no"],
            "source": json.loads(src["raw_json"]),
            "barcode": src["barcode"],
            "market": src["market"],
            "rule_version": src["rule_version"],
            "input_digest": src["input_digest"],
        }
        if inp["job_pk"] is None or inp["state"] != IN_CONFIRMED:
            raise ValueError(
                f"line {src['line_no']} has no confirmed result"
                f" (input state={inp['state']})"
            )
        job = get_job(conn, inp["job_pk"])
        fin = slot_final(conn, job["id"], int(inp["slot"]))
        if fin is None:  # defensive, coverage already guarantees this
            raise ValueError(f"line {src['line_no']}: slot result missing")
        record.update(
            {
                "state": fin["business_status"],
                "normalized_value": fin["normalized_value"],
                "error_code": fin["error_code"],
                "client_slot": inp["slot"],
                "job_id": job["server_job_id"],
                "result_source": {
                    "job_id": job["server_job_id"],
                    "slot": inp["slot"],
                    "page_index": fin["page_index"],
                    "payload_hash": fin["payload_hash"],
                    "input_digest": src["input_digest"],
                },
            }
        )
        yield record
