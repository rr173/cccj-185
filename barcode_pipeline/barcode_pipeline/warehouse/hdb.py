"""Warehouse SQLite schema and read-only enrichment-DB evidence access.

Design notes
------------

* The warehouse is a separate database file from the pipeline DB.  It copies
  every piece of evidence a query needs (original source row, confirmed
  service result, job/slot/payload provenance) into its own tables, so the
  source enrichment DB may be moved or deleted after import while old
  ``knowledge_seq`` answers stay reproducible.
* The source DB is opened **read-only** through a URI connection and held
  inside one deferred transaction for the whole import, fixing one
  consistent snapshot.  The source DB is never modified and the warehouse
  never talks to a network service.
* Every revision of every claim is retained immutably.  A query at
  ``knowledge_seq = s`` selects, per claim, the highest revision admitted in
  batches ``<= s`` *before* considering whether its interval covers the
  business time.  Retractions are revisions whose kind is ``retract``.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from typing import Any, Iterator

from ..digest import canonical_json, sha256_hex

WAREHOUSE_SCHEMA_VERSION = "wh1"

# Revision kinds
KIND_UPSERT = "upsert"
KIND_RETRACT = "retract"

# Answer / lookup states
STATE_NORMALIZED = "normalized"
STATE_INVALID = "invalid"
STATE_CONFLICT = "conflict"
STATE_MISSING = "missing"

# Signed 64-bit integer bounds (all business/validity times live inside these).
INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1


class WarehouseError(Exception):
    """A user-correctable warehouse failure (bad input, conflict, ...)."""


# --------------------------------------------------------------------------
# Warehouse connection / schema
# --------------------------------------------------------------------------

def open_warehouse(path: str) -> sqlite3.Connection:
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)
    conn = sqlite3.connect(path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    # isolation_level=None -> autocommit at the driver level; every
    # transaction is started explicitly (BEGIN IMMEDIATE for writers,
    # deferred BEGIN for multi-statement reads), so concurrency semantics are
    # visible in the code instead of hidden in implicit transactions.
    # busy_timeout must precede journal_mode: on a brand-new file the WAL
    # pragma takes an exclusive lock and raises immediately rather than
    # waiting, so it additionally gets a short retry loop.
    conn.execute("PRAGMA busy_timeout=30000")
    last_exc: sqlite3.OperationalError | None = None
    for _ in range(50):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            last_exc = None
            break
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if "locked" not in str(exc):
                raise
            time.sleep(0.05)
    if last_exc is not None:
        raise last_exc
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    # Concurrent writers must never allocate the same knowledge_seq: they
    # serialize on SQLite write locks. Readers keep seeing the last committed
    # snapshot (WAL) and never a half-applied batch.
    _create_schema(conn)
    return conn


def _create_schema(conn: sqlite3.Connection) -> None:
    # One serialized transaction: concurrent first-open processes take turns
    # on the BEGIN IMMEDIATE lock instead of racing inside DDL.  Individual
    # CREATE statements are used because executescript commits implicitly.
    conn.execute("BEGIN IMMEDIATE")
    try:
        for ddl in _SCHEMA_DDL:
            conn.execute(ddl)
        conn.execute(
            "INSERT OR IGNORE INTO wh_meta(key, value) VALUES(?, ?)",
            ("schema_version", WAREHOUSE_SCHEMA_VERSION),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


_SCHEMA_DDL = (
    """
    CREATE TABLE IF NOT EXISTS wh_meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS wh_batches (
        knowledge_seq   INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id        TEXT NOT NULL UNIQUE,
        status          TEXT NOT NULL,           -- admitted | rejected
        change_digest   TEXT NOT NULL,           -- sha256 of raw file bytes
        evidence_digest TEXT,                    -- sha256 over resolved rows
        source_db_path  TEXT,
        source_db_digest TEXT,                   -- identity of source DB file
        change_count    INTEGER NOT NULL,
        new_revisions   INTEGER NOT NULL,        -- genuinely new revisions
        duplicate_rows  INTEGER NOT NULL,        -- identical re-presentations
        retractions     INTEGER NOT NULL,
        detail          TEXT,                    -- rejection reason / summary
        created_at      REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS wh_claims (
        claim_id        TEXT PRIMARY KEY,
        supplier        TEXT NOT NULL,
        item_id         TEXT NOT NULL,
        market          TEXT NOT NULL,
        head_revision   INTEGER NOT NULL,
        head_kind       TEXT NOT NULL,
        head_seq        INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_claims_product"
    " ON wh_claims(supplier, item_id, market)",
    """
    CREATE TABLE IF NOT EXISTS wh_claim_revisions (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        claim_id        TEXT NOT NULL,
        revision        INTEGER NOT NULL,
        kind            TEXT NOT NULL,           -- upsert | retract
        knowledge_seq   INTEGER NOT NULL,
        batch_id        TEXT NOT NULL,
        -- Product key is stored on retract rows too (copied from the
        -- established claim on admission) so product-indexed as-of queries
        -- can select retractions; the change file itself only carries
        -- claim_id/revision.
        supplier        TEXT NOT NULL,
        item_id         TEXT NOT NULL,
        market          TEXT NOT NULL,
        valid_from      INTEGER,                  -- NULL for retract
        valid_to        INTEGER,                  -- NULL = +infinity
        source_line     INTEGER,                  -- NULL for retract
        source_raw      TEXT,                     -- canonical original row
        business_status TEXT,
        normalized_value TEXT,
        error_code      TEXT,
        result_source   TEXT,                     -- canonical provenance JSON
        content_digest  TEXT NOT NULL,
        UNIQUE(claim_id, revision),
        FOREIGN KEY(knowledge_seq) REFERENCES wh_batches(knowledge_seq)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_rev_claim_seq"
    " ON wh_claim_revisions(claim_id, knowledge_seq)",
    # Covering index for the hot as-of path: product restriction, knowledge
    # cutoff, per-claim max revision and interval filtering are all served
    # from this one index without touching the table for the boundary ints.
    "CREATE INDEX IF NOT EXISTS idx_rev_product_seq"
    " ON wh_claim_revisions(supplier, item_id, market, knowledge_seq,"
    " claim_id, revision, valid_from, valid_to, kind)",
    "CREATE INDEX IF NOT EXISTS idx_rev_product"
    " ON wh_claim_revisions(supplier, item_id, market)",
    # Replication ledger: one row per admitted batch on both source and
    # managed replicas, carrying the prefix digest *after* that batch.  The
    # row is inserted in the same transaction as the batch itself, so a
    # prefix is never visible without its replication registration.  On a
    # plain wh1 warehouse the table simply stays empty.
    """
    CREATE TABLE IF NOT EXISTS wh_replication_log (
        knowledge_seq   INTEGER PRIMARY KEY,
        prefix_digest   TEXT NOT NULL,
        log_kind        TEXT NOT NULL,           -- source | source-backfill
                                                 -- | replica-apply
        recorded_at     REAL NOT NULL,
        FOREIGN KEY(knowledge_seq) REFERENCES wh_batches(knowledge_seq)
    )
    """,
)


def latest_seq(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT MAX(knowledge_seq) AS s FROM wh_batches WHERE status='admitted'"
    ).fetchone()
    return int(row["s"]) if row["s"] is not None else 0


# --------------------------------------------------------------------------
# Read-only source snapshot
# --------------------------------------------------------------------------

def open_source_snapshot(path: str) -> sqlite3.Connection:
    """Open an enrichment DB strictly read-only and fix a snapshot.

    ``immutable=0`` keeps normal locking semantics while ``mode=ro`` forbids
    writes; the deferred transaction pins one consistent view for the whole
    import.  No remote service is involved.
    """
    if not os.path.exists(path):
        raise WarehouseError(f"source enrichment db not found: {path}")
    uri = "file:" + os.path.abspath(path) + "?mode=ro"
    src = sqlite3.connect(uri, uri=True, isolation_level=None)
    src.row_factory = sqlite3.Row
    src.execute("PRAGMA query_only=ON")
    # One read-only deferred transaction pins a single consistent snapshot for
    # every source read performed by the whole import.
    src.execute("BEGIN")
    return src


def source_delivery_ok(src: sqlite3.Connection) -> tuple[bool, str]:
    """Replicate the pipeline's delivery gate against a read-only source DB.

    A source is only usable when every input is confirmed and no job is in an
    unfinished state.  Missing/incompatible tables fail the batch outright.
    """
    try:
        tables = {
            r[0]
            for r in src.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        required = {
            "source_rows",
            "inputs",
            "jobs",
            "job_slots",
            "slot_observations",
            "page_cursor",
        }
        missing = required - tables
        if missing:
            return False, f"source db missing tables: {sorted(missing)}"
        bad_inputs = src.execute(
            "SELECT COUNT(*) AS n FROM inputs WHERE state != 'confirmed'"
        ).fetchone()["n"]
        if bad_inputs:
            return False, f"source db has {bad_inputs} unconfirmed input(s)"
        bad_jobs = src.execute(
            "SELECT COUNT(*) AS n FROM jobs WHERE status != 'confirmed'"
        ).fetchone()["n"]
        if bad_jobs:
            return False, f"source db has {bad_jobs} unconfirmed job(s)"
        # Coverage gate: every confirmed input must hold exactly one final,
        # non-contradicting observation (the pipeline confirms jobs this way,
        # but a hand-edited DB could bypass that).
        gap = src.execute(
            "SELECT COUNT(*) AS n FROM inputs i"
            " LEFT JOIN slot_observations o"
            "   ON o.job_pk=i.job_pk AND o.slot=i.slot"
            "  AND o.kind='final' AND o.contradicting=0"
            " WHERE i.state='confirmed' AND o.id IS NULL"
        ).fetchone()["n"]
        if gap:
            return False, (
                f"source db has {gap} confirmed input(s) without a final"
                " observation"
            )
    except sqlite3.DatabaseError as exc:
        return False, f"source db unreadable: {exc}"
    return True, "ok"


def resolve_source_evidence(
    src: sqlite3.Connection, source_line: int
) -> dict[str, Any]:
    """Extract the full evidence for one source line from the source DB.

    The answer (normalized value or invalid error code) and its complete
    service provenance are read from the *already confirmed* enrichment
    result; the change file never supplies an answer of its own.
    """
    row = src.execute(
        "SELECT * FROM source_rows WHERE line_no=?", (source_line,)
    ).fetchone()
    if row is None:
        raise WarehouseError(f"source_line {source_line} does not exist")
    try:
        source_obj = json.loads(row["raw_json"])
    except ValueError as exc:
        raise WarehouseError(
            f"source_line {source_line}: stored row is not valid JSON: {exc}"
        )
    missing = [k for k in ("supplier", "item_id") if k not in source_obj]
    if missing:
        raise WarehouseError(
            f"source_line {source_line}: business columns missing: {missing}"
        )
    if "market" not in source_obj:
        raise WarehouseError(
            f"source_line {source_line}: market missing on source row"
        )

    inp = src.execute(
        "SELECT * FROM inputs WHERE input_digest=?", (row["input_digest"],)
    ).fetchone()
    if inp is None or inp["state"] != "confirmed":
        raise WarehouseError(
            f"source_line {source_line}: input is not confirmed"
            f" (state={None if inp is None else inp['state']})"
        )
    finals = src.execute(
        "SELECT * FROM slot_observations"
        " WHERE job_pk=? AND slot=? AND kind='final' AND contradicting=0"
        " ORDER BY id ASC",
        (inp["job_pk"], inp["slot"]),
    ).fetchall()
    if len(finals) != 1:
        raise WarehouseError(
            f"source_line {source_line}: expected exactly one final observation,"
            f" found {len(finals)}"
        )
    fin = finals[0]
    job = src.execute("SELECT * FROM jobs WHERE id=?", (inp["job_pk"],)).fetchone()
    result_source = {
        "job_id": job["server_job_id"],
        "slot": int(inp["slot"]),
        "page_index": int(fin["page_index"]),
        "payload_hash": fin["payload_hash"],
        "input_digest": row["input_digest"],
    }
    business_status = fin["business_status"]
    if business_status not in ("normalized", "invalid"):
        raise WarehouseError(
            f"source_line {source_line}: unexpected business_status"
            f" {business_status!r}"
        )
    if business_status == "normalized" and not fin["normalized_value"]:
        raise WarehouseError(
            f"source_line {source_line}: normalized result has no value"
        )
    if business_status == "invalid" and not fin["error_code"]:
        raise WarehouseError(
            f"source_line {source_line}: invalid result has no error_code"
        )
    return {
        "source_line": source_line,
        "source_obj": source_obj,
        "supplier": str(source_obj["supplier"]),
        "item_id": str(source_obj["item_id"]),
        "market": str(source_obj["market"]),
        "source_raw": row["raw_json"],
        "business_status": business_status,
        "normalized_value": fin["normalized_value"],
        "error_code": fin["error_code"],
        "result_source": result_source,
    }


# --------------------------------------------------------------------------
# Helpers shared by importer/query/facts
# --------------------------------------------------------------------------

def product_key(supplier: str, item_id: str, market: str) -> str:
    """Canonical text identity of one product (also used on the CLI/API)."""
    return sha256_hex(
        canonical_json(
            {"supplier": supplier, "item_id": item_id, "market": market}
        )
    )[:32]


def is_int64(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and (
        INT64_MIN <= value <= INT64_MAX
    )


def inspect_warehouse(conn: sqlite3.Connection) -> dict[str, Any]:
    batches = [
        dict(r)
        for r in conn.execute(
            "SELECT knowledge_seq, batch_id, status, change_digest,"
            " evidence_digest, source_db_path, change_count, new_revisions,"
            " duplicate_rows, retractions, detail, created_at"
            " FROM wh_batches ORDER BY knowledge_seq ASC"
        ).fetchall()
    ]
    claims = [
        dict(r)
        for r in conn.execute(
            "SELECT claim_id, supplier, item_id, market, head_revision,"
            " head_kind, head_seq FROM wh_claims ORDER BY claim_id ASC"
        ).fetchall()
    ]
    rev_count = conn.execute(
        "SELECT COUNT(*) AS n FROM wh_claim_revisions"
    ).fetchone()["n"]
    return {
        "schema_version": WAREHOUSE_SCHEMA_VERSION,
        "latest_seq": latest_seq(conn),
        "batch_count": len(batches),
        "claim_count": len(claims),
        "revision_count": rev_count,
        "batches": batches,
        "claims": claims,
    }


def iter_revisions_for_product(
    conn: sqlite3.Connection, supplier: str, item_id: str, market: str
) -> Iterator[sqlite3.Row]:
    yield from conn.execute(
        "SELECT * FROM wh_claim_revisions"
        " WHERE supplier=? AND item_id=? AND market=?"
        " ORDER BY claim_id, revision, knowledge_seq",
        (supplier, item_id, market),
    )
