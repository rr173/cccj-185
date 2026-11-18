"""Origin lifecycle, role guards and replica status.

Roles
-----

* a *plain* wh1 warehouse has no ``repl_role`` meta row: normal
  ``warehouse import`` works exactly as before;
* a *source* warehouse has an origin identity.  Normal imports remain its
  only write path; every admitted batch also gets a replication-log row in
  the same transaction;
* a *replica* warehouse is an empty managed copy bound to one origin.  Direct
  ``warehouse import`` (CLI or public API) is refused; its only write path is
  package acceptance through :mod:`replication.replica`.

The origin identity lives in ``wh_meta`` (logical database content), not in
the file name, so moving the database path keeps the identity and a physical
backup copies it -- which is exactly why prefix digests, not origin+seq,
decide whether histories can be joined.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Any

from ..hdb import WarehouseError, latest_seq
from . import identity
from .identity import (
    EMPTY_PREFIX_DIGEST,
    META_ORIGIN_CREATED_AT,
    META_ORIGIN_ID,
    META_ROLE,
    ROLE_REPLICA,
    ROLE_SOURCE,
)


class OriginError(WarehouseError):
    pass


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute(
        "SELECT value FROM wh_meta WHERE key=?", (key,)
    ).fetchone()
    return None if row is None else row["value"]


def role_of(conn: sqlite3.Connection) -> str | None:
    return get_meta(conn, META_ROLE)


def origin_of(conn: sqlite3.Connection) -> str | None:
    return get_meta(conn, META_ORIGIN_ID)


def _has_business_history(conn: sqlite3.Connection) -> bool:
    return conn.execute(
        "SELECT EXISTS(SELECT 1 FROM wh_batches) AS e"
    ).fetchone()["e"] == 1


def initialize_origin(
    conn: sqlite3.Connection, *, origin_id: str | None = None
) -> dict[str, Any]:
    """Bind a durable origin identity to this warehouse (source role).

    Repeatable: a second call with the same identity is a no-op.  The
    identity can never be changed, existing batches are never renumbered and
    existing business history is never modified -- the replication log is
    back-filled inside the same transaction by re-reading immutable rows.

    A plain warehouse that already holds batches may be initialized (the
    documented "existing wh1 warehouse as the starting point" migration); an
    existing *replica* can never become a source.
    """
    role = role_of(conn)
    if role == ROLE_REPLICA:
        raise OriginError(
            "warehouse is a managed replica and cannot be initialized as a"
            " source origin"
        )
    existing = origin_of(conn)
    if existing is not None:
        if origin_id is not None and origin_id != existing:
            raise OriginError(
                f"origin identity is immutable: existing={existing!r}"
                f" requested={origin_id!r}"
            )
        return {
            "event": "origin_init",
            "replayed": True,
            "origin_id": existing,
            "role": ROLE_SOURCE,
        }
    new_id = origin_id or identity.new_origin_id()
    if not isinstance(new_id, str) or not new_id:
        raise OriginError("origin_id must be a non-empty string")
    conn.execute("BEGIN IMMEDIATE")
    try:
        created = time.time()
        conn.execute(
            "INSERT INTO wh_meta(key, value) VALUES(?, ?)",
            (META_ROLE, ROLE_SOURCE),
        )
        conn.execute(
            "INSERT INTO wh_meta(key, value) VALUES(?, ?)",
            (META_ORIGIN_ID, new_id),
        )
        conn.execute(
            "INSERT INTO wh_meta(key, value) VALUES(?, ?)",
            (META_ORIGIN_CREATED_AT, str(created)),
        )
        _backfill_replication_log(conn)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "event": "origin_init",
        "replayed": False,
        "origin_id": new_id,
        "role": ROLE_SOURCE,
    }


def _backfill_replication_log(conn: sqlite3.Connection) -> None:
    """Populate wh_replication_log for batches admitted before init.

    Prefix digests are recomputed purely from stored business history, so the
    back-filled values match what an init-before-import source would hold.
    """
    already = conn.execute(
        "SELECT COUNT(*) AS n FROM wh_replication_log"
    ).fetchone()["n"]
    if already:
        return
    chain = EMPTY_PREFIX_DIGEST
    batches = conn.execute(
        "SELECT * FROM wh_batches WHERE status='admitted' ORDER BY"
        " knowledge_seq ASC"
    ).fetchall()
    for b in batches:
        seq = int(b["knowledge_seq"])
        rows = conn.execute(
            "SELECT * FROM wh_claim_revisions WHERE knowledge_seq=?"
            " ORDER BY id ASC",
            (seq,),
        ).fetchall()
        recs = [identity.revision_row_to_rec(r) for r in rows]
        chain = identity.chain_step(
            chain, identity.batch_unit(dict(b), recs)
        )
        conn.execute(
            "INSERT INTO wh_replication_log(knowledge_seq, prefix_digest,"
            " log_kind, recorded_at) VALUES(?, ?, ?, ?)",
            (seq, chain, "source-backfill", time.time()),
        )


def require_source(conn: sqlite3.Connection) -> str:
    """Guard for export: return the origin id or raise."""
    role = role_of(conn)
    origin = origin_of(conn)
    if role != ROLE_SOURCE or origin is None:
        raise OriginError(
            "this warehouse has no source origin; run 'warehouse origin init'"
            " first (replicas cannot export)"
        )
    return origin


def guard_import_allowed(conn: sqlite3.Connection) -> None:
    """Refuse ordinary business imports on managed replicas."""
    if role_of(conn) == ROLE_REPLICA:
        raise OriginError(
            "direct warehouse import is disabled on a managed replica;"
            " history can only arrive through replication package acceptance"
        )


def initialize_replica(conn: sqlite3.Connection, origin_id: str) -> dict[str, Any]:
    """Create an empty managed replica bound to ``origin_id``.

    Only an empty plain wh1 warehouse (schema tables exist, zero batches and
    zero revisions) can become a replica; an existing business warehouse can
    never be overwritten by impersonating an empty copy.
    """
    if not isinstance(origin_id, str) or not origin_id:
        raise OriginError("origin_id must be a non-empty string")
    role = role_of(conn)
    if role == ROLE_SOURCE:
        raise OriginError(
            "this warehouse is a source origin; it cannot become a replica"
        )
    if role == ROLE_REPLICA:
        bound = origin_of(conn)
        if bound != origin_id:
            raise OriginError(
                f"replica already bound to origin {bound!r}"
            )
        return {
            "event": "replica_init",
            "replayed": True,
            "origin_id": bound,
            "role": ROLE_REPLICA,
        }
    if _has_business_history(conn):
        raise OriginError(
            "refusing to overwrite a non-empty business warehouse as a"
            " replica (existing batches found)"
        )
    rev_count = conn.execute(
        "SELECT COUNT(*) AS n FROM wh_claim_revisions"
    ).fetchone()["n"]
    if rev_count:
        raise OriginError(
            "refusing replica init: warehouse already contains revisions"
        )
    conn.execute("BEGIN IMMEDIATE")
    try:
        _bind_replica_meta(conn, origin_id)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "event": "replica_init",
        "replayed": False,
        "origin_id": origin_id,
        "role": ROLE_REPLICA,
    }


def _bind_replica_meta(conn: sqlite3.Connection, origin_id: str) -> None:
    """Insert the replica meta rows inside the caller's open transaction.

    Used by package acceptance so the origin binding and the applied history
    commit atomically: a kill before commit leaves no binding behind.  All
    eligibility checks are the caller's responsibility.
    """
    conn.execute(
        "INSERT INTO wh_meta(key, value) VALUES(?, ?)",
        (META_ROLE, ROLE_REPLICA),
    )
    conn.execute(
        "INSERT INTO wh_meta(key, value) VALUES(?, ?)",
        (META_ORIGIN_ID, origin_id),
    )
    conn.execute(
        "INSERT INTO wh_meta(key, value) VALUES(?, ?)",
        (META_ORIGIN_CREATED_AT, str(time.time())),
    )


def assert_replica_binding_allowed(
    conn: sqlite3.Connection, origin_id: str
) -> None:
    """Validate that an empty plain warehouse may bind to ``origin_id``."""
    if not isinstance(origin_id, str) or not origin_id:
        raise OriginError("origin_id must be a non-empty string")
    role = role_of(conn)
    if role == ROLE_SOURCE:
        raise OriginError(
            "this warehouse is a source origin; it cannot become a replica"
        )
    if role == ROLE_REPLICA:
        bound = origin_of(conn)
        if bound != origin_id:
            raise OriginError(
                f"replica already bound to origin {bound!r}"
            )
        return
    if _has_business_history(conn):
        raise OriginError(
            "refusing to overwrite a non-empty business warehouse as a"
            " replica (existing batches found)"
        )
    rev_count = conn.execute(
        "SELECT COUNT(*) AS n FROM wh_claim_revisions"
    ).fetchone()["n"]
    if rev_count:
        raise OriginError(
            "refusing replica init: warehouse already contains revisions"
        )


def replica_status(conn: sqlite3.Connection) -> dict[str, Any]:
    """Public status of a managed copy (also answers on plain/source DBs)."""
    role = role_of(conn)
    origin = origin_of(conn)
    seq = latest_seq(conn)
    log = conn.execute(
        "SELECT prefix_digest FROM wh_replication_log WHERE knowledge_seq=?",
        (seq,),
    ).fetchone() if role is not None else None
    empty_digest = EMPTY_PREFIX_DIGEST if seq == 0 else None
    prefix_digest = (
        log["prefix_digest"] if log is not None else empty_digest
    )
    return {
        "role": role or "plain",
        "origin_id": origin,
        "committed_seq": seq,
        "prefix_digest": prefix_digest,
        "prefix_scheme": identity.PREFIX_SCHEME,
        "replication_entries": conn.execute(
            "SELECT COUNT(*) AS n FROM wh_replication_log"
        ).fetchone()["n"],
    }
