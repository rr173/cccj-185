"""Offline package export from a source-origin warehouse.

Consistency model
-----------------

The export opens its **own** read-only SQLite connection and starts one
deferred transaction before reading anything, pinning a single read snapshot
for the whole run.  A concurrent process admitting batches while the export
streams cannot change the declared boundaries, ledger rows or revision
content: every statement executes against that one snapshot.  The boundary
``through`` (including the ``latest`` shorthand) is resolved once, at start.

The package never contains history before ``after`` and never embeds the
SQLite file: only the batch frames of ``(after, through]`` plus their
revisions and the two boundary prefix digests travel across.
"""

from __future__ import annotations

import datetime as _dt
import os
import sqlite3
from typing import Any, Callable

from ..hdb import WarehouseError, latest_seq
from . import identity
from .origin import require_source
from .package import PROTOCOL_VERSION, PackageError, PackageWriter


def _read_only_connection(db_path: str) -> sqlite3.Connection:
    uri = "file:" + os.path.abspath(db_path) + "?mode=ro"
    snap = sqlite3.connect(uri, uri=True, isolation_level=None)
    snap.row_factory = sqlite3.Row
    snap.execute("PRAGMA query_only=ON")
    # Keep the writer's WAL growth bounded without weakening the snapshot:
    # the pragma only caps the page cache of this read connection.
    snap.execute("PRAGMA cache_size=-20000")
    # One deferred read transaction pins the snapshot for the whole export.
    snap.execute("BEGIN")
    return snap


def _prefix_digest_at(
    conn: sqlite3.Connection, seq: int
) -> str:
    if seq == 0:
        return identity.EMPTY_PREFIX_DIGEST
    row = conn.execute(
        "SELECT prefix_digest FROM wh_replication_log WHERE knowledge_seq=?",
        (seq,),
    ).fetchone()
    if row is None:
        raise WarehouseError(
            f"no replication registration at seq {seq}; is this an"
            " initialized source origin?"
        )
    return row["prefix_digest"]


def _batch_revisions(
    conn: sqlite3.Connection, seq: int
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM wh_claim_revisions WHERE knowledge_seq=?"
        " ORDER BY id ASC",
        (seq,),
    ).fetchall()
    return [identity.revision_row_to_rec(r) for r in rows]


def export_package(
    warehouse_path: str,
    after: int,
    through: int | str,
    target_path: str,
    *,
    gate: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
    """Write the offline package for range ``(after, through]``.

    ``through="latest"`` is resolved once against the pinned snapshot at
    start.  ``gate(after, through)`` (if given) runs after the snapshot and
    both boundary digests have been fixed but before any frame is written --
    tests use it as a deterministic synchronization point, never a sleep.

    Raises :class:`FileExistsError` when the target already exists; a failed
    export leaves only a ``.tmp`` staging file.
    """
    if not isinstance(after, int) or isinstance(after, bool) or after < 0:
        raise WarehouseError("after must be a non-negative integer")
    snap = _read_only_connection(warehouse_path)
    try:
        origin_id = require_source(snap)
        top = latest_seq(snap)
        if isinstance(through, str):
            if through != "latest":
                raise WarehouseError(
                    f"through must be an integer or 'latest', got {through!r}"
                )
            fixed_through = top
        else:
            if isinstance(through, bool) or not isinstance(through, int):
                raise WarehouseError("through must be an integer or 'latest'")
            fixed_through = through
        if fixed_through < after:
            raise WarehouseError(
                f"through {fixed_through} must be >= after {after}"
            )
        if fixed_through > top:
            raise WarehouseError(
                f"through {fixed_through} does not exist yet (latest={top})"
            )
        after_digest = _prefix_digest_at(snap, after)
        through_digest = _prefix_digest_at(snap, fixed_through)

        if gate is not None:
            gate(after, fixed_through)

        manifest = {
            "protocol_version": PROTOCOL_VERSION,
            "origin_id": origin_id,
            "prefix_scheme": identity.PREFIX_SCHEME,
            "after": after,
            "through": fixed_through,
            "after_digest": after_digest,
            "through_digest": through_digest,
            "created_at_utc": _dt.datetime.now(_dt.timezone.utc)
            .isoformat(timespec="seconds"),
        }
        try:
            writer = PackageWriter(target_path, manifest)
        except FileExistsError:
            raise
        try:
            # Stream one batch at a time; at most one batch's revisions are
            # resident.
            for seq in range(after + 1, fixed_through + 1):
                batch_row = snap.execute(
                    "SELECT * FROM wh_batches WHERE knowledge_seq=?",
                    (seq,),
                ).fetchone()
                if batch_row is None or batch_row["status"] != "admitted":
                    raise PackageError(
                        f"seq {seq} has no admitted batch registration"
                    )
                revisions = _batch_revisions(snap, seq)
                if len(revisions) != int(batch_row["new_revisions"]):
                    raise PackageError(
                        f"seq {seq}: stored revision count"
                        f" {len(revisions)} != ledger new_revisions"
                        f" {batch_row['new_revisions']}"
                    )
                writer.write_batch(dict(batch_row), revisions)
            result = writer.finish()
        except Exception:
            writer.abort()
            raise
        # Independently verify the stored registration against a recomputation
        # of the same snapshot before declaring success.
        recomputed, _ = identity.compute_prefix_at(snap, fixed_through)
        if recomputed != through_digest:
            raise PackageError(
                "export self-check failed: recomputed prefix disagrees with"
                " the replication log"
            )
        return {
            "event": "package_export",
            "origin_id": origin_id,
            "after": after,
            "through": fixed_through,
            "after_digest": after_digest,
            "through_digest": through_digest,
            **result,
        }
    finally:
        snap.execute("COMMIT")
        snap.close()
