"""Origin identity and logical-prefix digests for offline warehouse copies.

A *managed* warehouse carries one durable origin identity, established once by
``warehouse origin init`` and never changed afterwards.  Because a physical
file copy (or a SQLite online backup) duplicates that identity, two databases
may share an origin and even the same maximum ``knowledge_seq`` while holding
different histories.  Origin plus seq therefore cannot decide whether two
histories are the same history.  The *logical prefix digest* defined here is
that decision: it is a pure function of the admitted business history,
independent of the physical database page layout, file name, export clock
(``created_at``), absolute source paths or the row ids assigned by SQLite.

Digest definition (version ``wh1-prefix-v1``)
---------------------------------------------

All hashing uses SHA-256 over :func:`barcode_pipeline.digest.canonical_json`
bytes (UTF-8, sorted keys, compact separators, ``ensure_ascii=False``).

1. *Revision unit.* One digest input per revision, identical in field scope
   and ordering to the admission-time ``content_digest``:

   * retraction -- the exact object
     ``{"claim_id", "revision", "kind": "retract"}``;
   * upsert -- an object with exactly the keys
     ``claim_id, revision, kind, supplier, item_id, market, valid_from,
     valid_to, source_line, source, business_status, normalized_value,
     error_code, result_source`` where ``source`` is the parsed original
     partner business row and ``result_source`` is the service provenance
     object (``job_id, slot, page_index, payload_hash, input_digest``).

   The normalized value alone is never an identity: the original business
   columns and the service provenance are part of the digest.

2. *Batch unit.* For batch seq ``s`` the input is a two-element JSON array:
   ``[batch, revisions]``.

   * ``batch`` contains exactly
     ``knowledge_seq, batch_id, change_digest, evidence_digest,
     source_db_path, source_db_digest, change_count, new_revisions,
     duplicate_rows, retractions`` (all admitted-batch ledger columns except
     the physical ``id``/status/``detail``/``created_at``);
   * ``revisions`` is the list of revision units (rule 1) for revisions whose
     *first admission* happened in this batch, ordered by their insertion
     order, i.e. ``wh_claim_revisions.id`` ascending -- the same order in
     which the change file presented them.  Batches whose rows are all
     consistent duplicates contribute an empty list but still a batch unit,
     so a zero-new-revision batch is part of the identity.

3. *Prefix chain.* ``H(0)`` is the hash of the fixed string
   ``"wh1-prefix-v1 empty"``.  For every admitted batch in
   ``knowledge_seq`` order 1..n,
   ``H(s) = sha256(H(s-1) + "|" + sha256(batch_unit(s)))`` using lowercase
   hex ASCII throughout.  ``H(n)`` identifies the whole logical prefix.
"""

from __future__ import annotations

import hashlib
import secrets
import sqlite3
from typing import Any, Iterable

from ...digest import canonical_json, sha256_hex

PREFIX_SCHEME = "wh1-prefix-v1"
EMPTY_PREFIX_DIGEST = sha256_hex(f"{PREFIX_SCHEME} empty".encode("utf-8"))

# wh_meta keys
META_ROLE = "repl_role"            # source | replica, absent on plain wh1
META_ORIGIN_ID = "origin_id"
META_ORIGIN_CREATED_AT = "origin_created_at"

ROLE_SOURCE = "source"
ROLE_REPLICA = "replica"


def new_origin_id() -> str:
    """Fresh 128-bit origin identity (32 lowercase hex characters)."""
    return secrets.token_hex(16)


def revision_digest_payload(rec: dict[str, Any]) -> dict[str, Any]:
    """Canonical digest input for one revision (same scope as admission).

    Accepts both the importer's in-memory record (``source_obj``) and the
    stored/wire record (``source``); the canonical object always uses the
    key ``source`` so the digest is identical in both representations.
    """
    payload: dict[str, Any] = {
        "claim_id": rec["claim_id"],
        "revision": rec["revision"],
        "kind": rec["kind"],
    }
    if rec["kind"] == "upsert":
        source = rec.get("source", rec.get("source_obj"))
        payload.update(
            {
                "supplier": rec["supplier"],
                "item_id": rec["item_id"],
                "market": rec["market"],
                "valid_from": rec["valid_from"],
                "valid_to": rec["valid_to"],
                "source_line": rec["source_line"],
                "source": source,
                "business_status": rec["business_status"],
                "normalized_value": rec["normalized_value"],
                "error_code": rec["error_code"],
                "result_source": rec["result_source"],
            }
        )
    return payload


def revision_digest(rec: dict[str, Any]) -> str:
    return sha256_hex(canonical_json(revision_digest_payload(rec)))


# Exact key order of the batch-level object (sort_keys makes it deterministic;
# the tuple documents the covered field range).
BATCH_FIELDS = (
    "knowledge_seq",
    "batch_id",
    "change_digest",
    "evidence_digest",
    "source_db_path",
    "source_db_digest",
    "change_count",
    "new_revisions",
    "duplicate_rows",
    "retractions",
)


def batch_unit(batch: dict[str, Any], revisions: Iterable[dict[str, Any]]) -> bytes:
    """Canonical bytes of one batch unit (rule 2)."""
    batch_obj = {key: batch[key] for key in BATCH_FIELDS}
    rev_objs = [revision_digest_payload(rec) for rec in revisions]
    return canonical_json([batch_obj, rev_objs])


def chain_step(prev_digest: str, batch_unit_bytes: bytes) -> str:
    """H(s) = sha256(H(s-1) + '|' + sha256(batch_unit)) (rule 3)."""
    inner = sha256_hex(batch_unit_bytes)
    return hashlib.sha256(
        prev_digest.encode("ascii") + b"|" + inner.encode("ascii")
    ).hexdigest()


def revision_row_to_rec(row: sqlite3.Row) -> dict[str, Any]:
    """Decode a stored ``wh_claim_revisions`` row into a digest record."""
    import json

    rec: dict[str, Any] = {
        "claim_id": row["claim_id"],
        "revision": int(row["revision"]),
        "kind": row["kind"],
    }
    if row["kind"] == "upsert":
        rec.update(
            {
                "supplier": row["supplier"],
                "item_id": row["item_id"],
                "market": row["market"],
                "valid_from": row["valid_from"],
                "valid_to": row["valid_to"],
                "source_line": row["source_line"],
                "source": json.loads(row["source_raw"]),
                "business_status": row["business_status"],
                "normalized_value": row["normalized_value"],
                "error_code": row["error_code"],
                "result_source": json.loads(row["result_source"]),
            }
        )
    return rec


def compute_prefix_at(
    conn: sqlite3.Connection,
    upto_seq: int,
    *,
    start_digest: str = EMPTY_PREFIX_DIGEST,
) -> tuple[str, list[str]]:
    """Recompute prefix digests from stored history up to ``upto_seq``.

    Returns the final digest and the per-seq digest list (index 0 holds the
    empty-prefix digest).  Streams one batch/revision set at a time so a large
    warehouse never needs the whole history resident.
    """
    digests = [start_digest]
    current = start_digest
    batches = conn.execute(
        "SELECT * FROM wh_batches WHERE status='admitted'"
        " AND knowledge_seq <= ? ORDER BY knowledge_seq ASC",
        (upto_seq,),
    ).fetchall()
    for b in batches:
        seq = int(b["knowledge_seq"])
        if seq != len(digests):
            raise ValueError(
                f"non-contiguous batch history at seq {seq} (expected"
                f" {len(digests)})"
            )
        rev_rows = conn.execute(
            "SELECT * FROM wh_claim_revisions WHERE knowledge_seq=?"
            " ORDER BY id ASC",
            (seq,),
        ).fetchall()
        batch = dict(b)
        unit = batch_unit(
            batch, (revision_row_to_rec(r) for r in rev_rows)
        )
        current = chain_step(current, unit)
        digests.append(current)
    return current, digests
