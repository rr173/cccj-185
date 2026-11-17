"""Change-file ingestion: validation, idempotent batches, atomic admission.

A batch is one change JSONL file plus the read-only enrichment DB it points
at.  All validation (structure, intervals, source-line evidence, product-key
immutability, same claim/revision content conflicts) finishes *before* the
single write transaction begins, so a rejected batch leaves no revisions and
consumes no ``knowledge_seq``.

Idempotency
-----------

The caller supplies ``batch_id``.  Re-presenting the same batch_id with the
same change-file bytes and the same resolved source evidence returns the
original ``knowledge_seq`` without creating a new batch.  The same batch_id
with different material is rejected.  A *different* batch_id whose changes
are all identical duplicates is still a new admitted batch and gets its own
seq (the batch ledger must explain every admission).
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import time
from typing import Any

from ..digest import canonical_json, sha256_hex
from . import hdb
from .hdb import (
    INT64_MAX,
    KIND_RETRACT,
    KIND_UPSERT,
    WarehouseError,
)

UPSERT_KEYS = {"claim_id", "revision", "source_line", "valid_from", "valid_to"}
RETRACT_KEYS = {"claim_id", "revision"}


def _test_kill(point: str) -> None:
    """Deterministic crash hook (SIGKILL); dormant unless the env var is set."""
    if os.environ.get("BARCODE_KILL_AFTER") == point:
        os.kill(os.getpid(), signal.SIGKILL)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _content_digest(record: dict[str, Any]) -> str:
    """Full-content identity of one revision.

    Covers the parsed original business row, the service result and the
    service provenance — never just the normalized value — plus the interval
    and product key, so a same claim/revision re-presentation with any
    differing detail is detectable.
    """
    payload = {
        "claim_id": record["claim_id"],
        "revision": record["revision"],
        "kind": record["kind"],
    }
    if record["kind"] == KIND_UPSERT:
        payload.update(
            {
                "supplier": record["supplier"],
                "item_id": record["item_id"],
                "market": record["market"],
                "valid_from": record["valid_from"],
                "valid_to": record["valid_to"],
                "source_line": record["source_line"],
                "source": record["source_obj"],
                "business_status": record["business_status"],
                "normalized_value": record["normalized_value"],
                "error_code": record["error_code"],
                "result_source": record["result_source"],
            }
        )
    return sha256_hex(canonical_json(payload))


def _evidence_record(record: dict[str, Any]) -> dict[str, Any]:
    """One element of the batch evidence digest (file order preserved)."""
    out = {
        "claim_id": record["claim_id"],
        "revision": record["revision"],
        "kind": record["kind"],
    }
    if record["kind"] == KIND_UPSERT:
        out.update(
            {
                "valid_from": record["valid_from"],
                "valid_to": record["valid_to"],
                "source_line": record["source_line"],
                "source": record["source_obj"],
                "business_status": record["business_status"],
                "normalized_value": record["normalized_value"],
                "error_code": record["error_code"],
                "result_source": record["result_source"],
            }
        )
    return out


def _parse_change_file(path: str) -> list[tuple[int, dict[str, Any]]]:
    """Parse and structurally validate the change file (no DB access)."""
    if not os.path.exists(path):
        raise WarehouseError(f"change file not found: {path}")
    parsed: list[tuple[int, dict[str, Any]]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            if not raw.strip():
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise WarehouseError(
                    f"change line {line_no}: invalid JSON: {exc}"
                ) from exc
            if not isinstance(row, dict):
                raise WarehouseError(f"change line {line_no}: must be an object")
            claim_id = row.get("claim_id")
            if not isinstance(claim_id, str) or not claim_id:
                raise WarehouseError(
                    f"change line {line_no}: claim_id must be a non-empty string"
                )
            revision = row.get("revision")
            if (
                not isinstance(revision, int)
                or isinstance(revision, bool)
                or revision <= 0
                or revision > INT64_MAX
            ):
                raise WarehouseError(
                    f"change line {line_no}: revision must be a positive integer"
                )
            keys = set(row)
            if keys == RETRACT_KEYS:
                parsed.append((line_no, {"kind": KIND_RETRACT, **row}))
                continue
            if keys != UPSERT_KEYS:
                raise WarehouseError(
                    f"change line {line_no}: upsert rows must contain exactly"
                    f" {sorted(UPSERT_KEYS)}, got {sorted(keys)}"
                )
            source_line = row["source_line"]
            if (
                not isinstance(source_line, int)
                or isinstance(source_line, bool)
                or source_line <= 0
            ):
                raise WarehouseError(
                    f"change line {line_no}: source_line must be a positive int"
                )
            valid_from = row["valid_from"]
            valid_to = row["valid_to"]
            if not hdb.is_int64(valid_from):
                raise WarehouseError(
                    f"change line {line_no}: valid_from must be a signed 64-bit int"
                )
            if valid_to is not None:
                if not hdb.is_int64(valid_to):
                    raise WarehouseError(
                        f"change line {line_no}: valid_to must be int64 or null"
                    )
                if not valid_from < valid_to:
                    raise WarehouseError(
                        f"change line {line_no}: require valid_from < valid_to"
                        f" ({valid_from} !< {valid_to})"
                    )
            parsed.append(
                (
                    line_no,
                    {
                        "kind": KIND_UPSERT,
                        "claim_id": claim_id,
                        "revision": revision,
                        "source_line": source_line,
                        "valid_from": valid_from,
                        "valid_to": valid_to,
                    },
                )
            )
    if not parsed:
        raise WarehouseError("change file contains no records")
    return parsed


class _ClaimState:
    """In-memory, file-order evolution of one claim inside a batch."""

    def __init__(self, supplier: str | None, item_id: str | None,
                 market: str | None, head_rev: int | None,
                 head_kind: str | None) -> None:
        self.supplier = supplier
        self.item_id = item_id
        self.market = market
        self.head_rev = head_rev
        self.head_kind = head_kind
        # (revision) -> content_digest seen in this batch
        self.batch_digests: dict[int, str] = {}

    def check_key(self, supplier: str, item_id: str, market: str,
                  line_no: int) -> None:
        if self.supplier is None:
            self.supplier, self.item_id, self.market = supplier, item_id, market
            return
        if (self.supplier, self.item_id, self.market) != (
            supplier,
            item_id,
            market,
        ):
            raise WarehouseError(
                f"change line {line_no}: claim product key is immutable;"
                f" existing=({self.supplier},{self.item_id},{self.market})"
                f" new=({supplier},{item_id},{market})"
            )


def import_batch(
    conn: sqlite3.Connection,
    source_db_path: str,
    change_path: str,
    batch_id: str,
) -> dict[str, Any]:
    """Admit one change batch against a read-only enrichment DB.

    Returns a summary containing the allocated (or replayed)
    ``knowledge_seq``.  Raises :class:`WarehouseError` on any validation
    failure; the warehouse is then left exactly as it was.
    """
    if not isinstance(batch_id, str) or not batch_id:
        raise WarehouseError("batch_id must be a non-empty string")

    change_digest = sha256_file(change_path)
    existing = conn.execute(
        "SELECT * FROM wh_batches WHERE batch_id=?", (batch_id,)
    ).fetchone()
    if existing is not None:
        if existing["change_digest"] != change_digest:
            raise WarehouseError(
                f"batch_id {batch_id!r} already used with different change"
                " material"
            )
        # Same change bytes under a known batch_id is the durable idempotent
        # identity: return the original seq straight from the ledger.  The
        # source DB is not reopened, so a retry still replays after the source
        # file has been moved or deleted (the evidence was copied in at
        # admission time).
        return {
            "knowledge_seq": int(existing["knowledge_seq"]),
            "batch_id": batch_id,
            "replayed": True,
            "status": existing["status"],
            "change_count": int(existing["change_count"]),
            "new_revisions": int(existing["new_revisions"]),
            "duplicate_rows": int(existing["duplicate_rows"]),
            "retractions": int(existing["retractions"]),
            "evidence_digest": existing["evidence_digest"],
        }

    summary = _validate_and_resolve(
        conn, source_db_path, change_path, change_digest
    )
    _commit_batch(conn, batch_id, source_db_path, summary)
    return {
        "knowledge_seq": summary["knowledge_seq"],
        "batch_id": batch_id,
        "replayed": False,
        "status": "admitted",
        **{k: summary[k] for k in (
            "change_count", "new_revisions", "duplicate_rows",
            "retractions", "evidence_digest",
        )},
    }


def _validate_and_resolve(
    conn: sqlite3.Connection,
    source_db_path: str,
    change_path: str,
    change_digest: str,
) -> dict[str, Any]:
    parsed = _parse_change_file(change_path)

    src = hdb.open_source_snapshot(source_db_path)
    try:
        ok, reason = hdb.source_delivery_ok(src)
        if not ok:
            raise WarehouseError(f"source db not deliverable: {reason}")

        # Seed per-claim state from already-admitted revisions.
        states: dict[str, _ClaimState] = {}
        for claim in conn.execute("SELECT * FROM wh_claims").fetchall():
            states[claim["claim_id"]] = _ClaimState(
                claim["supplier"], claim["item_id"], claim["market"],
                int(claim["head_revision"]), claim["head_kind"],
            )

        # Preload existing revision digests for conflict detection.
        existing_digests: dict[tuple[str, int], str] = {
            (r["claim_id"], int(r["revision"])): r["content_digest"]
            for r in conn.execute(
                "SELECT claim_id, revision, content_digest"
                " FROM wh_claim_revisions"
            ).fetchall()
        }

        resolved: list[dict[str, Any]] = []
        new_revisions: list[dict[str, Any]] = []
        duplicate_rows = 0
        retractions = 0

        for line_no, change in parsed:
            cid = change["claim_id"]
            rev = int(change["revision"])
            state = states.setdefault(cid, _ClaimState(None, None, None, None, None))

            if change["kind"] == KIND_RETRACT:
                key = (cid, rev)
                digest = sha256_hex(canonical_json(
                    {"claim_id": cid, "revision": rev, "kind": KIND_RETRACT}
                ))
                # Duplicates are identified by stored content alone and never
                # re-validated against the *current* head: an old batch must
                # replay even after the claim was retracted or recovered.
                if key in existing_digests or rev in state.batch_digests:
                    seen = (existing_digests.get(key)
                            or state.batch_digests.get(rev))
                    if seen != digest:
                        raise WarehouseError(
                            f"change line {line_no}: claim {cid} revision {rev}"
                            " already exists with different content"
                        )
                    duplicate_rows += 1
                    # A stored revision always carries the established key.
                    resolved.append({
                        "kind": KIND_RETRACT, "claim_id": cid, "revision": rev,
                        "content_digest": digest,
                        "supplier": state.supplier, "item_id": state.item_id,
                        "market": state.market,
                    })
                    continue
                # New retraction: the claim must exist and the revision be
                # higher than every revision admitted so far.
                if state.head_kind is None or state.supplier is None:
                    raise WarehouseError(
                        f"change line {line_no}: retract of unknown claim {cid!r}"
                    )
                if state.head_rev is not None and rev <= state.head_rev:
                    raise WarehouseError(
                        f"change line {line_no}: retract revision {rev} must be"
                        f" higher than current head revision {state.head_rev}"
                    )
                # Retractions inherit the established product key so the
                # revision stays selectable through product-key indexes.
                retract_record = {
                    "kind": KIND_RETRACT, "claim_id": cid, "revision": rev,
                    "content_digest": digest,
                    "supplier": state.supplier, "item_id": state.item_id,
                    "market": state.market,
                }
                resolved.append(dict(retract_record))
                new_revisions.append(retract_record)
                retractions += 1
                state.batch_digests[rev] = digest
                state.head_rev, state.head_kind = rev, KIND_RETRACT
                continue

            # ---- upsert --------------------------------------------------
            evidence = hdb.resolve_source_evidence(src, change["source_line"])
            record = {
                "kind": KIND_UPSERT,
                "claim_id": cid,
                "revision": rev,
                "valid_from": change["valid_from"],
                "valid_to": change["valid_to"],
                **evidence,
            }
            digest = _content_digest(record)
            record["content_digest"] = digest
            key = (cid, rev)
            # Same content as a stored or already-seen revision: an
            # unconditional duplicate, regardless of intervening retractions
            # or recoveries.  A differing digest at the same claim/revision
            # rejects the whole batch (comparison covers the parsed source
            # row, service result and provenance, not just the value).
            if key in existing_digests:
                if existing_digests[key] != digest:
                    raise WarehouseError(
                        f"change line {line_no}: claim {cid} revision {rev}"
                        " already exists with different content (whole-batch"
                        " rejection)"
                    )
                duplicate_rows += 1
                resolved.append(record)
                continue
            if rev in state.batch_digests:
                if state.batch_digests[rev] != digest:
                    raise WarehouseError(
                        f"change line {line_no}: in-batch claim {cid} revision"
                        f" {rev} appears with different content"
                    )
                duplicate_rows += 1
                resolved.append(record)
                continue

            # Genuinely new revision: head-state rules now apply.
            state.check_key(
                evidence["supplier"], evidence["item_id"],
                evidence["market"], line_no,
            )
            if state.head_kind == KIND_RETRACT and rev <= state.head_rev:
                raise WarehouseError(
                    f"change line {line_no}: claim {cid} was retracted at"
                    f" revision {state.head_rev}; only a higher revision may"
                    " re-establish it"
                )
            new_revisions.append(record)
            resolved.append(record)
            state.batch_digests[rev] = digest
            if state.head_rev is None or rev > state.head_rev:
                state.head_rev, state.head_kind = rev, KIND_UPSERT

        evidence_digest = sha256_hex(
            canonical_json([_evidence_record(r) for r in resolved])
        )
    finally:
        src.close()

    return {
        "change_digest": change_digest,
        "evidence_digest": evidence_digest,
        "change_count": len(parsed),
        "new_revisions": len(new_revisions),
        "duplicate_rows": duplicate_rows,
        "retractions": retractions,
        "new_records": new_revisions,
        "states": states,
        "source_db_digest": sha256_file(source_db_path),
    }


def _commit_batch(
    conn: sqlite3.Connection,
    batch_id: str,
    source_db_path: str,
    summary: dict[str, Any],
) -> None:
    # BEGIN IMMEDIATE serializes concurrent writers; SQLite's autoincrement
    # then guarantees distinct knowledge_seq values and readers never see a
    # partial transaction.
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT INTO wh_batches(batch_id, status, change_digest,"
            " evidence_digest, source_db_path, source_db_digest,"
            " change_count, new_revisions, duplicate_rows, retractions,"
            " detail, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                batch_id,
                "admitted",
                summary["change_digest"],
                summary["evidence_digest"],
                os.path.abspath(source_db_path),
                summary["source_db_digest"],
                summary["change_count"],
                summary["new_revisions"],
                summary["duplicate_rows"],
                summary["retractions"],
                json.dumps(
                    {
                        "new_claims": [
                            cid
                            for cid, st in summary["states"].items()
                            if st.head_kind is not None
                        ],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                time.time(),
            ),
        )
        seq = int(cur.lastrowid)
        for rec in summary["new_records"]:
            if rec["kind"] == KIND_RETRACT:
                conn.execute(
                    "INSERT INTO wh_claim_revisions(claim_id, revision, kind,"
                    " knowledge_seq, batch_id, supplier, item_id, market,"
                    " valid_from, valid_to, source_line, source_raw,"
                    " business_status, normalized_value, error_code,"
                    " result_source, content_digest)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        rec["claim_id"], rec["revision"], KIND_RETRACT, seq,
                        batch_id, rec["supplier"], rec["item_id"],
                        rec["market"], None, None, None, None, None, None,
                        None, None, rec["content_digest"],
                    ),
                )
                continue
            conn.execute(
                "INSERT INTO wh_claim_revisions(claim_id, revision, kind,"
                " knowledge_seq, batch_id, supplier, item_id, market,"
                " valid_from, valid_to, source_line, source_raw,"
                " business_status, normalized_value, error_code,"
                " result_source, content_digest)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    rec["claim_id"], rec["revision"], KIND_UPSERT, seq,
                    batch_id, rec["supplier"], rec["item_id"], rec["market"],
                    rec["valid_from"], rec["valid_to"], rec["source_line"],
                    json.dumps(rec["source_obj"], ensure_ascii=False,
                               sort_keys=True),
                    rec["business_status"], rec["normalized_value"],
                    rec["error_code"],
                    json.dumps(rec["result_source"], ensure_ascii=False,
                               sort_keys=True),
                    rec["content_digest"],
                ),
            )
        for cid, st in summary["states"].items():
            # Only claims touched by this batch need a head upsert.
            if st.batch_digests:
                conn.execute(
                    "INSERT INTO wh_claims(claim_id, supplier, item_id, market,"
                    " head_revision, head_kind, head_seq)"
                    " VALUES(?,?,?,?,?,?,?) ON CONFLICT(claim_id) DO UPDATE SET"
                    " head_revision=excluded.head_revision,"
                    " head_kind=excluded.head_kind, head_seq=excluded.head_seq",
                    (
                        cid, st.supplier, st.item_id, st.market,
                        st.head_rev, st.head_kind, seq,
                    ),
                )
        # Crash point: everything is staged in the uncommitted transaction.
        # A SIGKILL here leaves the database without this batch, so a new
        # process can retry the same batch_id cleanly.
        _test_kill("wh_batch_staged")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    summary["knowledge_seq"] = seq
