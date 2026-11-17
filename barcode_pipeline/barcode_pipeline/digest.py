"""Canonical digest helpers.

A *complete input* is the triple the partner service charges quota for:
(client_slot is assigned per request and is not part of the input identity)

    {"barcode": ..., "market": ..., "rule_version": ...}

The same barcode under a different market or rule_version is a different
input and may get a different answer, so the digest always covers all three
fields.  JSON serialization is canonical (sorted keys, compact separators,
ensure_ascii=False) and hashed with SHA-256.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

INPUT_FIELDS = ("barcode", "market", "rule_version")


def canonical_json(obj: Any) -> bytes:
    """Deterministic JSON encoding used for every digest in this project."""
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def input_digest(barcode: str, market: str, rule_version: str) -> str:
    """Stable identity of one complete service input."""
    payload = {
        "barcode": barcode,
        "market": market,
        "rule_version": rule_version,
    }
    return sha256_hex(canonical_json(payload))


def input_obj(digest: str, barcode: str, market: str, rule_version: str) -> dict:
    return {
        "input_digest": digest,
        "barcode": barcode,
        "market": market,
        "rule_version": rule_version,
    }


def request_body_digest(items: list[Mapping[str, Any]]) -> str:
    """Digest over the full POST /jobs body item list.

    Order-insensitive but content-sensitive: items are re-serialized
    canonically and sorted by their input digest.  The server recomputes
    this and rejects a body whose claimed digest does not match.
    """
    norm = []
    for it in items:
        norm.append(
            {
                "client_slot": it["client_slot"],
                "barcode": it["barcode"],
                "market": it["market"],
                "rule_version": it["rule_version"],
            }
        )
    norm.sort(key=lambda x: (x["client_slot"], input_digest(
        x["barcode"], x["market"], x["rule_version"])))
    return sha256_hex(canonical_json(norm))


def idempotency_key(job_group: str, body_digest: str) -> str:
    """Deterministic idempotency key.

    Derived (never random) so that a process killed between the server
    accepting the POST and the client persisting the job_id can safely
    re-POST: the same batch body always produces the same key and the
    server therefore returns the same job_id instead of charging quota
    twice.  ``job_group`` separates distinct batches that happen to share
    a body (e.g. different pipeline runs / batch indices).
    """
    return "idem-" + sha256_hex(
        canonical_json({"group": job_group, "body_digest": body_digest})
    )[:32]
