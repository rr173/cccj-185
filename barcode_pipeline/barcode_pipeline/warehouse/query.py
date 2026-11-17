"""As-of-knowledge-sequence point lookups and maximal-segment timelines.

Selection rule (applied per claim, *before* any interval test):

  among revisions admitted at ``knowledge_seq <= s`` choose the highest
  revision number; if that revision is a retraction the claim does not exist.

Low revisions arriving late therefore never override a higher one, and
interval fragments of superseded revisions never survive.  When several
different claims cover one business instant the answers are *combined*, never
ranked: identical normalized values (or identical invalid error codes) merge
with all supporting claims listed; any divergence is a ``conflict`` and no
winner is chosen by revision number or import time.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from .hdb import (
    INT64_MAX,
    STATE_CONFLICT,
    STATE_INVALID,
    STATE_MISSING,
    STATE_NORMALIZED,
    WarehouseError,
    is_int64,
    latest_seq,
)


def resolve_seq(conn: sqlite3.Connection, seq: int | str | None) -> int:
    """Map the caller's seq argument to a concrete, pinned knowledge seq.

    ``None`` or ``"latest"`` means the newest admitted seq *now*; callers
    resolve this once at operation start so a long fact file never mixes
    versions when another process imports midway.
    """
    if seq is None or seq == "latest":
        return latest_seq(conn)
    if isinstance(seq, str):
        try:
            seq = int(seq)
        except ValueError as exc:
            raise WarehouseError(f"invalid knowledge_seq {seq!r}") from exc
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
        raise WarehouseError("knowledge_seq must be a non-negative integer")
    top = latest_seq(conn)
    if seq > top:
        raise WarehouseError(
            f"knowledge_seq {seq} does not exist yet (latest={top})"
        )
    return seq


def active_revisions(
    conn: sqlite3.Connection,
    supplier: str,
    item_id: str,
    market: str,
    seq: int,
) -> list[sqlite3.Row]:
    """Rows of the per-claim highest revisions admitted up to ``seq``.

    Retractions and revisions whose interval does not cover the queried time
    are filtered by the callers (:func:`covering_revisions`); this function
    only implements the revision-selection rule.  The product filter is pushed
    *inside* the per-claim aggregation so a fact lookup scans only that
    product's revisions through ``idx_rev_product`` rather than the whole
    revision table — essential when the warehouse holds many products.
    """
    return conn.execute(
        "SELECT r.* FROM wh_claim_revisions r"
        " JOIN ("
        "   SELECT claim_id, MAX(revision) AS max_revision"
        "   FROM wh_claim_revisions"
        "   WHERE knowledge_seq <= ? AND supplier=? AND item_id=? AND market=?"
        "   GROUP BY claim_id"
        " ) m ON m.claim_id=r.claim_id AND m.max_revision=r.revision"
        " WHERE r.supplier=? AND r.item_id=? AND r.market=?",
        (seq, supplier, item_id, market, supplier, item_id, market),
    ).fetchall()


def covering_revisions(
    rows: list[sqlite3.Row], event_time: int
) -> list[sqlite3.Row]:
    """Drop retractions and intervals not covering ``event_time``."""
    out: list[sqlite3.Row] = []
    for r in rows:
        if r["kind"] != "upsert":
            continue
        v_from = int(r["valid_from"])
        v_to = r["valid_to"]
        if event_time < v_from:
            continue
        if v_to is not None and event_time >= int(v_to):
            continue
        out.append(r)
    return out


def _support(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "claim_id": row["claim_id"],
        "revision": int(row["revision"]),
        "knowledge_seq": int(row["knowledge_seq"]),
        "batch_id": row["batch_id"],
        "valid_from": row["valid_from"],
        "valid_to": row["valid_to"],
        "source_line": row["source_line"],
        "source": json.loads(row["source_raw"]),
        "business_status": row["business_status"],
        "normalized_value": row["normalized_value"],
        "error_code": row["error_code"],
        "result_source": json.loads(row["result_source"]),
    }


def _sorted_supports(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    supports = [_support(r) for r in rows]
    supports.sort(key=lambda s: (s["claim_id"], s["revision"]))
    return supports


def resolve_answer(rows: list[sqlite3.Row]) -> dict[str, Any]:
    """Combine covering claim revisions into one of the four answer states."""
    if not rows:
        return {
            "state": STATE_MISSING,
            "normalized_value": None,
            "error_code": None,
            "supports": [],
        }
    supports = _sorted_supports(rows)
    statuses = {r["business_status"] for r in rows}
    if statuses == {STATE_NORMALIZED}:
        values = {r["normalized_value"] for r in rows}
        if len(values) == 1:
            return {
                "state": STATE_NORMALIZED,
                "normalized_value": next(iter(values)),
                "error_code": None,
                "supports": supports,
            }
    if statuses == {STATE_INVALID}:
        codes = {r["error_code"] for r in rows}
        if len(codes) == 1:
            return {
                "state": STATE_INVALID,
                "normalized_value": None,
                "error_code": next(iter(codes)),
                "supports": supports,
            }
    # Divergence: different values, different codes, or a normalized/invalid
    # mix.  Every distinct answer keeps its own supporting claims; no winner
    # is picked across claims.
    groups: dict[tuple[str, str | None], list[sqlite3.Row]] = {}
    for r in rows:
        key = (r["business_status"], r["normalized_value"]
               if r["business_status"] == STATE_NORMALIZED else r["error_code"])
        groups.setdefault(key, []).append(r)
    disagreements = []
    for (status, answer), members in sorted(
        groups.items(), key=lambda kv: (kv[0][0], str(kv[0][1]))
    ):
        disagreements.append(
            {
                "business_status": status,
                "normalized_value": answer if status == STATE_NORMALIZED else None,
                "error_code": answer if status == STATE_INVALID else None,
                "supports": _sorted_supports(members),
            }
        )
    return {
        "state": STATE_CONFLICT,
        "normalized_value": None,
        "error_code": None,
        "supports": supports,
        "disagreements": disagreements,
    }


def _check_product_args(supplier: str, item_id: str, market: str) -> None:
    for name, value in (
        ("supplier", supplier),
        ("item_id", item_id),
        ("market", market),
    ):
        if not isinstance(value, str) or not value:
            raise WarehouseError(f"{name} must be a non-empty string")


def lookup(
    conn: sqlite3.Connection,
    supplier: str,
    item_id: str,
    market: str,
    event_time: int,
    seq: int | str | None = None,
) -> dict[str, Any]:
    """Point-in-time answer with full provenance for one product."""
    _check_product_args(supplier, item_id, market)
    if not is_int64(event_time):
        raise WarehouseError("event_time must be a signed 64-bit integer")
    pinned = resolve_seq(conn, seq)
    rows = covering_revisions(
        active_revisions(conn, supplier, item_id, market, pinned), event_time
    )
    answer = resolve_answer(rows)
    return {
        "supplier": supplier,
        "item_id": item_id,
        "market": market,
        "event_time": event_time,
        "knowledge_seq": pinned,
        **answer,
    }


def _answer_signature(answer: dict[str, Any]) -> tuple[Any, ...]:
    support_ids = tuple(
        sorted((s["claim_id"], s["revision"]) for s in answer["supports"])
    )
    return (
        answer["state"],
        answer["normalized_value"],
        answer["error_code"],
        tuple(
            (d["business_status"], d["normalized_value"], d["error_code"])
            for d in answer.get("disagreements", [])
        ),
        support_ids,
    )


def timeline(
    conn: sqlite3.Connection,
    supplier: str,
    item_id: str,
    market: str,
    time_from: int,
    time_to: int,
    seq: int | str | None = None,
) -> dict[str, Any]:
    """Maximal contiguous segments covering finite range [time_from, time_to).

    A new segment starts whenever the state, value/error code *or* the set of
    supporting claim revisions changes — equal answers with different
    evidence are never merged.
    """
    _check_product_args(supplier, item_id, market)
    if not is_int64(time_from) or not is_int64(time_to):
        raise WarehouseError("timeline bounds must be signed 64-bit integers")
    if not time_from < time_to:
        raise WarehouseError("require time_from < time_to")
    pinned = resolve_seq(conn, seq)
    selected = active_revisions(conn, supplier, item_id, market, pinned)
    upserts = [r for r in selected if r["kind"] == "upsert"]

    boundaries = {time_from, time_to}
    for r in upserts:
        v_from = max(int(r["valid_from"]), time_from)
        v_to = min(INT64_MAX if r["valid_to"] is None else int(r["valid_to"]),
                   time_to)
        if v_from < time_to and v_from >= time_from:
            boundaries.add(v_from)
        if v_to > time_from and v_to <= time_to:
            boundaries.add(v_to)
    points = sorted(boundaries)

    segments: list[dict[str, Any]] = []
    for left, right in zip(points, points[1:]):
        if left >= right:
            continue
        covering = covering_revisions(upserts, left)
        answer = resolve_answer(covering)
        segment = {
            "start": left,
            "end": right,
            "state": answer["state"],
            "normalized_value": answer["normalized_value"],
            "error_code": answer["error_code"],
            "supports": answer["supports"],
        }
        if "disagreements" in answer:
            segment["disagreements"] = answer["disagreements"]
        if segments and _answer_signature(segments[-1]) == _answer_signature(segment):
            segments[-1]["end"] = right
        else:
            segments.append(segment)

    return {
        "supplier": supplier,
        "item_id": item_id,
        "market": market,
        "time_from": time_from,
        "time_to": time_to,
        "knowledge_seq": pinned,
        "segments": segments,
    }
