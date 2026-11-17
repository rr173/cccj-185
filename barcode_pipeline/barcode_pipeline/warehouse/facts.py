"""Streaming fact enrichment and two-sequence impact comparison.

Both commands stream their JSONL inputs line by line; nothing resembling the
whole fact file is held in memory (the impact writer keeps one open output
handle and a rolling state, not the two full answer sets).

The knowledge sequence is resolved **once when the operation starts**.  A
batch imported by another process while a long enrichment is running cannot
make one file mix two warehouse versions.
"""

from __future__ import annotations

import bisect
import json
import os
from collections import OrderedDict
from typing import Any, Iterator

from .hdb import WarehouseError
from .query import active_revisions, resolve_answer, resolve_seq

REQUIRED_FACT_FIELDS = ("supplier", "item_id", "market", "event_time")

# Bounded per-run cache of a product's head revisions at the pinned seq.
# Facts repeatedly reference the same products out of order; with the seq
# pinned their head selection never changes, so each product is resolved once.
# The cap keeps whole-table-style memory use impossible regardless of how
# many products a fact file touches.
_HEAD_CACHE_PRODUCTS = 64


class _HeadCache:
    def __init__(self, conn, max_products: int = _HEAD_CACHE_PRODUCTS) -> None:
        self._conn = conn
        self._max = max_products
        self._od: "OrderedDict[tuple, _ProductHeads]" = OrderedDict()

    def covering(self, row: dict[str, Any], seq: int) -> list:
        key = (row["supplier"], row["item_id"], row["market"], seq)
        heads = self._od.get(key)
        if heads is None:
            heads = _ProductHeads(
                active_revisions(
                    self._conn, row["supplier"], row["item_id"],
                    row["market"], seq,
                )
            )
            self._od[key] = heads
            while len(self._od) > self._max:
                self._od.popitem(last=False)
        else:
            self._od.move_to_end(key)
        return heads.covering(row["event_time"])


class _ProductHeads:
    """Head upsert revisions of one product at one seq, interval-indexed."""

    def __init__(self, rows: list) -> None:
        upserts = [r for r in rows if r["kind"] == "upsert"]
        upserts.sort(
            key=lambda r: (
                int(r["valid_from"]), r["claim_id"], int(r["revision"])
            )
        )
        self._starts = [int(r["valid_from"]) for r in upserts]
        self._upserts = upserts

    def covering(self, event_time: int) -> list:
        # Only rows whose interval starts at or before t can cover it.
        hi = bisect.bisect_right(self._starts, event_time)
        out = []
        for r in self._upserts[:hi]:
            v_to = r["valid_to"]
            if v_to is None or event_time < int(v_to):
                out.append(r)
        return out


def _stream_facts(path: str) -> Iterator[tuple[int, dict[str, Any]]]:
    if not os.path.exists(path):
        raise WarehouseError(f"fact file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        line_no = 0
        for raw in fh:
            if not raw.strip():
                continue
            line_no += 1
            try:
                row = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise WarehouseError(
                    f"fact line {line_no}: invalid JSON: {exc}"
                ) from exc
            if not isinstance(row, dict):
                raise WarehouseError(f"fact line {line_no}: must be an object")
            missing = [k for k in REQUIRED_FACT_FIELDS if k not in row]
            if missing:
                raise WarehouseError(
                    f"fact line {line_no}: missing fields: {missing}"
                )
            if not isinstance(row["event_time"], int) or isinstance(
                row["event_time"], bool
            ):
                raise WarehouseError(
                    f"fact line {line_no}: event_time must be an integer"
                )
            for name in ("supplier", "item_id", "market"):
                if not isinstance(row[name], str) or not row[name]:
                    raise WarehouseError(
                        f"fact line {line_no}: {name} must be a non-empty string"
                    )
            yield line_no, row


def _answer_for(row: dict[str, Any], seq: int, cache: _HeadCache) -> dict[str, Any]:
    revisions = cache.covering(row, seq)
    return resolve_answer(revisions)


def _refuse_existing(path: str) -> None:
    if os.path.exists(path):
        raise FileExistsError(f"output already exists: {path}")


def enrich_file(
    conn,
    fact_path: str,
    output_path: str,
    seq: int | str | None = None,
) -> dict[str, Any]:
    """Enrich every fact line, preserving order and exact duplicates.

    Output lines echo the full original record (including any extra columns
    and duplicate ``fact_id`` values) plus the answer and its explanation.
    The target must not pre-exist; on failure no success-shaped output is
    left behind (a ``.tmp`` file is atomically renamed only at the end).
    """
    _refuse_existing(output_path)
    pinned = resolve_seq(conn, seq)
    parent = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(parent, exist_ok=True)
    tmp = output_path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)
    cache = _HeadCache(conn)
    counts = {"normalized": 0, "invalid": 0, "conflict": 0, "missing": 0}
    row_count = 0
    try:
        with open(tmp, "w", encoding="utf-8") as out:
            for line_no, row in _stream_facts(fact_path):
                answer = _answer_for(row, pinned, cache)
                counts[answer["state"]] += 1
                record = dict(row)  # preserve every original column/order
                record["line_no"] = line_no
                record["knowledge_seq"] = pinned
                record["state"] = answer["state"]
                record["normalized_value"] = answer["normalized_value"]
                record["error_code"] = answer["error_code"]
                record["explanation"] = {
                    "supports": answer["supports"],
                    **(
                        {"disagreements": answer["disagreements"]}
                        if "disagreements" in answer
                        else {}
                    ),
                }
                out.write(
                    json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
                )
                row_count += 1
        os.replace(tmp, output_path)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return {
        "output_path": os.path.abspath(output_path),
        "fact_file": os.path.abspath(fact_path),
        "knowledge_seq": pinned,
        "row_count": row_count,
        "state_counts": counts,
    }


def _answer_material(answer: dict[str, Any]) -> tuple[str, str | None]:
    """Only state + value/error code trigger a recompute, not evidence sets."""
    return answer["state"], (
        answer["normalized_value"]
        if answer["state"] == "normalized"
        else answer["error_code"]
    )


def impact_file(
    conn,
    fact_path: str,
    output_path: str,
    old_seq: int | str,
    new_seq: int | str,
) -> dict[str, Any]:
    """Compare two seqs over one fact file; emit only materially changed lines.

    A change of supporting evidence alone (different claims/revisions but the
    same state and value/error code) is intentionally not emitted; lookup and
    timeline still expose the new evidence.
    """
    _refuse_existing(output_path)
    old = resolve_seq(conn, old_seq)
    new = resolve_seq(conn, new_seq)
    if old > new:
        raise WarehouseError(
            f"old_seq {old} must not be greater than new_seq {new}"
        )
    parent = os.path.dirname(os.path.abspath(output_path))
    os.makedirs(parent, exist_ok=True)
    tmp = output_path + ".tmp"
    if os.path.exists(tmp):
        os.remove(tmp)
    cache = _HeadCache(conn)
    changed = 0
    total = 0
    try:
        with open(tmp, "w", encoding="utf-8") as out:
            for line_no, row in _stream_facts(fact_path):
                total += 1
                old_answer = _answer_for(row, old, cache)
                new_answer = _answer_for(row, new, cache)
                if _answer_material(old_answer) == _answer_material(new_answer):
                    continue
                changed += 1
                record = {
                    "line_no": line_no,
                    "fact": row,
                    "old": {
                        "knowledge_seq": old,
                        "state": old_answer["state"],
                        "normalized_value": old_answer["normalized_value"],
                        "error_code": old_answer["error_code"],
                        "evidence": old_answer["supports"],
                        **(
                            {"disagreements": old_answer["disagreements"]}
                            if "disagreements" in old_answer
                            else {}
                        ),
                    },
                    "new": {
                        "knowledge_seq": new,
                        "state": new_answer["state"],
                        "normalized_value": new_answer["normalized_value"],
                        "error_code": new_answer["error_code"],
                        "evidence": new_answer["supports"],
                        **(
                            {"disagreements": new_answer["disagreements"]}
                            if "disagreements" in new_answer
                            else {}
                        ),
                    },
                }
                out.write(
                    json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
                )
        os.replace(tmp, output_path)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise
    return {
        "output_path": os.path.abspath(output_path),
        "fact_file": os.path.abspath(fact_path),
        "old_seq": old,
        "new_seq": new,
        "row_count": total,
        "changed_count": changed,
    }
