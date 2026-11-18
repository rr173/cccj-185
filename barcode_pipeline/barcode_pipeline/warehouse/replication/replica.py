"""Package inspection and acceptance on managed replica warehouses.

Acceptance unit is always the *whole* package.  Full material is verified
before any write; the apply itself is one ``BEGIN IMMEDIATE`` transaction, so
concurrent receivers serialize and readers only ever see the old complete
prefix or the new complete prefix.  A killed receiver leaves either nothing
(new process sees the old prefix and can apply the package cleanly) or the
whole package (a new process verifies it as a replay).
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import time
from typing import Any

from .. import hdb
from ..hdb import KIND_RETRACT, KIND_UPSERT, WarehouseError, latest_seq
from . import identity
from . import origin
from .identity import EMPTY_PREFIX_DIGEST
from .origin import (
    origin_of,
    replica_status,
    role_of,
)
from .package import PackageError, read_package

# Public inspection verdicts.
VERDICT_APPENDABLE = "appendable"
VERDICT_REPLAY = "complete_replay"
VERDICT_MISSING_PREFIX = "missing_prefix"
VERDICT_ORIGIN_MISMATCH = "origin_mismatch"
VERDICT_DIVERGED = "overlap_diverged"
VERDICT_CORRUPT = "material_corrupt"
VERDICT_INELIGIBLE_TARGET = "ineligible_target"
VERDICT_UNKNOWN_PROTOCOL = "unknown_protocol"

# Kill hooks (dormant unless the env var is set).
_KILL_APPLY_STAGED = "wh_replica_staged"
_KILL_APPLY_BEFORE_TXN = "wh_replica_before_txn"


def _test_kill(point: str) -> None:
    if os.environ.get("BARCODE_KILL_AFTER") == point:
        os.kill(os.getpid(), signal.SIGKILL)


# --------------------------------------------------------------------------
# Semantic validation of package material (warehouse rules, not just hashes)
# --------------------------------------------------------------------------

_UPSERT_REQUIRED = (
    "claim_id", "revision", "kind", "supplier", "item_id", "market",
    "valid_from", "valid_to", "source_line", "source", "business_status",
    "normalized_value", "error_code", "result_source",
)
_RETRACT_REQUIRED = ("claim_id", "revision", "kind")


class _FrameValidator:
    """Stateful, streaming validator for package frames.

    Holds only cross-frame sets (seen revisions, product keys) and the
    running prefix chain, so a large package is validated one frame at a time
    without buffering it.  Used by both the in-memory and streaming
    acceptance paths, guaranteeing identical checks.
    """

    def __init__(self, manifest: dict[str, Any]):
        self.manifest = manifest
        self.seen_revisions: set[tuple[str, int]] = set()
        self.claim_products: dict[str, tuple[str, str, str]] = {}
        self.chain = manifest["after_digest"]
        self.expected_seq = int(manifest["after"])
        self.batch_count = 0
        self.revision_count = 0

    def feed(self, batch: dict[str, Any], revs: list[dict[str, Any]]) -> None:
        seq = int(batch["knowledge_seq"])
        self.expected_seq += 1
        if seq != self.expected_seq:
            raise PackageError(
                f"seq {seq} out of order; expected {self.expected_seq}"
            )
        if set(batch) != set(identity.BATCH_FIELDS):
            raise PackageError(f"seq {seq}: batch frame has wrong field set")
        new_count = int(batch["new_revisions"])
        if new_count != len(revs):
            raise PackageError(
                f"seq {seq}: frame carries {len(revs)} revisions but ledger"
                f" says new_revisions={new_count}"
            )
        change_count = int(batch["change_count"])
        dup = int(batch["duplicate_rows"])
        retractions = int(batch["retractions"])
        if change_count < new_count or dup < 0 or retractions < 0:
            raise PackageError(f"seq {seq}: invalid batch count fields")
        counted_retract = 0
        for rec in revs:
            kind = rec.get("kind")
            cid = rec.get("claim_id")
            rev = rec.get("revision")
            if not isinstance(cid, str) or not cid:
                raise PackageError(f"seq {seq}: bad claim_id in revision")
            if not isinstance(rev, int) or isinstance(rev, bool) or rev <= 0:
                raise PackageError(f"seq {seq}: bad revision number")
            key = (cid, rev)
            if key in self.seen_revisions:
                raise PackageError(
                    f"seq {seq}: duplicate revision {cid} rev {rev} across"
                    " package frames"
                )
            if kind == KIND_RETRACT:
                if set(rec) != set(_RETRACT_REQUIRED):
                    raise PackageError(
                        f"seq {seq}: retract revision has wrong field set"
                    )
                counted_retract += 1
                self.seen_revisions.add(key)
                continue
            if kind != KIND_UPSERT:
                raise PackageError(
                    f"seq {seq}: unknown revision kind {kind!r}"
                )
            if set(rec) != set(_UPSERT_REQUIRED):
                raise PackageError(
                    f"seq {seq}: upsert revision has wrong field set"
                )
            product = (rec["supplier"], rec["item_id"], rec["market"])
            if not all(isinstance(v, str) and v for v in product):
                raise PackageError(f"seq {seq}: bad product key")
            prior = self.claim_products.get(cid)
            if prior is not None and prior != product:
                raise PackageError(
                    f"seq {seq}: claim {cid} product key changed across"
                    f" revisions: {prior} -> {product}"
                )
            self.claim_products[cid] = product
            v_from, v_to = rec["valid_from"], rec["valid_to"]
            if not isinstance(v_from, int) or isinstance(v_from, bool):
                raise PackageError(
                    f"seq {seq}: valid_from must be an integer"
                )
            if v_to is not None and (
                not isinstance(v_to, int) or isinstance(v_to, bool)
                or not v_from < v_to
            ):
                raise PackageError(f"seq {seq}: invalid validity interval")
            if (not isinstance(rec["source_line"], int)
                    or rec["source_line"] <= 0):
                raise PackageError(f"seq {seq}: bad source_line")
            status = rec["business_status"]
            if status == "normalized":
                if not rec["normalized_value"] or rec["error_code"] is not None:
                    raise PackageError(
                        f"seq {seq}: normalized revision needs a value and no"
                        " error code"
                    )
            elif status == "invalid":
                if not rec["error_code"] or rec["normalized_value"] is not None:
                    raise PackageError(
                        f"seq {seq}: invalid revision needs an error code"
                    )
            else:
                raise PackageError(
                    f"seq {seq}: unexpected business_status {status!r}"
                )
            src = rec["source"]
            if not isinstance(src, dict) or not all(
                k in src for k in ("supplier", "item_id", "market")
            ):
                raise PackageError(
                    f"seq {seq}: source business row missing product columns"
                )
            provenance = rec["result_source"]
            if not isinstance(provenance, dict) or set(provenance) != {
                "job_id", "slot", "page_index", "payload_hash",
                "input_digest",
            }:
                raise PackageError(
                    f"seq {seq}: result_source provenance has wrong field set"
                )
            self.seen_revisions.add(key)
        if counted_retract != retractions:
            raise PackageError(
                f"seq {seq}: retraction count {counted_retract} disagrees with"
                f" ledger {retractions}"
            )
        self.chain = identity.chain_step(
            self.chain, identity.batch_unit(batch, revs)
        )
        self.batch_count += 1
        self.revision_count += len(revs)

    def finish(self, footer: dict[str, Any]) -> None:
        if self.batch_count != (
            int(self.manifest["through"]) - int(self.manifest["after"])
        ):
            raise PackageError("frame count does not match declared range")
        if self.chain != self.manifest["through_digest"]:
            raise PackageError(
                "through_digest does not match recomputed frame chain"
            )
        if footer["final_digest"] != self.chain:
            raise PackageError("footer final_digest does not match frame chain")
        if footer["batch_count"] != self.batch_count:
            raise PackageError("footer batch_count mismatch")
        if footer["revision_count"] != self.revision_count:
            raise PackageError("footer revision_count mismatch")
        for key in ("origin_id", "after", "through"):
            if footer[key] != self.manifest[key]:
                raise PackageError(f"footer {key} disagrees with manifest")


def _validate_package_material(contents) -> None:
    """Validate every frame against warehouse semantics (in-memory path)."""
    validator = _FrameValidator(contents.manifest)
    for batch, revs in contents.batches:
        validator.feed(batch, revs)
    validator.finish(contents.footer)


def _expected_digest(rec: dict[str, Any]) -> str:
    # Kept as an explicit named hook so tests can assert that the digest
    # written on acceptance is exactly the canonical revision digest.
    return identity.revision_digest(rec)


# --------------------------------------------------------------------------
# Read-only inspection against a target warehouse
# --------------------------------------------------------------------------

def inspect_package(
    package_path: str, conn: sqlite3.Connection
) -> dict[str, Any]:
    """Read-only verdict: can this package be accepted by ``conn``?

    Never writes anything.  Returns a report whose ``verdict`` is one of the
    public ``VERDICT_*`` constants plus the evidence behind the decision.
    """
    try:
        contents = read_package(package_path)
    except PackageError as exc:
        return {
            "verdict": VERDICT_CORRUPT,
            "reason": str(exc),
            "package_path": os.path.abspath(package_path),
        }
    manifest = contents.manifest
    if manifest["protocol_version"] != 1:  # pragma: no cover - guarded earlier
        return {
            "verdict": VERDICT_UNKNOWN_PROTOCOL,
            "reason": f"protocol_version {manifest['protocol_version']}",
        }
    try:
        _validate_package_material(contents)
    except PackageError as exc:
        return {
            "verdict": VERDICT_CORRUPT,
            "reason": str(exc),
            "package_path": os.path.abspath(package_path),
        }

    report: dict[str, Any] = {
        "verdict": None,
        "package_path": os.path.abspath(package_path),
        "origin_id": manifest["origin_id"],
        "after": manifest["after"],
        "through": manifest["through"],
        "after_digest": manifest["after_digest"],
        "through_digest": manifest["through_digest"],
        "batch_count": len(contents.batches),
        "revision_count": sum(len(r) for _, r in contents.batches),
        "target": replica_status(conn),
    }

    role = role_of(conn)
    if role is None:
        report["verdict"] = VERDICT_INELIGIBLE_TARGET
        report["reason"] = (
            "target is a plain business warehouse; create a managed replica"
            " with 'warehouse replica init' first"
        )
        return report
    bound = origin_of(conn)
    if bound != manifest["origin_id"]:
        report["verdict"] = VERDICT_ORIGIN_MISMATCH
        report["reason"] = (
            f"target origin {bound!r} != package origin"
            f" {manifest['origin_id']!r}"
        )
        return report

    committed = latest_seq(conn)
    after, through = manifest["after"], manifest["through"]

    # Gap: a package that starts only *after* the target prefix (after >= c)
    # cannot be attached -- its left boundary was never established here.
    # (after == c with content would merely be a zero-overlap append, which
    # still cannot verify the boundary, so it is rejected as a gap too; the
    # usable append path always carries an overlapping, verifiable region or
    # starts at the empty prefix after=0.)
    if after >= committed and not (after == 0 and committed == 0):
        report["verdict"] = VERDICT_MISSING_PREFIX
        report["reason"] = (
            f"package starts at seq {after} but target prefix ends at"
            f" seq {committed}; the left boundary history is missing"
        )
        return report

    # Overlap region (after, min(committed, through)] must be byte-identical.
    overlap_end = min(committed, through)
    if overlap_end > after:
        divergence = _compare_overlap(conn, contents, after, overlap_end)
        if divergence is not None:
            report["verdict"] = VERDICT_DIVERGED
            report["reason"] = divergence
            return report

    # Whole package already contained, identical history: pure replay.
    if through <= committed:
        report["verdict"] = VERDICT_REPLAY
        report["reason"] = (
            f"package range ({after},{through}] is already present with"
            " identical content"
        )
        return report

    report["verdict"] = VERDICT_APPENDABLE
    report["reason"] = (
        f"overlap ({after},{overlap_end}] identical; suffix"
        f" ({overlap_end},{through}] would be appended"
    )
    return report


def _compare_overlap(
    conn, contents, after: int, overlap_end: int
) -> str | None:
    """Return a human-readable divergence reason, or None when identical."""
    stored_log = {
        int(r["knowledge_seq"]): r["prefix_digest"]
        for r in conn.execute(
            "SELECT knowledge_seq, prefix_digest FROM wh_replication_log"
            " WHERE knowledge_seq > ? AND knowledge_seq <= ?"
            " ORDER BY knowledge_seq",
            (after, overlap_end),
        ).fetchall()
    }
    # Recompute the chain over package frames for the same region, starting
    # from the target's own stored digest at `after`, so a divergence is
    # detected even when both sides share the same final seq.
    start_row = conn.execute(
        "SELECT prefix_digest FROM wh_replication_log WHERE knowledge_seq=?",
        (after,),
    ).fetchone()
    chain = start_row["prefix_digest"] if start_row else EMPTY_PREFIX_DIGEST
    for batch, revs in contents.batches:
        seq = int(batch["knowledge_seq"])
        if seq > overlap_end:
            break
        chain = identity.chain_step(
            chain, identity.batch_unit(batch, revs)
        )
        if stored_log.get(seq) != chain:
            return (
                f"seq {seq}: prefix digest differs (same origin, different"
                " history -- branches cannot be joined)"
            )
        # Field-level proof: same seq/batch_id/normalized value is not enough;
        # compare every stored revision row and batch ledger column.
        stored_batch = conn.execute(
            "SELECT * FROM wh_batches WHERE knowledge_seq=?", (seq,)
        ).fetchone()
        if stored_batch is None or stored_batch["batch_id"] != batch["batch_id"]:
            return f"seq {seq}: batch_id differs in overlap"
        for key in identity.BATCH_FIELDS:
            if stored_batch[key] != batch[key]:
                return (
                    f"seq {seq}: ledger field {key!r} differs in overlap"
                    " (business or source provenance changed)"
                )
        stored_revs = conn.execute(
            "SELECT * FROM wh_claim_revisions WHERE knowledge_seq=?"
            " ORDER BY id ASC",
            (seq,),
        ).fetchall()
        stored_recs = [identity.revision_row_to_rec(r) for r in stored_revs]
        if stored_recs != revs:
            return (
                f"seq {seq}: revision material differs in overlap (original"
                " business fields or service source changed)"
            )
    return None


# --------------------------------------------------------------------------
# Atomic whole-package acceptance
# --------------------------------------------------------------------------

class _ApplyLock:
    """Cross-process advisory lock serializing all receivers of one target.

    Two receivers processing the same or overlapping packages must not both
    inspect an empty warehouse and then race to bind it.  An exclusive
    ``flock`` on a sibling lock file covers the whole verify-and-apply
    critical section; inside it, each receiver re-reads the committed
    position, so a later process always observes the earlier one's result and
    takes the append/replay path instead of replaying a stale decision.
    """

    def __init__(self, db_path: str):
        import fcntl

        self._fcntl = fcntl
        self._path = os.path.abspath(db_path) + ".apply.lock"
        self._fh = None

    def __enter__(self) -> "_ApplyLock":
        self._fh = open(self._path, "a+", encoding="utf-8")
        self._fcntl.flock(self._fh.fileno(), self._fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc) -> None:
        assert self._fh is not None
        self._fcntl.flock(self._fh.fileno(), self._fcntl.LOCK_UN)
        self._fh.close()
        self._fh = None


def apply_package(
    package_path: str,
    conn: sqlite3.Connection,
    *,
    allow_replica_init: bool = True,
    pre_commit_hook=None,
) -> dict[str, Any]:
    """Verify and accept one whole package, or leave the target untouched.

    On an empty plain warehouse the first full-prefix package (after=0)
    binds the origin; failure leaves no binding.  Re-presenting an already
    contained package is a verified replay -- material is re-checked in full,
    never skipped because a batch_id or seq was seen before.

    ``pre_commit_hook``, if given, runs inside the write transaction after
    every new row is staged and immediately before commit (tests use it as a
    deterministic synchronization point).  An exception in the hook rolls the
    whole acceptance back.

    Concurrent receivers serialize on a sibling advisory lock and re-read the
    committed position inside it, so they converge on one complete prefix.
    A *fresh* connection is opened inside that lock: a receiver process may
    have opened its connection before another receiver committed the origin
    binding, and an already-open SQLite connection can keep an older WAL read
    snapshot.  The fresh connection always observes the newest committed
    prefix; the caller's connection is used only to locate the database file.
    """
    db_file = conn.execute("PRAGMA database_list").fetchone()[2]
    # Parse only the manifest first so the binding-wait decision never needs
    # the (potentially huge) batch frames resident.
    manifest = _read_manifest(package_path)

    # A receiver whose package does not start at the empty prefix cannot bind
    # a fresh warehouse itself.  When such a process wins the advisory lock
    # before any full-prefix receiver has run, it must *wait* for the binding
    # rather than fail: lock acquisition order across processes is kernel
    # defined and carries no ordering meaning.  The wait happens inside the
    # flock on a bounded poll, and every recheck opens a fresh connection so a
    # newly committed WAL prefix is always observed.
    deadline = time.monotonic() + _BIND_WAIT_SECONDS
    while True:
        with _ApplyLock(db_file):
            work_conn = hdb.open_warehouse(db_file)
            try:
                role = role_of(work_conn)
                if role is not None or manifest["after"] == 0:
                    return _apply_package_locked_streaming(
                        package_path, work_conn,
                        allow_replica_init=allow_replica_init,
                        pre_commit_hook=pre_commit_hook,
                    )
            finally:
                work_conn.close()
        if time.monotonic() >= deadline:
            raise WarehouseError(
                "target is a plain business warehouse and no full-prefix"
                " (after=0) package established the origin binding within"
                f" {_BIND_WAIT_SECONDS:.0f}s; only a full-prefix package can"
                " initialize a managed replica"
            )
        time.sleep(_BIND_WAIT_POLL_SECONDS)


def _read_manifest(package_path: str) -> dict[str, Any]:
    from .package import TAG_MANIFEST, iter_frames
    for tag, obj in iter_frames(package_path):
        if tag == TAG_MANIFEST:
            return obj
    raise PackageError("package has no manifest")


def _apply_package_locked_streaming(
    package_path: str,
    conn: sqlite3.Connection,
    *,
    allow_replica_init: bool,
    pre_commit_hook,
) -> dict[str, Any]:
    """Stream frames through validation and one write transaction.

    Pass 1 (before any write) streams every frame through
    :class:`_FrameValidator`, so a truncated or otherwise defective tail
    rejects the whole package before the target changes.  Pass 2 re-streams
    the frames inside a single ``BEGIN IMMEDIATE`` transaction, inserting one
    batch at a time.  Only one frame's payload is resident at any moment.
    """
    from .package import TAG_BATCH, TAG_FOOTER, TAG_MANIFEST, iter_frames

    # ---- Pass 1: full structural + semantic validation, no writes ----
    manifest: dict[str, Any] | None = None
    footer: dict[str, Any] | None = None
    validator: _FrameValidator | None = None
    frame_tags: list[bytes] = []
    for tag, obj in iter_frames(package_path):
        frame_tags.append(tag)
        if tag == TAG_MANIFEST:
            if manifest is not None:
                raise PackageError("duplicate manifest frame")
            manifest = obj
            if manifest.get("protocol_version") != 1:
                raise PackageError(
                    f"unknown protocol_version"
                    f" {manifest.get('protocol_version')!r}"
                )
            validator = _FrameValidator(manifest)
        elif tag == TAG_BATCH:
            if validator is None:
                raise PackageError("batch frame before manifest")
            validator.feed(obj["batch"], obj["revisions"])
        else:
            footer = obj
    if manifest is None or footer is None or validator is None:
        raise PackageError("package missing manifest or footer")
    if TAG_FOOTER in frame_tags[:-1]:
        raise PackageError("frames appear after footer")
    validator.finish(footer)

    # Deterministic crash point A: validation done, nothing written yet.
    _test_kill(_KILL_APPLY_BEFORE_TXN)

    role = role_of(conn)
    needs_binding = False
    if role is None:
        if not allow_replica_init or manifest["after"] != 0:
            raise WarehouseError(
                "target is a plain business warehouse; only a full-prefix"
                " package (after=0) can initialize a managed replica"
            )
        origin.assert_replica_binding_allowed(conn, manifest["origin_id"])
        needs_binding = True
    elif origin_of(conn) != manifest["origin_id"]:
        raise WarehouseError(
            f"origin mismatch: target {origin_of(conn)!r} package"
            f" {manifest['origin_id']!r}"
        )

    # ---- Pass 2: atomic, streaming material application ----
    conn.execute("BEGIN IMMEDIATE")
    try:
        if needs_binding:
            origin._bind_replica_meta(conn, manifest["origin_id"])
        committed = latest_seq(conn)
        after, through = int(manifest["after"]), int(manifest["through"])
        if after >= committed and not (after == 0 and committed == 0):
            raise WarehouseError(
                f"missing prefix: package starts at {after}, target ends at"
                f" {committed} -- the left boundary history is not present"
            )
        boundary = (
            EMPTY_PREFIX_DIGEST if after == 0
            else conn.execute(
                "SELECT prefix_digest FROM wh_replication_log"
                " WHERE knowledge_seq=?", (after,),
            ).fetchone()["prefix_digest"]
        )
        if boundary != manifest["after_digest"]:
            raise WarehouseError(
                f"left boundary digest at seq {after} disagrees with the"
                " package: histories with the same origin cannot be joined"
            )
        chain = boundary
        applied_batches = 0
        applied_revisions = 0
        replayed_batches = 0
        for tag, obj in iter_frames(package_path):
            if tag != TAG_BATCH:
                continue
            batch, revs = obj["batch"], obj["revisions"]
            seq = int(batch["knowledge_seq"])
            chain = identity.chain_step(
                chain, identity.batch_unit(batch, revs)
            )
            if seq <= committed:
                stored_log = conn.execute(
                    "SELECT prefix_digest FROM wh_replication_log"
                    " WHERE knowledge_seq=?", (seq,),
                ).fetchone()
                if stored_log is None or stored_log["prefix_digest"] != chain:
                    raise WarehouseError(
                        f"seq {seq}: diverged history inside overlap"
                        " (same origin, different content)"
                    )
                _assert_stored_material_equal(conn, seq, batch, revs)
                replayed_batches += 1
                continue
            if seq != committed + applied_batches + 1:
                raise WarehouseError(
                    f"seq {seq}: non-contiguous append after committed"
                    f" seq {committed}"
                )
            _insert_batch(conn, seq, batch, revs, chain)
            applied_batches += 1
            applied_revisions += len(revs)
        if chain != manifest["through_digest"]:
            raise WarehouseError(
                "through_digest mismatch after material application"
            )
        if footer["final_digest"] != chain:
            raise WarehouseError("package footer digest mismatch")

        _test_kill(_KILL_APPLY_STAGED)
        if pre_commit_hook is not None and applied_batches > 0:
            pre_commit_hook()
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "event": "package_apply",
        "origin_id": manifest["origin_id"],
        "after": after,
        "through": through,
        "committed_seq": latest_seq(conn),
        "batches_applied": applied_batches,
        "revisions_applied": applied_revisions,
        "batches_replayed": replayed_batches,
        "replayed": applied_batches == 0,
        "prefix_digest": chain,
    }


_BIND_WAIT_SECONDS = 30.0
_BIND_WAIT_POLL_SECONDS = 0.05


def _assert_stored_material_equal(
    conn, seq: int, batch: dict[str, Any], revs: list[dict[str, Any]]
) -> None:
    stored = conn.execute(
        "SELECT * FROM wh_batches WHERE knowledge_seq=?", (seq,)
    ).fetchone()
    if stored is None:
        raise WarehouseError(f"seq {seq}: batch missing during overlap check")
    for key in identity.BATCH_FIELDS:
        if stored[key] != batch[key]:
            raise WarehouseError(
                f"seq {seq}: ledger field {key!r} differs in overlap"
            )
    stored_revs = [
        identity.revision_row_to_rec(r)
        for r in conn.execute(
            "SELECT * FROM wh_claim_revisions WHERE knowledge_seq=?"
            " ORDER BY id ASC",
            (seq,),
        ).fetchall()
    ]
    if stored_revs != revs:
        raise WarehouseError(
            f"seq {seq}: revision material differs in overlap"
        )


def _insert_batch(
    conn,
    seq: int,
    batch: dict[str, Any],
    revs: list[dict[str, Any]],
    prefix_digest: str,
) -> None:
    conn.execute(
        "INSERT INTO wh_batches(knowledge_seq, batch_id, status,"
        " change_digest, evidence_digest, source_db_path, source_db_digest,"
        " change_count, new_revisions, duplicate_rows, retractions,"
        " detail, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            seq,
            batch["batch_id"],
            "admitted",
            batch["change_digest"],
            batch["evidence_digest"],
            # Stored only as provenance text; never reopened on the replica.
            batch["source_db_path"],
            batch["source_db_digest"],
            int(batch["change_count"]),
            int(batch["new_revisions"]),
            int(batch["duplicate_rows"]),
            int(batch["retractions"]),
            json.dumps({"replicated": True}, sort_keys=True),
            time.time(),
        ),
    )
    for rec in revs:
        if rec["kind"] == KIND_RETRACT:
            product = _product_of_existing(conn, rec["claim_id"])
            conn.execute(
                "INSERT INTO wh_claim_revisions(claim_id, revision, kind,"
                " knowledge_seq, batch_id, supplier, item_id, market,"
                " valid_from, valid_to, source_line, source_raw,"
                " business_status, normalized_value, error_code,"
                " result_source, content_digest)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    rec["claim_id"], rec["revision"], KIND_RETRACT, seq,
                    batch["batch_id"], product[0], product[1], product[2],
                    None, None, None, None, None, None, None, None,
                    identity.revision_digest(rec),
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
                batch["batch_id"], rec["supplier"], rec["item_id"],
                rec["market"], rec["valid_from"], rec["valid_to"],
                rec["source_line"],
                json.dumps(rec["source"], ensure_ascii=False, sort_keys=True),
                rec["business_status"], rec["normalized_value"],
                rec["error_code"],
                json.dumps(rec["result_source"], ensure_ascii=False,
                           sort_keys=True),
                identity.revision_digest(rec),
            ),
        )
    # Recompute claim heads from the material, exactly as admission does.
    for cid in {r["claim_id"] for r in revs}:
        head = conn.execute(
            "SELECT revision, kind, supplier, item_id, market"
            " FROM wh_claim_revisions WHERE claim_id=?"
            " ORDER BY revision DESC, id DESC LIMIT 1",
            (cid,),
        ).fetchone()
        conn.execute(
            "INSERT INTO wh_claims(claim_id, supplier, item_id, market,"
            " head_revision, head_kind, head_seq)"
            " VALUES(?,?,?,?,?,?,?) ON CONFLICT(claim_id) DO UPDATE SET"
            " head_revision=excluded.head_revision,"
            " head_kind=excluded.head_kind, head_seq=excluded.head_seq",
            (
                cid, head["supplier"], head["item_id"], head["market"],
                int(head["revision"]), head["kind"], seq,
            ),
        )
    conn.execute(
        "INSERT INTO wh_replication_log(knowledge_seq, prefix_digest,"
        " log_kind, recorded_at) VALUES(?, ?, ?, ?)",
        (seq, prefix_digest, "replica-apply", time.time()),
    )


def _product_of_existing(conn, claim_id: str) -> tuple[str, str, str]:
    row = conn.execute(
        "SELECT supplier, item_id, market FROM wh_claim_revisions"
        " WHERE claim_id=? LIMIT 1",
        (claim_id,),
    ).fetchone()
    if row is None:
        raise PackageError(
            f"retraction of unknown claim {claim_id!r} in package"
        )
    return row["supplier"], row["item_id"], row["market"]
