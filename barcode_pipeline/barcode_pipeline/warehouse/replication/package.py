"""Offline replication package wire format (version 1).

A package is *not* a SQLite database copy.  It is a self-describing framed
byte stream that tests can read and re-construct independently.  All integer
lengths are unsigned 64-bit big-endian.  Every frame is separately checksummed
and the whole stream is authenticated end-to-end by the prefix-digest chain,
so truncation, byte flips, dropped frames and wrong digests are all detectable.

Layout (single file, written to a ``.tmp`` name and atomically renamed)::

    MAGIC                         8 bytes: b"BCPWHPKG"
    FRAME header                  ("H", JSON manifest frame)
    1..n FRAME batch+revisions    ("B" frames, seq ascending)
    FRAME footer                  ("F", digest chain + counts)

Each frame::

    frame_tag     1 byte        ASCII "H" / "B" / "F"
    payload_len   8 bytes       uint64 BE
    payload       payload_len   canonical JSON bytes (see below)
    payload_sha   32 bytes      raw SHA-256 of the payload bytes

Manifest payload (frame H) keys, exactly:
    protocol_version, origin_id, prefix_scheme, after, through,
    after_digest, through_digest, created_at_utc

Batch frame (B) payload keys, exactly:
    batch     -- admitted wh_batches row, identity fields only (see identity)
    revisions -- revision records first admitted by this batch, in
                 wh_claim_revisions.id order; empty for duplicate-only batches

Footer frame (F) payload keys, exactly:
    origin_id, after, through, final_digest, batch_count, revision_count

A reader must reject unknown ``protocol_version`` values, out-of-order or
duplicated frames, a missing/duplicated manifest or footer, a frame whose
hash does not match, trailing garbage, or a footer whose ``final_digest``
does not equal the chain recomputed from the frames.
"""

from __future__ import annotations

import hashlib
import json
import os
import struct
from dataclasses import dataclass
from typing import Any, Iterator

from ..hdb import WarehouseError
from . import identity

MAGIC = b"BCPWHPKG"
PROTOCOL_VERSION = 1
TAG_MANIFEST = b"H"
TAG_BATCH = b"B"
TAG_FOOTER = b"F"
_SUFFIX_TMP = ".tmp"


class PackageError(WarehouseError):
    """A package is malformed, truncated, tampered with or unsupported."""


# --------------------------------------------------------------------------
# Writing
# --------------------------------------------------------------------------

def _frame(tag: bytes, payload: bytes) -> bytes:
    if len(tag) != 1:
        raise ValueError("frame tag must be one byte")
    return (
        tag
        + struct.pack(">Q", len(payload))
        + payload
        + hashlib.sha256(payload).digest()
    )


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


class PackageWriter:
    """Stream frames to a temporary file; nothing readable until ``finish``.

    The target path is refused up front if it already exists.  ``finish``
    atomically renames the staging file into place; abandoning the writer
    (or a process kill) leaves only a ``<target>.tmp`` staging file, never a
    success-shaped package.
    """

    def __init__(self, target_path: str, manifest: dict[str, Any]) -> None:
        if os.path.exists(target_path):
            raise FileExistsError(f"package already exists: {target_path}")
        self.target_path = target_path
        self.tmp_path = target_path + _SUFFIX_TMP
        if os.path.exists(self.tmp_path):
            os.remove(self.tmp_path)
        parent = os.path.dirname(os.path.abspath(target_path))
        os.makedirs(parent, exist_ok=True)
        self._fh = open(self.tmp_path, "wb")
        self._fh.write(MAGIC)
        self._closed = False
        self._finished = False
        self.batch_count = 0
        self.revision_count = 0
        required = {
            "protocol_version", "origin_id", "prefix_scheme", "after",
            "through", "after_digest", "through_digest", "created_at_utc",
        }
        if set(manifest) != required:
            raise PackageError(
                f"manifest keys mismatch: got {sorted(manifest)}"
            )
        if manifest["protocol_version"] != PROTOCOL_VERSION:
            raise PackageError(
                f"unsupported protocol_version {manifest['protocol_version']}"
            )
        self.manifest = manifest
        self._fh.write(_frame(TAG_MANIFEST, _json_bytes(manifest)))
        # Running prefix chain; starts at the declared left boundary.
        self._chain = manifest["after_digest"]
        self._last_seq = manifest["after"]

    def write_batch(self, batch: dict[str, Any],
                    revisions: list[dict[str, Any]]) -> None:
        if self._closed:
            raise PackageError("writer is closed")
        seq = int(batch["knowledge_seq"])
        if seq != self._last_seq + 1:
            raise PackageError(
                f"non-contiguous batch frame: seq {seq} after"
                f" {self._last_seq}"
            )
        # Recompute the chain from actual frame content; callers later compare
        # it to the boundary digest they declared.
        unit = identity.batch_unit(batch, revisions)
        self._chain = identity.chain_step(self._chain, unit)
        self._fh.write(
            _frame(TAG_BATCH, _json_bytes({
                "batch": {key: batch[key] for key in identity.BATCH_FIELDS},
                "revisions": [
                    identity.revision_digest_payload(rec) for rec in revisions
                ],
            }))
        )
        self._last_seq = seq
        self.batch_count += 1
        self.revision_count += len(revisions)

    def finish(self) -> dict[str, Any]:
        if self._finished:
            raise PackageError("already finished")
        if self._chain != self.manifest["through_digest"]:
            self.abort()
            raise PackageError(
                "through_digest does not match the content actually written"
            )
        if self._last_seq != self.manifest["through"]:
            self.abort()
            raise PackageError("written batches do not reach declared through")
        footer = {
            "origin_id": self.manifest["origin_id"],
            "after": self.manifest["after"],
            "through": self.manifest["through"],
            "final_digest": self._chain,
            "batch_count": self.batch_count,
            "revision_count": self.revision_count,
        }
        self._fh.write(_frame(TAG_FOOTER, _json_bytes(footer)))
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._closed = True
        os.replace(self.tmp_path, self.target_path)
        self._finished = True
        return {
            "path": os.path.abspath(self.target_path),
            **footer,
        }

    def abort(self) -> None:
        if not self._closed:
            self._fh.close()
            self._closed = True
        if os.path.exists(self.tmp_path):
            os.remove(self.tmp_path)


# --------------------------------------------------------------------------
# Reading
# --------------------------------------------------------------------------

@dataclass
class PackageContents:
    manifest: dict[str, Any]
    footer: dict[str, Any]
    # (batch dict, revision record list) in frame order
    batches: list[tuple[dict[str, Any], list[dict[str, Any]]]]


def _read_exact(fh, n: int, what: str) -> bytes:
    data = fh.read(n)
    if len(data) != n:
        raise PackageError(f"truncated package while reading {what}")
    return data


def iter_frames(path: str) -> Iterator[tuple[bytes, bytes]]:
    """Yield ``(tag, payload)`` frames, validating magic and per-frame hash.

    Streams the file; nothing larger than one frame is held in memory.
    """
    if not os.path.exists(path):
        raise PackageError(f"package not found: {path}")
    with open(path, "rb") as fh:
        magic = fh.read(len(MAGIC))
        if magic != MAGIC:
            raise PackageError("not a replication package (bad magic)")
        while True:
            tag = fh.read(1)
            if tag == b"":
                return
            if tag not in (TAG_MANIFEST, TAG_BATCH, TAG_FOOTER):
                raise PackageError(f"unknown frame tag {tag!r}")
            (length,) = struct.unpack(">Q", _read_exact(fh, 8, "frame length"))
            payload = _read_exact(fh, length, "frame payload")
            digest = _read_exact(fh, 32, "frame digest")
            if hashlib.sha256(payload).digest() != digest:
                raise PackageError(f"frame {tag!r} checksum mismatch")
            try:
                obj = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise PackageError(f"frame {tag!r} payload not valid JSON: {exc}")
            yield tag, obj


def read_package(path: str) -> PackageContents:
    """Fully parse and authenticate a package (used by inspect/apply)."""
    manifest: dict[str, Any] | None = None
    footer: dict[str, Any] | None = None
    batches: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
    saw_footer = False
    for tag, obj in iter_frames(path):
        if saw_footer:
            raise PackageError("trailing frames after footer")
        if tag == TAG_MANIFEST:
            if manifest is not None:
                raise PackageError("duplicate manifest frame")
            manifest = obj
        elif tag == TAG_BATCH:
            if manifest is None:
                raise PackageError("batch frame before manifest")
            batches.append((obj["batch"], obj["revisions"]))
        else:
            footer = obj
            saw_footer = True
    if manifest is None:
        raise PackageError("package has no manifest")
    if footer is None:
        raise PackageError("package has no footer (truncated)")
    _validate_structure(manifest, footer, batches)
    return PackageContents(manifest=manifest, footer=footer, batches=batches)


def _validate_structure(
    manifest: dict[str, Any],
    footer: dict[str, Any],
    batches: list[tuple[dict[str, Any], list[dict[str, Any]]]],
) -> None:
    if manifest.get("protocol_version") != PROTOCOL_VERSION:
        raise PackageError(
            f"unknown protocol_version {manifest.get('protocol_version')!r};"
            f" this build supports {PROTOCOL_VERSION}"
        )
    if manifest.get("prefix_scheme") != identity.PREFIX_SCHEME:
        raise PackageError(
            f"unsupported prefix scheme {manifest.get('prefix_scheme')!r}"
        )
    after = manifest["after"]
    through = manifest["through"]
    if not (isinstance(after, int) and isinstance(through, int)
            and 0 <= after <= through):
        raise PackageError("invalid after/through boundaries")
    if len(batches) != through - after:
        raise PackageError(
            f"frame count {len(batches)} does not match range"
            f" ({after},{through}]"
        )
    chain = manifest["after_digest"]
    rev_total = 0
    expected_seq = after
    for batch, revs in batches:
        expected_seq += 1
        if int(batch["knowledge_seq"]) != expected_seq:
            raise PackageError(
                f"batch seq {batch['knowledge_seq']} out of order; expected"
                f" {expected_seq}"
            )
        if set(batch) != set(identity.BATCH_FIELDS):
            raise PackageError("batch frame has unexpected field set")
        for rec in revs:
            if "claim_id" not in rec or "revision" not in rec or "kind" not in rec:
                raise PackageError("revision frame missing identity fields")
        chain = identity.chain_step(
            chain, identity.batch_unit(batch, revs)
        )
        rev_total += len(revs)
    if chain != manifest["through_digest"]:
        raise PackageError(
            "manifest through_digest does not match recomputed frame chain"
        )
    if chain != footer["final_digest"]:
        raise PackageError("footer final_digest does not match frame chain")
    if footer["batch_count"] != len(batches):
        raise PackageError("footer batch_count mismatch")
    if footer["revision_count"] != rev_total:
        raise PackageError("footer revision_count mismatch")
    for key in ("origin_id", "after", "through"):
        if footer[key] != manifest[key]:
            raise PackageError(f"footer {key} disagrees with manifest")
