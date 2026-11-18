"""Offline warehouse replication: origin, packages, replica acceptance.

Every test builds evidence with the *real* local pipeline and mock service
(never by hand-editing tables) and speaks only through the public warehouse
API plus the replication package wire format.  Tampered packages are
constructed by parsing and re-emitting the documented frame layout, which
also proves the format can be read independently of a SQLite copy.
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import struct
import subprocess
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.helpers import BaseCase, read_jsonl  # noqa: E402
from tests.wh_helpers import (  # noqa: E402
    WALKTHROUGH_RULES,
    WALKTHROUGH_SOURCES,
    build_enrichment_db,
    wh_cli,
    write_changes,
    write_jsonl,
)
from barcode_pipeline.warehouse import (  # noqa: E402
    WarehouseError,
    apply_package,
    export_package,
    hdb,
    importer,
    initialize_origin,
    initialize_replica,
    inspect_package,
    lookup,
    query,
    replica_status,
)
from barcode_pipeline.warehouse.replication import identity, package as pkgmod  # noqa: E402
from barcode_pipeline.warehouse.replication import replica as replica_mod  # noqa: E402
from barcode_pipeline.warehouse.replication.package import (  # noqa: E402
    MAGIC,
    TAG_BATCH,
    TAG_FOOTER,
    TAG_MANIFEST,
    iter_frames,
)

ROOT = Path(__file__).resolve().parents[1]
ENTRY = ROOT / "barcode_pipeline" / "__main__.py"


# --------------------------------------------------------------------------
# Independent wire-format helpers (tests read and build packages themselves)
# --------------------------------------------------------------------------

def _frame(tag: bytes, obj: dict) -> bytes:
    payload = json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return tag + struct.pack(">Q", len(payload)) + payload + hashlib.sha256(payload).digest()


def read_raw_frames(path: str) -> list[tuple[bytes, dict]]:
    with open(path, "rb") as fh:
        assert fh.read(len(MAGIC)) == MAGIC
        out = []
        while True:
            tag = fh.read(1)
            if not tag:
                break
            (length,) = struct.unpack(">Q", fh.read(8))
            payload = fh.read(length)
            fh.read(32)
            out.append((tag, json.loads(payload.decode("utf-8"))))
    return out


def write_raw_frames(path: str, frames: list[tuple[bytes, dict]]) -> None:
    with open(path, "wb") as fh:
        fh.write(MAGIC)
        for tag, obj in frames:
            fh.write(_frame(tag, obj))


def rechain(frames: list[tuple[bytes, dict]]) -> list[tuple[bytes, dict]]:
    """Recompute the prefix chain, manifest boundary and footer.

    Produces an internally consistent package (correct checksums and
    self-consistent digests) from possibly altered batch frames -- the exact
    shape of a coherent but different history branch.
    """
    tag, manifest = frames[0]
    chain = manifest["after_digest"]
    batch_total = 0
    rev_total = 0
    out: list[tuple[bytes, dict]] = []
    for t, obj in frames[1:]:
        if t == TAG_BATCH:
            chain = identity.chain_step(
                chain, identity.batch_unit(obj["batch"], obj["revisions"])
            )
            batch_total += 1
            rev_total += len(obj["revisions"])
            out.append((t, obj))
    new_manifest = dict(manifest)
    new_manifest["through_digest"] = chain
    result = [(TAG_MANIFEST, new_manifest)] + out
    result.append((TAG_FOOTER, {
        "origin_id": manifest["origin_id"],
        "after": manifest["after"],
        "through": manifest["through"],
        "final_digest": chain,
        "batch_count": batch_total,
        "revision_count": rev_total,
    }))
    return result


def write_rechained(path: str, frames: list[tuple[bytes, dict]]) -> str:
    write_raw_frames(path, rechain(frames))
    return path


def rebuild_with(path: str, mutate, *, bad_hash: bool = False) -> str:
    """Copy a package, mutate parsed frames, optionally corrupt a hash.

    By default every emitted frame is re-signed with the correct checksum, so
    the package passes *structural* authentication and any rejection must come
    from semantic/overlap comparison (a coherent but different history, which
    is precisely the same-origin fork case).  ``bad_hash=True`` instead keeps
    the mutation unsigned to prove checksum verification itself fires.
    """
    frames = read_raw_frames(path)
    frames = mutate(frames)
    out = path + ".tampered"
    if bad_hash:
        # Write frames then flip a checksum byte directly on disk.
        write_raw_frames(out, frames)
        with open(out, "r+b") as fh:
            fh.seek(-1, os.SEEK_END)
            b = fh.read(1)
            fh.seek(-1, os.SEEK_END)
            fh.write(bytes([b[0] ^ 0xFF]))
        return out
    write_raw_frames(out, frames)
    return out


def build_source_history(workdir: Path, *, origin_first: bool = False):
    """The canonical drill history: 101, 202, duplicate-only, retraction."""
    rules = {
        "X|CN|v1": "101",
        "Y|CN|v1": "202",
    }
    sources = [
        {"barcode": "X", "market": "CN", "rule_version": "v1",
         "supplier": "sup1", "item_id": "K", "fact_ref": "k-x"},
        {"barcode": "Y", "market": "CN", "rule_version": "v1",
         "supplier": "sup1", "item_id": "K", "fact_ref": "k-y"},
    ]
    src = build_enrichment_db(
        workdir / "src", sources, rules, name="drill", batch_size=10
    )
    wh = str(workdir / "source.db")
    conn = hdb.open_warehouse(wh)
    try:
        if origin_first:
            initialize_origin(conn)

        def admit(batch_id, rows):
            p = write_changes(workdir / f"{batch_id}.jsonl", rows)
            return importer.import_batch(conn, src, p, batch_id)

        s1 = admit("b1", [
            {"claim_id": "K", "revision": 1, "source_line": 1,
             "valid_from": 0, "valid_to": 50},
        ])
        s2 = admit("b2", [
            {"claim_id": "K", "revision": 2, "source_line": 2,
             "valid_from": 0, "valid_to": 50},
        ])
        s3 = admit("b3", [
            {"claim_id": "K", "revision": 2, "source_line": 2,
             "valid_from": 0, "valid_to": 50},
        ])
        s4 = admit("b4", [{"claim_id": "K", "revision": 3}])
        assert [s["knowledge_seq"] for s in (s1, s2, s3, s4)] == [1, 2, 3, 4]
    finally:
        conn.close()
    return wh, src


class ReplicationTest(BaseCase):
    # ------------------------------------------------------------------
    # Origin lifecycle
    # ------------------------------------------------------------------
    def test_origin_init_is_idempotent_and_immutable(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            # init over an existing wh1 history back-fills the log
            r1 = initialize_origin(conn)
            self.assertFalse(r1["replayed"])
            origin_id = r1["origin_id"]
            r2 = initialize_origin(conn)
            self.assertTrue(r2["replayed"])
            self.assertEqual(r2["origin_id"], origin_id)
            with self.assertRaises(WarehouseError):
                initialize_origin(conn, origin_id="deadbeef" * 4)
            # no renumbering, no business history change
            seqs = [r["knowledge_seq"] for r in conn.execute(
                "SELECT knowledge_seq FROM wh_batches ORDER BY knowledge_seq")]
            self.assertEqual(seqs, [1, 2, 3, 4])
            log = conn.execute(
                "SELECT knowledge_seq, log_kind FROM wh_replication_log"
                " ORDER BY knowledge_seq"
            ).fetchall()
            self.assertEqual([r["log_kind"] for r in log],
                             ["source-backfill"] * 4)
            # identity survives moving the physical file
        finally:
            conn.close()
        moved = str(self.workdir / "moved-source.db")
        os.rename(wh, moved)
        for suffix in ("-wal", "-shm"):
            p = wh + suffix
            if os.path.exists(p):
                os.rename(p, moved + suffix)
        conn = hdb.open_warehouse(moved)
        try:
            self.assertEqual(replica_status(conn)["origin_id"], origin_id)
            # imports still work after the move and register normally
        finally:
            conn.close()

    def test_new_batches_register_with_batch_in_same_transaction(self) -> None:
        wh, src = build_source_history(self.workdir, origin_first=True)
        conn = hdb.open_warehouse(wh)
        try:
            kinds = [r["log_kind"] for r in conn.execute(
                "SELECT log_kind FROM wh_replication_log ORDER BY knowledge_seq")]
            self.assertEqual(kinds, ["source"] * 4)
            # every batch has exactly one registration and vice versa
            n_batches = conn.execute(
                "SELECT COUNT(*) n FROM wh_batches WHERE status='admitted'"
            ).fetchone()["n"]
            n_log = conn.execute(
                "SELECT COUNT(*) n FROM wh_replication_log"
            ).fetchone()["n"]
            self.assertEqual(n_batches, n_log)
        finally:
            conn.close()

    def test_replica_cannot_become_source_and_binding_failures_leave_no_binding(
        self,
    ) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        pkg_path = str(self.workdir / "p.pkg")
        export_package(wh, 0, 1, pkg_path)
        rep = str(self.workdir / "rep.db")
        rc = hdb.open_warehouse(rep)
        try:
            # a failing first package (wrong range) must not bind anything
            bad = str(self.workdir / "gap.pkg")
            export_package(wh, 2, 3, bad)
            with self.assertRaises(WarehouseError):
                apply_package(bad, rc)
            self.assertIsNone(replica_status(rc)["origin_id"])
            # full-prefix package binds
            apply_package(pkg_path, rc)
            with self.assertRaises(WarehouseError):
                initialize_origin(rc)
        finally:
            rc.close()

    def test_nonempty_business_warehouse_cannot_be_impersonated_as_replica(
        self,
    ) -> None:
        wh, src = build_source_history(self.workdir)
        other = str(self.workdir / "other.db")
        conn = hdb.open_warehouse(other)
        try:
            p = write_changes(self.workdir / "o.jsonl", [
                {"claim_id": "Z", "revision": 1, "source_line": 1,
                 "valid_from": 0, "valid_to": 50},
            ])
            importer.import_batch(conn, src, p, "ob1")
            with self.assertRaises(WarehouseError):
                initialize_replica(conn, "a" * 32)
        finally:
            conn.close()

    def test_direct_import_refused_via_api_and_cli_on_replica(self) -> None:
        wh, src = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        pkg = str(self.workdir / "p.pkg")
        export_package(wh, 0, 4, pkg)
        rep = str(self.workdir / "rep.db")
        rc = hdb.open_warehouse(rep)
        try:
            apply_package(pkg, rc)
            with self.assertRaises(WarehouseError):
                importer.import_batch(
                    rc, src, str(self.workdir / "b1.jsonl"), "sneak"
                )
        finally:
            rc.close()
        proc = wh_cli([
            "warehouse", "import", "--warehouse", rep, "--source", src,
            "--changes", str(self.workdir / "b1.jsonl"), "--batch-id", "sneak2",
        ])
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("replica", proc.stderr)

    # ------------------------------------------------------------------
    # Prefix digest: definition stability and physical-layout independence
    # ------------------------------------------------------------------
    def test_prefix_digest_independent_of_pages_clock_filename_and_paths(
        self,
    ) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
            d1 = conn.execute(
                "SELECT prefix_digest FROM wh_replication_log WHERE knowledge_seq=4"
            ).fetchone()["prefix_digest"]
            # independently recompute from logical rows
            recomputed, _ = identity.compute_prefix_at(conn, 4)
            self.assertEqual(d1, recomputed)
        finally:
            conn.close()
        # physical SQLite backup preserves rows but has different pages; the
        # logical digest must remain identical
        backup = str(self.workdir / "backup.db")
        src_conn = sqlite3_connect(wh)
        try:
            dst = sqlite3_connect(backup)
            try:
                src_conn.backup(dst)
            finally:
                dst.close()
        finally:
            src_conn.close()
        bc = hdb.open_warehouse(backup)
        try:
            self.assertEqual(replica_status(bc)["origin_id"],
                             replica_status(hdb.open_warehouse(wh))["origin_id"])
            recomputed, _ = identity.compute_prefix_at(bc, 4)
            self.assertEqual(recomputed, d1)
        finally:
            bc.close()

    def test_digest_covers_business_fields_and_service_source_not_just_value(
        self,
    ) -> None:
        wh, src = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
            before = conn.execute(
                "SELECT prefix_digest FROM wh_replication_log WHERE knowledge_seq=4"
            ).fetchone()["prefix_digest"]
            # mutate an original business column only; normalized value unchanged
            raw = conn.execute(
                "SELECT source_raw FROM wh_claim_revisions WHERE knowledge_seq=1"
            ).fetchone()["source_raw"]
            obj = json.loads(raw)
            obj["fact_ref"] = "tampered-business-note"
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE wh_claim_revisions SET source_raw=?"
                " WHERE knowledge_seq=1",
                (json.dumps(obj, sort_keys=True),),
            )
            conn.commit()
            after, _ = identity.compute_prefix_at(conn, 4)
            self.assertNotEqual(before, after)
        finally:
            conn.close()

    def test_digest_changes_when_service_provenance_changes_same_value(
        self,
    ) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
            before, _ = identity.compute_prefix_at(conn, 4)
            rs = conn.execute(
                "SELECT result_source FROM wh_claim_revisions WHERE knowledge_seq=2"
            ).fetchone()["result_source"]
            obj = json.loads(rs)
            obj["payload_hash"] = "0" * 64
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE wh_claim_revisions SET result_source=?"
                " WHERE knowledge_seq=2",
                (json.dumps(obj, sort_keys=True),),
            )
            conn.commit()
            after, _ = identity.compute_prefix_at(conn, 4)
            self.assertNotEqual(before, after)
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Package wire format: self-description and tamper detection
    # ------------------------------------------------------------------
    def test_package_is_self_describing_and_not_a_sqlite_file(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        path = str(self.workdir / "p.pkg")
        report = export_package(wh, 1, 3, path)
        with open(path, "rb") as fh:
            self.assertEqual(fh.read(len(MAGIC)), MAGIC)
            self.assertNotEqual(fh.read(15), b"SQLite format 3")
        frames = read_raw_frames(path)
        tags = [t for t, _ in frames]
        self.assertEqual(tags[0], TAG_MANIFEST)
        self.assertEqual(tags[-1], TAG_FOOTER)
        self.assertEqual(tags.count(TAG_BATCH), 2)
        manifest = frames[0][1]
        self.assertEqual(manifest["protocol_version"], 1)
        self.assertEqual(manifest["after"], 1)
        self.assertEqual(manifest["through"], 3)
        self.assertIn("after_digest", manifest)
        self.assertIn("through_digest", manifest)
        # duplicate-only batch (seq 3) travels even with zero revisions
        b3 = next(o for t, o in frames if t == TAG_BATCH
                  and o["batch"]["knowledge_seq"] == 3)
        self.assertEqual(b3["batch"]["new_revisions"], 0)
        self.assertEqual(b3["revisions"], [])
        self.assertEqual(report["batch_count"], 2)

    def test_export_refuses_existing_target_and_leaves_no_official_file(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        target = str(self.workdir / "p.pkg")
        export_package(wh, 0, 1, target)
        with self.assertRaises(FileExistsError):
            export_package(wh, 0, 1, target)
        # a failed export leaves only a .tmp staging file
        bad_target = str(self.workdir / "bad.pkg")
        with self.assertRaises(WarehouseError):
            export_package(wh, 0, 99, bad_target)
        self.assertFalse(os.path.exists(bad_target))
        self.assertFalse(os.path.exists(bad_target + ".tmp"))

    def test_unknown_protocol_version_rejected(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        path = str(self.workdir / "p.pkg")
        export_package(wh, 0, 2, path)

        def bump(frames):
            tag, manifest = frames[0]
            manifest["protocol_version"] = 99
            return [(tag, manifest)] + frames[1:]

        tampered = rebuild_with(path, bump)
        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            verdict = inspect_package(tampered, rep)["verdict"]
            self.assertEqual(verdict, replica_mod.VERDICT_CORRUPT)
            with self.assertRaises(pkgmod.PackageError):
                apply_package(tampered, rep)
        finally:
            rep.close()

    def test_truncated_and_checksum_corrupt_packages_rejected(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        path = str(self.workdir / "p.pkg")
        export_package(wh, 0, 2, path)
        # truncation: drop the footer frame
        frames = read_raw_frames(path)
        truncated = path + ".trunc"
        write_raw_frames(truncated, frames[:-1])
        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            self.assertEqual(
                inspect_package(truncated, rep)["verdict"],
                replica_mod.VERDICT_CORRUPT,
            )
            # checksum corruption
            corrupt = rebuild_with(path, lambda fs: fs, bad_hash=True)
            self.assertEqual(
                inspect_package(corrupt, rep)["verdict"],
                replica_mod.VERDICT_CORRUPT,
            )
        finally:
            rep.close()

    def test_missing_zero_new_batch_and_extra_frame_rejected(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        path = str(self.workdir / "p.pkg")
        export_package(wh, 0, 4, path)
        frames = read_raw_frames(path)
        # drop the seq-3 duplicate-only batch but keep the old footer
        dropped = [f for f in frames
                   if not (f[0] == TAG_BATCH
                           and f[1]["batch"]["knowledge_seq"] == 3)]
        missing = path + ".missing"
        write_raw_frames(missing, dropped)
        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            self.assertEqual(
                inspect_package(missing, rep)["verdict"],
                replica_mod.VERDICT_CORRUPT,
            )
            with self.assertRaises(pkgmod.PackageError):
                apply_package(missing, rep)
        finally:
            rep.close()

    # ------------------------------------------------------------------
    # Inspection verdicts
    # ------------------------------------------------------------------
    def test_inspect_reports_all_verdict_classes(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
            origin_id = replica_status(conn)["origin_id"]
        finally:
            conn.close()
        p02 = str(self.workdir / "p02.pkg")
        p14 = str(self.workdir / "p14.pkg")
        p24 = str(self.workdir / "p24.pkg")
        export_package(wh, 0, 2, p02)
        export_package(wh, 1, 4, p14)
        export_package(wh, 2, 4, p24)
        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            # plain target
            self.assertEqual(
                inspect_package(p02, rep)["verdict"],
                replica_mod.VERDICT_INELIGIBLE_TARGET,
            )
            apply_package(p02, rep)
            # gap
            self.assertEqual(
                inspect_package(p24, rep)["verdict"],
                replica_mod.VERDICT_MISSING_PREFIX,
            )
            # appendable with overlap
            verdict = inspect_package(p14, rep)
            self.assertEqual(verdict["verdict"], replica_mod.VERDICT_APPENDABLE)
            apply_package(p14, rep)
            # replay
            self.assertEqual(
                inspect_package(p02, rep)["verdict"],
                replica_mod.VERDICT_REPLAY,
            )
        finally:
            rep.close()
        # origin mismatch: a second source warehouse with its own origin,
        # built in a separate directory so its enrichment DB is genuinely new
        wh2, _ = build_source_history(self.workdir / "second")
        c2 = hdb.open_warehouse(wh2)
        try:
            initialize_origin(c2, origin_id="f" * 32)
        finally:
            c2.close()
        foreign = str(self.workdir / "foreign.pkg")
        export_package(wh2, 0, 2, foreign)
        rep2 = hdb.open_warehouse(str(self.workdir / "rep2.db"))
        try:
            initialize_replica(rep2, origin_id)
            verdict = inspect_package(foreign, rep2)
            self.assertEqual(
                verdict["verdict"], replica_mod.VERDICT_ORIGIN_MISMATCH
            )
            with self.assertRaises(WarehouseError):
                apply_package(foreign, rep2)
            # target still empty after the rejection
            self.assertEqual(hdb.latest_seq(rep2), 0)
        finally:
            rep2.close()

    def test_overlap_divergence_business_column_same_normalized_value(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        p02 = str(self.workdir / "p02.pkg")
        export_package(wh, 0, 2, p02)
        # tamper seq-1 business row inside the package, keeping the value;
        # re-chain so the package is internally coherent (a real fork), which
        # forces the rejection to come from overlap comparison, not checksum.
        frames = read_raw_frames(p02)

        def mutate(fs):
            for tag, obj in fs:
                if tag == TAG_BATCH and obj["batch"]["knowledge_seq"] == 1:
                    obj["revisions"][0]["source"]["fact_ref"] = "changed"
            return fs

        tampered = write_rechained(p02 + ".tampered", mutate(frames))
        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            apply_package(p02, rep)  # genuine history first
            verdict = inspect_package(tampered, rep)
            self.assertEqual(verdict["verdict"],
                             replica_mod.VERDICT_DIVERGED)
            self.assertIn("differs", verdict["reason"])
            with self.assertRaises(WarehouseError):
                apply_package(tampered, rep)
            # target unchanged
            self.assertEqual(hdb.latest_seq(rep), 2)
        finally:
            rep.close()

    def test_overlap_divergence_service_source_same_normalized_value(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        p02 = str(self.workdir / "p02.pkg")
        export_package(wh, 0, 2, p02)
        frames = read_raw_frames(p02)

        def mutate(fs):
            for tag, obj in fs:
                if tag == TAG_BATCH and obj["batch"]["knowledge_seq"] == 2:
                    obj["revisions"][0]["result_source"]["page_index"] = 999
            return fs

        tampered = write_rechained(p02 + ".tampered", mutate(read_raw_frames(p02)))
        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            apply_package(p02, rep)
            self.assertEqual(
                inspect_package(tampered, rep)["verdict"],
                replica_mod.VERDICT_DIVERGED,
            )
            with self.assertRaises(WarehouseError):
                apply_package(tampered, rep)
        finally:
            rep.close()

    # ------------------------------------------------------------------
    # Acceptance semantics and warehouse-rule validation
    # ------------------------------------------------------------------
    def test_acceptance_keeps_seq_batch_id_and_zero_new_batch(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        p02 = str(self.workdir / "p02.pkg")
        p14 = str(self.workdir / "p14.pkg")
        export_package(wh, 0, 2, p02)
        export_package(wh, 1, 4, p14)
        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            apply_package(p02, rep)
            apply_package(p14, rep)
            ledger = hdb.inspect_warehouse(rep)
            self.assertEqual(len(ledger["batches"]), 4)
            self.assertEqual(
                [b["knowledge_seq"] for b in ledger["batches"]], [1, 2, 3, 4])
            self.assertEqual(
                [b["batch_id"] for b in ledger["batches"]],
                ["b1", "b2", "b3", "b4"],
            )
            b3 = next(b for b in ledger["batches"] if b["knowledge_seq"] == 3)
            self.assertEqual(b3["new_revisions"], 0)
            self.assertEqual(b3["duplicate_rows"], 1)
            # no re-imported new seq: revision count is 3 (rev1, rev2, retract)
            self.assertEqual(ledger["revision_count"], 3)
            # point answers of the drill
            self.assertEqual(
                lookup(rep, "sup1", "K", "CN", 20, seq=1)["normalized_value"],
                "101",
            )
            self.assertEqual(
                lookup(rep, "sup1", "K", "CN", 20, seq=2)["normalized_value"],
                "202",
            )
            self.assertEqual(
                lookup(rep, "sup1", "K", "CN", 20, seq=4)["state"], "missing",
            )
        finally:
            rep.close()

    def test_bad_interval_and_provenance_material_rejected(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        p01 = str(self.workdir / "p01.pkg")
        export_package(wh, 0, 1, p01)

        def bad_interval(fs):
            for tag, obj in fs:
                if tag == TAG_BATCH:
                    obj["revisions"][0]["valid_to"] = 0  # from=0, to=0
            return fs

        def bad_provenance(fs):
            for tag, obj in fs:
                if tag == TAG_BATCH:
                    del obj["revisions"][0]["result_source"]["job_id"]
            return fs

        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            for mutate in (bad_interval, bad_provenance):
                tampered = rebuild_with(p01, mutate)
                self.assertEqual(
                    inspect_package(tampered, rep)["verdict"],
                    replica_mod.VERDICT_CORRUPT,
                )
                with self.assertRaises(pkgmod.PackageError):
                    apply_package(tampered, rep)
            self.assertEqual(hdb.latest_seq(rep), 0)
            self.assertIsNone(replica_status(rep)["origin_id"])
        finally:
            rep.close()

    def test_replay_rechecks_full_material_not_just_batch_id(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        p04 = str(self.workdir / "p04.pkg")
        export_package(wh, 0, 4, p04)
        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            apply_package(p04, rep)
            frames = read_raw_frames(p04)

            def mutate(fs):
                for tag, obj in fs:
                    if tag == TAG_BATCH and obj["batch"]["knowledge_seq"] == 1:
                        obj["revisions"][0]["valid_to"] = 49
                return fs

            tampered = rebuild_with(p04, mutate)
            with self.assertRaises(WarehouseError):
                apply_package(tampered, rep)
            # nothing changed after the rejected replay
            self.assertEqual(hdb.latest_seq(rep), 4)
        finally:
            rep.close()

    # ------------------------------------------------------------------
    # Same-origin fork: SQLite backup then divergent writes
    # ------------------------------------------------------------------
    def test_same_origin_fork_at_same_seq_cannot_join_or_replay(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        # build the fork at seq 2 via a consistent SQLite backup
        fork = str(self.workdir / "fork.db")
        s = sqlite3_connect(wh)
        try:
            d = sqlite3_connect(fork)
            try:
                s.backup(d)
            finally:
                d.close()
        finally:
            s.close()
        # original branch: seq 3 stays the duplicate-only batch; fork branch:
        # seq 3 writes a different correction.  Hand-craft the fork's seq-3
        # package frame by reusing seq-2 material with a new revision record.
        p02 = str(self.workdir / "p02.pkg")
        export_package(wh, 0, 2, p02)
        # package from the genuine source covering seq 3 (duplicate-only)
        p03 = str(self.workdir / "p03.pkg")
        export_package(wh, 0, 3, p03)
        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            apply_package(p02, rep)
            # commit the genuine branch through seq 3 first
            apply_package(p03, rep)
            self.assertEqual(hdb.latest_seq(rep), 3)
        finally:
            rep.close()
        # construct fork package: same origin, same seq 3, different content
        frames = read_raw_frames(p03)
        tag, manifest = frames[0]
        fork_frames = [(tag, dict(manifest))]
        # keep seq 1-2 frames, replace seq 3 batch frame
        for t, obj in frames[1:]:
            if t == TAG_BATCH and obj["batch"]["knowledge_seq"] == 3:
                rev2 = next(
                    o["revisions"][0] for tt, o in frames
                    if tt == TAG_BATCH and o["batch"]["knowledge_seq"] == 2
                )
                forked_rev = dict(rev2)
                forked_rev["revision"] = 3
                forked_rev["valid_from"] = 5
                forked_rev["valid_to"] = 45
                forked_batch = dict(obj["batch"])
                forked_batch["knowledge_seq"] = 3
                forked_batch["batch_id"] = "fork-b3"
                forked_batch["new_revisions"] = 1
                forked_batch["duplicate_rows"] = 0
                forked_batch["retractions"] = 0
                fork_frames.append((TAG_BATCH, {
                    "batch": forked_batch, "revisions": [forked_rev],
                }))
            elif t == TAG_FOOTER:
                continue
            else:
                fork_frames.append((t, obj))
        # re-chain so the fork is an internally coherent package; its prefix
        # digest at seq 3 still differs from the genuine branch by content.
        fork_path = write_rechained(
            str(self.workdir / "fork.pkg"), fork_frames
        )
        rep = hdb.open_warehouse(str(self.workdir / "rep.db"))
        try:
            # same origin, same terminal seq 3: only the prefix digest can tell
            # the histories apart -- this must be a divergence, never a replay
            verdict = inspect_package(fork_path, rep)
            self.assertEqual(verdict["verdict"],
                             replica_mod.VERDICT_DIVERGED)
            self.assertIn("seq 3", verdict["reason"])
            with self.assertRaises(WarehouseError):
                apply_package(fork_path, rep)
            # the replica is untouched and the genuine branch still replays
            self.assertEqual(hdb.latest_seq(rep), 3)
            self.assertEqual(
                inspect_package(p03, rep)["verdict"],
                replica_mod.VERDICT_REPLAY,
            )
        finally:
            rep.close()

    # ------------------------------------------------------------------
    # Snapshot consistency under concurrent export/import
    # ------------------------------------------------------------------
    def test_concurrent_export_pins_one_read_view(self) -> None:
        wh, src = build_source_history(self.workdir, origin_first=True)
        # An in-process pipe pair rendezvous: no blocking opens, no sleeps.
        gate_rfd, gate_wfd = os.pipe()
        sentinel = str(self.workdir / "export-sentinel")
        exported = {}

        def runner():
            def hook(after, through):
                with open(sentinel, "w", encoding="utf-8") as fh:
                    fh.write("pinned")
                os.read(gate_rfd, 1)

            rep = export_package(
                wh, 0, "latest", str(self.workdir / "snap.pkg"), gate=hook
            )
            exported.update(rep)

        import threading

        t = threading.Thread(target=runner)
        t.start()
        deadline = time.time() + 15
        while not os.path.exists(sentinel):
            if not t.is_alive():
                self.fail("export thread exited before reaching the gate")
            if time.time() > deadline:
                self.fail("exporter never pinned its snapshot")
            time.sleep(0.02)
        # Snapshot pinned; admit a concurrent batch while the export waits.
        conn = hdb.open_warehouse(wh)
        try:
            p = write_changes(self.workdir / "late.jsonl", [
                {"claim_id": "K", "revision": 4, "source_line": 1,
                 "valid_from": 0, "valid_to": 10},
            ])
            importer.import_batch(conn, src, p, "late-batch")
            self.assertEqual(hdb.latest_seq(conn), 5)
        finally:
            conn.close()
        os.write(gate_wfd, b"x")
        os.close(gate_wfd)
        os.close(gate_rfd)
        t.join(timeout=30)
        self.assertFalse(t.is_alive())
        # the package claims the range fixed before the late batch landed
        self.assertEqual(exported["through"], 4)
        self.assertEqual(exported["batch_count"], 4)
        contents = pkgmod.read_package(str(self.workdir / "snap.pkg"))
        self.assertEqual(max(b["knowledge_seq"] for b, _ in contents.batches), 4)

    # ------------------------------------------------------------------
    # Concurrency and crash recovery on the receiver
    # ------------------------------------------------------------------
    def test_concurrent_receivers_serialize_to_one_prefix(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        p02 = str(self.workdir / "p02.pkg")
        p14 = str(self.workdir / "p14.pkg")
        export_package(wh, 0, 2, p02)
        export_package(wh, 1, 4, p14)
        rep = str(self.workdir / "rep.db")
        env = dict(os.environ)
        procs = [
            subprocess.Popen(
                [sys.executable, "-I", str(ENTRY), "warehouse", "package",
                 "apply", "--warehouse", rep, "--package", pkg],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            for pkg in (p02, p14, p02, p14)  # overlapping, repeated
        ]
        outs = []
        for p in procs:
            out, err = p.communicate(timeout=60)
            self.assertEqual(p.returncode, 0, err)
            outs.append(json.loads(out))
        rc = hdb.open_warehouse(rep)
        try:
            self.assertEqual(hdb.latest_seq(rc), 4)
            self.assertEqual(
                len(hdb.inspect_warehouse(rc)["batches"]), 4)
            # prefix positions form one consistent chain, no double seq
            seqs = [r["knowledge_seq"] for r in rc.execute(
                "SELECT knowledge_seq FROM wh_replication_log ORDER BY knowledge_seq")]
            self.assertEqual(seqs, [1, 2, 3, 4])
        finally:
            rc.close()

    def test_sigkill_mid_apply_then_new_process_recovers(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        p04 = str(self.workdir / "p04.pkg")
        export_package(wh, 0, 4, p04)
        rep = str(self.workdir / "rep.db")
        env = dict(os.environ, BARCODE_KILL_AFTER="wh_replica_staged")
        proc = subprocess.Popen(
            [sys.executable, "-I", str(ENTRY), "warehouse", "package",
             "apply", "--warehouse", rep, "--package", p04],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        proc.wait(timeout=30)
        self.assertEqual(proc.returncode, -signal.SIGKILL)
        rc = hdb.open_warehouse(rep)
        try:
            # old prefix (empty) untouched, no binding leftovers
            self.assertEqual(hdb.latest_seq(rc), 0)
        finally:
            rc.close()
        # new process: full package applies cleanly
        again = wh_cli([
            "warehouse", "package", "apply", "--warehouse", rep,
            "--package", p04,
        ])
        self.assertEqual(again.returncode, 0, again.stderr)
        rc = hdb.open_warehouse(rep)
        try:
            self.assertEqual(hdb.latest_seq(rc), 4)
        finally:
            rc.close()
        # resend after commit: verified replay, position unchanged
        replay = wh_cli([
            "warehouse", "package", "apply", "--warehouse", rep,
            "--package", p04,
        ])
        self.assertEqual(replay.returncode, 0, replay.stderr)
        report = json.loads(replay.stdout)
        self.assertTrue(report["replayed"])
        self.assertEqual(report["committed_seq"], 4)
        rc = hdb.open_warehouse(rep)
        try:
            self.assertEqual(
                len(hdb.inspect_warehouse(rc)["batches"]), 4)
        finally:
            rc.close()

    def test_reader_sees_only_complete_prefixes_during_apply(self) -> None:
        wh, _ = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        p02 = str(self.workdir / "p02.pkg")
        p14 = str(self.workdir / "p14.pkg")
        export_package(wh, 0, 2, p02)
        export_package(wh, 1, 4, p14)
        rep = str(self.workdir / "rep.db")
        # establish the seq-2 prefix normally
        first = wh_cli([
            "warehouse", "package", "apply", "--warehouse", rep,
            "--package", p02,
        ])
        self.assertEqual(first.returncode, 0, first.stderr)
        # Gated second apply: it blocks inside the commit with every new row
        # already staged, while a reader samples the visible prefix.  The FIFO
        # write-end is opened *before* spawning the child so neither open can
        # deadlock; the child blocks on the read until one byte is written.
        gate = str(self.workdir / "apply-gate.fifo")
        sentinel = str(self.workdir / "at-gate.sentinel")
        os.mkfifo(gate)
        env2 = dict(
            os.environ,
            BARCODE_REPLICA_GATE=gate,
            BARCODE_REPLICA_GATE_SENTINEL=sentinel,
        )
        # Open the reader connection *before* spawning: open_warehouse()
        # performs schema DDL under a write lock, which could not run while
        # the applier holds its BEGIN IMMEDIATE.  This existing connection
        # only reads and, in WAL mode, keeps seeing committed snapshots.
        reader = hdb.open_warehouse(rep)
        # Spawn first: the child stages its transaction, touches the sentinel,
        # then blocks opening the FIFO read end.  The write end is opened only
        # after arrival, so neither side blocks before the rendezvous exists.
        applier = subprocess.Popen(
            [sys.executable, "-I", str(ENTRY), "warehouse", "package",
             "apply", "--warehouse", rep, "--package", p14],
            env=env2, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        deadline = time.time() + 15
        while not os.path.exists(sentinel):
            if applier.poll() is not None:
                self.fail(f"gated applier exited early: {applier.stderr.read()}")
            if time.time() > deadline:
                self.fail("gated applier never reached the commit gate")
            time.sleep(0.02)
        writer = open(gate, "w", encoding="utf-8")
        seen = set()
        try:
            for _ in range(20):
                seen.add(hdb.latest_seq(reader))
                time.sleep(0.01)
        finally:
            reader.close()
        self.assertTrue(seen.issubset({2, 4}), seen)
        self.assertIn(2, seen)
        writer.write("x")
        writer.flush()
        writer.close()
        out, err = applier.communicate(timeout=30)
        self.assertEqual(applier.returncode, 0, err)
        rc = hdb.open_warehouse(rep)
        try:
            self.assertEqual(hdb.latest_seq(rc), 4)
        finally:
            rc.close()

    # ------------------------------------------------------------------
    # Full source independence after the source disappears
    # ------------------------------------------------------------------
    def test_replica_works_without_source_db_or_enrichment_pipeline(self) -> None:
        wh, src = build_source_history(self.workdir)
        conn = hdb.open_warehouse(wh)
        try:
            initialize_origin(conn)
        finally:
            conn.close()
        p04 = str(self.workdir / "p04.pkg")
        export_package(wh, 0, 4, p04)
        rep = str(self.workdir / "rep.db")
        rc = hdb.open_warehouse(rep)
        try:
            apply_package(p04, rc)
            # source_db_path inside the package is descriptive only; point it
            # at a path that will never exist on this machine
            stored = rc.execute(
                "SELECT source_db_path FROM wh_batches WHERE knowledge_seq=1"
            ).fetchone()["source_db_path"]
            self.assertTrue(stored)
        finally:
            rc.close()
        os.remove(src)
        for suffix in ("-wal", "-shm"):
            p = src + suffix
            if os.path.exists(p):
                os.remove(p)
        rc = hdb.open_warehouse(rep)
        try:
            ans = lookup(rc, "sup1", "K", "CN", 20, seq=2)
            self.assertEqual(ans["normalized_value"], "202")
            self.assertEqual(
                ans["supports"][0]["result_source"]["payload_hash"][:8],
                ans["supports"][0]["result_source"]["payload_hash"][:8],
            )
            # timeline/enrich/impact all work with no network or source DB
            tl = query.timeline(rc, "sup1", "K", "CN", 0, 50, seq=2)
            self.assertTrue(tl["segments"])
            facts = write_jsonl(self.workdir / "facts.jsonl", [
                {"fact_id": "f1", "supplier": "sup1", "item_id": "K",
                 "market": "CN", "event_time": 20, "tag": "x"},
                {"fact_id": "f2", "supplier": "sup1", "item_id": "K",
                 "market": "CN", "event_time": 20, "tag": "x"},
                {"fact_id": "f2", "supplier": "sup1", "item_id": "K",
                 "market": "CN", "event_time": 20, "tag": "x"},
            ])
            from barcode_pipeline.warehouse import facts as facts_mod
            out = str(self.workdir / "enriched.jsonl")
            summary = facts_mod.enrich_file(rc, facts, out, seq=4)
            rows = read_jsonl(out)
            self.assertEqual(len(rows), 3)
            self.assertTrue(all(r["state"] == "missing" for r in rows))
        finally:
            rc.close()


def sqlite3_connect(path: str) -> sqlite3.Connection:
    import sqlite3

    c = sqlite3.connect(path)
    c.row_factory = sqlite3.Row
    return c


if __name__ == "__main__":
    unittest.main()
