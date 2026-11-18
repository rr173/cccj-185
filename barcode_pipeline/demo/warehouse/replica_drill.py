#!/usr/bin/env python3
"""End-to-end offline-replication drill with hand-written expectations.

Everything is generated at runtime into the given workdir; no large data or
package is ever committed.  The drill:

  1. builds a real enrichment DB through the local pipeline + mock service
     (confirmed results 101 and 202 for the same product K);
  2. admits seq1..4 on the source warehouse: K rev1 ->101, K rev2 ->202, a
     duplicate-only batch, then a K rev3 retraction;
  3. builds a managed replica from (0,2], then (1,4], then replays (0,2];
  4. deletes the source warehouse and enrichment DB on the receiver side and
     proves lookup/timeline/enrich/impact, source explanations and a shuffled
     fact file still match independent expectations;
  5. builds a same-origin SQLite-backup fork at seq 2 and shows it can
     neither join nor replay;
  6. shows every rejection class: missing prefix, business-column-only and
     service-source-only divergence (same normalized value), corrupt tail and
     a dropped zero-new-revision batch -- with ledger/position/query checks
     before and after;
  7. exercises concurrent export/import, concurrent receivers and a real
     process kill at deterministic synchronization points;
  8. (separate driver, see replica_scale_driver.py) the 70k-revision,
     ~4 KiB-note, 256 MiB address-space streaming run.

Usage: python3 -I demo/warehouse/replica_drill.py --workdir DIR
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import struct
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "vendor"))
sys.path.insert(0, str(ROOT))

from tests.wh_helpers import (  # noqa: E402
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
from barcode_pipeline.warehouse import facts as facts_mod  # noqa: E402
from barcode_pipeline.warehouse.replication import identity  # noqa: E402
from barcode_pipeline.warehouse.replication import replica as replica_mod  # noqa: E402
from barcode_pipeline.warehouse.replication.package import (  # noqa: E402
    MAGIC,
    TAG_BATCH,
    TAG_FOOTER,
    TAG_MANIFEST,
)

ENTRY = ROOT / "barcode_pipeline" / "__main__.py"
P = ("sup1", "K", "CN")


def check(label, got, expected):
    ok = got == expected
    print(f"  [{'ok ' if ok else 'FAIL'}] {label}: got={got!r} want={expected!r}")
    if not ok:
        raise AssertionError(label)


def _frame(tag, obj):
    import hashlib

    payload = json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return tag + struct.pack(">Q", len(payload)) + payload + hashlib.sha256(payload).digest()


def read_frames(path):
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


def write_frames(path, frames):
    with open(path, "wb") as fh:
        fh.write(MAGIC)
        for tag, obj in frames:
            fh.write(_frame(tag, obj))


def rechain(frames):
    tag, manifest = frames[0]
    chain = manifest["after_digest"]
    out = []
    batch_total = rev_total = 0
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
        "origin_id": manifest["origin_id"], "after": manifest["after"],
        "through": manifest["through"], "final_digest": chain,
        "batch_count": batch_total, "revision_count": rev_total,
    }))
    return result


def build_drill_source(workdir: Path):
    print("== 1. real enrichment DB via local pipeline (101 and 202) ==")
    rules = {"X|CN|v1": "101", "Y|CN|v1": "202"}
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
        print("== 2. source origin, then seq1..4 admissions ==")
        init = initialize_origin(conn)
        origin_id = init["origin_id"]
        print(f"   origin_id={origin_id}")

        def admit(batch_id, rows):
            p = write_changes(workdir / f"{batch_id}.jsonl", rows)
            s = importer.import_batch(conn, src, p, batch_id)
            print(f"   {batch_id}: seq={s['knowledge_seq']}"
                  f" new={s['new_revisions']} dup={s['duplicate_rows']}")
            return s

        admit("b1", [{"claim_id": "K", "revision": 1, "source_line": 1,
                      "valid_from": 0, "valid_to": 50}])
        admit("b2", [{"claim_id": "K", "revision": 2, "source_line": 2,
                      "valid_from": 0, "valid_to": 50}])
        s3 = admit("b3", [{"claim_id": "K", "revision": 2, "source_line": 2,
                           "valid_from": 0, "valid_to": 50}])
        admit("b4", [{"claim_id": "K", "revision": 3}])
        check("seq3 duplicate-only new_revisions", s3["new_revisions"], 0)
        check("seq3 duplicate_rows", s3["duplicate_rows"], 1)
        # idempotent re-init keeps identity and history
        again = initialize_origin(conn)
        check("origin init idempotent", again["origin_id"], origin_id)
    finally:
        conn.close()
    return wh, src, origin_id


def drill_replication(workdir: Path, wh, origin_id):
    print("== 3. replica from (0,2], then (1,4], then replay (0,2] ==")
    p02 = str(workdir / "pkg-0-2.bin")
    p14 = str(workdir / "pkg-1-4.bin")
    r1 = export_package(wh, 0, 2, p02)
    r2 = export_package(wh, 1, 4, p14)
    print(f"   (0,2] -> batches={r1['batch_count']} revs={r1['revision_count']}")
    print(f"   (1,4] -> batches={r2['batch_count']} revs={r2['revision_count']}")
    check("p02 range", (r1["after"], r1["through"]), (0, 2))
    check("p14 range", (r2["after"], r2["through"]), (1, 4))
    # package is not a SQLite file
    with open(p02, "rb") as fh:
        check("package magic", fh.read(len(MAGIC)), MAGIC)

    rep = str(workdir / "replica.db")
    rc = hdb.open_warehouse(rep)
    try:
        a1 = apply_package(p02, rc)
        check("first apply seq", a1["committed_seq"], 2)
        check("first apply binding", replica_status(rc)["origin_id"], origin_id)
        verdict = inspect_package(p14, rc)
        check("p14 verdict", verdict["verdict"],
              replica_mod.VERDICT_APPENDABLE)
        a2 = apply_package(p14, rc)
        check("second apply seq", a2["committed_seq"], 4)
        check("second apply new batches", a2["batches_applied"], 2)
        check("second apply replayed overlap", a2["batches_replayed"], 1)
        a3 = apply_package(p02, rc)
        check("replay leaves seq", a3["committed_seq"], 4)
        check("replay adds nothing", a3["batches_applied"], 0)
        check("replay rechecks all", a3["batches_replayed"], 2)

        print("== hand-written point answers on the replica ==")
        check("seq1 @20",
              (lookup(rc, *P, 20, seq=1)["state"],
               lookup(rc, *P, 20, seq=1)["normalized_value"]),
              ("normalized", "101"))
        check("seq2 @20",
              (lookup(rc, *P, 20, seq=2)["state"],
               lookup(rc, *P, 20, seq=2)["normalized_value"]),
              ("normalized", "202"))
        check("seq3 @20 still 202",
              lookup(rc, *P, 20, seq=3)["normalized_value"], "202")
        check("seq4 @20 missing",
              lookup(rc, *P, 20, seq=4)["state"], "missing")
        ledger = hdb.inspect_warehouse(rc)
        check("four registrations", len(ledger["batches"]), 4)
        check("seq ids kept",
              [b["knowledge_seq"] for b in ledger["batches"]], [1, 2, 3, 4])
        check("batch ids kept",
              [b["batch_id"] for b in ledger["batches"]],
              ["b1", "b2", "b3", "b4"])
        check("seq3 exists with zero new revisions",
              [b["new_revisions"] for b in ledger["batches"]], [1, 1, 0, 1])
        check("only three stored revisions", ledger["revision_count"], 3)
    finally:
        rc.close()
    return rep, p02, p14


def drill_source_removed(workdir: Path, rep, src):
    print("== 4. receiver loses source warehouse and enrichment DB ==")
    os.remove(src)
    for suffix in ("-wal", "-shm"):
        p = src + suffix
        if os.path.exists(p):
            os.remove(p)
    # also move the source warehouse away so no local origin DB remains
    hidden = str(workdir / "source.db.hidden")
    os.rename(str(workdir / "source.db"), hidden)
    for suffix in ("-wal", "-shm"):
        p = str(workdir / "source.db") + suffix
        if os.path.exists(p):
            os.rename(p, hidden + suffix)

    # independent expected answers, hand-written
    expected = {
        1: ("normalized", "101"),
        2: ("normalized", "202"),
        3: ("normalized", "202"),
        4: ("missing", None),
    }
    facts = write_jsonl(workdir / "facts-shuffled.jsonl", [
        {"fact_id": "f3", "supplier": "sup1", "item_id": "K",
         "market": "CN", "event_time": 20, "note": "third"},
        {"fact_id": "f1", "supplier": "sup1", "item_id": "K",
         "market": "CN", "event_time": 20, "note": "first"},
        {"fact_id": "f1", "supplier": "sup1", "item_id": "K",
         "market": "CN", "event_time": 20, "note": "first-dup"},
        {"fact_id": "f2", "supplier": "sup1", "item_id": "K",
         "market": "CN", "event_time": 99, "note": "outside"},
    ])
    rc = hdb.open_warehouse(rep)
    try:
        for s, (state, value) in expected.items():
            ans = lookup(rc, *P, 20, seq=s)
            check(f"isolated lookup seq{s}",
                  (ans["state"], ans["normalized_value"]), (state, value))
        # source explanation survives with full service provenance
        support = lookup(rc, *P, 20, seq=2)["supports"][0]
        check("provenance keys",
              sorted(support["result_source"]),
              sorted(["job_id", "slot", "page_index",
                      "payload_hash", "input_digest"]))
        check("original business note kept",
              support["source"]["fact_ref"], "k-y")
        tl = query.timeline(rc, *P, 0, 50, seq=2)
        check("timeline segments",
              [(s["start"], s["end"], s["state"]) for s in tl["segments"]],
              [(0, 50, "normalized")])
        out = str(workdir / "enriched-isolated.jsonl")
        summary = facts_mod.enrich_file(rc, facts, out, seq=4)
        rows = [json.loads(line) for line in open(out, encoding="utf-8")
                if line.strip()]
        check("fact order and duplicates", [r["fact_id"] for r in rows],
              ["f3", "f1", "f1", "f2"])
        check("fact states", [r["state"] for r in rows],
              ["missing", "missing", "missing", "missing"])
        check("fact row count", summary["row_count"], 4)
        impact = str(workdir / "impact-1-2.jsonl")
        summary2 = facts_mod.impact_file(rc, facts, impact, 1, 2)
        changed = [json.loads(line) for line in open(impact, encoding="utf-8")
                   if line.strip()]
        # at seq1 the [0,50) claim is 101; at seq2 it is 202 -> the three
        # event_time=20 rows change, the outside one stays missing
        check("impact changed lines", summary2["changed_count"], 3)
        check("impact line numbers", [c["line_no"] for c in changed],
              [1, 2, 3])
        check("impact values",
              [(c["old"]["normalized_value"], c["new"]["normalized_value"])
               for c in changed],
              [("101", "202")] * 3)
    finally:
        rc.close()
    # restore the source for the remaining scenarios
    os.rename(hidden, str(workdir / "source.db"))
    for suffix in ("-wal", "-shm"):
        p = hidden + suffix
        if os.path.exists(p):
            os.rename(p, str(workdir / "source.db") + suffix)
    return str(workdir / "source.db")


def drill_rejections(workdir: Path, wh, rep):
    print("== 5. rejection classes with before/after invariants ==")
    rc = hdb.open_warehouse(rep)
    try:
        before_seq = hdb.latest_seq(rc)
        before_batches = len(hdb.inspect_warehouse(rc)["batches"])
        before_answer = lookup(rc, *P, 20, seq=2)["normalized_value"]
    finally:
        rc.close()

    # 5a. missing prefix: a package starting beyond the replica's prefix.
    # (3,4] starts at seq 3 on a replica whose committed prefix is empty (0).
    gap = str(workdir / "gap.pkg")
    export_package(wh, 3, 4, gap)
    fresh = str(workdir / "fresh-replica.db")
    fc = hdb.open_warehouse(fresh)
    try:
        initialize_replica(fc, replica_status(hdb.open_warehouse(wh))["origin_id"])
        v = inspect_package(gap, fc)
        check("missing prefix verdict", v["verdict"],
              replica_mod.VERDICT_MISSING_PREFIX)
        try:
            apply_package(gap, fc)
            raise AssertionError("gap package was accepted")
        except WarehouseError:
            print("   [ok ] gap package rejected on apply")
        check("gap target stays empty", hdb.latest_seq(fc), 0)
    finally:
        fc.close()

    # 5b/5c. coherent forks: business column or service source changed while
    # the normalized value stays identical
    p02 = str(workdir / "pkg-0-2.bin")
    frames = read_frames(p02)

    def mutate_business(fs):
        for t, obj in fs:
            if t == TAG_BATCH and obj["batch"]["knowledge_seq"] == 1:
                obj["revisions"][0]["source"]["fact_ref"] = "rewritten"
        return fs

    def mutate_source(fs):
        for t, obj in fs:
            if t == TAG_BATCH and obj["batch"]["knowledge_seq"] == 2:
                obj["revisions"][0]["result_source"]["payload_hash"] = "a" * 64
        return fs

    rc = hdb.open_warehouse(rep)
    try:
        for name, mutate in (("business-column", mutate_business),
                             ("service-source", mutate_source)):
            path = str(workdir / f"fork-{name}.pkg")
            write_frames(path, rechain(mutate(read_frames(p02))))
            v = inspect_package(path, rc)
            check(f"{name} divergence verdict",
                  v["verdict"], replica_mod.VERDICT_DIVERGED)
            try:
                apply_package(path, rc)
                raise AssertionError(f"{name} fork was accepted")
            except WarehouseError:
                print(f"   [ok ] {name} fork rejected on apply")
            check(f"{name}: position unchanged", hdb.latest_seq(rc), before_seq)
    finally:
        rc.close()

    # 5d. corrupt package tail (truncation / bad checksum)
    trunc = str(workdir / "truncated.pkg")
    write_frames(trunc, read_frames(p02)[:-1])  # drop footer
    rc = hdb.open_warehouse(rep)
    try:
        check("truncated verdict",
              inspect_package(trunc, rc)["verdict"],
              replica_mod.VERDICT_CORRUPT)
    finally:
        rc.close()
    badhash = str(workdir / "badhash.pkg")
    write_frames(badhash, read_frames(p02))
    with open(badhash, "r+b") as fh:
        fh.seek(-1, os.SEEK_END)
        b = fh.read(1)
        fh.seek(-1, os.SEEK_END)
        fh.write(bytes([b[0] ^ 0xFF]))
    rc = hdb.open_warehouse(rep)
    try:
        check("bad checksum verdict",
              inspect_package(badhash, rc)["verdict"],
              replica_mod.VERDICT_CORRUPT)
    finally:
        rc.close()

    # 5e. omit the zero-new-revision batch (seq 3)
    p04 = str(workdir / "pkg-0-4.bin")
    export_package(wh, 0, 4, p04)
    dropped = str(workdir / "dropped-seq3.pkg")
    kept = [f for f in read_frames(p04)
            if not (f[0] == TAG_BATCH
                    and f[1]["batch"]["knowledge_seq"] == 3)]
    write_frames(dropped, kept)
    rc = hdb.open_warehouse(rep)
    try:
        check("missing zero-new batch verdict",
              inspect_package(dropped, rc)["verdict"],
              replica_mod.VERDICT_CORRUPT)
        try:
            apply_package(dropped, rc)
            raise AssertionError("dropped-batch package was accepted")
        except Exception as exc:
            check("dropped-batch error type",
                  type(exc).__name__, "PackageError")
    finally:
        rc.close()

    rc = hdb.open_warehouse(rep)
    try:
        check("after rejections: seq", hdb.latest_seq(rc), before_seq)
        check("after rejections: batch count",
              len(hdb.inspect_warehouse(rc)["batches"]), before_batches)
        check("after rejections: old query",
              lookup(rc, *P, 20, seq=2)["normalized_value"], before_answer)
    finally:
        rc.close()


def drill_fork(workdir: Path, wh, rep):
    print("== 6. same-origin SQLite-backup fork at seq 2 ==")
    import sqlite3

    fork_db = str(workdir / "fork-source.db")
    s = sqlite3.connect(wh)
    try:
        d = sqlite3.connect(fork_db)
        try:
            s.backup(d)  # consistent backup copies the origin identity
        finally:
            d.close()
    finally:
        s.close()
    fc = hdb.open_warehouse(fork_db)
    try:
        check("backup shares origin",
              replica_status(fc)["origin_id"],
              replica_status(hdb.open_warehouse(wh))["origin_id"])
    finally:
        fc.close()

    p03 = str(workdir / "pkg-0-3.bin")
    export_package(wh, 0, 3, p03)
    frames = read_frames(p03)
    manifest = frames[0][1]
    # original seq 3 is the duplicate-only batch; the fork writes a different
    # correction at the same seq 3.
    rev2 = next(o["revisions"][0] for t, o in frames
                if t == TAG_BATCH and o["batch"]["knowledge_seq"] == 2)
    fork_frames = [(TAG_MANIFEST, dict(manifest))]
    for t, obj in frames[1:]:
        if t == TAG_BATCH and obj["batch"]["knowledge_seq"] == 3:
            rev = dict(rev2)
            rev["revision"] = 3
            rev["valid_from"] = 5
            rev["valid_to"] = 45
            batch = dict(obj["batch"])
            batch.update(batch_id="fork-b3", new_revisions=1,
                         duplicate_rows=0, retractions=0)
            fork_frames.append((TAG_BATCH,
                                {"batch": batch, "revisions": [rev]}))
        elif t == TAG_FOOTER:
            continue
        else:
            fork_frames.append((t, obj))
    fork_pkg = str(workdir / "same-origin-fork.pkg")
    write_frames(fork_pkg, rechain(fork_frames))

    rc = hdb.open_warehouse(rep)
    try:
        # make sure the replica holds the genuine seq 3
        if hdb.latest_seq(rc) < 3:
            apply_package(p03, rc)
        v = inspect_package(fork_pkg, rc)
        check("same-origin fork verdict",
              v["verdict"], replica_mod.VERDICT_DIVERGED)
        try:
            apply_package(fork_pkg, rc)
            raise AssertionError("same-origin fork was accepted")
        except WarehouseError:
            print("   [ok ] same-origin fork cannot join or replay")
        check("replica still on genuine seq", hdb.latest_seq(rc), 4)
        check("genuine package replays",
              inspect_package(p03, rc)["verdict"],
              replica_mod.VERDICT_REPLAY)
    finally:
        rc.close()


def drill_concurrency_and_kill(workdir: Path, wh, src):
    print("== 7. concurrency and real process termination ==")
    # 7a. concurrent export while a new batch lands: fixed snapshot.
    # An os.pipe pair (not a FIFO) gives non-blocking opens on both ends and
    # a deterministic rendezvous with no sleeps.
    gate_rfd, gate_wfd = os.pipe()
    sentinel = str(workdir / "export-sentinel")
    import threading

    def export_runner():
        def hook(after, through):
            # Deterministic arrival: announce the pinned snapshot, then block
            # on the pipe until the writer has finished its concurrent batch.
            with open(sentinel, "w", encoding="utf-8") as fh:
                fh.write("pinned")
            os.read(gate_rfd, 1)
        export_package(wh, 0, "latest",
                       str(workdir / "snapshot.pkg"), gate=hook)

    t = threading.Thread(target=export_runner)
    t.start()
    deadline = time.time() + 15
    while not os.path.exists(sentinel):
        time.sleep(0.02)
        if time.time() > deadline:
            raise AssertionError("exporter never pinned its snapshot")
    # Snapshot is pinned; the exporter now waits on the pipe.  Admit the late
    # batch concurrently, then release the gate.
    conn = hdb.open_warehouse(wh)
    try:
        p = write_changes(workdir / "late.jsonl", [
            {"claim_id": "K", "revision": 4, "source_line": 1,
             "valid_from": 0, "valid_to": 10},
        ])
        importer.import_batch(conn, src, p, "late-batch")
        check("writer reached seq 5", hdb.latest_seq(conn), 5)
    finally:
        conn.close()
    os.write(gate_wfd, b"x")
    os.close(gate_wfd)
    os.close(gate_rfd)
    t.join(timeout=30)
    check("export thread finished", t.is_alive(), False)
    from barcode_pipeline.warehouse.replication.package import read_package
    snap = read_package(str(workdir / "snapshot.pkg"))
    check("exported through fixed at 4", snap.manifest["through"], 4)
    check("exported batches fixed at 4", len(snap.batches), 4)

    # 7b. concurrent receivers of overlapping packages converge on one prefix
    rep = str(workdir / "concurrent-replica.db")
    p02 = str(workdir / "pkg-0-2.bin")
    p14 = str(workdir / "pkg-1-4.bin")
    procs = [
        subprocess.Popen(
            [sys.executable, "-I", str(ENTRY), "warehouse", "package",
             "apply", "--warehouse", rep, "--package", pkg],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        for pkg in (p02, p14, p02, p14)
    ]
    for p in procs:
        out, err = p.communicate(timeout=60)
        check("receiver exit 0", p.returncode, 0)
        if p.returncode:
            print(err)
    rc = hdb.open_warehouse(rep)
    try:
        check("concurrent final seq", hdb.latest_seq(rc), 4)
        check("concurrent registrations",
              [r["knowledge_seq"] for r in rc.execute(
                  "SELECT knowledge_seq FROM wh_replication_log"
                  " ORDER BY knowledge_seq")],
              [1, 2, 3, 4])
    finally:
        rc.close()

    # 7c. real SIGKILL at two deterministic points, then new-process recovery.
    p04 = str(workdir / "pkg-0-4.bin")

    # Point A: kill after full validation but before any write transaction.
    # The origin binding has not been committed, so no binding must remain.
    pre_rep = str(workdir / "killed-pre-txn.db")
    env_pre = dict(os.environ,
                   BARCODE_KILL_AFTER="wh_replica_before_txn")
    proc = subprocess.Popen(
        [sys.executable, "-I", str(ENTRY), "warehouse", "package",
         "apply", "--warehouse", pre_rep, "--package", p04],
        env=env_pre, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    proc.wait(timeout=30)
    check("pre-txn killed by SIGKILL", proc.returncode, -signal.SIGKILL)
    status = wh_cli(["warehouse", "replica", "status",
                     "--warehouse", pre_rep])
    report = json.loads(status.stdout)
    check("no binding after pre-txn kill", report["origin_id"], None)
    check("no prefix after pre-txn kill", report["committed_seq"], 0)

    # Point B: kill with the whole package staged but uncommitted.  Nothing
    # is visible: no binding, no batches, no consumed prefix.
    kill_rep = str(workdir / "killed-replica.db")
    env = dict(os.environ, BARCODE_KILL_AFTER="wh_replica_staged")
    proc = subprocess.Popen(
        [sys.executable, "-I", str(ENTRY), "warehouse", "package",
         "apply", "--warehouse", kill_rep, "--package", p04],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    proc.wait(timeout=30)
    check("staged killed by SIGKILL", proc.returncode, -signal.SIGKILL)
    status = wh_cli(["warehouse", "replica", "status",
                     "--warehouse", kill_rep])
    check("status command after kill", status.returncode, 0)
    report = json.loads(status.stdout)
    check("no partial prefix after kill", report["committed_seq"], 0)
    check("no binding after failed first apply", report["origin_id"], None)
    # A new process queries status, then resends: whole package applies once.
    again = wh_cli(["warehouse", "package", "apply",
                    "--warehouse", kill_rep, "--package", p04])
    check("resend succeeds", again.returncode, 0)
    check("resend reaches seq 4",
          json.loads(again.stdout)["committed_seq"], 4)
    third = wh_cli(["warehouse", "package", "apply",
                    "--warehouse", kill_rep, "--package", p04])
    check("third send is replay",
          json.loads(third.stdout)["replayed"], True)
    # the pre-txn-killed replica also recovers cleanly with the same package
    recover_pre = wh_cli(["warehouse", "package", "apply",
                          "--warehouse", pre_rep, "--package", p04])
    check("pre-txn replica resend succeeds", recover_pre.returncode, 0)
    check("pre-txn replica reaches seq 4",
          json.loads(recover_pre.stdout)["committed_seq"], 4)


def main(workdir: Path) -> int:
    workdir.mkdir(parents=True, exist_ok=True)
    wh, src, origin_id = build_drill_source(workdir)
    rep, _, _ = drill_replication(workdir, wh, origin_id)
    drill_source_removed(workdir, rep, src)
    # rebuild a source enrichment path reference for later scenarios: the
    # source warehouse was restored, the enrichment DB itself stays deleted to
    # prove replication never reopens it.
    drill_rejections(workdir, wh, rep)
    drill_fork(workdir, wh, rep)
    # concurrency needs a readable enrichment DB for the late source batch;
    # rebuild a small one through the real pipeline (allowed: only receiver
    # isolation forbids it, and this runs on the source side).
    src2 = build_enrichment_db(
        workdir / "src2",
        [{"barcode": "X", "market": "CN", "rule_version": "v1",
          "supplier": "sup1", "item_id": "K", "fact_ref": "k-x"}],
        {"X|CN|v1": "101"}, name="late", batch_size=10,
    )
    drill_concurrency_and_kill(workdir, wh, src2)
    print("ALL DRILL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workdir", required=True)
    raise SystemExit(main(Path(ap.parse_args().workdir)))
