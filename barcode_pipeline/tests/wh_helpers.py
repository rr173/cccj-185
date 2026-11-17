"""Helpers for warehouse tests: real enrichment DBs, change files, facts.

Enrichment databases are produced by the *actual* pipeline (start against the
mock service, then resume until confirmed) — never hand-edited into a
deliverable-looking state.  The source rows carry the warehouse product key
columns ``supplier`` / ``item_id`` (plus ``market``).
"""

from __future__ import annotations

import json
import os
import sys
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "vendor"))
sys.path.insert(0, str(ROOT))

from tests.helpers import ServerHarness, run_cli, write_config  # noqa: E402


def write_jsonl(path: str | Path, rows: list[dict]) -> str:
    path = str(path)
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return path


def write_changes(path: str | Path, rows: list[dict]) -> str:
    # Change files keep their given key order is irrelevant; canonical per line.
    return write_jsonl(path, rows)


def build_enrichment_db(
    workdir: str | Path,
    sources: list[dict],
    rules: dict[str, str],
    *,
    name: str = "enrich",
    batch_size: int = 50,
) -> str:
    """Run the real pipeline to a confirmed, deliverable enrichment DB.

    Returns the path of the SQLite database.  The caller controls the mock
    server's rule table (``barcode|market|rule_version`` -> answer).
    """
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    source_path = write_jsonl(workdir / f"{name}-source.jsonl", sources)
    with ServerHarness({"rules": rules, "page_size": batch_size}) as srv:
        cfg = write_config(
            workdir / f"{name}-cfg.json", srv.base_url, batch_size=batch_size
        )
        db_path = str(workdir / f"{name}.db")
        proc = run_cli(
            ["--config", cfg, "start", "--db", db_path, "--source", source_path]
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"enrichment start failed: {proc.stderr}\n{proc.stdout}"
            )
    return db_path


def wh_cli(args: list[str], env: dict | None = None) -> subprocess.CompletedProcess:
    """Run a warehouse subcommand in a fresh process under isolated mode."""
    full_env = dict(os.environ)
    if env:
        full_env.update(env)
    entry = str(ROOT / "barcode_pipeline" / "__main__.py")
    cmd = [sys.executable, "-I", entry, *args]
    return subprocess.run(
        cmd, cwd=str(ROOT), env=full_env, capture_output=True, text=True,
        timeout=120,
    )


# Hand-written rule table used by the canonical walkthrough.
WALKTHROUGH_RULES = {
    # product P (barcode X, CN): normalized 001 — two source rows (1-2)
    "X|CN|v1": "001",
    # product P again, but a partner alternative barcode Y -> 999.
    # The product key (supplier,item_id,market) is still P/CN; the service
    # input identity includes the barcode, so this is a separate confirmed
    # result that can conflict with the X claim.
    "Y|CN|v1": "999",
    # product Q kept for unrelated-product checks
    "Z|CN|v1": "777",
    # product E (barcode BAD, CN): definitively invalid
    "BAD|CN|v1": "INVALID:BAD_BARCODE",
    # second invalid product with a different error code
    "DUD|CN|v1": "INVALID:CHECKSUM_FAIL",
    # product P in a different market (context isolation)
    "X|EU|v1": "555",
}

# Source rows: line numbers are the source_line identities referenced by
# change files.  P uses lines 1-2 (barcode X -> 001) and line 3 (barcode Y
# -> 999, same product key); Q line 4, E line 5, D line 6, P/EU line 7.
WALKTHROUGH_SOURCES = [
    {"barcode": "X", "market": "CN", "rule_version": "v1",
     "supplier": "sup1", "item_id": "P", "fact_ref": "p-1"},
    {"barcode": "X", "market": "CN", "rule_version": "v1",
     "supplier": "sup1", "item_id": "P", "fact_ref": "p-2"},
    {"barcode": "Y", "market": "CN", "rule_version": "v1",
     "supplier": "sup1", "item_id": "P", "fact_ref": "p-alt"},
    {"barcode": "Z", "market": "CN", "rule_version": "v1",
     "supplier": "sup1", "item_id": "Q", "fact_ref": "q-1"},
    {"barcode": "BAD", "market": "CN", "rule_version": "v1",
     "supplier": "sup1", "item_id": "E", "fact_ref": "e-1"},
    {"barcode": "DUD", "market": "CN", "rule_version": "v1",
     "supplier": "sup1", "item_id": "D", "fact_ref": "d-1"},
    {"barcode": "X", "market": "EU", "rule_version": "v1",
     "supplier": "sup1", "item_id": "P", "fact_ref": "p-eu-1"},
]
