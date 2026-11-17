"""Shared test helpers: in-process mock server, subprocess client, scenarios."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "vendor"
FIXTURES = ROOT / "tests" / "fixtures"

# Ensure the vendored httpx tree is importable even under `python -I`.
sys.path.insert(0, str(VENDOR))
sys.path.insert(0, str(ROOT))

import httpx  # noqa: E402

from barcode_pipeline.server import build_server  # noqa: E402


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


# Hand-written expectations for the main walkthrough (never produced by the
# pipeline code itself).
MAIN_EXPECTED = [
    {
        "line": 1,
        "barcode": "X",
        "market": "CN",
        "rule_version": "v1",
        "state": "normalized",
        "normalized_value": "001",
        "error_code": None,
    },
    {
        "line": 2,
        "barcode": "X",
        "market": "CN",
        "rule_version": "v1",
        "state": "normalized",
        "normalized_value": "001",
        "error_code": None,
    },
    {
        "line": 3,
        "barcode": "X",
        "market": "EU",
        "rule_version": "v1",
        "state": "normalized",
        "normalized_value": "999",
        "error_code": None,
    },
    {
        "line": 4,
        "barcode": "BAD",
        "market": "CN",
        "rule_version": "v1",
        "state": "invalid",
        "normalized_value": None,
        "error_code": "BAD_BARCODE",
    },
]

MAIN_RULES = {
    "X|CN|v1": "001",
    "X|EU|v1": "999",
    "BAD|CN|v1": "INVALID:BAD_BARCODE",
}


class ServerHarness:
    def __init__(self, scenario: dict | None = None, snapshot: str | None = None,
                 log_path: str | None = None) -> None:
        self.port = free_port()
        self.httpd = build_server(
            "127.0.0.1", self.port,
            snapshot_path=snapshot, log_path=log_path,
        )
        self.thread = threading.Thread(
            target=self.httpd.serve_forever, daemon=True
        )
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._scenario = scenario or {}

    def __enter__(self) -> "ServerHarness":
        self.thread.start()
        self.wait_ready()
        if self._scenario:
            self.set_scenario(self._scenario)
        return self

    def __exit__(self, *exc) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    def wait_ready(self, timeout: float = 5.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                r = httpx.get(f"{self.base_url}/health", timeout=1.0)
                if r.status_code == 200:
                    return
            except httpx.TransportError:
                time.sleep(0.01)
        raise RuntimeError("mock server did not become ready")

    def client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, timeout=5.0)

    def set_scenario(self, scenario: dict) -> dict:
        r = httpx.post(
            f"{self.base_url}/control/scenario", json=scenario, timeout=5.0
        )
        r.raise_for_status()
        return r.json()

    def resolve_pending(self) -> None:
        r = httpx.post(f"{self.base_url}/control/resolve-pending", timeout=5.0)
        r.raise_for_status()

    def reset_server(self) -> None:
        r = httpx.post(f"{self.base_url}/control/reset", timeout=5.0)
        r.raise_for_status()

    def submission_log(self) -> list[dict]:
        r = httpx.get(f"{self.base_url}/control/log", timeout=5.0)
        r.raise_for_status()
        return r.json()["events"]


def write_config(path: str | Path, base_url: str, *, batch_size: int = 2,
                 max_attempts: int = 4, ingestion: str = "streaming") -> str:
    cfg = {
        "base_url": base_url,
        "batch_size": batch_size,
        "ingestion": ingestion,
        "timeout_s": 5.0,
        "retry": {
            "max_attempts": max_attempts,
            "backoff_base_s": 0.01,
            "backoff_max_s": 0.1,
        },
    }
    path = str(path)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh)
    return path


def run_cli(args: list[str], env: dict | None = None) -> subprocess.CompletedProcess:
    """Run the pipeline CLI in a fresh process under Python isolated mode."""
    full_env = dict(os.environ)
    if env:
        full_env.update(env)
    entry = str(ROOT / "barcode_pipeline" / "__main__.py")
    cmd = [sys.executable, "-I", entry, *args]
    return subprocess.run(
        cmd, cwd=str(ROOT), env=full_env, capture_output=True, text=True, timeout=60
    )


def read_jsonl(path: str | Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


class BaseCase(unittest.TestCase):
    def make_workdir(self) -> Path:
        d = Path(self._workdir)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def setUp(self) -> None:
        import tempfile
        self._tmp = tempfile.TemporaryDirectory()
        self._workdir = self._tmp.name

    def tearDown(self) -> None:
        self._tmp.cleanup()

    @property
    def workdir(self) -> Path:
        return Path(self._tmp.name)
