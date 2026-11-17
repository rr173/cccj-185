"""Controllable local mock of the partner batch barcode-normalization service.

Real HTTP semantics, implemented with the stdlib only (the client side uses
HTTPX).  Behaviour is driven by a *scenario* that tests manipulate through
POST /control/scenario:

  rules            {"<barcode>|<market>|<rule_version>": "001" | "INVALID:CODE"}
  page_size        chunk size when pages are generated automatically
  page_order       "asc" (default) or "desc"
  custom_pages     explicit list of pages, each a list of slots — supports
                   omission and duplication while keeping item totals equal
  pending_slots    slots reported as business_status="pending"
  contradictions   {slot: {"normalized_value": ...}} emits a second, different
                   final payload for that slot
  spoof            {"unknown_slot": s, "bad_digest_slot": s,
                    "wrong_job_id_slot": s}
  faults           per-endpoint FIFO queue of transient faults:
                   {"post_jobs": [...], "get_job": [...], "get_results": [...]}
                   entries are "404", "503" or "reset"

Protocol properties under test:
  * POST /jobs needs header Idempotency-Key and body.body_digest; the server
    recomputes the digest and rejects mismatches.  Same key + same body ->
    same job_id; same key + different body -> 409.
  * Slots are unique within one job.
  * GET /jobs/{id} -> {job_id, status, expected_slots}; may transiently 404.
  * GET /jobs/{id}/results?token=... -> immutable paged envelope
    {next_token, items, complete}; tokens are opaque and encode a result-set
    generation (scenario changes bump the generation; stale tokens 404).
  * Every submission (new or idempotent replay) is recorded and exposed via
    GET /control/log, and optionally appended to a JSONL log file.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .digest import canonical_json, request_body_digest, sha256_hex

BIZ_NORMALIZED = "normalized"
BIZ_INVALID = "invalid"
BIZ_PENDING = "pending"


def _rule_key(barcode: str, market: str, rule_version: str) -> str:
    return f"{barcode}|{market}|{rule_version}"


def encode_token(job_id: str, generation: int, page_index: int) -> str:
    raw = json.dumps(
        {"j": job_id, "g": generation, "i": page_index}, sort_keys=True
    ).encode("utf-8")
    return "tok_" + base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_token(token: str) -> dict[str, Any]:
    if not token.startswith("tok_"):
        raise ValueError("bad token prefix")
    pad = "=" * (-len(token[4:]) % 4)
    raw = base64.urlsafe_b64decode(token[4:] + pad)
    return json.loads(raw.decode("utf-8"))


@dataclass
class ServerJob:
    job_id: str
    idempotency_key: str
    body_digest: str
    items: list[dict[str, Any]]
    created_at: float
    # result set, built lazily on first results request
    generation: int = 0
    pages: list[list[dict[str, Any]]] | None = None
    resolved: bool = False


@dataclass
class ServerState:
    scenario: dict[str, Any] = field(default_factory=dict)
    jobs: dict[str, ServerJob] = field(default_factory=dict)
    idem_index: dict[str, str] = field(default_factory=dict)
    log: list[dict[str, Any]] = field(default_factory=list)
    generation: int = 0
    counter: int = 0
    lock: threading.RLock = field(default_factory=threading.RLock)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "scenario": self.scenario,
                "generation": self.generation,
                "counter": self.counter,
                "idem_index": self.idem_index,
                "log": self.log,
                "jobs": [
                    {
                        "job_id": j.job_id,
                        "idempotency_key": j.idempotency_key,
                        "body_digest": j.body_digest,
                        "items": j.items,
                        "created_at": j.created_at,
                        "generation": j.generation,
                        "resolved": j.resolved,
                    }
                    for j in self.jobs.values()
                ],
            }

    @classmethod
    def restore(cls, data: dict[str, Any]) -> "ServerState":
        st = cls()
        st.scenario = data.get("scenario", {})
        st.generation = data.get("generation", 0)
        st.counter = data.get("counter", 0)
        st.idem_index = data.get("idem_index", {})
        st.log = data.get("log", [])
        for jd in data.get("jobs", []):
            st.jobs[jd["job_id"]] = ServerJob(
                job_id=jd["job_id"],
                idempotency_key=jd["idempotency_key"],
                body_digest=jd["body_digest"],
                items=jd["items"],
                created_at=jd["created_at"],
                generation=jd.get("generation", 0),
                pages=None,
                resolved=jd.get("resolved", False),
            )
        return st


def _atomic_write(path: str, data: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


class MockService:
    """All stateful logic; the HTTP handler is a thin wrapper around it."""

    def __init__(
        self,
        snapshot_path: str | None = None,
        log_path: str | None = None,
    ) -> None:
        self.snapshot_path = snapshot_path
        self.log_path = log_path
        if snapshot_path and os.path.exists(snapshot_path):
            with open(snapshot_path, "r", encoding="utf-8") as fh:
                self.state = ServerState.restore(json.load(fh))
        else:
            self.state = ServerState()

    # -- persistence --------------------------------------------------------
    def _persist_locked(self) -> None:
        if self.snapshot_path:
            _atomic_write(
                self.snapshot_path,
                json.dumps(self.state.snapshot(), ensure_ascii=False, sort_keys=True),
            )

    def _append_log(self, event: dict[str, Any]) -> None:
        if self.log_path:
            with open(self.log_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")

    # -- control ------------------------------------------------------------
    def set_scenario(self, scenario: dict[str, Any]) -> dict[str, Any]:
        with self.state.lock:
            self.state.scenario = dict(scenario)
            self.state.generation += 1
            # force every job to rebuild its result set on next read
            for job in self.state.jobs.values():
                job.pages = None
            self._persist_locked()
            return dict(self.state.scenario)

    def resolve_pending(self) -> None:
        with self.state.lock:
            self.state.generation += 1
            for job in self.state.jobs.values():
                job.resolved = True
                job.pages = None
            self._persist_locked()

    def reset(self) -> None:
        with self.state.lock:
            self.state = ServerState()
            self._persist_locked()

    # -- faults -------------------------------------------------------------
    def _take_fault(self, endpoint: str) -> str | None:
        queue = self.state.scenario.get("faults", {}).get(endpoint, [])
        if not queue:
            return None
        fault = queue.pop(0)
        return str(fault)

    # -- POST /jobs ---------------------------------------------------------
    def submit_job(
        self, idem_key: str | None, body: dict[str, Any]
    ) -> tuple[int, dict[str, Any], bool]:
        """Returns (status, payload, registered_before_response).

        The third element is True when the job was durably registered but the
        caller must be disconnected without a response (fault "reset").
        """
        with self.state.lock:
            if not idem_key:
                return 400, {"error": "missing Idempotency-Key header"}, False
            items = body.get("items")
            if not isinstance(items, list) or not items:
                return 400, {"error": "body.items must be a non-empty list"}, False

            slots = [it.get("client_slot") for it in items]
            if len(set(slots)) != len(slots):
                return 422, {"error": "client_slot values must be unique"}, False

            try:
                actual_digest = request_body_digest(items)
            except (KeyError, TypeError):
                return 400, {"error": "malformed item"}, False
            if body.get("body_digest") != actual_digest:
                return 422, {
                    "error": "body_digest mismatch",
                    "claimed": body.get("body_digest"),
                    "actual": actual_digest,
                }, False

            existing_id = self.state.idem_index.get(idem_key)
            if existing_id is not None:
                job = self.state.jobs[existing_id]
                if job.body_digest != actual_digest:
                    event = {
                        "ts": time.time(),
                        "event": "conflict",
                        "idempotency_key": idem_key,
                        "existing_job_id": existing_id,
                    }
                    self.state.log.append(event)
                    self._append_log(event)
                    self._persist_locked()
                    return 409, {
                        "error": "idempotency key reused with a different body",
                        "existing_job_id": existing_id,
                    }, False
                event = {
                    "ts": time.time(),
                    "event": "replay",
                    "idempotency_key": idem_key,
                    "job_id": existing_id,
                }
                self.state.log.append(event)
                self._append_log(event)
                self._persist_locked()
                return 200, {"job_id": existing_id, "replayed": True}, False

            # New registration.  A "reset" fault registers the job durably and
            # then drops the connection: exactly the ambiguous-outage case the
            # client must recover from with the same idempotency key.
            fault = self._take_fault("post_jobs")
            self.state.counter += 1
            job_id = f"job-{self.state.counter}"
            job = ServerJob(
                job_id=job_id,
                idempotency_key=idem_key,
                body_digest=actual_digest,
                items=[dict(it) for it in items],
                created_at=time.time(),
                generation=self.state.generation,
            )
            self.state.jobs[job_id] = job
            self.state.idem_index[idem_key] = job_id
            event = {
                "ts": time.time(),
                "event": "submit",
                "idempotency_key": idem_key,
                "job_id": job_id,
                "slots": list(slots),
                "item_count": len(items),
            }
            self.state.log.append(event)
            self._append_log(event)
            self._persist_locked()
            if fault == "reset":
                return 0, {}, True
            if fault in ("404", "503"):
                # registration happened, but pretend the request failed early
                return int(fault), {"error": f"transient {fault}"}, False
            return 201, {"job_id": job_id, "replayed": False}, False

    # -- GET /jobs/{id} -----------------------------------------------------
    def get_job(self, job_id: str) -> tuple[int, dict[str, Any]]:
        with self.state.lock:
            fault = self._take_fault("get_job")
            if fault in ("404", "503"):
                return int(fault), {"error": f"transient {fault}"}
            job = self.state.jobs.get(job_id)
            if job is None:
                return 404, {"error": "unknown job"}
            pending = set(self.state.scenario.get("pending_slots", []))
            status = "processing" if (pending and not job.resolved) else "complete"
            return 200, {
                "job_id": job.job_id,
                "status": status,
                "expected_slots": len(job.items),
            }

    # -- result-set construction -------------------------------------------
    def _build_pages(self, job: ServerJob) -> list[list[dict[str, Any]]]:
        sc = self.state.scenario
        rules: dict[str, str] = sc.get("rules", {})
        pending_slots = set(sc.get("pending_slots", []))
        contradictions: dict[str, dict[str, Any]] = sc.get("contradictions", {})
        spoof: dict[str, Any] = sc.get("spoof", {})

        def make_item(slot: int, override: dict[str, Any] | None = None) -> dict[str, Any]:
            src = next(i for i in job.items if i["client_slot"] == slot)
            key = _rule_key(src["barcode"], src["market"], src["rule_version"])
            rule = rules.get(key)
            base_item = {
                "job_id": job.job_id,
                "slot": slot,
                "input_digest": self._digest_of(src),
            }
            if not job.resolved and slot in pending_slots:
                item = {
                    **base_item,
                    "business_status": BIZ_PENDING,
                    "normalized_value": None,
                    "error_code": None,
                }
            elif rule is None:
                item = {
                    "job_id": job.job_id,
                    "slot": slot,
                    "input_digest": self._digest_of(src),
                    "business_status": BIZ_INVALID,
                    "normalized_value": None,
                    "error_code": "NO_RULE",
                }
            elif rule.startswith("INVALID:"):
                item = {
                    "job_id": job.job_id,
                    "slot": slot,
                    "input_digest": self._digest_of(src),
                    "business_status": BIZ_INVALID,
                    "normalized_value": None,
                    "error_code": rule.split(":", 1)[1],
                }
            else:
                item = {
                    "job_id": job.job_id,
                    "slot": slot,
                    "input_digest": self._digest_of(src),
                    "business_status": BIZ_NORMALIZED,
                    "normalized_value": rule,
                    "error_code": None,
                }
            if override:
                item.update(override)
            # spoofing helpers for client-side rejection tests
            if slot == spoof.get("unknown_slot"):
                item["slot"] = 99000 + slot
            if slot == spoof.get("bad_digest_slot"):
                item["input_digest"] = "sha256:spoofed"
            if slot == spoof.get("wrong_job_id_slot"):
                item["job_id"] = "job-other"
            return item

        slots = sorted(it["client_slot"] for it in job.items)
        custom = sc.get("custom_pages")
        if custom:
            pages = [
                [make_item(int(s)) for s in page]
                for page in custom
            ]
        else:
            ordered = list(slots)
            if sc.get("page_order") == "desc":
                ordered.reverse()
            size = int(sc.get("page_size", max(1, len(ordered))))
            chunks = [ordered[i:i + size] for i in range(0, len(ordered), size)]
            pages = [[make_item(s) for s in chunk] for chunk in chunks]

        # contradictory second payload for selected slots: each gets its own
        # extra page so the client durably records the first final before the
        # conflicting final arrives and trips the block
        for slot_str, override in contradictions.items():
            slot = int(slot_str)
            pages.append([make_item(slot, override=dict(override))])

        return pages

    @staticmethod
    def _digest_of(src: dict[str, Any]) -> str:
        from .digest import input_digest

        return input_digest(src["barcode"], src["market"], src["rule_version"])

    def get_results(
        self, job_id: str, token: str | None
    ) -> tuple[int, dict[str, Any]]:
        with self.state.lock:
            fault = self._take_fault("get_results")
            if fault in ("404", "503"):
                return int(fault), {"error": f"transient {fault}"}
            job = self.state.jobs.get(job_id)
            if job is None:
                return 404, {"error": "unknown job"}
            if job.pages is None:
                job.generation = self.state.generation
                job.pages = self._build_pages(job)

            if token in (None, ""):
                page_idx = 0
                gen = job.generation
            else:
                try:
                    decoded = decode_token(token)
                except Exception:
                    return 404, {"error": "malformed token"}
                if decoded["j"] != job.job_id:
                    return 404, {"error": "token belongs to another job"}
                if decoded["g"] != job.generation:
                    return 404, {"error": "stale_token", "code": "stale_token"}
                page_idx = decoded["i"]
                gen = decoded["g"]
            if page_idx >= len(job.pages):
                return 404, {"error": "page not found", "code": "stale_token"}
            items = job.pages[page_idx]
            last = page_idx == len(job.pages) - 1
            envelope = {
                "items": items,
                "complete": last,
                # token addressing THIS page, so a client that paged a
                # single-page job still holds a generation-bound token it can
                # probe after the server rebuilds its result set
                "current_token": encode_token(job.job_id, gen, page_idx),
                "next_token": None
                if last
                else encode_token(job.job_id, gen, page_idx + 1),
            }
            return 200, envelope


# --------------------------------------------------------------------------
# HTTP wrapper
# --------------------------------------------------------------------------

class _Handler(BaseHTTPRequestHandler):
    server_version = "MockBarcodeService/1.0"

    @property
    def service(self) -> MockService:
        return self.server.service  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet by default
        if getattr(self.server, "verbose", False):
            super().log_message(fmt, *args)

    def handle_one_request(self) -> None:
        # A deliberate "reset" closes the socket mid-request; the base class
        # would then raise while flushing.  That is the intended behaviour, so
        # swallow the resulting I/O error and end the connection.
        try:
            super().handle_one_request()
        except (OSError, ValueError):
            self.close_connection = True

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict[str, Any] | None:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            self._send_json(400, {"error": "invalid JSON"})
            return None
        if not isinstance(data, dict):
            self._send_json(400, {"error": "body must be a JSON object"})
            return None
        return data

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        if path == "/health":
            self._send_json(200, {"ok": True})
            return
        if path == "/control/scenario":
            with self.service.state.lock:
                self._send_json(200, dict(self.service.state.scenario))
            return
        if path == "/control/log":
            with self.service.state.lock:
                self._send_json(200, {"events": list(self.service.state.log)})
            return
        if path.startswith("/jobs/") and path.endswith("/results"):
            job_id = path[len("/jobs/"): -len("/results")]
            token = qs.get("token", [None])[0]
            status, payload = self.service.get_results(job_id, token)
            self._send_json(status, payload)
            return
        if path.startswith("/jobs/"):
            job_id = path[len("/jobs/"):]
            status, payload = self.service.get_job(job_id)
            self._send_json(status, payload)
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/control/scenario":
            body = self._read_json()
            if body is None:
                return
            scenario = self.service.set_scenario(body)
            self._send_json(200, {"ok": True, "scenario": scenario})
            return
        if path == "/control/resolve-pending":
            self.service.resolve_pending()
            self._send_json(200, {"ok": True})
            return
        if path == "/control/reset":
            self.service.reset()
            self._send_json(200, {"ok": True})
            return
        if path == "/jobs":
            body = self._read_json()
            if body is None:
                return
            idem = self.headers.get("Idempotency-Key")
            try:
                status, payload, must_reset = self.service.submit_job(idem, body)
            except Exception as exc:  # pragma: no cover - defensive
                self._send_json(500, {"error": f"server bug: {exc}"})
                return
            if must_reset:
                # Durably registered, then drop the TCP connection without
                # sending any response.  BaseHTTPRequestHandler would still
                # try to flush on the way out, so detach the sockets first.
                self.close_connection = True
                try:
                    self.wfile.close()
                except OSError:
                    pass
                return
            self._send_json(status, payload)
            return
        self._send_json(404, {"error": "not found"})


def build_server(
    host: str,
    port: int,
    snapshot_path: str | None = None,
    log_path: str | None = None,
    verbose: bool = False,
) -> ThreadingHTTPServer:
    service = MockService(snapshot_path=snapshot_path, log_path=log_path)
    httpd = ThreadingHTTPServer((host, port), _Handler)
    httpd.service = service  # type: ignore[attr-defined]
    httpd.verbose = verbose  # type: ignore[attr-defined]
    return httpd


def serve_forever(
    host: str = "127.0.0.1",
    port: int = 8080,
    snapshot_path: str | None = None,
    log_path: str | None = None,
    verbose: bool = False,
) -> None:  # pragma: no cover - exercised via tests/CLI
    httpd = build_server(host, port, snapshot_path, log_path, verbose)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
