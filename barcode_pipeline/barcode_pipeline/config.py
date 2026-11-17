"""Pipeline configuration (JSON file, no third-party deps)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RetryConfig:
    max_attempts: int = 4          # total attempts per network call
    backoff_base_s: float = 0.05  # kept tiny: tests never rely on long sleeps
    backoff_max_s: float = 1.0


@dataclass
class Config:
    base_url: str = "http://127.0.0.1:8080"
    batch_size: int = 100
    # streaming: import + dedupe + dispatch batch by batch while scanning
    # prescan:   scan whole source, build the unique input domain, then dispatch
    ingestion: str = "streaming"
    timeout_s: float = 5.0
    retry: RetryConfig = field(default_factory=RetryConfig)

    @staticmethod
    def load(path: str | None) -> "Config":
        if not path:
            return Config()
        with open(path, "r", encoding="utf-8") as fh:
            raw: dict[str, Any] = json.load(fh)
        retry_raw = raw.get("retry", {}) or {}
        retry = RetryConfig(
            max_attempts=int(retry_raw.get("max_attempts", 4)),
            backoff_base_s=float(retry_raw.get("backoff_base_s", 0.05)),
            backoff_max_s=float(retry_raw.get("backoff_max_s", 1.0)),
        )
        ingestion = str(raw.get("ingestion", "streaming"))
        if ingestion not in ("streaming", "prescan"):
            raise ValueError(f"invalid ingestion mode: {ingestion!r}")
        return Config(
            base_url=str(raw.get("base_url", "http://127.0.0.1:8080")),
            batch_size=int(raw.get("batch_size", 100)),
            ingestion=ingestion,
            timeout_s=float(raw.get("timeout_s", 5.0)),
            retry=retry,
        )

    def env_override(self) -> "Config":
        url = os.environ.get("BARCODE_SERVICE_URL")
        if url:
            self.base_url = url
        return self
