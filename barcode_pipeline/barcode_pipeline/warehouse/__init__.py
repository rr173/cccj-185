"""Bi-temporal historical warehouse for barcode enrichment claims.

The warehouse consumes *already completed* enrichment databases (the SQLite
databases produced by the pipeline's ``start``/``resume``/``export`` flow) plus
change files describing partner corrections.  It answers two distinct
questions:

* **as of business time** — which barcode applied when the procurement fact
  happened (interval ``[valid_from, valid_to)``);
* **as of knowledge time** — what the warehouse knew when a report was last
  run (each accepted import gets a monotonically increasing
  ``knowledge_seq``; old sequences remain queryable forever).

Modules:
  hdb        warehouse schema, read-only source snapshots, evidence extraction
  importer   change-file validation and atomic, idempotent batch admission
  query      point lookup and maximal-segment timeline reconstruction
  facts      streaming fact enrichment and two-sequence impact comparison
  cli        warehouse import/lookup/timeline/enrich/impact/inspect commands
"""

from .hdb import (
    WarehouseError,
    open_warehouse,
    open_source_snapshot,
    inspect_warehouse,
    latest_seq,
)
from .importer import import_batch
from .query import lookup, timeline
from .facts import enrich_file, impact_file
from .replication.origin import (
    initialize_origin,
    initialize_replica,
    replica_status,
)
from .replication.exporter import export_package
from .replication.replica import inspect_package, apply_package

__all__ = [
    "WarehouseError",
    "open_warehouse",
    "open_source_snapshot",
    "inspect_warehouse",
    "latest_seq",
    "import_batch",
    "lookup",
    "timeline",
    "enrich_file",
    "impact_file",
    "initialize_origin",
    "initialize_replica",
    "replica_status",
    "export_package",
    "inspect_package",
    "apply_package",
]
