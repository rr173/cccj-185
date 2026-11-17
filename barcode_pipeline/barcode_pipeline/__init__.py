"""Resumable barcode-normalization enrichment pipeline.

Components:
  * digest   - canonical input-identity / body-digest helpers
  * db       - SQLite persistence (source rows, inputs, jobs, pages, results)
  * server   - controllable local mock of the partner batch barcode service
  * client   - HTTPX client implementing the resumable enrichment protocol
  * cli      - start / resume / inspect / export / explain / serve commands
"""

__version__ = "1.0.0"
