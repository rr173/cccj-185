# Historical barcode warehouse (`warehouse` subcommand)

A second, independent SQLite database that consumes **already completed**
enrichment databases (the ones produced by `start`/`resume`/`export`) and adds
the two time dimensions a procurement fact store needs:

* **business time** — partner claims carry a validity interval
  `[valid_from, valid_to)` (signed 64-bit integers; `null` end means
  +infinity); a fact is associated with the barcode valid *when the fact
  happened*;
* **knowledge time** — every successfully imported change batch gets a
  monotonically increasing `knowledge_seq` assigned in admission order (never
  from the machine clock or business time).  Any old seq stays queryable
  forever, so both “what should the barcode have been?” and “what did we know
  when the last report ran?” are answerable.

The warehouse never modifies the source DB (opened strictly read-only, one
consistent snapshot per import), never calls a network service, and copies all
required evidence into its own tables — moving or deleting the source DB after
import does not affect old-seq reproducibility.

## Change file contract (JSONL)

Two row shapes only:

```json
{"claim_id": "A", "revision": 1, "source_line": 1, "valid_from": 10, "valid_to": 40}
{"claim_id": "A", "revision": 2}
```

* **upsert** keys are exactly `claim_id, revision, source_line, valid_from,
  valid_to`. `source_line` is the 1-based `source_rows.line_no` in the
  enrichment DB. The product key `(supplier, item_id, market)` and the answer
  (confirmed `normalized` value or `invalid` error code plus the full
  `job/slot/page/payload_hash` provenance) are read from that DB — the change
  file never supplies an answer itself. Source business rows must contain
  `supplier` and `item_id`; `market` comes from the same row.
* **retract** rows contain exactly `claim_id, revision` and withdraw the whole
  claim.
* `revision` is a positive integer; low revisions may arrive late. Every
  revision replaces the whole claim (no field patching). The first appearance
  of a claim must be an upsert; later upserts cannot change its product key;
  after a retraction only a strictly higher revision can re-establish the
  claim. `valid_from` must be smaller than a finite `valid_to`.
* Same `claim_id`/`revision` with identical full content (parsed source row,
  service result **and** provenance — not just the normalized value) is a
  duplicate; any difference rejects the whole batch. Batch-internal duplicates
  follow the same rule.

## Batches, seqs and idempotency

The caller names every batch with `--batch-id`:

* same `batch_id` + identical change-file bytes + identical resolved source
  evidence → the original `knowledge_seq` is returned (`replayed: true`), no
  new batch row;
* same `batch_id` with different material → rejected;
* a different `batch_id` whose rows are all consistent duplicates is still a
  new admitted batch with its own seq (the ledger records
  `new_revisions=0, duplicate_rows=N`).

Validation runs entirely before the single write transaction
(`BEGIN IMMEDIATE`), so bad tail records, missing source lines, undeliverable
source DBs, interval/field errors and revision conflicts all roll back with no
visible revision and no consumed seq. Concurrent writers serialize and never
share a seq; concurrent readers only ever see whole committed batches. A
SIGKILL while a batch is staged leaves nothing committed — the same
`batch_id` retried in a new process is admitted exactly once.

## Answer model

For a query at seq `s`, each claim contributes its **highest revision admitted
at seq ≤ s**; if that revision is a retraction the claim does not exist. Only
then is the surviving interval tested against the business time. All covering
claims are combined — never ranked by revision number or import time:

| state | when |
| --- | --- |
| `normalized` | all covering claims agree on one normalized value (all supports listed) |
| `invalid` | all covering claims are invalid with the same error code |
| `conflict` | values differ, error codes differ, or normalized/invalid mix; every disagreement and its supports are listed |
| `missing` | no claim covers the point (never conflated with invalid) |

## Commands

All commands are offline and work under `python3 -I`.

```bash
# admit one change batch against a read-only enrichment DB
python3 -I barcode_pipeline/__main__.py warehouse import \
    --warehouse wh.db --source pipe.db \
    --changes changes-s1.jsonl --batch-id s1

# point lookup (--seq defaults to latest, pinned at operation start)
python3 -I barcode_pipeline/__main__.py warehouse lookup \
    --warehouse wh.db --supplier sup1 --item-id P --market CN \
    --event-time 27 --seq 3

# maximal constant segments over a finite half-open range; a new segment
# starts whenever state/value/code OR the supporting claim/revision set changes
python3 -I barcode_pipeline/__main__.py warehouse timeline \
    --warehouse wh.db --supplier sup1 --item-id P --market CN \
    --from 10 --to 40 --seq 3

# stream fact JSONL (supplier,item_id,market,event_time + any extra columns),
# preserving input order and exact duplicates; output must not pre-exist
python3 -I barcode_pipeline/__main__.py warehouse enrich \
    --warehouse wh.db --facts facts.jsonl --output enriched.jsonl [--seq N]

# recompute list between two seqs: only state or value/error-code changes are
# emitted; evidence-only changes do not create impact rows
python3 -I barcode_pipeline/__main__.py warehouse impact \
    --warehouse wh.db --facts facts.jsonl --output impact.jsonl \
    --old-seq 2 --new-seq 3

# ledger: all batches and claim heads; add --batch-id/--claim for full detail
python3 -I barcode_pipeline/__main__.py warehouse inspect \
    --warehouse wh.db [--json] [--batch-id s1] [--claim A]
```

### Programmatic API

```python
from barcode_pipeline.warehouse import (
    open_warehouse, import_batch, lookup, timeline, enrich_file, impact_file,
)
```

`lookup` / `timeline` return the answer plus a `supports` list whose entries
trace to `batch_id`/`knowledge_seq`, `claim_id`/`revision`, the source DB line
and the service `job_id/slot/page_index/payload_hash/input_digest`.

## Fact enrichment semantics

* The seq is resolved **once when the operation starts** (default `latest`).
  A batch imported by another process mid-run cannot make one output file mix
  warehouse versions; every output row echoes the pinned `knowledge_seq`.
* Each output line contains the full original fact (extra columns and
  duplicate `fact_id`s included), its 1-based input `line_no`, one of the four
  states, the value/error code and an explanation with all supporting claims
  (plus `disagreements` on conflicts).
* Outputs are written to a `.tmp` file and atomically renamed; an existing
  target is refused and a failed run leaves no success-shaped file.

## Reproducible demos and tests

```bash
# canonical hand-checked walkthrough (s1..s4, conflict, dual support,
# retract/recover, invalid merge, isolation, half-open endpoints, impact)
python3 -I demo/warehouse/walkthrough_demo.py --workdir /tmp/wh-demo

# 20,000 claims / 200,000 facts generated at runtime, checked under a
# 256 MiB RLIMIT_AS process cap (counts, anchors, peak RSS)
python3 -I demo/warehouse/scale_driver.py build --workdir /tmp/wh-scale
python3 -I demo/warehouse/scale_driver.py check \
    --workdir /tmp/wh-scale --output /tmp/wh-scale/out.jsonl

# the whole suite (pipeline + warehouse)
python3 -I tests/run_all.py
```

The warehouse test modules are `tests/test_warehouse_walkthrough.py`,
`tests/test_warehouse_semantics.py` and
`tests/test_warehouse_scale_and_pinning.py`. They build enrichment DBs with the
real pipeline and mock service (never by hand-editing tables), cover whole-
batch rollback (tail bad record, same-revision different content), a real
SIGKILL-then-retry admission, concurrent imports, source-DB deletion, version
pinning during a long fact read, and the 256 MiB streaming check. Runtime-
generated scale data is written only to the chosen workdir, never into the
repository.
