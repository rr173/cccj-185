# Resumable Barcode-Normalization Enrichment Pipeline

A Python + **HTTPX** + **SQLite** pipeline that enriches JSONL source rows with
normalized barcodes from a partner batch service — designed around the hard
parts of that integration:

* the service accepts work first and returns pages **later**, in **any order**;
* a batch can contain definitively invalid barcodes, still-pending slots and
  retransmitted/duplicated items;
* processes die between the service accepting a POST and the client learning
  the `job_id`, and between result pages landing and paging progress being
  confirmed;
* the same barcode under a different `market` or `rule_version` is a different
  input and may get a different answer.

Everything needed to recover lives in one SQLite database. Nothing recoverable
is kept only in memory, and the final normalized result is written back onto
**every original line** — the service response is never treated as the delivery.

## Layout

```
barcode_pipeline/
  digest.py   canonical SHA-256 input identity, body digest, idempotency keys
  config.py   JSON configuration (URL, batch size, ingestion mode, retry budget)
  db.py       SQLite schema and all persistence/coverage/contradiction logic
  server.py   controllable local HTTP mock of the partner service (stdlib)
  client.py   HTTPX client: ingest, dispatch, paging, retry, export
  cli.py      start / resume / inspect / export / explain / serve commands
  __main__.py entry point that works under `python -I`
tests/        unittest suite (hand-written expectations, no oracle reuse)
vendor/       vendored httpx + pure-Python dependencies (no system pip needed)
```

## Quick start

```bash
# 1. start the controllable mock service
python3 -I barcode_pipeline/__main__.py serve --port 8091 \
    --snapshot demo/server-state.json --log demo/server-submit.jsonl

# 2. give it a scenario (rules, page order, faults, omissions, ...)
curl -sS -X POST http://127.0.0.1:8091/control/scenario \
    -H 'Content-Type: application/json' -d @demo/scenario.json

# 3. run the pipeline against a JSONL source
python3 -I barcode_pipeline/__main__.py --config demo/config.json \
    start --db demo/pipe.db --source tests/fixtures/main.jsonl

# 4. inspect / explain / export
python3 -I barcode_pipeline/__main__.py --config demo/config.json \
    inspect --db demo/pipe.db [--json]
python3 -I barcode_pipeline/__main__.py --config demo/config.json \
    explain --db demo/pipe.db --line 2
python3 -I barcode_pipeline/__main__.py --config demo/config.json \
    export  --db demo/pipe.db --output demo/out.jsonl

# after any crash, network outage or unfinished server slots:
python3 -I barcode_pipeline/__main__.py --config demo/config.json \
    resume --db demo/pipe.db
```

`-I` (isolated mode) is used throughout so an untrusted working directory can
not shadow stdlib modules; the entry script adds `vendor/` and the package
directory explicitly.

## Source and output contracts

Each source line is a JSON object containing at least
`barcode`, `market`, `rule_version` plus any other business columns.
The **1-based line number is the occurrence identity**: two byte-identical
lines produce two delivery rows.

The complete service input (and quota unit) is the triple
`(barcode, market, rule_version)`; its `input_digest` is the canonical
SHA-256 over those three fields. All distinct source lines sharing a digest
map to one service slot, while all their occurrence mappings are retained.

Each output line contains, in original line order:

* the full original business row (`source`);
* `state` = `normalized` or `invalid`, plus `normalized_value` / `error_code`;
* the `client_slot`, server `job_id` and a `result_source` block
  (`job_id`, `slot`, `page_index`, `payload_hash`, `input_digest`) proving
  where the value came from.

## Recovery model

1. **Deterministic idempotency keys.** A key is derived from the sealed batch
   body (`idempotency_key`), never random. The job is persisted locally as
   `sealed` *before* the POST. If the response is lost, resume re-POSTs the
   same key and the service returns the same `job_id`; a new key is never
   minted to route around a possibly-succeeded request.
2. **Page + cursor commit together.** Each result page, its observations and
   the next paging cursor are written in one SQLite transaction. A kill after
   the rows landed simply re-fetches the same page; identical retransmissions
   collapse onto one content-addressed observation.
3. **Result-set epochs.** If the server rebuilt its result set (its paging
   tokens carry a generation), a `stale_token` 404 starts a new local epoch
   and re-reads from page 0. Superseded pages stay in `page_log` for audit.
4. **Network vs. business failures are distinct.** Exhausting the retry
   budget marks a job `incomplete` (resumable, results kept). A server-side
   `pending` slot also stays `incomplete` and is **never** rewritten to
   `invalid`. Only a service-returned business `invalid` is delivered as such.
5. **Coverage gate, not item counts.** `complete=true` only proves paging
   ended. A job is confirmed when every expected slot holds exactly one
   consistent final observation. Unknown slots, foreign `job_id`, input
   digest mismatches are rejected; two different finals for one slot record
   both payloads (one flagged `contradicting`) and block the job — the
   pipeline never picks an answer.
6. **Delivery gate.** Export is refused until every input is confirmed and no
   job is `sealed/submitted/fetching/incomplete/blocked`. An existing output
   path is refused unless `--force`; export is streamed and registered in the
   `exports` table. An empty source is a valid zero-row delivery.

## Ingestion modes

* `streaming` (default): lines are imported, de-duplicated and sealed in
  batches while the file is scanned; slots follow first-appearance order and
  are globally unique. A source that grows later joins new lines to slots that
  may already be confirmed, and only genuinely new inputs create jobs.
* `prescan`: the whole source is imported first, the unique input domain is
  built (ordered by digest), then jobs are dispatched.

Both modes produce identical business output and result payloads for the same
source (covered by `test_both_ingestion_modes_agree`).

## Mock service control surface

`POST /control/scenario` accepts:

| key | meaning |
| --- | --- |
| `rules` | `"<barcode>|<market>|<rule_version>": "001"` or `"INVALID:CODE"` |
| `page_size`, `page_order` (`asc`/`desc`) | automatic paging, any order |
| `custom_pages` | explicit pages of slot lists, enabling omission + duplication |
| `pending_slots` | slots returned as business `pending` until `/control/resolve-pending` |
| `contradictions` | extra, different final payload per slot |
| `spoof` | `unknown_slot` / `bad_digest_slot` / `wrong_job_id_slot` |
| `faults` | per-endpoint FIFO queues of `404`, `503`, `reset` (POST only) |

Other endpoints: `GET /control/log` (submit/replay/conflict events),
`POST /control/resolve-pending`, `POST /control/reset`, `GET /health`.
When `--snapshot`/`--log` are supplied, all service state survives a restart.

The partner protocol implemented by the mock:

* `POST /jobs` requires `Idempotency-Key` and a recomputed `body_digest`
  (mismatch → 422; same key + same body → same `job_id`; same key + different
  body → 409; slots must be unique);
* `GET /jobs/{id}` → `{job_id, status, expected_slots}`;
* `GET /jobs/{id}/results?token=...` →
  `{items, complete, next_token, current_token}` with opaque,
  generation-bound, immutable tokens.

## Tests

```bash
python3 -I tests/run_all.py
```

18 tests, all expectations hand-written (the back-filler is never used to
generate them). Highlights:

* main walkthrough: 4 lines, EU page before CN/BAD, duplicate CN mapping,
  business invalid; both ingestion modes agree;
* two real **new-process** resumes after SIGKILL at the two required
  interruption points, with server submission-log and slot/coverage checks;
* omitted slot + duplicated slot keeping the item total equal → `incomplete`,
  then repair + resume; contradictory finals → `blocked`, both payloads kept;
* idempotency-key/body conflict, wrong digest, transient 404/503 retries,
  retry exhaustion then recovery, empty input, existing-output refusal,
  pending-then-resolved, spoofed slot/digest/job-id rejection;
* identical duplicate lines, 3000-line streaming file with small batches,
  late source-line mapping onto an already confirmed slot, service snapshot
  restart.
