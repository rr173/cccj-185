# Offline warehouse replication (`origin` / `replica` / `package`)

The report machine cannot reach the partner service, cannot obtain the raw
enrichment library and only receives history by physically carried packages.
This subsystem gives a second machine a **managed local copy** of a historical
warehouse that directly serves the existing `lookup`, `timeline`, `enrich` and
`impact` commands — no second query language, no re-enrichment, no network.

The decision "does this package actually attach to the history I hold?" is made
from **logical prefix digests**, never from the largest `knowledge_seq`.

## Roles

| role | how created | writes |
| --- | --- | --- |
| plain | an ordinary `wh1` warehouse | normal `warehouse import` only |
| source | `warehouse origin init` on a plain warehouse | normal `warehouse import`; every batch also writes a replication registration |
| replica | `warehouse package apply` of a full-prefix package onto an *empty* warehouse | only package acceptance; direct `warehouse import` is refused |

The origin identity and role are logical rows in `wh_meta`.  Moving the
database file keeps the identity; a physical file/SQLite backup **copies** it,
which is exactly why origin+seq cannot distinguish two branches — the prefix
digest can.

## Logical prefix digest (exact definition)

Scheme id `wh1-prefix-v1`; all hashing is SHA-256 over the project's canonical
JSON bytes (UTF-8, sorted keys, compact separators, `ensure_ascii=False`).

1. **Revision unit.** Identical in field scope and ordering to the
   admission-time `content_digest`.
   * retraction: exactly `{"claim_id", "revision", "kind": "retract"}`;
   * upsert: exactly the keys `claim_id, revision, kind, supplier, item_id,
     market, valid_from, valid_to, source_line, source, business_status,
     normalized_value, error_code, result_source`.  `source` is the parsed
     original partner business row; `result_source` is the service provenance
     (`job_id, slot, page_index, payload_hash, input_digest`).

   Identity therefore covers the original business columns **and** the service
   answer/provenance, not just the current normalized value or a claim header.
2. **Batch unit.** For seq `s`, the JSON array `[batch, revisions]` where
   * `batch` has exactly `knowledge_seq, batch_id, change_digest,
     evidence_digest, source_db_path, source_db_digest, change_count,
     new_revisions, duplicate_rows, retractions` (ledger identity columns; no
     physical row id, status, detail or export clock);
   * `revisions` is the revisions *first admitted* by that batch in
     `wh_claim_revisions.id` order — an empty list for a duplicate-only batch,
     which still contributes a batch unit.
3. **Prefix chain.** `H(0) = sha256("wh1-prefix-v1 empty")` and, for every
   admitted batch in seq order,
   `H(s) = sha256(H(s-1) + "|" + sha256(batch_unit(s)))` (lowercase hex).

The digest is a pure function of logical content: SQLite page layout, the
database file name, absolute paths and `created_at` clocks cannot change it.
`H(n)` is stored per seq in `wh_replication_log`, written in the **same
transaction** as the batch it describes.

## Source operations

```bash
# One-time origin binding. Works on an existing wh1 warehouse that already
# holds history: the replication log is back-filled and nothing is renumbered.
python3 -I barcode_pipeline/__main__.py warehouse origin init \
    --warehouse wh.db [--origin-id <32 hex chars>]

# Afterwards the normal import is still the only write path; the batch and
# its replication registration commit together.
python3 -I barcode_pipeline/__main__.py warehouse import \
    --warehouse wh.db --source pipe.db --changes c.jsonl --batch-id b5
```

`origin init` is repeatable but cannot change the identity, renumber seqs or
modify business history.  A replica can never be re-initialized as a source.

## Packages

Exported range is the half-open interval `(after, through]`; `after=0` is the
empty prefix and `through=latest` is resolved once, at start, against one
pinned read-only snapshot.  A concurrent import during export cannot change the
declared boundaries, registrations or content.

```bash
python3 -I barcode_pipeline/__main__.py warehouse package export \
    --warehouse wh.db --after 0 --through 2 --output p02.pkg
python3 -I barcode_pipeline/__main__.py warehouse package export \
    --warehouse wh.db --after 1 --through latest --output p1n.pkg
```

A package is a **self-describing framed byte stream, not a SQLite file**
(magic `BCPWHPKG`, protocol version 1).  It carries: a manifest (protocol
version, origin, both boundary seqs and both prefix digests), one frame per
admitted batch in range (full batch registration plus every first-admitted
revision and its query-required source — duplicate-only batches included, with
an empty revision list), and a footer with the recomputed chain and counts.
Every frame has its own SHA-256 checksum.  No history before `after` and no
SQLite database bytes are embedded.  An existing target is refused; an
unfinished export leaves only a `.tmp` staging file.

## Inspecting before accepting (read-only, never mutates)

```bash
python3 -I barcode_pipeline/__main__.py warehouse package inspect \
    --warehouse replica.db --package p1n.pkg
```

The JSON report's `verdict` is one of:

| verdict | meaning |
| --- | --- |
| `appendable` | overlap region is byte-identical; a verifiable suffix would be added |
| `complete_replay` | the whole range is already present with identical content; nothing added |
| `missing_prefix` | the package starts beyond the target's prefix; the left boundary was never established here |
| `origin_mismatch` | package origin differs from the bound replica origin |
| `overlap_diverged` | same origin, overlapping seq, but different content (a branch): business columns, service source, ledger fields or prefix digest differ |
| `material_corrupt` | bad magic/checksum, truncation, dropped/extra frame, unknown protocol version, count/interval/provenance violation or wrong digest |
| `ineligible_target` | target is a plain business warehouse, not a managed replica |

The CLI exits non-zero on every non-append/non-replay verdict so scripts can
branch.  Inspection never writes.

## Acceptance

```bash
# On an empty warehouse, the first full-prefix (after=0) package binds origin.
python3 -I barcode_pipeline/__main__.py warehouse package apply \
    --warehouse replica.db --package p02.pkg
python3 -I barcode_pipeline/__main__.py warehouse package apply \
    --warehouse replica.db --package p1n.pkg

python3 -I barcode_pipeline/__main__.py warehouse replica status \
    --warehouse replica.db
```

Rules:

* the unit of acceptance is the **whole package** — full material is verified
  before any write; a bad tail rolls everything back, leaving no partial
  prefix;
* target prefix `c`: a package starting at `after > c` (or `after == c` with no
  overlap) is a gap and rejected; when `after <= c`, the entire overlap is
  compared and only the missing suffix is appended;
* a package fully inside the held history with identical content is a replay:
  no new batch, no seq movement, but the material is re-checked in full (a
  seen batch_id never skips verification);
* same terminal seq, same `batch_id` or same normalized value is **not**
  equality — different original business fields or service source diverge and
  are rejected; different origins are rejected outright;
* revisions keep their original seq/batch/first-admission position, retraction
  records, late low revisions and source explanations; they are never
  re-imported as new seqs;
* the carried `source_db_path` is descriptive provenance only and is never
  reopened — acceptance connects to no source DB, enrichment DB or network;
* concurrent receivers serialize on an advisory lock plus the SQLite write
  transaction, re-read the committed position inside the lock and converge on
  one prefix without duplicate seq allocation; readers see only the old or the
  new complete prefix;
* a SIGKILL before commit leaves either the old prefix (resend then applies
  once) or, after commit, a whole package that verifies as a replay — no
  manual database rebuild is ever required.

## Public API

```python
from barcode_pipeline.warehouse import (
    initialize_origin, initialize_replica, replica_status,
    export_package, inspect_package, apply_package,
    # existing query/fact APIs work unchanged on a replica:
    lookup, timeline, enrich_file, impact_file,
)
```

`import_batch` raises on a managed replica, through both the API and the CLI.

## Migration boundaries

* Plain `wh1` warehouses keep working unchanged; the replication log table is
  created lazily and stays empty until `origin init`.
* Only an **empty** plain warehouse can become a replica; an existing business
  warehouse cannot be overwritten by impersonating an empty copy.
* Replicas are single-origin: a package from another origin is rejected.
* Package data and large generated datasets are operational artifacts carried
  out-of-band, never committed to the repository.
