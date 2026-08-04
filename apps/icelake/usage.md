# icelake

`icelake` reads one JSON object per line on stdin and writes them into Apache
Iceberg tables on S3-compatible object storage, as ZSTD-compressed Parquet.

It is a thin wrapper around the icelake library and has no logic of its own.
Batching, the local Parquet cache and its retention, local-only mode, crash
replay and backpressure are all the library's behaviour, passed along. If
something here looks like a decision this program made, it is not.

What you need to run it: a directory it can write to, a JSON file declaring your
tables, and — eventually, not immediately — a bucket.

## Install

    mise use -g github:gvkhna/icelake   # from the first tagged release

Released binaries are the supported route: one `tar.gz` per platform — linux and
macOS, amd64 and arm64 — each containing this one binary, alongside a
`checksums.txt`. The binary is statically linked and pure Go, so it has no
runtime dependencies at all: no libc to match, no SQLite to install.

Nothing is tagged yet, so nothing is released yet. Until then, from a clone:

    go build -o icelake ./apps/icelake

There is deliberately no `go install` path and never will be: the app modules in
this repository carry a local `replace` directive, so the module proxy cannot
resolve them.

## Five minutes, no bucket

Declare a table:

    cat > schema.json <<'EOF'
    {"tables": [
      {"namespace": "market", "table": "fills", "fields": [
        {"name": "symbol", "type": "string",        "fieldid": 1},
        {"name": "price",  "type": "decimal(18,9)", "fieldid": 2},
        {"name": "ts_ms",  "type": "timestamptz",   "fieldid": 3},
        {"name": "note",   "type": "string",        "fieldid": 4, "optional": true}
      ]}
    ]}
    EOF

Point it at a directory and run it with no bucket at all:

    export ICELAKE_DATA_DIR=./icelake-data
    export ICELAKE_SCHEMA_FILE=./schema.json
    export ICELAKE_LOCAL_ONLY=true
    mkdir -p "$ICELAKE_DATA_DIR"

    your-producer | jq -c '{table:"market.fills", row:.}' | icelake run

Then read what it wrote, with DuckDB, off the local disk:

    duckdb -c "SELECT count(*), min(ts_ms), max(ts_ms)
               FROM read_parquet('./icelake-data/cache/market/fills/data/*.parquet')"

Nothing has reached a bucket. When you are ready for one, see "From local-only to
a bucket" below: the data written during this period uploads and commits by
itself on the next start.

## Commands

    icelake run          read JSON lines on stdin and write them
    icelake rebuild      rebuild the local catalog database from the bucket
    icelake usage        print this manual
    icelake version      print the version
    icelake help         print the environment variables and their defaults

`icelake run` takes no flags. It is configured entirely by environment
variables, which is what a process manager configures a child with, and what
keeps a credential from ever becoming a flag's default value — the `flag`
package prints those back in the block it produces for `-h` and for every parse
error.

`icelake rebuild` takes two flags and nothing else: `-replace` to overwrite a
catalog database that already exists, and `-dry-run` to discover and report
without writing. Everything else comes from the same environment `run` uses, so
recovering a lost catalog needs no new configuration at the moment you least
want to be writing any. It does not read `ICELAKE_SCHEMA_FILE`, because a
rebuild opens no schema document: the bucket is the only thing it is told about.
It refuses outright when `ICELAKE_LOCAL_ONLY` is true — a local-only store opens
no catalog, so there is none to rebuild.

### Exit codes

    0    clean end of input, or a signal that drained in time
    1    runtime failure — the bucket was unreachable, the disk filled, a line was bad
    2    configuration or usage refusal — restarting will not help

A supervisor should treat 1 as "restart me" and 2 as "a human has to change
something". That distinction is the reason this command does not use a flat
exit 1 the way the other tools in this repository do.

## Environment

Required always:

    ICELAKE_DATA_DIR              directory holding staging.db, catalog.db and the cache
    ICELAKE_SCHEMA_FILE           JSON schema document declaring the tables

Required unless `ICELAKE_LOCAL_ONLY` is true, and refused when it is:

    ICELAKE_ENDPOINT              S3-compatible endpoint base URL, including scheme
    ICELAKE_BUCKET                bucket the warehouse lives in
    ICELAKE_PREFIX                warehouse key prefix each table hangs under
    ICELAKE_ACCESS_KEY_ID         access key id
    ICELAKE_SECRET_ACCESS_KEY     secret access key

Paths, derived from the data directory unless set:

    ICELAKE_STAGING_PATH          default <data dir>/staging.db
    ICELAKE_CATALOG_PATH          default <data dir>/catalog.db  (refused in local-only)
    ICELAKE_CACHE_DIR             default <data dir>/cache

Everything else:

    ICELAKE_LOCAL_ONLY            default false   write to the cache with no bucket at all
    ICELAKE_REGION                default auto    region to sign requests for
    ICELAKE_FLUSH_INTERVAL        default 15m     longest a batch may sit unflushed
    ICELAKE_FLUSH_MAX_BYTES       default 128MB   bytes a batch may accumulate
    ICELAKE_FLUSH_MAX_RECORDS     default 100000  records a batch may accumulate
    ICELAKE_CACHE_MAX_AGE         default 720h    how long a committed cache file is kept
    ICELAKE_CACHE_MAX_BYTES       default 10GB    total size of committed cache files
    ICELAKE_STAGING_MAX_RECORDS   default 5000000 uncommitted records before Insert refuses
    ICELAKE_STAGING_MAX_BYTES     default 4GB     uncommitted bytes before Insert refuses
    ICELAKE_ZSTD_LEVEL            default 19      Parquet compression level, 1 to 22
    ICELAKE_SHUTDOWN_TIMEOUT      default 30s     how long a drain may take
    ICELAKE_MAX_LINE_BYTES        default 16MB    longest input line accepted

Sizes are powers of 1024 — `KB`, `MB`, `GB` and `TB` — with no fractional part.
`128MB` is 134217728 bytes; `1.5GB` is refused rather than rounded, so that
every size in a unit file is an exact number of bytes somebody can compute.
Durations are Go durations: `250ms`, `15m`, `720h`.

Upload cadence is not a separate setting. "Every fifteen minutes or every
128MB, whichever comes first" is exactly what `ICELAKE_FLUSH_INTERVAL` and
`ICELAKE_FLUSH_MAX_BYTES` already mean.

Every problem with the configuration is reported at once, before anything is
opened. A run that is misconfigured in four ways tells you all four.

No credential is ever printed. The daemon prints its resolved configuration to
stderr once at startup with the secret reported as `set` or `unset` — not
truncated, not masked in part, because a redaction that shows a prefix leaks a
prefix.

## The schema document

    {"tables": [
      {"namespace": "market", "table": "fills", "fields": [
        {"name": "symbol", "type": "string",        "fieldid": 1},
        {"name": "price",  "type": "decimal(18,9)", "fieldid": 2},
        {"name": "ts_ms",  "type": "timestamptz",   "fieldid": 3},
        {"name": "note",   "type": "string",        "fieldid": 4, "optional": true}
      ]}
    ]}

Types are exactly nine: `boolean`, `int`, `long`, `float`, `double`, `string`,
`binary`, `decimal(P,S)` and `timestamptz`. A `timestamptz` value is epoch
milliseconds as a number, or an RFC3339 string at whole-millisecond precision
(anything finer is refused rather than rounded). A `decimal(P,S)` value is
written as digits — as a JSON number or
as a string, never as a floating-point number — with at most `S` digits after
the point; `P` is at most 18. A `binary` value is standard base64 with padding.

`fieldid` is mandatory, starts at 1, and must be the field's position in the
list. **A field id is permanent.** It is how a column is identified for the rest
of the table's life, so:

- Never renumber a field. Never reuse a number a removed field had.
- Adding a column means the next unused number, and `"optional": true`, because
  the rows already written have no value for it.
- Renaming a column is changing its `name` and keeping its `fieldid`. That is a
  rename; changing the id instead is a new column that happens to look like the
  old one.

An unknown key anywhere in the document is refused rather than ignored: a
misspelled `"feildid"` that parsed as "no field id" is exactly the silent shape
change the permanence rule exists to make impossible.

## The input

One JSON object per line, always this envelope:

    {"table":"market.fills","row":{"symbol":"ABC","price":"1.234567890","ts_ms":1700000000000}}

`table` is `namespace.name` and must be a table the document declares. `row` is
the record. There is deliberately no bare-row mode and no "default table"
setting: two accepted shapes would let a malformed envelope be read as a row,
silently, which is the one failure this grammar exists to prevent. An unknown key
in the envelope itself is refused for the same reason.

Blank lines are skipped.

### What a bad line does

The run dies, loudly, naming the line number, and exits 1. That includes: a line
that is not JSON, an envelope missing `table` or `row`, a table the document does
not declare, a row with an unknown key or a value the column cannot hold, and a
line longer than `ICELAKE_MAX_LINE_BYTES`.

This is the claim the whole design rests on for a pipe, so it is worth being
exact about it:

- **Every record accepted before the bad line is durable.** It was written to the
  staging database before `icelake` returned from accepting it. Start the daemon
  again against the same `ICELAKE_DATA_DIR` and it replays and commits, exactly
  once, before it reads a byte of new input.
- **Whatever was still in the pipe was never icelake's.** It is your producer's,
  and it is your producer's to resend.

So the safe response to a malformed line is: fix the producer, restart. Nothing
is lost by that, and nothing is written twice.

### When staging fills

If the bucket is unreachable for long enough, the staging store reaches the
ceiling `ICELAKE_STAGING_MAX_BYTES` and `ICELAKE_STAGING_MAX_RECORDS` set. The
daemon then stops reading stdin and retries the record it is holding, backing off
from 100ms to 5s, for as long as it takes. It prints one line when it starts
holding and one when it resumes.

That is backpressure, and it is deliberate: it propagates up the pipe to your
producer, which already knows what to do about a slow consumer. Exiting instead
would turn a bucket outage into data loss upstream.

## Files on disk

    <data dir>/staging.db                               accepted, not yet committed
    <data dir>/catalog.db                               which metadata file is current
    <data dir>/cache/<namespace>/<table>/data/*.parquet  the local Parquet cache

**Back up `staging.db`, plus any cache file that has not been uploaded yet.**
Those are the only things here that are not reconstructable. `catalog.db` is
rebuildable from the bucket at any time (`icelake rebuild`), and a cache file
that has been uploaded and committed is byte-for-byte identical to an object
already in the bucket.

The cache is the same bytes that were uploaded, under the same names, in the same
layout — so DuckDB can read it directly:

    duckdb -c "SELECT * FROM read_parquet('<data dir>/cache/market/fills/data/*.parquet') LIMIT 10"

Files are deleted once they are older than `ICELAKE_CACHE_MAX_AGE` or the cache
is over `ICELAKE_CACHE_MAX_BYTES`, oldest first — and only ever files that have
been uploaded *and* committed. A file the bucket has never confirmed is never
deleted, however far over the bound the cache goes; the cache shrinks by itself
once the backlog commits.

## From local-only to a bucket

Run with `ICELAKE_LOCAL_ONLY=true` for as long as you like. When you want a
bucket, keep the same `ICELAKE_DATA_DIR`, drop `ICELAKE_LOCAL_ONLY`, and add:

    export ICELAKE_ENDPOINT=https://<account>.r2.cloudflarestorage.com
    export ICELAKE_BUCKET=my-bucket
    export ICELAKE_PREFIX=warehouse
    export ICELAKE_ACCESS_KEY_ID=...
    export ICELAKE_SECRET_ACCESS_KEY=...

On the next start, everything written while it was local-only is uploaded and
committed before the daemon reads any new input — each file against the shape it
was written under, in the order it was written, exactly once. Interrupting that
is safe: it resumes where it stopped.

The two modes are exclusive both ways. Local-only with a bucket configured is
refused, and so is a bucket configuration with pieces missing, because a daemon
that silently decided it was local-only would look like it was working.

## Operations

**Signals.** `SIGINT` and `SIGTERM` stop the daemon reading and start a drain
bounded by `ICELAKE_SHUTDOWN_TIMEOUT`; it exits 0 if the drain finishes. A second
signal ends the process immediately. Records that did not make it are in
`staging.db` for the next start.

**Flush errors.** A batch that cannot reach the bucket is retried by the library
on its own schedule; each failed cycle prints one line to stderr naming the
table, the batch and the error. Nothing is lost while that is happening — the
records stay staged — but a table whose lines keep appearing is a table that is
not committing, and eventually staging fills and backpressure starts.

**systemd:**

    [Service]
    ExecStart=/usr/local/bin/icelake run
    StandardInput=socket
    Environment=ICELAKE_DATA_DIR=/var/lib/icelake
    Environment=ICELAKE_SCHEMA_FILE=/etc/icelake/schema.json
    EnvironmentFile=/etc/icelake/env          # the credentials
    Restart=on-failure
    RestartPreventExitStatus=2                # a configuration refusal will not fix itself

**mise:**

    [tasks.ingest]
    run = "my-producer | icelake run"

## For agents

Terse recap of everything above.

- Binary reads NDJSON on stdin; one envelope per line:
  `{"table":"<ns>.<name>","row":{...}}`. No bare rows. Unknown keys refused.
- Configuration is environment-only. `icelake run` accepts no flags. Required:
  `ICELAKE_DATA_DIR`, `ICELAKE_SCHEMA_FILE`. Bucket mode also requires
  `ICELAKE_ENDPOINT`, `ICELAKE_BUCKET`, `ICELAKE_PREFIX`,
  `ICELAKE_ACCESS_KEY_ID`, `ICELAKE_SECRET_ACCESS_KEY`; set
  `ICELAKE_LOCAL_ONLY=true` instead to run with no bucket, in which case those
  five and `ICELAKE_CATALOG_PATH`, `ICELAKE_CACHE_MAX_AGE` and
  `ICELAKE_CACHE_MAX_BYTES` must be unset.
- Exit codes: 0 clean, 1 runtime, 2 configuration. Never retry a 2.
- A malformed line kills the run at that line. Records before it are durable and
  replay on the next start; the rest of the pipe was never accepted. Fix the
  producer and restart — no deduplication is needed, the batch key is a content
  hash.
- Schema types: `boolean int long float double string binary decimal(P,S)
  timestamptz`. `fieldid` is mandatory, 1..N in order, and permanent. New columns
  get the next id and `"optional": true`.
- Decimals are digits, as a JSON number or a string, never a float. Timestamps
  are epoch millis or RFC3339 at whole-millisecond precision. Binary is standard
  base64.
- `staging.db` and any never-uploaded cache file are the only unrecoverable
  state. `catalog.db` is rebuildable with `icelake rebuild`.
- Sizes are powers of 1024, no fractions. Durations are Go durations.
