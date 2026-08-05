# icelake

`icelake` reads records from stdin — JSON objects one per line, CBOR items, or
both in the same pipe — and writes them into Apache Iceberg tables on
S3-compatible object storage, as ZSTD-compressed Parquet.

It is a thin wrapper around the icelake library and has no logic of its own.
Batching, the local Parquet cache and its retention, local-only mode, crash
replay and backpressure are all the library's behaviour, passed along. If
something here looks like a decision this program made, it is not.

What you need to run it: a directory it can write to, a JSON file declaring your
tables, and — eventually, not immediately — a bucket.

## Install

    mise use -g github:gvkhna/icelake

Released binaries are the supported route: one `tar.gz` per platform — linux and
macOS, amd64 and arm64 — each containing this one binary, alongside a
`checksums.txt`. The binary is statically linked and pure Go, so it has no
runtime dependencies at all: no libc to match, no SQLite to install. mise
resolves the latest stable release and upgrades with `mise up`; or build from
a clone:

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

    icelake run          read records on stdin and write them
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
    ICELAKE_MAX_LINE_BYTES        default 16MB    longest record accepted (a JSON line, or a CBOR item)

Sizes are powers of 1024 — `KB`, `MB`, `GB` and `TB` — with no fractional part.
`128MB` is 134217728 bytes; `1.5GB` is refused rather than rounded, so that
every size in a unit file is an exact number of bytes somebody can compute.
Durations are Go durations: `250ms`, `15m`, `720h`.

Upload cadence is not a separate setting. "Every fifteen minutes or every
128MB, whichever comes first" is exactly what `ICELAKE_FLUSH_INTERVAL` and
`ICELAKE_FLUSH_MAX_BYTES` already mean.

`ICELAKE_MAX_LINE_BYTES` bounds **any** record: one JSON line, not counting its
terminator, or one CBOR data item. The name says "line" because it was named
before CBOR arrived and renaming a variable an operator already has in a unit
file would break that unit file for no gain. It is a real tension and it is
written down here rather than quietly resolved: the variable means the longest
record, and it will keep its name.

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

Under `cbor` the same nine types take the same values in that format's own
spellings: an integer for `int`, `long` and epoch-millisecond `timestamptz`; a
text string for `string`, for an RFC3339 `timestamptz` and for a `decimal`'s
digits; a byte string for `binary`, with **no base64** — the bytes are the value;
a float for `float` and `double` and never for a `decimal`. An integer is also
accepted for a `decimal` with no fraction digits, exactly as a JSON integer is.
The refusals are the same refusals: a float in a decimal column, a timestamp
finer than a millisecond, an integer outside its column's range.

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

One envelope per record, always this shape:

    {"table":"market.fills","row":{"symbol":"ABC","price":"1.234567890","ts_ms":1700000000000}}

`table` is `namespace.name` and must be a table the document declares. `row` is
the record. There is deliberately no bare-row mode and no "default table"
setting: two accepted shapes would let a malformed envelope be read as a row,
silently, which is the one failure this grammar exists to prevent. An unknown key
in the envelope itself is refused for the same reason.

### Two encodings, no setting

The envelope may be written as JSON or as CBOR, and **which one it is, is decided
per record, from that record's own first byte**. There is no setting for it
anywhere:

- A record starting with `{` is a **JSON envelope**. It runs to the next newline,
  LF or CRLF. Ordinary NDJSON.
- A record starting with a **CBOR map header** — a byte from `0xa0` to `0xbb` —
  is one CBOR data item, and the item is the whole record. That is a CBOR
  sequence (RFC 8742): no separator, no newlines, because a CBOR item already
  says how long it is.
- Whitespace between records — spaces, tabs, CR, LF — is skipped. That is what
  makes a blank line a blank line.
- Anything else is refused at that record, naming its position. A UTF-8
  byte-order mark is refused rather than skipped: it is not whitespace, and a
  stream that starts with one was written by a tool that thinks it is producing
  text for a person.

**The two starting bytes cannot collide, which is why no setting is needed.** In
CBOR, `0x7b` — the byte `{` — begins a text string, and a bare text string is not
a map and can never be an envelope. In JSON, nothing at all begins with `0xa0` to
`0xbb`. So one byte decides, and nothing here is guessing.

What that buys you is the thing a setting could not: **two producers can write
into the same pipe, one sending JSON and one sending CBOR, interleaved, with
nothing configured and no restart.**

    { your-json-producer & your-cbor-producer & } | icelake run

The CBOR envelope is a map with exactly the two text keys `table` and `row`, and
`row` is a map with text keys. A few things are refused that a permissive decoder
would accept, and each is refused for the same reason the JSON side refuses a
second spelling of a value:

- **Every CBOR tag**, including tag 0 and tag 1 for date/time and tags 2 and 3
  for bignums. See "The schema document" below for what a `timestamptz` and a
  `decimal` take instead. If your encoder tags timestamps by default, turn that
  off.
- **`undefined`**, which is not the same thing as `null`.
- **Indefinite-length maps, arrays and strings.** A value has one encoding.
- A map with the same key twice.

What CBOR buys a producer is exactness with no encoding step: an integer is an
integer rather than digits to re-parse — including an integer past 2^53, which
JSON cannot carry safely — and a `binary` column takes a byte string as it stands
rather than base64. What it costs is that a pipe is no longer something you can
read with `head`.

### Which number an error names

Every refusal names the **record number**, counting from one, because that is the
one coordinate every record has. It names the **line number** too when the record
was a JSON one, because that is what you look at in a file:

    icelake: record 3 (line 5): ...

A CBOR record is not on a line, so it gets a record number and nothing else:

    icelake: record 3: ...

In a stream of nothing but JSON the two numbers differ only by the blank lines
before the record, which are lines and are not records. A file with two blank
lines before its third record reports `record 3 (line 5)`.

### How much it reads at once

Records are written in chunks rather than one at a time. The daemon blocks for the
first record of a chunk, then takes whatever else has already arrived without
waiting for it, and writes the whole chunk to the staging database in one
transaction per table. That transaction is one disk sync instead of one per
record, which is the difference between a few hundred records a second and six
figures.

The read-ahead this costs is bounded and is a **record count**:

- At most **1024 records** sit between the pipe and the chunk being written.
- A chunk is at most **4096 records**, or **8 MiB of record bytes plus one
  record**, whichever comes first. The "plus one record" is exact rather than a
  hedge: the daemon checks the size before it takes the next record, not after,
  so the record that crosses 8 MiB is still part of the chunk. At the default
  `ICELAKE_MAX_LINE_BYTES` of 16 MiB that makes the true worst case 24 MiB.

Everything past that is still in the pipe, which is what makes the backpressure
below work at all. The memory the bound costs is the record count times your
actual record length, so raising `ICELAKE_MAX_LINE_BYTES` a long way raises the
worst case with it.

None of those four numbers is this command's; they are the library's defaults,
and this page quotes them.

### What a bad record does

The run dies, loudly, naming the record — see "Which number an error names" above
— and exits 1. That includes: a byte that starts neither kind of record, bytes
that are not JSON or not a CBOR item, an envelope missing `table` or `row`, a
table the document does not declare, a row with an unknown key or a value the
column cannot hold, and a record longer than `ICELAKE_MAX_LINE_BYTES`.

This is the claim the whole design rests on for a pipe, so it is worth being
exact about it:

- **Every record accepted before the bad line is durable.** It was written to the
  staging database before `icelake` moved on. Start the daemon again against the
  same `ICELAKE_DATA_DIR` and it replays and commits, exactly once, before it
  reads a byte of new input.
- **Whatever was still in the pipe was never icelake's.** It is your producer's,
  and it is your producer's to resend.

**Records are written in chunks, so one extra step is taken to keep that promise
exact, and it is worth knowing it happens.** A bad line caught by the framing —
not JSON, a bad envelope, an undeclared table — is caught while the
chunk is being read, before anything in that chunk has been written, so the chunk
is simply cut at that line and everything before it is written and durable. A bad
line the *library* catches — a row with an unknown key, or a value its column
cannot hold — is caught while the chunk is being written, and it refuses that
table's whole group, because the group is one transaction. The group is then
written again, cut at the bad line, so the lines before it are durable too.
Either way the rule holds without an asterisk: everything before the bad line is
on disk, and the daemon dies naming that line.

The one thing it cannot undo is a group it had already finished writing. That is
only reachable when a single chunk carries lines for two different tables, and
what it means is that a few of the *other* table's lines from after the bad line
may be durable as well.

So the safe response to a malformed line is unchanged: fix the producer, restart.
Resending from the failed line is what the daemon's own report asks for, and on a
single-table stream nothing is written twice. On a stream that interleaves two
tables in one chunk, a resend from the failed line can rewrite the handful of the
other table's lines that had already been committed inside it; if you cannot
tolerate that, send one table per pipe.

### When staging fills

If the bucket is unreachable for long enough, the staging store reaches the
ceiling `ICELAKE_STAGING_MAX_BYTES` and `ICELAKE_STAGING_MAX_RECORDS` set. The
daemon then stops reading stdin and retries what it is holding, backing off from
100ms to 5s, for as long as it takes. It prints one line when it starts holding
and one when it resumes.

What it is holding is now a chunk's worth of one table's rows rather than a
single record, and the ceiling refuses that the same way it refuses a record: all
or nothing, every time. A group that does not fit is halved and tried again
before anything sleeps, down to a single record — so the daemon writes the
largest prefix that currently fits instead of stalling on a group that is simply
bigger than your whole ceiling — and it only reports holding when one record does
not fit. Every attempt is a transaction that either lands whole or does not land
at all, and records are always taken in input order, so there is never a state
where part of a group landed and the daemon has to work out which part.

That is backpressure, and it is deliberate: it propagates up the pipe to your
producer, which already knows what to do about a slow consumer. Exiting instead
would turn a bucket outage into data loss upstream.

**One case holds stdin forever rather than for as long as it takes, and it is not
a bucket problem.** A single record whose encoded size is larger than
`ICELAKE_STAGING_MAX_BYTES` can never fit, however empty the staging store gets,
so the daemon halves down to that one record and then waits on it for good. It
says so on stderr with the line number, once, and then goes quiet. Two things fix
it and nothing else does: raise `ICELAKE_STAGING_MAX_BYTES` above that record's
size, or fix the producer that is sending a record that large. A staging ceiling
smaller than a single record is a misconfiguration, and this is what it looks
like from the outside.

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
bounded by `ICELAKE_SHUTDOWN_TIMEOUT`; it exits 0 if the drain finishes. That is
true whether the signal arrives between chunks or in the middle of writing one:
a chunk the signal cut off was refused whole, so those records were never
accepted and are still the producer's, exactly as if the signal had landed a
moment earlier. A second signal ends the process immediately. Records that did
not make it are in `staging.db` for the next start.

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
- The same envelope may also be a **CBOR map**, and there is **no setting** for
  it: a record starting with `{` is JSON to the newline, a record starting with
  `0xa0`-`0xbb` is one CBOR item (RFC 8742 sequence, no separators). The two
  starting bytes cannot collide, so one pipe can carry both, interleaved. Refused
  in CBOR: every tag (0, 1, 2, 3 included), `undefined`, indefinite-length items,
  duplicate keys. Byte strings go into `binary` columns as they stand, no base64.
  Integers are exact, including past 2^53.
- Errors name `record N`, plus `(line M)` when the record was JSON. A CBOR record
  is on no line. Blank lines are lines and are not records.
- Configuration is environment-only. `icelake run` accepts no flags. Required:
  `ICELAKE_DATA_DIR`, `ICELAKE_SCHEMA_FILE`. Bucket mode also requires
  `ICELAKE_ENDPOINT`, `ICELAKE_BUCKET`, `ICELAKE_PREFIX`,
  `ICELAKE_ACCESS_KEY_ID`, `ICELAKE_SECRET_ACCESS_KEY`; set
  `ICELAKE_LOCAL_ONLY=true` instead to run with no bucket, in which case those
  five and `ICELAKE_CATALOG_PATH`, `ICELAKE_CACHE_MAX_AGE` and
  `ICELAKE_CACHE_MAX_BYTES` must be unset.
- Exit codes: 0 clean, 1 runtime, 2 configuration. Never retry a 2.
- Records are written in chunks: block for one, take whatever else has already
  arrived, write one transaction per table. Read-ahead is at most 1024 records; a
  chunk is at most 4096 records, or 8 MiB plus one record. All four are the
  library's defaults, not this command's.
- A malformed line kills the run at that line, and everything before that line is
  durable — including when the library is the one that refuses a row, because the
  daemon rewrites that table's group cut at the bad line. The only extra is that
  a chunk carrying two tables may already have committed the other table's later
  lines. Fix the producer and restart — replay is exactly once, and on a
  single-table stream a resend from the failed line writes nothing twice.
- Staging-full refuses a whole group, never part of one; the daemon halves the
  group and retries, down to one record, and holds stdin only when one record
  does not fit.
- Schema types: `boolean int long float double string binary decimal(P,S)
  timestamptz`. `fieldid` is mandatory, 1..N in order, and permanent. New columns
  get the next id and `"optional": true`.
- Decimals are digits, as a number or a string, never a float. Timestamps
  are epoch millis or RFC3339 at whole-millisecond precision. Binary is standard
  base64 under `json` and a raw byte string under `cbor`.
- `staging.db` and any never-uploaded cache file are the only unrecoverable
  state. `catalog.db` is rebuildable with `icelake rebuild`.
- Sizes are powers of 1024, no fractions. Durations are Go durations.
