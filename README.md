# icelake

A Go library that takes batches of typed records to queryable Apache Iceberg tables on Cloudflare R2 (Parquet + ZSTD). You declare a table's shape as a tagged struct and insert records; icelake handles durable local staging, batching, Parquet encoding, upload, and Iceberg commits. Anything that speaks Iceberg (DuckDB, PyIceberg, …) can query the result directly off the bucket.

It is deliberately **not** a service, not a query engine, and not a managed-platform dependency — one library, one bucket you own (or no bucket at all, in local-only mode).

> **Status: released. The library write path and the `icelake` command are feature-complete and proven end to end.** Declare a table, insert records, and they are durably staged, batched, encoded as ZSTD-compressed Parquet, uploaded, and committed to a real Iceberg table you can query with anything. The quickstart below is a [compiled example](example_test.go) that CI runs against a real object store on every change, so it cannot drift from the code. Crash recovery, catalog rebuild, flush failure, schema evolution and the flush floor are all proven end to end: a test kills the process mid-flush and restarts it, another deletes `catalog.db` outright and rebuilds it from the bucket, another cuts the network mid-run and watches the table heal by itself once it comes back, and another adds a column to a table with committed data behind it and checks that not one existing file was rewritten. The one-off verification run against a real Cloudflare R2 bucket has been performed and its outcome recorded (see `ARCHITECTURE.md`'s open-questions ledger). Two of the four features are built and proven end to end: a local Parquet cache of everything uploaded, with its own retention, and a local-only mode that needs no bucket at all and uploads its backlog when you give it one — a test writes a week's worth of records with no container running at all, then opens the same files against a bucket and watches the backlog land in a real table, in order, exactly once. Schema declaration from a JSON document instead of a Go struct is built too, with the dynamic writer over it: a table can be declared at runtime and fed parsed JSON, and a table declared either way is the same table — a test opens one through both front doors and checks that the two produce the same schema fingerprint, which is what the object names are computed from. And `icelake`, the command that reads JSON lines on stdin so a non-Go program can use all of it, is built: a test pipes NDJSON into the real binary against a real bucket and reads both tables back with DuckDB, another kills it mid-stream with `SIGTERM` and checks that everything it had accepted was committed by the drain, and a third feeds it a malformed line, watches it die at that line, and restarts it to see every earlier record commit exactly once. One piece of finishing work remains on the roadmap rather than in the release: a smaller dependency graph for library consumers (an S3-only file IO in place of the multi-cloud one) — see [PLAN.md](PLAN.md). Open source under [Apache-2.0](LICENSE).

## Install

The `icelake` command — a daemon over the library that reads one JSON object per line on stdin and writes them to Iceberg tables, so nothing else has to be written in Go:

```sh
mise use -g github:gvkhna/icelake
```

That fetches the released binary for your platform: one archive per platform (linux and macOS, amd64 and arm64), each holding a single statically linked, pure-Go binary with no runtime dependencies. The same archives are on the [Releases page](https://github.com/gvkhna/icelake/releases), or build from a clone with `go build -o icelake ./apps/icelake`.

There is deliberately no `go install` route and never will be — the app modules carry a local `replace` directive, so the module proxy cannot resolve them; released binaries are the supported path.

The Go library, for embedding the same pipeline in your own program:

```sh
go get github.com/gvkhna/icelake
```

```sh
# A schema document declares the tables; no bucket needed to try it.
export ICELAKE_DATA_DIR=/var/lib/icelake
export ICELAKE_SCHEMA_FILE=/etc/icelake/schema.json
export ICELAKE_LOCAL_ONLY=true

your-producer | icelake run
# {"table":"market.fills","row":{"symbol":"ABC","price":"1.234567890","ts_ms":1700000000000}}
```

Point it at a bucket later — add the endpoint, bucket, prefix and credentials, drop `ICELAKE_LOCAL_ONLY` — and everything written while it was local uploads and commits by itself on the next start.

`icelake usage` prints the full manual: every environment variable and its default, the schema document format, the line grammar and what happens to a malformed one, the cache layout and its retention, and the local-only-to-bucket transition.

## Quickstart

```go
// The table's shape: one tagged struct. Field ids are permanent — see "Rules that matter".
type Fill struct {
    Symbol      string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
    Price       int64  `parquet:"name=price, logical=decimal, precision=18, scale=9, fieldid=2" arrow:"price"`
    VenueTimeMS int64  `parquet:"name=venue_timestamp_ms, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=3" arrow:"venue_timestamp_ms"`
}

func main() {
    ctx := context.Background()

    store, err := icelake.Open(ctx, icelake.Config{
        StagingPath:       "/var/lib/myservice/staging.db",
        CatalogPath:       "/var/lib/myservice/catalog.db",
        Endpoint:          "https://<account>.r2.cloudflarestorage.com",
        Bucket:            "my-bucket",
        WarehousePrefix:   "warehouse",
        AccessKeyID:       os.Getenv("R2_ACCESS_KEY_ID"),
        SecretAccessKey:   os.Getenv("R2_SECRET_ACCESS_KEY"),
        FlushMaxRecords:   50_000,
        FlushMaxBytes:     128 << 20,          // 128 MiB
        FlushInterval:     15 * time.Minute,
        ZSTDLevel:         19,                 // use 1 in dev/tests
        StagingMaxRecords: 5_000_000,
        StagingMaxBytes:   4 << 30,            // 4 GiB — also bounds memory

        // Optional: where a flush that used up its retry budget is reported.
        // Runs in the background — log it, count it, page someone, return.
        OnFlushError: func(e icelake.FlushError) { log.Printf("icelake: %v", e) },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close(ctx)

    fills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
        Namespace: "myservice",
        Table:     "fills",
    })
    if err != nil {
        log.Fatal(err)
    }

    // Insert returns once the record is durably staged locally.
    // Batching, upload, and Iceberg commit happen in the background.
    err = fills.Insert(ctx, Fill{
        Symbol:      "ABC",
        Price:       1_234_567_890, // 1.234567890 at scale 9
        VenueTimeMS: time.Now().UnixMilli(),
    })
    if err != nil {
        log.Fatal(err)
    }

    // Optional: a checkpoint. Flush returns once everything accepted so far is
    // committed and queryable. A long-running service lets the thresholds do
    // this and only calls Close on shutdown.
    if err := fills.Flush(ctx); err != nil {
        log.Fatal(err)
    }
}
```

Read it back with anything that speaks Iceberg — no icelake involved:

```sql
-- DuckDB, pointed at the same bucket (INSTALL iceberg; INSTALL httpfs; plus
-- your s3_endpoint / s3_access_key_id / s3_secret_access_key settings).
-- Point it at the table's current metadata file…
SELECT count(*) FROM iceberg_scan(
    's3://my-bucket/warehouse/myservice/fills/metadata/00007-<uuid>.metadata.json');

-- …or at the table directory, if you let DuckDB pick the latest itself:
SET unsafe_enable_version_guessing = true;
SELECT count(*) FROM iceberg_scan('s3://my-bucket/warehouse/myservice/fills');
```

## Rules that matter

These are the rules you cannot infer from the type signatures — the ones integrations get wrong:

- **One process, one `staging.db`, one `catalog.db`, one cache directory, one writer per table.** (Local-only mode has no `catalog.db` at all.) Two processes writing the same table silently lose snapshots — nothing can detect that from inside either one. Two writers on one table inside *one* store is detectable, and is refused with `ErrTableInUse`; closing a writer releases its table.
- **Back up `staging.db`, and any cache file that has not been uploaded yet.** `staging.db` holds accepted-but-not-yet-uploaded records. `catalog.db` is the opposite: disposable, rebuildable from the bucket at any time (`rebuild` package, or `icelake rebuild`). **Rebuild with the writer stopped** — it takes the highest-numbered metadata file it can read to be current, which is only true while nothing is committing.
- **The local Parquet cache is disposable, but a file it has not uploaded yet is not.** With `CacheDir` set, every file icelake uploads is also kept on local disk under the same name, and DuckDB can `read_parquet()` the directory. Once a file is uploaded *and* committed, it is a copy of an object already in the bucket and retention will delete it on its own schedule — throw those away freely. A file that has never been uploaded is the only copy of those records, is never evicted no matter how far over your size cap the cache goes, and is part of your backup. In local-only mode that is every file there is.
- **Local-only mode has no Iceberg table until you give it a bucket.** It writes Parquet to the cache directory and nothing else — no catalog, no snapshots, no schema evolution across files. It is for trying icelake out and for collecting before you have a bucket; point the same data directory at a bucket and the backlog uploads and commits itself, in order, exactly once.
- **A `fieldid`, once shipped, is permanent.** Never reordered, never reused — it is the column's identity forever.
- **Fields added after a table exists must be pointer types** (`*string`, not `string`). New columns must be optional; icelake enforces this loudly.
- **The staging ceiling is a memory bound, not just a disk bound.** When it fills, `Insert` refuses with `ErrStagingFull` — that is the loud backstop, not the intended flow control; watch `Pending()`.
- **Money is unscaled integers** (`DECIMAL` columns declared on `int64`); scale your values before insert. Never `float64` for money. A value that needs more digits than the declared precision — or a `string` field carrying bytes that are not valid UTF-8 — is refused by `Insert` itself with a `PoisonError` naming the column. The record never enters staging, so it is still yours to fix or drop, and the writer carries on.
- **A flush that fails is retried, not lost.** An unreachable bucket costs you nothing: the batch stays in the staging file under its own key, retries on a bounded backoff, and keeps its place at the head of the queue so nothing commits out of order — then commits by itself when the bucket answers again. `OnFlushError` is how you hear about a cycle that gave up; it runs in the background, so never call back into the writer from it. The one batch icelake will *not* retry forever is one it cannot encode at all: those rows are marked in place in `staging.db` (`quarantined_at`, `quarantine_error`) for you to inspect and delete, and they keep counting against the ceiling until you do.
- **An `OnAccept` error does not mean the record was refused.** The optional per-table `OnAccept` callback is where you write your own local mirror of a record, inside the same call that accepted it, so "this record exists" is decided in one place instead of two. It runs synchronously, on your goroutine, *after* the record is durably staged — so an error it returns comes back from `Insert` as a `MirrorError` meaning "your mirror fell behind", not "try again". Re-inserting on it writes the record twice.
- **`Status()` is the cost check.** Object storage bills every write; batching exists to keep that count low. `RecordsAccepted` over `FlushesCommitted` is your real average batch size, and `FlushFloorEngaged` climbing means your thresholds no longer match your traffic — icelake coalesces the excess triggers rather than making a flood of tiny, separately-billed writes, and that counter is how you find out it is doing so.
- **Drain a table before retiring it.** Rows staged for a table no writer claims are kept, deliberately, and keep counting against the ceiling.
- **Credentials are explicit.** Two key strings or one provider — icelake never falls back to ambient AWS credentials. Or, in local-only mode, neither: a bucket-less store must carry no credentials at all.

## Documentation

- **API reference:** godoc on every exported symbol — rendered at pkg.go.dev once published. That, plus this README, is the integration documentation.
- **Design decision records** (the *why*; rarely change): [ARCHITECTURE.md](ARCHITECTURE.md) · [SCHEMA.md](SCHEMA.md) · [TESTING.md](TESTING.md) — test philosophy is binding · [PLAN.md](PLAN.md) — build order, distribution
- **Working in this repo:** [AGENTS.md](AGENTS.md)
