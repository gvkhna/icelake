-- Hand-written SQL for staging.db. sqlc turns this into the generated code
-- committed alongside it; see sqlc.yaml at the repo root.
--
-- Every statement here touches rows by primary key or scans the whole (bounded)
-- table in id order. That is the entire access pattern ARCHITECTURE.md describes
-- for this store, and it is why the schema carries no secondary index.
--
-- Keep every comment in this file ASCII, and treat that as a rule rather than a
-- style preference. The pinned sqlc measures a statement's extent in one unit
-- and slices the source in another, so a single multi-byte character anywhere
-- above a query -- an em dash in a sentence like this one -- shifts the text it
-- copies into the generated Go by the difference, silently: the query loses its
-- last characters and the next one gains the fragment. Nothing warns, the
-- generator exits zero, and the failure surfaces at run time as a SQL syntax
-- error in a statement that reads perfectly here. Found at M11 by writing the
-- spool's comments the way the rest of this repo writes prose.

-- InsertStagedRow durably accepts one record. AUTOINCREMENT guarantees the
-- returned id is monotonic and never reused after a delete, which is what makes
-- "ORDER BY id" mean "insert order" even though rows are continuously pruned.
--
-- The id comes back as the statement's last insert rowid rather than through a
-- RETURNING clause, and the difference is not cosmetic: RETURNING makes this a
-- query, so every row pays for a result set to be opened, stepped, scanned and
-- closed, where the rowid is a number the driver already has once the insert has
-- run. Measured at M15 inside one transaction, that is about 43 microseconds a
-- row against about 14, which is most of the cost of a batch insert. Nothing
-- about the value differs: for a single-row insert the last insert rowid is the
-- id the row was given.
-- name: InsertStagedRow :execlastid
INSERT INTO staging (namespace, table_name, batch_key, schema_fp, payload, byte_len, created_at)
VALUES (?, ?, NULL, ?, ?, ?, ?);

-- UpsertSchema records the descriptor a fingerprint names. It is a no-op when
-- the fingerprint is already recorded, because staging_schemas is
-- content-addressed: the same fingerprint always names the same bytes.
-- name: UpsertSchema :exec
INSERT INTO staging_schemas (fp, descriptor)
VALUES (?, ?)
ON CONFLICT (fp) DO NOTHING;

-- GetSchema reads back the shape a staged payload was encoded under. Replay
-- decodes by this, never by the current declaration.
-- name: GetSchema :one
SELECT descriptor FROM staging_schemas WHERE fp = ?;

-- DeleteUnreferencedSchemas drops descriptors no live staged row names any
-- more. Kept out of the delete-on-commit path so that path stays purely
-- primary-key; this one scans, and its caller chooses when to pay for that.
-- name: DeleteUnreferencedSchemas :execrows
DELETE FROM staging_schemas
WHERE fp NOT IN (SELECT schema_fp FROM staging);

-- ScanStagedRows is the ordered pass over the whole table that process startup
-- makes once and every writer open makes again. It reads no payloads: it
-- measures the totals the ceiling counters are seeded from and builds the
-- in-memory replay index at once, and the payloads of whichever rows a caller
-- claims are then read back by primary key. quarantined_at is selected because
-- a quarantined row still counts toward the ceiling but is excluded from
-- replay, so one pass has to tell them apart.
-- name: ScanStagedRows :many
SELECT id, namespace, table_name, batch_key, schema_fp, byte_len, quarantined_at
FROM staging
ORDER BY id;

-- StagedTotals is the second, independent way of obtaining the numbers the
-- staging ceiling's in-memory counters carry, which is what makes it the way to
-- check those counters against the database. It reads no payloads, and the
-- table it aggregates is itself bounded by the ceiling those numbers enforce.
-- Nothing in the write path runs it: the counters are seeded from the totals
-- the ordered pass Open makes at startup accumulates as it goes, which is one
-- of the two passes ScanStagedRows above serves rather than the only one.
--
-- Corrected at the completeness audit's fifth round (2026-08-01): this comment
-- called it "the aggregate that seeds the staging ceiling's counters", which no
-- code has ever done. It is the third copy of that sentence the round found,
-- after the accept comment in the root package's store.go and the doc comment
-- on Store.Counters in this package, and it was found one level below both of
-- those because a query's comment is generated into Go rather than written
-- there.
-- name: StagedTotals :one
SELECT COUNT(*) AS records, COALESCE(SUM(byte_len), 0) AS bytes FROM staging;

-- GetStagedPayload reads one row's payload back by primary key, which is how a
-- table claims the rows its replay index told it about.
-- name: GetStagedPayload :one
SELECT id, namespace, table_name, batch_key, schema_fp, payload, byte_len, created_at, quarantined_at
FROM staging
WHERE id = ?;

-- SealStagedRow stamps a batch key on one row. Three guards make sealing
-- structurally exactly-once and structurally single-table: batch_key IS NULL, so
-- a row already claimed by another batch fails to update rather than silently
-- moving between batches; quarantined_at IS NULL, so a marked row is never
-- pulled into a future batch; and the table identity, so a batch can only ever
-- contain rows of the one table its key was computed for.
-- name: SealStagedRow :execrows
UPDATE staging SET batch_key = ?
WHERE id = ? AND namespace = ? AND table_name = ?
  AND batch_key IS NULL AND quarantined_at IS NULL;

-- SealStagedRowRecoded is SealStagedRow for a row being re-encoded under the
-- current shape in the same transaction that seals it, so a sealed batch is
-- always fingerprint-homogeneous.
-- name: SealStagedRowRecoded :execrows
UPDATE staging SET batch_key = ?, schema_fp = ?, payload = ?, byte_len = ?
WHERE id = ? AND namespace = ? AND table_name = ?
  AND batch_key IS NULL AND quarantined_at IS NULL;

-- DeleteStagedRow prunes one committed row. Deleting a row that is already gone
-- is not an error: a crash between the catalog commit and the prune is recovered
-- by re-running the prune.
-- name: DeleteStagedRow :execrows
DELETE FROM staging WHERE id = ?;

-- GetStagedRowIdentity reads everything about a row except its payload, which is
-- what the quarantine guard needs to know whether the row belongs to a sealed
-- batch.
-- name: GetStagedRowIdentity :one
SELECT namespace, table_name, batch_key, quarantined_at
FROM staging
WHERE id = ?;

-- CountStagedRowsWithBatchKey counts a sealed batch's members. Quarantining
-- fewer than all of them would leave a batch whose stored key no longer names
-- its bytes, so the guard compares this count against what the caller asked for.
-- A batch is identified by its table as well as its key, matching the seal
-- statements above: a batch belongs to exactly one table, so two tables that
-- happened to carry the same key string are two batches, not one.
-- name: CountStagedRowsWithBatchKey :one
SELECT COUNT(*) FROM staging
WHERE namespace = ? AND table_name = ? AND batch_key = ?;

-- QuarantineStagedRow marks a row whose batch could not be encoded. Quarantined
-- rows are excluded from every future replay and deliberately keep counting
-- toward the ceiling, so a growing quarantine stops the writer loudly instead of
-- silently eating headroom.
-- name: QuarantineStagedRow :execrows
UPDATE staging SET quarantined_at = ?, quarantine_error = ?
WHERE id = ? AND quarantined_at IS NULL;

-- The local Parquet spool's own statements. Everything below touches spool_files
-- by primary key or scans it in a defined order, exactly as the staging
-- statements above do, and for the same reason: the table is bounded by the
-- retention the caller configured, so no secondary index is needed for any of it.

-- UpsertSpoolFile records a file that is now on disk. It has to tolerate a
-- conflict, because a rewrite of the same batch writes the same file name: the
-- name is the batch key, and a batch that is already sealed keeps its stored key
-- for the rest of its life rather than recomputing it.
--
-- Which columns the conflict updates is the load-bearing part, and each of the
-- four decisions is different.
--
-- schema_fp and byte_len ARE updated, because both really can move under one
-- key. A sealed batch's key is never recomputed, so a replay after a
-- schema-changing deploy re-encodes that batch's rows under the current shape
-- and writes a different file under the same name: a new shape, a new size, the
-- old key. The drain reads schema_fp to decide what to create the table as and
-- what to reconcile it to, so a stale one would hand it the shape the file used
-- to have and commit today's bytes against yesterday's columns.
--
-- created_at is NOT updated. Both of its readers want the first value: the drain
-- walks files in the order they were first written, and the age bound measures
-- from then rather than from the most recent attempt.
--
-- committed_at is NOT updated, and is not even mentioned in the update, which is
-- what makes it unreachable from this statement rather than merely left alone. A
-- rewrite must never un-commit a file a previous run already got into a table.
-- name: UpsertSpoolFile :exec
INSERT INTO spool_files (batch_key, namespace, table_name, schema_fp, byte_len, created_at, committed_at)
VALUES (?, ?, ?, ?, ?, ?, NULL)
ON CONFLICT (batch_key) DO UPDATE SET
    schema_fp = excluded.schema_fp,
    byte_len  = excluded.byte_len;

-- UpsertSpoolSchema records the shape a spool file was written under. It is a
-- no-op when the fingerprint is already recorded, because spool_schemas is
-- content-addressed exactly as staging_schemas is.
-- name: UpsertSpoolSchema :exec
INSERT INTO spool_schemas (fp, descriptor)
VALUES (?, ?)
ON CONFLICT (fp) DO NOTHING;

-- GetSpoolSchema reads back the shape a spool file was written under. The
-- transition drain decides what to create, and what to reconcile a live table
-- to, from this and from nothing the current process happens to declare.
-- name: GetSpoolSchema :one
SELECT descriptor FROM spool_schemas WHERE fp = ?;

-- MarkSpoolFileCommitted records that a file has been uploaded AND
-- catalog-committed, which is the moment it becomes evictable. Marking a file
-- that is already marked, or one no longer recorded, matches no row and is not
-- an error: the mark is re-run after a crash between the commit and it.
-- name: MarkSpoolFileCommitted :execrows
UPDATE spool_files SET committed_at = ?
WHERE batch_key = ? AND committed_at IS NULL;

-- SpoolBacklogForTable lists one table's files that have never reached a bucket,
-- in the order the transition drain walks them: creation order, with the batch
-- key breaking a tie so two files written in the same millisecond still have one
-- defined order rather than whichever the database felt like.
-- name: SpoolBacklogForTable :many
SELECT batch_key, schema_fp, byte_len, created_at FROM spool_files
WHERE namespace = ? AND table_name = ? AND committed_at IS NULL
ORDER BY created_at, batch_key;

-- SpoolTablesWithBacklog names every table holding files that have never reached
-- a bucket, which is what Store.DrainSpool walks: a table no writer reopens has
-- no declaration in the process and would otherwise never be drained.
-- name: SpoolTablesWithBacklog :many
SELECT DISTINCT namespace, table_name FROM spool_files
WHERE committed_at IS NULL
ORDER BY namespace, table_name;

-- SpoolCommittedBytes totals the files retention may actually evict. The size
-- bound is over committed files, so a backlog the bucket has never confirmed is
-- not merely protected from eviction: it is not even counted toward the bound
-- it cannot be evicted to satisfy.
-- name: SpoolCommittedBytes :one
SELECT COALESCE(SUM(byte_len), 0) AS bytes FROM spool_files
WHERE committed_at IS NOT NULL;

-- SpoolEvictable lists the files retention may delete, oldest first.
--
-- The WHERE clause is the whole of the protection for everything else, and that
-- is the design rather than an implementation detail: a file that has not been
-- uploaded and committed is not defended by policy code that could be edited
-- wrong, it is invisible to the statement that does the deleting. A size cap
-- enforced by a loop that skips uncommitted files is one refactor away from
-- being a size cap that deletes the only copy of a day's records.
-- name: SpoolEvictable :many
SELECT batch_key, namespace, table_name, byte_len, created_at, committed_at FROM spool_files
WHERE committed_at IS NOT NULL
ORDER BY created_at, batch_key;

-- DeleteSpoolFile forgets an evicted file. Deleting a row that is already gone
-- is not an error, for the same reason pruning a staged row twice is not.
-- name: DeleteSpoolFile :execrows
DELETE FROM spool_files WHERE batch_key = ?;

-- DeleteUnreferencedSpoolSchemas drops descriptors no spool file names any more.
-- It is spool_schemas' own prune, separate from staging_schemas': the two
-- tables have two lifetimes, which is exactly why they are two tables.
-- name: DeleteUnreferencedSpoolSchemas :execrows
DELETE FROM spool_schemas
WHERE fp NOT IN (SELECT schema_fp FROM spool_files);

-- StagedSummary is the read-only inspection's view of the staging table: how
-- many accepted records are waiting to be committed, their payload bytes, and
-- how many rows quarantine has taken out of replay. It deliberately splits
-- what StagedTotals above adds together, because an operator reading a report
-- needs "waiting" and "quarantined" to be two numbers -- the first drains on
-- its own and the second never will. The CASE form rather than an aggregate
-- FILTER clause, because the pinned sqlc's SQLite parser does not read FILTER.
-- name: StagedSummary :one
SELECT
    COALESCE(SUM(CASE WHEN quarantined_at IS NULL THEN 1 ELSE 0 END), 0)        AS waiting_records,
    COALESCE(SUM(CASE WHEN quarantined_at IS NULL THEN byte_len ELSE 0 END), 0) AS waiting_bytes,
    COALESCE(SUM(CASE WHEN quarantined_at IS NOT NULL THEN 1 ELSE 0 END), 0)    AS quarantined_records
FROM staging;

-- SpoolBacklogSummary counts the spool files that have never been uploaded and
-- catalog-committed, for the same read-only inspection. In bucket mode a
-- non-zero backlog is work the next writer open or Store.DrainSpool will move;
-- in local-only mode every file is a backlog file by definition, which the
-- reader of these numbers has to know rather than this query.
-- name: SpoolBacklogSummary :one
SELECT COUNT(*) AS files, COALESCE(SUM(byte_len), 0) AS bytes
FROM spool_files WHERE committed_at IS NULL;
