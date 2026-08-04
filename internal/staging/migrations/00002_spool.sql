-- The local Parquet spool's durable state, exactly as ARCHITECTURE.md's
-- "Decision (2026-08-01, M10): the spool's durable state lives in staging.db
-- too" sets it out.
--
-- These two tables live in staging.db rather than in a third file for the same
-- reason staging and the catalog live in two: a spool row and a staging row are
-- written and pruned in step with each other and have the same disposability
-- rules, so a disagreement between them across two files would be a consistency
-- problem with no transaction available to fix it.
--
-- spool_schemas is deliberately a separate table from staging_schemas rather
-- than a reuse of it. staging_schemas is pruned once no staged row references a
-- fingerprint, which is correct there because a descriptor exists to decode
-- staged payloads and there are none left to decode. A spool file outlives its
-- staged rows by design — its rows are pruned the moment the commit is confirmed,
-- or in local-only mode as soon as the file is on disk — so sharing one table
-- would prune a spool file's shape out from under it within seconds of recording
-- it, and that descriptor is what the transition drain reads to create the table
-- and to reconcile it forward. Two tables, two lifetimes, one prune each.
--
-- No index beyond each table's primary key, for the same reason migration 00001
-- adds none: this table is bounded by the retention the caller configured, every
-- statement against it either touches one row by primary key or scans it whole
-- in a defined order, and an index gets added only when a measured slow query
-- proves one is needed.

-- +goose Up
-- +goose StatementBegin
CREATE TABLE spool_files (
    batch_key    TEXT PRIMARY KEY, -- the content hash; also the file's name and the object's
    namespace    TEXT    NOT NULL,
    table_name   TEXT    NOT NULL,
    schema_fp    TEXT    NOT NULL, -- the shape these bytes were written under
    byte_len     INTEGER NOT NULL, -- the file's size, so the size bound needs no stat() walk
    created_at   INTEGER NOT NULL, -- unix millis; the age bound and the drain order read this
    committed_at INTEGER           -- NULL until the file is uploaded AND catalog-committed
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE spool_schemas (
    fp         TEXT PRIMARY KEY, -- hex hash over the descriptor, exactly as staging_schemas
    descriptor BLOB NOT NULL     -- icelake's own canonical descriptor form
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE spool_schemas;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE spool_files;
-- +goose StatementEnd
