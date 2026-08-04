-- The staging schema, exactly as ARCHITECTURE.md's "Scope, schema, and pragmas
-- of the staging store" section sets it out. One staging.db per process, shared
-- by every table.
--
-- No index beyond each table's primary key, deliberately: the whole access
-- pattern is insert a row, read rows back in id order, update and delete rows by
-- id, and the primary key already covers all four. A secondary index gets added
-- only when a measured slow query proves one is needed.

-- +goose Up
-- +goose StatementBegin
CREATE TABLE staging (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    namespace  TEXT    NOT NULL,
    table_name TEXT    NOT NULL,
    batch_key  TEXT,             -- NULL until the record's batch is sealed
    schema_fp  TEXT    NOT NULL, -- fingerprint of the schema the payload was encoded under
    payload    BLOB    NOT NULL, -- the canonical record encoding (see internal/canon)
    byte_len   INTEGER NOT NULL, -- payload length, so the ceiling needs no length() scan
    created_at INTEGER NOT NULL, -- unix millis; diagnostic only, never read by the write path

    quarantined_at   INTEGER,    -- NULL normally; set if the batch could not be encoded
    quarantine_error TEXT        -- the encoder error, kept for an operator to read
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE staging_schemas (
    fp         TEXT PRIMARY KEY, -- hex hash over the descriptor below
    descriptor BLOB NOT NULL     -- serialized (field id, name, type, nullability, and
                                 -- precision/scale for a decimal) list, ascending
                                 -- field-id order, icelake's own canonical form
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE staging_schemas;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE staging;
-- +goose StatementEnd
