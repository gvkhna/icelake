package icelake_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	_ "modernc.org/sqlite"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/canon"
)

// -- The fail-loudly sweep of 2026-08-01, and where its leftovers went.
//
// Every branch in this library that refuses something loudly was enumerated on
// 2026-08-01, site by site, and each was classified by whether a test holds it.
// Tests were written for the sites a test could honestly hold. This comment
// carries the reasoning for the sites named below, so that for those "no test"
// is not read as "nobody looked". It claims nothing about a site it does not
// name: it is a record of why these particular branches have no test, not a
// census of the sweep, and other sites were disposed of elsewhere or left
// filed as open gaps.
//
// Four categories, each accounting for the sites listed under it and no
// others:
//
//   - Unreachable because a higher layer refuses first. The binder's own
//     cross-check and null-in-a-required-column arms, every arm of
//     verifySchema, canon's unknown-Kind defaults, and the staging store's nil
//     descriptor and empty payload guards are all defended by a check that runs
//     earlier on the same input and has no branch that skips it. They stay in
//     the code as the cheap in-code defence they are; what they defend is a
//     library upgrade or a second in-process caller, not today's public path.
//
//   - Needs a fault the no-mocks rule forbids inducing. Every close that fails
//     (both StagingKindClose and CatalogKindClose — probed under a removed
//     directory, a read-only parent, a corrupted file, and a second close, and
//     nil every time), the in-memory Parquet encoder's three failure points,
//     the three arms where reading a pragma back *fails as a query* on a
//     connection that has just answered it, and a value over four gibibytes.
//     Inducing any of them means faking a substrate or injecting into the path
//     under test. The arms where a pragma reads back with the wrong *value* are
//     a different matter: see StagingErrorInMemoryHasNoWAL below.
//
//   - Reachable, but only through a network fault finer than this suite has.
//     The slow proxy rejects whole connections, so on every path where two
//     bucket calls run in order — the table's metadata read before the schema
//     commit, the Parquet upload before AddFiles and the catalog commit, the
//     first listing before the second — the earlier call always fails and the
//     later branch is never entered. Making them reachable means a proxy that
//     fails the nth request, which is a mechanism, not a mock; it has not been
//     built, and until it is these are unheld.
//
//   - Redundantly backstopped, so no test could fail for the branch's own sake.
//     The descriptor decoder's six short-read guards are the found example: a
//     blob that stops inside the column count, a field id, a type tag, a name
//     length or either of a decimal's two bytes is refused by a later check
//     anyway — the id range, the kind whitelist, the empty-name rule — which
//     was confirmed by disabling all six at once and watching every truncated
//     input still come back as an EncodingError. A test there would be a test
//     that cannot fail, which this project bans, so there is none. The two
//     equivalent stopping points in the *payload* decoder are not in this
//     category and do have tests, because losing either of them decodes
//     silently rather than refusing: see the truncation cases in
//     internal/canon's TestDecodeRefusesMalformedPayloads.

// Declarations that fail each of the schema-layer checks. They live here rather
// than being shared with the schemamap tests on purpose: this file is asserting
// the public error surface, not the derivation.

type badNameDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
}

type badFieldIDDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B string `parquet:"name=b, logical=String, fieldid=7" arrow:"b"`
}

type badCrossCheckDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B string `parquet:"name=b, logical=String, fieldid=2" arrow:"bee"`
}

type noColumnsDecl struct{}

type wordSizedDecl struct {
	N int `parquet:"name=n, fieldid=1" arrow:"n"`
}

// TestSchemaErrorsMatchThePublicNames proves the layering actually works:
// errors built deep inside the library, by packages that must never import this
// one, are matchable through this package's own exported names with their typed
// fields readable. The error types live in an internal leaf package and are
// re-exported here as aliases, which makes them the same type rather than a
// parallel copy — if that ever became a conversion or a wrapper, every one of
// these assertions would fail.
//
// Every case drives the public API, because that is where the claim lives: an
// alias is only worth anything if the error a caller catches on the way out of
// Open or OpenWriter is the aliased type. The one exception is called out and
// justified where it appears.
//
// None of this needs an object store. A declaration is refused before the table
// lifecycle runs, and a staging path that cannot be opened is refused before
// the catalog is touched, so every error here is produced before the first byte
// would leave the process.
func TestSchemaErrorsMatchThePublicNames(t *testing.T) {
	ctx := context.Background()

	t.Run("NameError", func(t *testing.T) {
		store := offlineStore(t)

		_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[badNameDecl]{Namespace: "trading", Table: "Fills"})
		if err == nil {
			t.Fatal("OpenWriter with an invalid table name succeeded, want a refusal")
		}

		var ne icelake.NameError
		if !errors.As(err, &ne) {
			t.Fatalf("error is %T, want icelake.NameError", err)
		}
		if ne.Kind != icelake.NameKindTable {
			t.Errorf("NameError.Kind = %q, want %q", ne.Kind, icelake.NameKindTable)
		}
		if ne.Value != "Fills" {
			t.Errorf("NameError.Value = %q, want %q", ne.Value, "Fills")
		}
	})

	t.Run("DeclarationError", func(t *testing.T) {
		store := offlineStore(t)

		_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[noColumnsDecl]{Namespace: "trading", Table: "fills"})
		if err == nil {
			t.Fatal("OpenWriter with a declaration that has no columns succeeded, want a refusal")
		}

		var de icelake.DeclarationError
		if !errors.As(err, &de) {
			t.Fatalf("error is %T, want icelake.DeclarationError", err)
		}
		if de.Kind != icelake.DeclarationKindNoFields {
			t.Errorf("DeclarationError.Kind = %q, want %q", de.Kind, icelake.DeclarationKindNoFields)
		}
		if de.Table != "fills" {
			t.Errorf("DeclarationError.Table = %q, want %q", de.Table, "fills")
		}
	})

	t.Run("DeclarationErrorUnsupportedGoType", func(t *testing.T) {
		store := offlineStore(t)

		_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[wordSizedDecl]{Namespace: "trading", Table: "fills"})
		if err == nil {
			t.Fatal("OpenWriter with a plain int field succeeded, want a refusal")
		}

		var de icelake.DeclarationError
		if !errors.As(err, &de) {
			t.Fatalf("error is %T, want icelake.DeclarationError", err)
		}
		if de.Kind != icelake.DeclarationKindUnsupportedGoType {
			t.Errorf("DeclarationError.Kind = %q, want %q", de.Kind, icelake.DeclarationKindUnsupportedGoType)
		}
		// The whitelist runs before any tag is parsed, so the error names the Go
		// field, not the declared column name.
		if de.Field != "N" {
			t.Errorf("DeclarationError.Field = %q, want %q", de.Field, "N")
		}
	})

	t.Run("FieldIDError", func(t *testing.T) {
		store := offlineStore(t)

		_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[badFieldIDDecl]{Namespace: "trading", Table: "fills"})
		if err == nil {
			t.Fatal("OpenWriter with an out-of-order field ID succeeded, want a refusal")
		}

		var fe icelake.FieldIDError
		if !errors.As(err, &fe) {
			t.Fatalf("error is %T, want icelake.FieldIDError", err)
		}
		if fe.Field != "b" || fe.FieldID != 7 || fe.WantID != 2 {
			t.Errorf("FieldIDError = {Field:%q FieldID:%d WantID:%d}, want {b 7 2}", fe.Field, fe.FieldID, fe.WantID)
		}
	})

	t.Run("CrossCheckError", func(t *testing.T) {
		store := offlineStore(t)

		_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[badCrossCheckDecl]{Namespace: "trading", Table: "fills"})
		if err == nil {
			t.Fatal("OpenWriter with a declaration whose two derivations disagree succeeded, want a refusal")
		}

		var ce icelake.CrossCheckError
		if !errors.As(err, &ce) {
			t.Fatalf("error is %T, want icelake.CrossCheckError", err)
		}
		if ce.Table != "fills" || ce.Column != "b" {
			t.Errorf("CrossCheckError = {Table:%q Column:%q}, want {fills b}", ce.Table, ce.Column)
		}
	})

	t.Run("StagingError", func(t *testing.T) {
		// A staging path the SQLite driver would read as a path plus DSN
		// parameters, and would therefore silently open somewhere else.
		cfg := offlineConfig(t)
		cfg.StagingPath = filepath.Join(filepath.Dir(cfg.StagingPath), "sta?ging.db")

		store, err := icelake.Open(ctx, cfg)
		if err == nil {
			t.Fatal("Open with a staging path icelake cannot represent succeeded, want a refusal")
		}
		if store != nil {
			t.Error("a refused Open returned a usable store")
		}

		var se icelake.StagingError
		if !errors.As(err, &se) {
			t.Fatalf("error is %T, want icelake.StagingError", err)
		}
		if se.Kind != icelake.StagingKindOpen {
			t.Errorf("StagingError.Kind = %q, want %q", se.Kind, icelake.StagingKindOpen)
		}
		if se.Path != cfg.StagingPath {
			t.Errorf("StagingError.Path = %q, want %q", se.Path, cfg.StagingPath)
		}
	})

	// Added at the post-M9 completeness audit's repair round (2026-08-01). The
	// migrate arm of the same error had no assertion anywhere on the public
	// side: TESTING.md scenario 4 is proven at the staging package's own tier,
	// which is the only place a deliberately broken migration set can be
	// substituted, and that left "a staging file icelake cannot bring to its
	// shipped schema stops Open, and the caller can tell why" resting on the
	// internal test alone. It costs one foreign database on disk to hold it
	// here, where the claim actually lives.
	t.Run("StagingErrorMigrate", func(t *testing.T) {
		cfg := offlineConfig(t)
		foreignDatabase(t, cfg.StagingPath)

		store, err := icelake.Open(ctx, cfg)
		if err == nil {
			t.Fatal("Open against a database that already has an unrelated staging table succeeded, want a refusal")
		}
		if store != nil {
			t.Error("a refused Open returned a usable store")
		}

		var se icelake.StagingError
		if !errors.As(err, &se) {
			t.Fatalf("error is %T, want icelake.StagingError", err)
		}
		if se.Kind != icelake.StagingKindMigrate {
			t.Errorf("StagingError.Kind = %q, want %q", se.Kind, icelake.StagingKindMigrate)
		}
		if se.Path != cfg.StagingPath {
			t.Errorf("StagingError.Path = %q, want %q", se.Path, cfg.StagingPath)
		}
	})

	// The '?' case above is a string check that never touches the filesystem,
	// so on its own it leaves "Open proves the staging file is usable before it
	// returns" resting on nothing. sql.Open is lazy: it validates no path and
	// contacts no file, so a staging path holding something that is not a
	// database would otherwise be discovered by the first Insert, long after a
	// caller was told the store was open. The refusal has to be an open failure
	// and not a migration one, because the two mean different things to an
	// operator — a file this build cannot bring to its shipped schema is a
	// staging database, and this is not one.
	t.Run("StagingErrorOpenUnusableFile", func(t *testing.T) {
		cfg := offlineConfig(t)
		if err := os.WriteFile(cfg.StagingPath, []byte("this is not a SQLite database"), 0o600); err != nil {
			t.Fatalf("writing an unusable staging file: %v", err)
		}

		store, err := icelake.Open(ctx, cfg)
		if err == nil {
			t.Fatal("Open against a staging path holding something that is not a database succeeded, want a refusal")
		}
		if store != nil {
			t.Error("a refused Open returned a usable store")
		}

		var se icelake.StagingError
		if !errors.As(err, &se) {
			t.Fatalf("error is %T, want icelake.StagingError", err)
		}
		if se.Kind != icelake.StagingKindOpen {
			t.Errorf("StagingError.Kind = %q, want %q", se.Kind, icelake.StagingKindOpen)
		}
		if se.Path != cfg.StagingPath {
			t.Errorf("StagingError.Path = %q, want %q", se.Path, cfg.StagingPath)
		}
	})

	// The pragma read-back, which proves the durability this whole layer exists
	// for was actually applied rather than merely asked for.
	//
	// **Corrected at the first repair round (2026-08-01).** The sweep filed this
	// branch as unreachable by design, on the stated ground that the pinned
	// driver errors on a pragma it cannot apply rather than ignoring it, so a
	// read-back could never disagree with the DSN. That is not how the driver
	// behaves. An in-memory staging path is accepted, and it answers
	// journal_mode with "memory": the DSN asked for WAL, the connection is not
	// in WAL, and nothing complained. So this is the only thing standing between
	// a caller who reached for ":memory:" — the first thing anyone reaches for —
	// and a staging database with no write-ahead log, handed back as if it had
	// one. Nothing refuses earlier: the path is non-empty and holds no '?'.
	//
	// The two checks after this one, on synchronous and busy_timeout, stay
	// unheld, and this case is why: the one path found that disagrees with its
	// DSN at all disagrees about journal_mode, which is read first.
	t.Run("StagingErrorInMemoryHasNoWAL", func(t *testing.T) {
		cfg := offlineConfig(t)
		cfg.StagingPath = ":memory:"

		store, err := icelake.Open(ctx, cfg)
		if err == nil {
			t.Fatal("Open against an in-memory staging path succeeded, want a refusal: it cannot be in WAL mode")
		}
		if store != nil {
			t.Error("a refused Open returned a usable store")
		}

		var se icelake.StagingError
		if !errors.As(err, &se) {
			t.Fatalf("error is %T, want icelake.StagingError", err)
		}
		if se.Kind != icelake.StagingKindOpen {
			t.Errorf("StagingError.Kind = %q, want %q", se.Kind, icelake.StagingKindOpen)
		}
		if se.Path != cfg.StagingPath {
			t.Errorf("StagingError.Path = %q, want %q", se.Path, cfg.StagingPath)
		}
	})

	// The read half of the same error, which had no assertion anywhere. Open's
	// own documentation calls its startup pass the first thing that would
	// refuse a staging database this build will not replay, and that promise
	// rests entirely on this error being typed: a scan failure surfacing as the
	// driver's raw complaint would be indistinguishable, to a caller deciding
	// whether to start serving, from a configuration mistake.
	//
	// The damage is real and is done with plain SQL while nothing holds the
	// file open, which is the mechanism scenario 8 already uses on a different
	// column: a staged row whose byte_len is not a number is a row the ordered
	// pass cannot read, and no version of icelake could have written it.
	t.Run("StagingErrorRead", func(t *testing.T) {
		cfg := offlineConfig(t)
		store, err := icelake.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := store.Close(ctx); err != nil {
			t.Fatalf("Store.Close: %v", err)
		}
		unreadableStagedRow(t, cfg.StagingPath)

		reopened, err := icelake.Open(ctx, cfg)
		if err == nil {
			t.Fatal("Open against a staging database holding a row it cannot read succeeded, want a refusal")
		}
		if reopened != nil {
			t.Error("a refused Open returned a usable store")
		}

		var se icelake.StagingError
		if !errors.As(err, &se) {
			t.Fatalf("error is %T, want icelake.StagingError", err)
		}
		if se.Kind != icelake.StagingKindRead {
			t.Errorf("StagingError.Kind = %q, want %q", se.Kind, icelake.StagingKindRead)
		}
		if se.Path != cfg.StagingPath {
			t.Errorf("StagingError.Path = %q, want %q", se.Path, cfg.StagingPath)
		}
	})

	t.Run("CatalogError", func(t *testing.T) {
		// The same hazard as the staging path above, on the other database: a
		// path the SQLite driver would read as a path plus DSN parameters, and
		// would therefore silently open somewhere else. The catalog is
		// rebuildable and the staging store is not, but "opened the wrong file"
		// is a failure either way, and each database has its own typed error
		// precisely so a caller can tell which one it was.
		cfg := offlineConfig(t)
		cfg.CatalogPath = filepath.Join(filepath.Dir(cfg.CatalogPath), "cata?log.db")

		store, err := icelake.Open(ctx, cfg)
		if err == nil {
			t.Fatal("Open with a catalog path icelake cannot represent succeeded, want a refusal")
		}
		if store != nil {
			t.Error("a refused Open returned a usable store")
		}

		var ce icelake.CatalogError
		if !errors.As(err, &ce) {
			t.Fatalf("error is %T, want icelake.CatalogError", err)
		}
		if ce.Kind != icelake.CatalogKindOpen {
			t.Errorf("CatalogError.Kind = %q, want %q", ce.Kind, icelake.CatalogKindOpen)
		}
		if ce.Path != cfg.CatalogPath {
			t.Errorf("CatalogError.Path = %q, want %q", ce.Path, cfg.CatalogPath)
		}
	})

	// The catalog's half of the same argument, and it bites harder here because
	// this is the only call on the whole open path that makes the catalog file
	// prove it exists: the SQL catalog creates its own two tables when it is
	// built, and everything before that — the '?' check above and sql.Open —
	// looks at a string. A path that cannot hold a database at all therefore
	// has to be refused here or nowhere, and a store handed back over one would
	// fail at its first commit instead.
	t.Run("CatalogErrorUnusablePath", func(t *testing.T) {
		cfg := offlineConfig(t)
		if err := os.Mkdir(cfg.CatalogPath, 0o700); err != nil {
			t.Fatalf("putting a directory where the catalog database would go: %v", err)
		}

		store, err := icelake.Open(ctx, cfg)
		if err == nil {
			t.Fatal("Open against a catalog path that is a directory succeeded, want a refusal")
		}
		if store != nil {
			t.Error("a refused Open returned a usable store")
		}

		var ce icelake.CatalogError
		if !errors.As(err, &ce) {
			t.Fatalf("error is %T, want icelake.CatalogError", err)
		}
		if ce.Kind != icelake.CatalogKindOpen {
			t.Errorf("CatalogError.Kind = %q, want %q", ce.Kind, icelake.CatalogKindOpen)
		}
		if ce.Path != cfg.CatalogPath {
			t.Errorf("CatalogError.Path = %q, want %q", ce.Path, cfg.CatalogPath)
		}
	})

	// The one case that cannot be driven through the public API, and the reason
	// is the point rather than a shortcut: a uuid column is a real Iceberg type
	// the canonical encoding has no representation for, and no declaration can
	// produce one — the Go field-type whitelist has nothing that derives to it,
	// so every public path to this error is closed by an earlier refusal. The
	// alias still has to hold for the day one of those paths opens, so it is
	// asserted at the seam that can actually reach it.
	t.Run("EncodingError", func(t *testing.T) {
		sc := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 1, Name: "col", Type: iceberg.PrimitiveTypes.UUID, Required: true})

		_, err := canon.Describe("fills", sc)
		if err == nil {
			t.Fatal("describing a uuid column succeeded, want a refusal")
		}

		var ee icelake.EncodingError
		if !errors.As(err, &ee) {
			t.Fatalf("error is %T, want icelake.EncodingError", err)
		}
		if ee.Kind != icelake.EncodingKindUnsupportedType {
			t.Errorf("EncodingError.Kind = %q, want %q", ee.Kind, icelake.EncodingKindUnsupportedType)
		}
		if ee.Table != "fills" || ee.Field != "col" {
			t.Errorf("EncodingError = {Table:%q Field:%q}, want {fills col}", ee.Table, ee.Field)
		}
	})
}

// offlineConfig is a valid configuration whose endpoint is never reached.
//
// It is a real, complete Config — validation runs against it in full — pointed
// at an address nothing answers on, which is safe precisely because every error
// this file asserts is raised before any request is made. If one of them ever
// started reaching the network, these tests would hang rather than quietly
// passing, which is the failure worth having.
func offlineConfig(t *testing.T) icelake.Config {
	t.Helper()

	dir := t.TempDir()

	return icelake.Config{
		StagingPath:       filepath.Join(dir, "staging.db"),
		CatalogPath:       filepath.Join(dir, "catalog.db"),
		Endpoint:          "http://127.0.0.1:9",
		Bucket:            "unreachable",
		WarehousePrefix:   "warehouse",
		Region:            "us-east-1",
		AccessKeyID:       "unused",
		SecretAccessKey:   "unused",
		FlushMaxRecords:   50,
		FlushMaxBytes:     1 << 20,
		FlushInterval:     time.Minute,
		MinFlushInterval:  time.Nanosecond,
		ZSTDLevel:         1,
		StagingMaxRecords: 1000,
		StagingMaxBytes:   1 << 20,
	}
}

// foreignDatabase writes a SQLite file at path that is not icelake's and never
// could be: it already holds an unrelated table called staging, so the first
// shipped migration cannot apply to it. This is what a staging path pointed at
// somebody else's database looks like from the inside, and it is produced with
// a real driver against a real file rather than described.
func foreignDatabase(t *testing.T, path string) {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("creating a foreign database at %s: %v", path, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("closing the foreign database: %v", err)
		}
	}()

	if _, err := db.Exec("CREATE TABLE staging (whatever TEXT)"); err != nil {
		t.Fatalf("creating the foreign staging table: %v", err)
	}
}

// unreadableStagedRow writes one staged row into an existing staging database
// whose byte_len is a string, which the ordered startup pass cannot scan into
// the integer the column is declared as.
//
// SQLite stores what it is given rather than what the column says, so this is a
// row that really is in the file and really cannot be read back — the same
// class of genuinely damaged database scenario 8 produces by overwriting a
// payload, done with plain SQL while no store holds the file open, rather than
// an error injected into the path under test.
func unreadableStagedRow(t *testing.T, path string) {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("opening the staging database at %s: %v", path, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("closing the staging database: %v", err)
		}
	}()

	const insert = `INSERT INTO staging (namespace, table_name, batch_key, schema_fp, payload, byte_len, created_at)
		VALUES ('market', 'fills', NULL, 'fp', X'0100', 'not a number', 0)`
	if _, err := db.Exec(insert); err != nil {
		t.Fatalf("writing an unreadable staged row: %v", err)
	}
}

// offlineStore opens a store against [offlineConfig] and closes it when the
// test ends. Opening one touches two local SQLite files and nothing else.
func offlineStore(t *testing.T) *icelake.Store {
	t.Helper()

	store, err := icelake.Open(context.Background(), offlineConfig(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(context.Background()); err != nil {
			t.Errorf("Store.Close: %v", err)
		}
	})

	return store
}
