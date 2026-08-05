package icelake_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// TESTING.md scenario 17: two-door identity.
//
// The property ARCHITECTURE.md names and the whole "disposable and
// reproducible" story rests on: inserting a batch through the live mirror, and
// loading the *same Parquet file* into a table with the same DDL through
// ClickHouse's own Parquet reader, produce the same rows.
//
// The test's value depends entirely on it being able to fail, which is why the
// three mutations below are part of the scenario rather than an appendix to it.

// TwinFacts carries all nine declared types twice — once required and once
// optional — plus a required ordinal, so both doors can be compared in a fixed
// order and every type has real nulls somewhere in it.
type TwinFacts struct {
	Seq int64 `parquet:"name=seq, fieldid=1" arrow:"seq"`

	CBool   bool    `parquet:"name=c_bool, fieldid=2" arrow:"c_bool"`
	CInt    int32   `parquet:"name=c_int, fieldid=3" arrow:"c_int"`
	CLong   int64   `parquet:"name=c_long, fieldid=4" arrow:"c_long"`
	CFloat  float32 `parquet:"name=c_float, fieldid=5" arrow:"c_float"`
	CDouble float64 `parquet:"name=c_double, fieldid=6" arrow:"c_double"`
	CDec    int64   `parquet:"name=c_dec, logical=decimal, precision=18, scale=9, fieldid=7" arrow:"c_dec"`
	CTS     int64   `parquet:"name=c_ts, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=8" arrow:"c_ts"`
	CStr    string  `parquet:"name=c_str, logical=String, fieldid=9" arrow:"c_str"`
	CBin    []byte  `parquet:"name=c_bin, type=BYTE_ARRAY, fieldid=10" arrow:"c_bin"`

	OBool   *bool    `parquet:"name=o_bool, fieldid=11" arrow:"o_bool"`
	OInt    *int32   `parquet:"name=o_int, fieldid=12" arrow:"o_int"`
	OLong   *int64   `parquet:"name=o_long, fieldid=13" arrow:"o_long"`
	OFloat  *float32 `parquet:"name=o_float, fieldid=14" arrow:"o_float"`
	ODouble *float64 `parquet:"name=o_double, fieldid=15" arrow:"o_double"`
	ODec    *int64   `parquet:"name=o_dec, logical=decimal, precision=18, scale=9, fieldid=16" arrow:"o_dec"`
	OTS     *int64   `parquet:"name=o_ts, logical=timestamp, logical.unit=millis, logical.isadjustedutc=true, fieldid=17" arrow:"o_ts"`
	OStr    *string  `parquet:"name=o_str, logical=String, fieldid=18" arrow:"o_str"`
	OBin    *[]byte  `parquet:"name=o_bin, type=BYTE_ARRAY, fieldid=19" arrow:"o_bin"`
}

// makeTwinFacts builds one deterministic record. The optional columns are
// populated on odd rows and null on even ones, so every optional column has
// real nulls in it and a mutation that turned nulls into zeros has something to
// be caught by.
func makeTwinFacts(i int) TwinFacts {
	r := TwinFacts{
		Seq:     int64(i),
		CBool:   i%2 == 0,
		CInt:    int32(i*3 - 5),
		CLong:   int64(i) * 1_000_000_007,
		CFloat:  float32(i) + 0.5,
		CDouble: float64(i) + 0.25,
		CDec:    1_234_567_890 + int64(i),
		CTS:     1_700_000_000_000 + int64(i)*1000,
		CStr:    fmt.Sprintf("row-%d-é中", i),
		CBin:    []byte{0x00, 0xff, byte(i), 0x10},
	}
	if i%2 == 0 {
		return r
	}

	b := i%4 == 1
	n32 := int32(i * 7)
	n64 := int64(i) * 3
	f32 := float32(i) * 1.25
	f64 := float64(i) * 2.5
	dec := int64(9_000_000_000 + i)
	ts := 1_600_000_000_000 + int64(i)*1000
	s := fmt.Sprintf("opt-%d", i)

	// The optional binary carries a null byte and a high byte on purpose: it is
	// the column where "null preserved as null, not as empty, not as zero" is
	// subtlest, because canon records a present-but-empty []byte as a value.
	bin := []byte{0x00, 0x7f, byte(i), 0xfe}

	r.OBool, r.OInt, r.OLong = &b, &n32, &n64
	r.OFloat, r.ODouble, r.ODec = &f32, &f64, &dec
	r.OTS, r.OStr, r.OBin = &ts, &s, &bin

	return r
}

// twinColumn is one column of the comparison: the name the mirror gives it, the
// ClickHouse type SCHEMA.md's mapping table says it is, and the expression that
// renders it exactly.
//
// The expressions are the point. A timestamp is compared as an integer count of
// microseconds rather than as a formatted string, so no precision can hide in
// the formatting. A decimal is compared through toString, which is exact
// digits. A binary column is compared as hex. And every nullable column renders
// a null as something no value can render as, which is what makes "null
// preserved as null, not as zero" an assertion rather than an assumption.
type twinColumn struct {
	name string
	typ  string
	expr string
}

// twinColumns is the comparison's own statement of what the mirror's schema
// must be, derived from SCHEMA.md's mapping table rather than from the
// generator, in the order the declaration states them.
var twinColumns = []twinColumn{
	{"seq", "Int64", "toString(seq)"},
	{"c_bool", "Bool", "toString(c_bool)"},
	{"c_int", "Int32", "toString(c_int)"},
	{"c_long", "Int64", "toString(c_long)"},
	{"c_float", "Float32", "toString(c_float)"},
	{"c_double", "Float64", "toString(c_double)"},
	{"c_dec", "Decimal(18, 9)", "toString(c_dec)"},
	{"c_ts", "DateTime64(6, 'UTC')", "toString(toUnixTimestamp64Micro(c_ts))"},
	{"c_str", "String", "c_str"},
	{"c_bin", "String", "hex(c_bin)"},
	{"o_bool", "Nullable(Bool)", nullable("toString(o_bool)", "o_bool")},
	{"o_int", "Nullable(Int32)", nullable("toString(o_int)", "o_int")},
	{"o_long", "Nullable(Int64)", nullable("toString(o_long)", "o_long")},
	{"o_float", "Nullable(Float32)", nullable("toString(o_float)", "o_float")},
	{"o_double", "Nullable(Float64)", nullable("toString(o_double)", "o_double")},
	{"o_dec", "Nullable(Decimal(18, 9))", nullable("toString(o_dec)", "o_dec")},
	{"o_ts", "Nullable(DateTime64(6, 'UTC'))", nullable("toString(toUnixTimestamp64Micro(o_ts))", "o_ts")},
	{"o_str", "Nullable(String)", nullable("o_str", "o_str")},
	{"o_bin", "Nullable(String)", nullable("hex(o_bin)", "o_bin")},
}

// nullable renders a null as a marker no value of any of the nine types can
// produce, so a zero written where a null belonged compares unequal rather than
// merely looking odd.
func nullable(expr, col string) string {
	return "if(isNull(" + col + "), '\\0NULL\\0', " + expr + ")"
}

// sqlString renders a Go string as a ClickHouse string literal. The structure
// handed to file() carries type names with quotes of their own —
// DateTime64(6, 'UTC') is the case that says so — so the escaping is load
// bearing rather than defensive.
func sqlString(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "\\'") + "'"
}

// fileStructure is the explicit column list handed to file(), which is what
// makes input_format_parquet_allow_missing_columns=0 mean anything: an inferred
// structure would accept whatever the file happened to contain and there would
// be nothing left to prove.
func fileStructure(cols []twinColumn) string {
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, "`"+c.name+"` "+c.typ)
	}

	return strings.Join(parts, ", ")
}

// TestTwoDoorIdentity is the scenario.
func TestTwoDoorIdentity(t *testing.T) {
	testsubstrate.RequireDocker(t)
	ch := testsubstrate.StartClickHouse(t)

	ctx := context.Background()
	cfg := localOnlyConfig(t, t.TempDir())
	cfg.ClickHouse = mirrorFor(ch)
	// One batch, so there is one Parquet file and one batch key: the comparison
	// is between a live insert and that same file read back, and two files
	// would only make the bookkeeping longer.
	cfg.FlushMaxRecords = 1000

	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[TwinFacts]{Namespace: "twin", Table: "facts"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	const records = 40
	for i := range records {
		if err := writer.Insert(ctx, makeTwinFacts(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Door one's result, and the file door two will read: the spool file, which
	// is byte-for-byte what a bucket-mode run would have uploaded.
	cached := cachedFiles(t, cfg.CacheDir)
	if len(cached) != 1 {
		t.Fatalf("the cache holds %v, want exactly one file so the two doors are about one batch", cached)
	}
	localPath := filepath.Join(cfg.CacheDir, cached[0])
	batchKey := strings.TrimSuffix(filepath.Base(cached[0]), ".parquet")

	conn := chConn(t, ch)
	live := mirrorDatabase + ".twin__facts"

	if n := chCount(t, conn, "SELECT count() FROM "+live); n != records {
		t.Fatalf("the live mirror holds %d rows, want %d", n, records)
	}

	ch.CopyFile(t, localPath, "twin.parquet")

	// The mirror's own CREATE statement, so the twin is built from the same DDL
	// rather than from a second spelling of it that could drift.
	create := showCreate(t, conn, mirrorDatabase, "twin__facts")

	t.Run("the two doors agree", func(t *testing.T) {
		twin := buildTwin(t, conn, create, "twin__facts_file", twinColumns, batchKey, true)
		compareDoors(t, conn, live, twin, twinColumns, records)
	})

	// The three mutations. Each is required to fail — at the twin's own insert
	// or at the comparison, and which of the two differs per mutation, so
	// pinning it would make the test brittle about ClickHouse's error surface
	// rather than about the property.
	t.Run("a millisecond mirror is caught", func(t *testing.T) {
		cols := mutateType(twinColumns, "c_ts", "DateTime64(3, 'UTC')")
		cols = mutateType(cols, "o_ts", "Nullable(DateTime64(3, 'UTC'))")
		requireFailure(t, conn, create, "mut_scale", cols, batchKey, true, live, records)
	})

	t.Run("a non-nullable optional column is caught", func(t *testing.T) {
		cols := mutateType(twinColumns, "o_long", "Int64")
		requireFailure(t, conn, create, "mut_notnull", cols, batchKey, true, live, records)
	})

	t.Run("dropping the strict settings is caught", func(t *testing.T) {
		// The same mutation as above with the two settings off. Now the read
		// succeeds and writes zeros where the lake holds nulls, which is exactly
		// the hazard SCHEMA.md records — and the comparison is what says so.
		cols := mutateType(twinColumns, "o_long", "Int64")
		requireFailure(t, conn, create, "mut_lax", cols, batchKey, false, live, records)
	})

	t.Run("and it still passes when nothing is wrong", func(t *testing.T) {
		twin := buildTwin(t, conn, create, "twin__facts_again", twinColumns, batchKey, true)
		compareDoors(t, conn, live, twin, twinColumns, records)
	})
}

// showCreate reads a table's own CREATE statement.
func showCreate(tb testing.TB, conn driver.Conn, database, table string) string {
	tb.Helper()

	got := chStrings(tb, conn, fmt.Sprintf("SHOW CREATE TABLE %s.%s", database, table))
	if len(got) != 1 {
		tb.Fatalf("SHOW CREATE TABLE returned %d rows", len(got))
	}

	return got[0]
}

// mutateType returns the column list with one column's ClickHouse type
// replaced, which is how each mutation is expressed.
func mutateType(cols []twinColumn, name, typ string) []twinColumn {
	out := append([]twinColumn(nil), cols...)
	for i := range out {
		if out[i].name == name {
			out[i].typ = typ
		}
	}

	return out
}

// buildTwin creates the twin table and loads the batch's own Parquet file into
// it through ClickHouse's Parquet reader. It returns the twin's qualified name.
func buildTwin(tb testing.TB, conn driver.Conn, create, name string, cols []twinColumn, batchKey string, strict bool) string {
	tb.Helper()

	target := mirrorDatabase + "." + name
	ddl := strings.Replace(create, "twin__facts", name, 1)
	for _, c := range cols {
		// The twin's own column is mutated in step with the structure handed to
		// file(), so a mutation is one disagreement rather than two.
		ddl = replaceColumnType(ddl, c.name, c.typ)
	}

	chExec(tb, conn, "DROP TABLE IF EXISTS "+target)
	chExec(tb, conn, ddl)

	names := make([]string, 0, len(cols)+1)
	for _, c := range cols {
		names = append(names, "`"+c.name+"`")
	}
	selected := strings.Join(names, ", ")
	names = append(names, "`_batch_key`")

	query := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s, %s FROM file('twin.parquet', Parquet, %s)",
		target, strings.Join(names, ", "), selected, sqlString(batchKey), sqlString(fileStructure(cols)))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if strict {
		// Both hazards, off. A null landing in a non-Nullable column is a
		// refusal rather than a zero, and a column the file does not have is a
		// refusal rather than a column of defaults.
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"input_format_null_as_default":               0,
			"input_format_parquet_allow_missing_columns": 0,
		}))
	}
	if err := conn.Exec(ctx, query); err != nil {
		tb.Fatalf("loading the Parquet file into %s: %v", target, err)
	}

	return target
}

// tryBuildTwin is [buildTwin] for a mutation, where the load failing is one of
// the two acceptable outcomes rather than a test failure.
func tryBuildTwin(conn driver.Conn, create, name string, cols []twinColumn, batchKey string, strict bool) (string, error) {
	target := mirrorDatabase + "." + name
	ddl := strings.Replace(create, "twin__facts", name, 1)
	for _, c := range cols {
		ddl = replaceColumnType(ddl, c.name, c.typ)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+target); err != nil {
		return "", err
	}
	if err := conn.Exec(ctx, ddl); err != nil {
		return "", err
	}

	names := make([]string, 0, len(cols)+1)
	for _, c := range cols {
		names = append(names, "`"+c.name+"`")
	}
	selected := strings.Join(names, ", ")
	names = append(names, "`_batch_key`")

	query := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s, %s FROM file('twin.parquet', Parquet, %s)",
		target, strings.Join(names, ", "), selected, sqlString(batchKey), sqlString(fileStructure(cols)))

	loadCtx := ctx
	if strict {
		loadCtx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"input_format_null_as_default":               0,
			"input_format_parquet_allow_missing_columns": 0,
		}))
	}
	if err := conn.Exec(loadCtx, query); err != nil {
		return "", err
	}

	return target, nil
}

// replaceColumnType rewrites one column's type inside a CREATE statement.
//
// It matches the backquoted name followed by its type up to the end of that
// column's line, which is the shape SHOW CREATE TABLE produces.
func replaceColumnType(ddl, name, typ string) string {
	marker := "`" + name + "` "
	at := strings.Index(ddl, marker)
	if at < 0 {
		return ddl
	}
	start := at + len(marker)
	end := start
	// To the end of the line rather than to the first comma: a type is allowed
	// to contain one, and Decimal(18, 9) is the case that says so.
	for end < len(ddl) && ddl[end] != '\n' {
		end++
	}
	tail := ""
	if end > start && ddl[end-1] == ',' {
		tail = ","
	}

	return ddl[:start] + typ + tail + ddl[end:]
}

// requireFailure runs one mutation and requires it to fail: either the twin's
// own load is refused, or the comparison against the live mirror disagrees.
func requireFailure(tb testing.TB, conn driver.Conn, create, name string, cols []twinColumn, batchKey string, strict bool, live string, records uint64) {
	tb.Helper()

	twin, err := tryBuildTwin(conn, create, name, cols, batchKey, strict)
	if err != nil {
		tb.Logf("the mutation was refused at the load, which is one of the two ways it may fail: %v", err)

		return
	}

	if diff := doorsDiffer(tb, conn, live, twin, cols, records); diff != "" {
		tb.Logf("the mutation was caught by the comparison: %s", diff)

		return
	}

	tb.Fatalf("the mutation %s neither failed to load nor compared unequal; a test that cannot fail is not proving the property", name)
}

// compareDoors asserts the two doors agree, column by column, in a fixed row
// order, and that the nulls on both sides are the same nulls.
func compareDoors(tb testing.TB, conn driver.Conn, live, twin string, cols []twinColumn, records uint64) {
	tb.Helper()

	if diff := doorsDiffer(tb, conn, live, twin, cols, records); diff != "" {
		tb.Errorf("the two doors disagree: %s", diff)
	}
}

// doorsDiffer returns a description of the first disagreement between the two
// doors, or the empty string when they agree.
func doorsDiffer(tb testing.TB, conn driver.Conn, live, twin string, cols []twinColumn, records uint64) string {
	tb.Helper()

	if n := chCount(tb, conn, "SELECT count() FROM "+twin); n != records {
		return fmt.Sprintf("the twin holds %d rows and %d were written", n, records)
	}

	// The values first, deliberately: a mutation that changes what is stored has
	// to be caught by what is stored, and a schema check running ahead of the
	// comparison would short-circuit the one mutation whose whole point is that
	// the data is wrong while the load succeeded.
	for _, c := range cols {
		order := " ORDER BY seq"
		a := chStrings(tb, conn, "SELECT "+c.expr+" FROM "+live+order)
		b := chStrings(tb, conn, "SELECT "+c.expr+" FROM "+twin+order)
		if !equalStrings(a, b) {
			for i := range a {
				if i >= len(b) || a[i] != b[i] {
					return fmt.Sprintf("column %s differs at row %d: live %q, twin %q", c.name, i, a[i], at(b, i))
				}
			}

			return fmt.Sprintf("column %s has %d values live and %d in the twin", c.name, len(a), len(b))
		}

		// Nulls counted separately, because two columns zeroed the same way
		// would compare equal to each other and be wrong together.
		if strings.HasPrefix(c.typ, "Nullable(") {
			liveNulls := chCount(tb, conn, "SELECT countIf(isNull("+c.name+")) FROM "+live)
			twinNulls := chCount(tb, conn, "SELECT countIf(isNull("+c.name+")) FROM "+twin)
			if liveNulls == 0 {
				return fmt.Sprintf("column %s has no nulls in the live mirror; the fixture is supposed to have them", c.name)
			}
			if liveNulls != twinNulls {
				return fmt.Sprintf("column %s has %d nulls live and %d in the twin", c.name, liveNulls, twinNulls)
			}
		}
	}

	// And then the schemas: "a table with the same DDL" is the other half of
	// the property, and a type that differs is a disagreement even where
	// today's values happen to survive it.
	liveTypes := chColumnTypes(tb, conn, mirrorDatabase, tableOf(live))
	twinTypes := chColumnTypes(tb, conn, mirrorDatabase, tableOf(twin))
	for _, c := range cols {
		if liveTypes[c.name] != twinTypes[c.name] {
			return fmt.Sprintf("column %s is %q in the live mirror and %q in the twin", c.name, liveTypes[c.name], twinTypes[c.name])
		}
	}

	return ""
}

// tableOf strips the database from a qualified name.
func tableOf(qualified string) string {
	if i := strings.IndexByte(qualified, '.'); i >= 0 {
		return qualified[i+1:]
	}

	return qualified
}

// at returns one element or a marker, so a mismatch report never panics on a
// short slice.
func at(s []string, i int) string {
	if i < len(s) {
		return s[i]
	}

	return "<missing>"
}
