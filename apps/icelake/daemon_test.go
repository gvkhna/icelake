package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/duckdb/duckdb-go/v2"
	_ "modernc.org/sqlite"

	"github.com/fxamacker/cbor/v2"

	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// twoTableDocument declares the two shapes the substrate tests write: a scalar
// fact table and one carrying an optional column, which is enough for a
// two-table run to be a two-table run rather than the same table twice.
const twoTableDocument = `{"tables": [
  {"namespace": "market", "table": "fills", "fields": [
    {"name": "symbol", "type": "string",        "fieldid": 1},
    {"name": "price",  "type": "decimal(18,9)", "fieldid": 2},
    {"name": "ts_ms",  "type": "timestamptz",   "fieldid": 3},
    {"name": "note",   "type": "string",        "fieldid": 4, "optional": true}
  ]},
  {"namespace": "archive", "table": "events", "fields": [
    {"name": "source_id",  "type": "string",      "fieldid": 1},
    {"name": "seq",        "type": "long",        "fieldid": 2},
    {"name": "observed_at","type": "timestamptz", "fieldid": 3},
    {"name": "payload",    "type": "binary",      "fieldid": 4}
  ]}
]}`

// fillLine and eventLine render one record of each table as an envelope, in the
// spellings an operator would actually pipe: a decimal as digits in a string, a
// timestamp as epoch milliseconds, a payload as base64.
func fillLine(i int) string {
	return fmt.Sprintf(`{"table":"market.fills","row":{"symbol":"SYM%d","price":"%d.500000000","ts_ms":%d}}`,
		i%7, i, 1_700_000_000_000+int64(i)*1000)
}

func eventLine(i int) string {
	return fmt.Sprintf(`{"table":"archive.events","row":{"source_id":"stream-%d","seq":%d,"observed_at":%d,"payload":"AQID"}}`,
		i%3, i, 1_700_000_000_000+int64(i)*1000)
}

// bucketEnv is a complete bucket-mode environment for the built binary.
//
// The compression level is the low, fast one for the reason TESTING.md's
// configuration section gives: a test should be quick rather than
// representative of production ratios. Everything else is left at the defaults
// an operator would actually run, so that what these tests exercise is the
// configuration the command ships with.
func bucketEnv(tb testing.TB, bucket testsubstrate.Bucket, dir, document string) []string {
	tb.Helper()

	return append(os.Environ(),
		envDataDir.name+"="+dir,
		envSchemaFile.name+"="+writeDocument(tb, dir, document),
		envEndpoint.name+"="+bucket.Endpoint,
		envBucket.name+"="+bucket.Bucket,
		envPrefix.name+"=warehouse",
		envRegion.name+"="+bucket.Region,
		envAccessKeyID.name+"="+bucket.AccessKeyID,
		envSecretAccessKey.name+"="+bucket.SecretAccessKey,
		envZSTDLevel.name+"=1",
	)
}

// pipeInto runs the built command with the given environment and input, and
// returns its exit code and everything it printed.
func pipeInto(tb testing.TB, env []string, input string) (int, string) {
	tb.Helper()

	cmd := exec.Command(binary(tb), "run")
	cmd.Env = env
	cmd.Stdin = strings.NewReader(input)

	out, err := cmd.CombinedOutput()
	if err == nil {
		return 0, string(out)
	}

	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		tb.Fatalf("running the command: %v\n%s", err, out)
	}

	return exit.ExitCode(), string(out)
}

// TestExitCodesAreThreeDistinctAnswers is scenario 13's exit-code clause,
// asserted on real runs of the built binary.
//
// A supervisor distinguishes "restart me" from "restarting will not help" by
// exactly this, and it is the one part of the contract that is invisible from
// inside the process — so it is checked from outside one.
//
// The expected codes are written as the literals 0, 2 and 1 rather than as this
// command's own constants, deliberately. A test that compared the code against
// the constant the code returns would pass whatever those constants were set
// to, including all three set to the same number; what a supervisor's
// configuration is written against is the numbers, so the numbers are what this
// asserts.
func TestExitCodesAreThreeDistinctAnswers(t *testing.T) {
	t.Run("a clean end of input exits zero", func(t *testing.T) {
		dir := t.TempDir()
		env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)

		code, out := pipeInto(t, env, fillLine(1)+"\n")
		if code != 0 {
			t.Errorf("exit code = %d, want 0\n%s", code, out)
		}
	})

	t.Run("a configuration refusal exits two", func(t *testing.T) {
		// A data directory and nothing else: no schema file, no bucket, no
		// local-only. Three refusals, none of which a restart would fix.
		env := append(os.Environ(), envDataDir.name+"="+t.TempDir())

		code, out := pipeInto(t, env, "")
		if code != 2 {
			t.Errorf("exit code = %d, want 2\n%s", code, out)
		}
		if !strings.Contains(out, envSchemaFile.name) {
			t.Errorf("the refusal does not name the missing schema file:\n%s", out)
		}
	})

	t.Run("a bad line exits one", func(t *testing.T) {
		dir := t.TempDir()
		env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)

		code, out := pipeInto(t, env, fillLine(1)+"\nnot json at all\n")
		if code != 1 {
			t.Errorf("exit code = %d, want 1\n%s", code, out)
		}
		if !strings.Contains(out, "line 2") {
			t.Errorf("the failure does not name the line:\n%s", out)
		}
	})
}

// TestOnlyAnEnvelopeIsAccepted is the load-bearing half of the line grammar.
//
// There is deliberately no bare-row mode: two accepted shapes would let a
// malformed envelope be read as a row of some table, silently, which is the one
// failure this grammar exists to prevent. So a perfectly valid JSON object that
// happens to be a row rather than an envelope has to die at that line rather
// than be guessed at — and this is the test a future "convenience" of accepting
// bare rows would have to delete before it could land, which is exactly the
// point of writing it down.
//
// The second case is the other end of the same rule and is what the strictness
// on the envelope itself buys. A line carrying a mistyped key alongside two good
// ones is *valid JSON* and would be accepted with the extra key silently
// dropped, so the record would be written and the operator would find out
// whenever they next looked for whatever that key was meant to configure. It is
// refused for the same reason the schema document refuses an unknown key.
func TestOnlyAnEnvelopeIsAccepted(t *testing.T) {
	cases := []struct {
		name string
		line string
	}{
		{
			// A row of market.fills, correct in every respect except that it is
			// not wrapped in an envelope.
			"a bare row",
			`{"symbol":"SYM1","price":"1.500000000","ts_ms":1700000000000}`,
		},
		{
			// A complete, writable envelope with one key too many.
			"an envelope with an unknown key",
			`{"table":"market.fills","tabel":"market.fills","row":{"symbol":"SYM1","price":"1.500000000","ts_ms":1700000000000}}`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)

			code, out := pipeInto(t, env, c.line+"\n")
			if code != 1 {
				t.Fatalf("exited %d, want 1\n%s", code, out)
			}
			if !strings.Contains(out, "line 1") {
				t.Errorf("the failure does not name line 1:\n%s", out)
			}
			if n := stagedRows(t, filepath.Join(dir, "staging.db")); n != 0 {
				t.Errorf("%d rows were staged from a line that is not an envelope", n)
			}
			if files, _ := filepath.Glob(filepath.Join(dir, "cache", "*", "*", "data", "*.parquet")); len(files) != 0 {
				t.Errorf("%d cache files were written from a line that is not an envelope", len(files))
			}
		})
	}
}

// TestTheLineBoundIsTheConfiguredNumber holds the two ends of
// ICELAKE_MAX_LINE_BYTES, and it exists because the obvious implementation
// enforces neither.
//
// A scanner refuses a token only when it has to grow past its maximum, so a
// generous starting buffer silently raises the bound for every value below it;
// and a maximum that does not budget for the line terminator turns "exactly
// the configured length" into a line that is accepted or refused depending on
// whether the input ended there or on how the producer ends its lines. The
// terminator — LF, CRLF, or none at the last line of a pipe — is not part of
// the line, so the table holds both ends of the bound under all three, at a
// bound far below the buffer size a default would have used.
func TestTheLineBoundIsTheConfiguredNumber(t *testing.T) {
	// A valid line, padded through the optional column so that its length is
	// something this test chooses rather than something it measures and hopes
	// stays put.
	line := func(pad int) string {
		return fmt.Sprintf(`{"table":"market.fills","row":{"symbol":"S","price":"1.000000000","ts_ms":1700000000000,"note":"%s"}}`,
			strings.Repeat("x", pad))
	}
	exact := line(64)
	bound := strconv.Itoa(len(exact))

	for _, term := range []struct {
		name string
		end  string
	}{
		{"an LF line", "\n"},
		{"a CRLF line", "\r\n"},
		{"an unterminated line", ""},
	} {
		t.Run(term.name+" exactly at the bound is accepted", func(t *testing.T) {
			dir := t.TempDir()
			env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)
			env = append(env, envMaxLineBytes.name+"="+bound)

			code, out := pipeInto(t, env, exact+term.end)
			if code != 0 {
				t.Fatalf("%s of exactly %s bytes exited %d, want 0\n%s", term.name, bound, code, out)
			}
			if n := stagedRows(t, filepath.Join(dir, "staging.db")); n != 0 {
				t.Errorf("%d rows are still staged after a clean run", n)
			}
		})

		t.Run(term.name+" one byte over the bound is refused", func(t *testing.T) {
			dir := t.TempDir()
			env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)
			env = append(env, envMaxLineBytes.name+"="+bound)

			code, out := pipeInto(t, env, line(65)+term.end)
			if code != 1 {
				t.Fatalf("%s one byte over %s exited %d, want 1\n%s", term.name, bound, code, out)
			}
			if !strings.Contains(out, envMaxLineBytes.name) {
				t.Errorf("the failure does not name %s:\n%s", envMaxLineBytes.name, out)
			}
		})
	}
}

// TestLocalOnlyRunWritesACacheDuckDBCanRead is the run that sits between the two
// halves of scenario 13: a real disk, the real binary an operator installs, and
// no container at all.
//
// It is the command's version of scenario 12's first half, and it needs no
// substrate for the same reason — local-only mode has none in it. This is also
// the exact five-minute start the manual opens with, so it is the clause that
// keeps that page honest.
func TestLocalOnlyRunWritesACacheDuckDBCanRead(t *testing.T) {
	dir := t.TempDir()
	env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)

	const records = 40
	var input strings.Builder
	for i := range records {
		input.WriteString(fillLine(i) + "\n")
	}

	if code, out := pipeInto(t, env, input.String()); code != exitOK {
		t.Fatalf("exit code = %d, want %d\n%s", code, exitOK, out)
	}

	duck := openDuckDB(t)
	table := fmt.Sprintf("read_parquet('%s')", filepath.Join(dir, "cache", "market", "fills", "data", "*.parquet"))

	var total int
	queryRow(t, duck, fmt.Sprintf("SELECT count(*) FROM %s", table), &total)
	if total != records {
		t.Errorf("the cache holds %d rows, want %d", total, records)
	}

	// The exactness scenario 1 holds, through the pipe: a decimal written as
	// digits comes back as those digits, and a timestamp as a real timestamp.
	var price, kind string
	queryRow(t, duck, fmt.Sprintf("SELECT CAST(price AS VARCHAR) FROM %s WHERE symbol = 'SYM0' AND price = 7.5", table), &price)
	if price != "7.500000000" {
		t.Errorf("price reads back %q, want %q", price, "7.500000000")
	}
	queryRow(t, duck, fmt.Sprintf("SELECT typeof(ts_ms) FROM %s LIMIT 1", table), &kind)
	if kind != "TIMESTAMP WITH TIME ZONE" {
		t.Errorf("ts_ms reads back as %s, want a real timestamp type", kind)
	}
}

// TestDaemonWritesTwoTablesToABucket is scenario 13's first substrate clause: a
// real pipe, two tables, a real bucket, and DuckDB validating what came out the
// far end exactly as scenario 1 does.
//
// It is the one test that proves the wrapper is wired to the library at all —
// everything else in this file is about the command's own claims.
func TestDaemonWritesTwoTablesToABucket(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	dir := t.TempDir()
	env := bucketEnv(t, bucket, dir, twoTableDocument)

	const records = 30
	var input strings.Builder
	for i := range records {
		input.WriteString(fillLine(i) + "\n")
		input.WriteString(eventLine(i) + "\n")
		if i%10 == 0 {
			// Blank lines are skipped rather than refused, and a pipe from a
			// shell has them.
			input.WriteString("\n")
		}
	}

	if code, out := pipeInto(t, env, input.String()); code != exitOK {
		t.Fatalf("exit code = %d, want %d\n%s", code, exitOK, out)
	}

	duck := openIcebergDuckDB(t, bucket)
	for _, c := range []struct{ namespace, table string }{{"market", "fills"}, {"archive", "events"}} {
		scan := icebergScan(t, bucket, "warehouse", c.namespace, c.table)

		var total int
		queryRow(t, duck, fmt.Sprintf("SELECT count(*) FROM %s", scan), &total)
		if total != records {
			t.Errorf("%s.%s holds %d rows, want %d", c.namespace, c.table, total, records)
		}
	}

	// The payload column, through base64 and back, and the decimal at its exact
	// digits: the two values a pipe is most likely to mangle.
	var payload, price string
	// hex() rather than a cast, so the assertion is about the bytes rather than
	// about how DuckDB chooses to render them as text.
	queryRow(t, duck, fmt.Sprintf("SELECT hex(payload) FROM %s WHERE seq = 3",
		icebergScan(t, bucket, "warehouse", "archive", "events")), &payload)
	if payload != "010203" {
		t.Errorf("payload reads back %s, want 010203, the three bytes \"AQID\" encodes", payload)
	}
	queryRow(t, duck, fmt.Sprintf("SELECT CAST(price AS VARCHAR) FROM %s WHERE price = 3.5",
		icebergScan(t, bucket, "warehouse", "market", "fills")), &price)
	if price != "3.500000000" {
		t.Errorf("price reads back %q, want %q", price, "3.500000000")
	}
}

// TestSignalDrainsRatherThanDrops is scenario 13's signal clause.
//
// The daemon is given records and then a SIGTERM with the pipe still open, which
// is what a process manager does to a daemon that is running normally. It must
// end within its shutdown timeout, exit zero, and have committed every record it
// had accepted — the difference between a drain and a kill.
//
// Nothing is committed before the signal, and that is asserted rather than
// assumed: the flush thresholds are left at their production defaults, so the
// only thing that can have written those records to the bucket is the drain.
func TestSignalDrainsRatherThanDrops(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	dir := t.TempDir()
	env := bucketEnv(t, bucket, dir, twoTableDocument)

	stdin, producer, err := os.Pipe()
	if err != nil {
		t.Fatalf("opening a pipe: %v", err)
	}
	defer func() { _ = producer.Close() }()

	cmd := exec.Command(binary(t), "run")
	cmd.Env = env
	cmd.Stdin = stdin
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the command: %v", err)
	}
	// However this test ends, the child goes with it: a daemon left running
	// against a temporary directory the test framework is about to delete
	// produces failures about the wrong thing entirely.
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	_ = stdin.Close()

	const records = 25
	for i := range records {
		if _, err := fmt.Fprintln(producer, fillLine(i)); err != nil {
			t.Fatalf("writing line %d: %v", i, err)
		}
	}

	// Wait until every record has been accepted — that is, is durable in the
	// staging database — so that "everything it had accepted" is a number this
	// test knows rather than a guess about timing.
	waitFor(t, "the daemon to accept every record", func() bool {
		return stagedRows(t, filepath.Join(dir, "staging.db")) == records
	})
	if objects := dataFiles(t, bucket, "warehouse", "market", "fills"); len(objects) != 0 {
		t.Fatalf("%d data files exist before the signal; the drain is what should write them", len(objects))
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signalling the command: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("the command exited %v after SIGTERM, want a clean drain", err)
	}

	duck := openIcebergDuckDB(t, bucket)
	var total int
	queryRow(t, duck, fmt.Sprintf("SELECT count(*) FROM %s", icebergScan(t, bucket, "warehouse", "market", "fills")), &total)
	if total != records {
		t.Errorf("the table holds %d rows after a drain, want the %d the daemon had accepted", total, records)
	}
	if n := stagedRows(t, filepath.Join(dir, "staging.db")); n != 0 {
		t.Errorf("%d rows are still staged after a clean drain", n)
	}
}

// TestMalformedLineDiesLoudlyAndLosesNothing is scenario 13's third substrate
// clause, and the claim the whole design rests on for a pipe.
//
// The run exits one, naming the line. Then a second run against the same staging
// file replays every record accepted before the bad line, exactly once — which
// is what makes "what was accepted is durable, and what was still in the pipe
// was never icelake's" a fact an operator can act on rather than a hope.
func TestMalformedLineDiesLoudlyAndLosesNothing(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	dir := t.TempDir()
	env := bucketEnv(t, bucket, dir, twoTableDocument)

	const good = 20
	var input strings.Builder
	for i := range good {
		input.WriteString(fillLine(i) + "\n")
	}
	// The bad line, and then more good ones that must never be written: a run
	// that carried on past it would show up here as extra rows.
	input.WriteString(`{"table":"market.fills","row":{"symbol":"X","price":"1.0","ts_ms":1,"typo":true}}` + "\n")
	for i := good; i < good+5; i++ {
		input.WriteString(fillLine(i) + "\n")
	}

	code, out := pipeInto(t, env, input.String())
	if code != exitRuntime {
		t.Fatalf("exit code = %d, want %d\n%s", code, exitRuntime, out)
	}
	if !strings.Contains(out, fmt.Sprintf("line %d", good+1)) {
		t.Errorf("the failure does not name line %d:\n%s", good+1, out)
	}
	if n := stagedRows(t, filepath.Join(dir, "staging.db")); n != good {
		t.Fatalf("%d rows are staged after the run died, want the %d accepted before the bad line", n, good)
	}

	// The second run, over the same data directory, with nothing new to read.
	// Replay happens before a byte of stdin is read, so an immediate end of
	// input is enough.
	if code, out := pipeInto(t, env, ""); code != exitOK {
		t.Fatalf("the second run exited %d, want %d\n%s", code, exitOK, out)
	}

	duck := openIcebergDuckDB(t, bucket)
	scan := icebergScan(t, bucket, "warehouse", "market", "fills")

	var total, distinct int
	queryRow(t, duck, fmt.Sprintf("SELECT count(*) FROM %s", scan), &total)
	queryRow(t, duck, fmt.Sprintf("SELECT count(DISTINCT ts_ms) FROM %s", scan), &distinct)
	if total != good || distinct != good {
		t.Errorf("the table holds %d rows and %d distinct timestamps, want %d of each: every accepted record commits exactly once",
			total, distinct, good)
	}
	if n := stagedRows(t, filepath.Join(dir, "staging.db")); n != 0 {
		t.Errorf("%d rows are still staged after the replay committed", n)
	}
}

// openDuckDB opens an in-process DuckDB with nothing configured, for reading
// Parquet files off a local disk.
func openDuckDB(tb testing.TB) *sql.DB {
	tb.Helper()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		tb.Fatalf("opening duckdb: %v", err)
	}
	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Errorf("closing duckdb: %v", err)
		}
	})

	return db
}

// openIcebergDuckDB opens DuckDB with the iceberg and httpfs extensions and the
// object store configured, which is how these tests read a table back the way an
// outside consumer would.
//
// A machine that cannot reach DuckDB's extension repository skips rather than
// fails, exactly as the library's own suite does: the validation is real and
// required in CI, and a developer offline should get a clear skip instead of a
// mystery failure.
func openIcebergDuckDB(tb testing.TB, bucket testsubstrate.Bucket) *sql.DB {
	tb.Helper()

	db := openDuckDB(tb)
	endpoint, err := url.Parse(bucket.Endpoint)
	if err != nil {
		tb.Fatalf("parsing the endpoint: %v", err)
	}

	for _, stmt := range []string{
		"INSTALL iceberg", "LOAD iceberg", "INSTALL httpfs", "LOAD httpfs",
		fmt.Sprintf("SET s3_endpoint = '%s'", endpoint.Host),
		fmt.Sprintf("SET s3_region = '%s'", bucket.Region),
		fmt.Sprintf("SET s3_access_key_id = '%s'", bucket.AccessKeyID),
		fmt.Sprintf("SET s3_secret_access_key = '%s'", bucket.SecretAccessKey),
		"SET s3_url_style = 'path'",
		fmt.Sprintf("SET s3_use_ssl = %t", endpoint.Scheme == "https"),
	} {
		if _, err := db.ExecContext(context.Background(), stmt); err != nil {
			if strings.HasPrefix(stmt, "INSTALL") {
				tb.Skipf("cannot install the DuckDB extensions this validation needs (%q): %v", stmt, err)
			}
			tb.Fatalf("duckdb %q: %v", stmt, err)
		}
	}

	return db
}

// queryRow runs a single-row query and scans it.
func queryRow(tb testing.TB, db *sql.DB, query string, dest ...any) {
	tb.Helper()

	if err := db.QueryRowContext(context.Background(), query).Scan(dest...); err != nil {
		tb.Fatalf("duckdb query %q: %v", query, err)
	}
}

// icebergScan is the table expression a query reads a committed table through:
// the Iceberg table at its current metadata file, found by listing the bucket
// rather than by asking the daemon's own catalog.
func icebergScan(tb testing.TB, bucket testsubstrate.Bucket, prefix, namespace, table string) string {
	tb.Helper()

	dir := path.Join(prefix, namespace, table, "metadata") + "/"
	out, err := testsubstrate.NewS3Client(bucket).ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket.Bucket),
		Prefix: aws.String(dir),
	})
	if err != nil {
		tb.Fatalf("listing %s: %v", dir, err)
	}

	best, bestVersion := "", -1
	for _, o := range out.Contents {
		name := path.Base(aws.ToString(o.Key))
		if !strings.HasSuffix(name, ".metadata.json") {
			continue
		}
		version, err := strconv.Atoi(name[:strings.IndexByte(name, '-')])
		if err != nil {
			continue
		}
		if version > bestVersion {
			best, bestVersion = aws.ToString(o.Key), version
		}
	}
	if best == "" {
		tb.Fatalf("no metadata file under %s", dir)
	}

	return fmt.Sprintf("iceberg_scan('s3://%s')", path.Join(bucket.Bucket, best))
}

// dataFiles lists a table's data files, which is how a test says "nothing has
// been committed yet" without believing the daemon about it.
func dataFiles(tb testing.TB, bucket testsubstrate.Bucket, prefix, namespace, table string) []string {
	tb.Helper()

	out, err := testsubstrate.NewS3Client(bucket).ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket.Bucket),
		Prefix: aws.String(path.Join(prefix, namespace, table, "data") + "/"),
	})
	if err != nil {
		tb.Fatalf("listing data files: %v", err)
	}

	keys := make([]string, 0, len(out.Contents))
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}

	return keys
}

// stagedRows counts the rows in a staging database, by opening the real file.
//
// It is what makes "everything the daemon had accepted" a number rather than a
// guess: a record is accepted exactly when it is durable here, which is the
// library's own definition and the one the manual states.
func stagedRows(tb testing.TB, stagingPath string) int {
	tb.Helper()

	if _, err := os.Stat(stagingPath); err != nil {
		return 0
	}

	db, err := sql.Open("sqlite", stagingPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		tb.Fatalf("opening the staging database: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRow("SELECT count(*) FROM staging").Scan(&n); err != nil {
		// This is polled while a daemon is starting, so it can meet the file
		// after SQLite has created it and before the migrations have run. That
		// is not a failure and not zero-by-accident: a database with no staging
		// table holds no staged rows, which is exactly what the caller is
		// asking.
		if strings.Contains(err.Error(), "no such table") {
			return 0
		}
		tb.Fatalf("counting staged rows: %v", err)
	}

	return n
}

// waitFor polls until cond holds, failing the test if it never does.
//
// It waits for work a running process is doing, never for a timer, which is why
// the budget can be generous without making a failure slow to notice.
func waitFor(tb testing.TB, what string, cond func() bool) {
	tb.Helper()

	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestBackpressureHoldsStdinAndResumes is the one loop in this command that
// answers an error by not giving up, so it is the one that most needs a test.
//
// When the bucket cannot be reached for long enough, the staging store reaches
// the ceiling and the library refuses the record — and the record is then still
// the producer's. The daemon holds stdin and retries rather than exiting,
// because exiting would turn a bucket outage into data loss upstream, and
// holding is what propagates the outage back to a producer that already knows
// what to do about a slow consumer.
//
// The shape is scenario 7's ceiling clause, moved out of the library and into a
// process: a deliberately tiny ceiling, a bucket that stops answering, and then
// one that answers again. What is asserted is all three parts of the claim —
// that the daemon does not exit, that it says so, and that everything it was
// holding is committed once the bucket comes back.
func TestBackpressureHoldsStdinAndResumes(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	gate := startGate(t, bucket.Endpoint)
	dir := t.TempDir()

	// A ceiling small enough to reach in a few lines, and a batch of one record
	// so that every line is a flush attempt: the point is to fill staging with
	// batches that cannot leave, not to test batching. The flush interval is
	// short so that the batches stuck behind the outage get a fresh attempt
	// promptly once it ends — the library retries on a trigger, and this is the
	// trigger it would otherwise wait fifteen minutes for.
	env := append(bucketEnv(t, bucket, dir, twoTableDocument),
		envEndpoint.name+"="+gate.Endpoint,
		envFlushMaxRecords.name+"=1",
		envFlushMaxBytes.name+"=1KB",
		envStagingMaxRecords.name+"=5",
		envStagingMaxBytes.name+"=1KB",
		envFlushInterval.name+"=1s",
	)

	stdin, producer, err := os.Pipe()
	if err != nil {
		t.Fatalf("opening a pipe: %v", err)
	}
	defer func() { _ = producer.Close() }()

	var printed lockedBuffer
	cmd := exec.Command(binary(t), "run")
	cmd.Env = env
	cmd.Stdin = stdin
	cmd.Stdout = &printed
	cmd.Stderr = &printed
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the command: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	_ = stdin.Close()

	// One record while the bucket is reachable, which is what creates the
	// tables: a writer cannot open against a bucket it cannot read.
	if _, err := fmt.Fprintln(producer, fillLine(0)); err != nil {
		t.Fatalf("writing the first line: %v", err)
	}
	waitFor(t, "the first record to commit", func() bool {
		return len(dataFiles(t, bucket, "warehouse", "market", "fills")) > 0
	})

	// From here nothing reaches the store.
	gate.SetReject(true)

	const held = 8
	for i := 1; i <= held; i++ {
		if _, err := fmt.Fprintln(producer, fillLine(i)); err != nil {
			t.Fatalf("writing line %d: %v", i, err)
		}
	}

	waitFor(t, "the daemon to report that staging is full", func() bool {
		return strings.Contains(printed.String(), "staging is full")
	})

	// It is holding, not dying. That is the whole claim, so it is checked
	// rather than inferred from the absence of an error: a process that had
	// exited would still have printed the line above.
	if cmd.ProcessState != nil {
		t.Fatalf("the daemon exited while staging was full:\n%s", printed.String())
	}
	if n := stagedRows(t, filepath.Join(dir, "staging.db")); n > 5 {
		t.Errorf("%d rows are staged, want no more than the ceiling of 5", n)
	}

	// And the bucket comes back.
	gate.SetReject(false)

	waitFor(t, "the daemon to resume reading", func() bool {
		return strings.Contains(printed.String(), "reading resumed")
	})

	if err := producer.Close(); err != nil {
		t.Fatalf("closing the pipe: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("the daemon exited %v after the outage ended:\n%s", err, printed.String())
	}

	// Nothing was dropped while it was holding: every line the producer wrote is
	// in the table, exactly once.
	duck := openIcebergDuckDB(t, bucket)
	var total, distinct int
	scan := icebergScan(t, bucket, "warehouse", "market", "fills")
	queryRow(t, duck, fmt.Sprintf("SELECT count(*) FROM %s", scan), &total)
	queryRow(t, duck, fmt.Sprintf("SELECT count(DISTINCT ts_ms) FROM %s", scan), &distinct)
	if want := held + 1; total != want || distinct != want {
		t.Errorf("the table holds %d rows and %d distinct timestamps, want %d of each", total, distinct, want)
	}
	if n := stagedRows(t, filepath.Join(dir, "staging.db")); n != 0 {
		t.Errorf("%d rows are still staged after a clean drain", n)
	}
}

// lockedBuffer collects a child process's output while a test reads it, which
// two goroutines otherwise do to the same bytes at once.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.String()
}

// TestABlankLineOnALiveStdinDoesNotEndTheRun holds the one thing chunked reading
// can get wrong that no bulk-input test can see: that a chunk which came back
// with nothing in it means "read again", not "the input is over".
//
// A blank line is skipped and still counted, which is a rule this command has had
// since it existed. Under chunked reading, a blank line that arrives on its own —
// with nothing queued behind it, which is what a real producer that pauses
// produces — makes the chunk come back empty while the input has not ended, and a
// loop that reads that as the end exits zero and silently drops the rest of the
// pipe. That is exactly the defect this test was written for, found in review at
// M15, and it survived the whole suite because every other test in this file
// hands the daemon its input in one write: the next line is already queued before
// the blank one is drained, so the chunk is never empty and the bug is never
// reached.
//
// So the input here is written in three separate writes with real pauses between
// them, which is the only shape that reaches it. What is asserted is the whole
// point: nothing after the blank line is lost.
func TestABlankLineOnALiveStdinDoesNotEndTheRun(t *testing.T) {
	dir := t.TempDir()
	env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)

	producer, wait := runStreaming(t, env)

	// One record, and then a pause long enough that the daemon is certainly
	// blocked waiting for the next line rather than still draining this one.
	writeLines(t, producer, fillLine(0))
	time.Sleep(streamPause)

	// The blank line, alone. This is the moment the defect fired.
	writeLines(t, producer, "")
	time.Sleep(streamPause)

	// And the rest of the pipe, which a daemon that had stopped reading would
	// never see.
	writeLines(t, producer, fillLine(1), fillLine(2))
	time.Sleep(streamPause)

	if err := producer.Close(); err != nil {
		t.Fatalf("closing the pipe: %v", err)
	}

	code, out := wait()
	if code != exitOK {
		t.Fatalf("exit code = %d, want %d\n%s", code, exitOK, out)
	}

	duck := openDuckDB(t)
	table := fmt.Sprintf("read_parquet('%s')", filepath.Join(dir, "cache", "market", "fills", "data", "*.parquet"))

	var total int
	queryRow(t, duck, fmt.Sprintf("SELECT count(*) FROM %s", table), &total)
	if total != 3 {
		t.Errorf("the cache holds %d rows, want 3: the two records written after the blank line are the ones a run that stopped reading would lose",
			total)
	}
}

// TestAStreamedBadLineWritesTheGoodPrefixOfItsChunk holds the promise chunked
// writing puts under the most pressure, at the granularity usage.md states it:
// everything before a bad line is durable, *including* when the bad line is one
// the library refuses rather than one this command catches while parsing.
//
// The two cases are not the same mechanically and that is the whole reason this
// test exists. A line this command refuses is caught before anything in the chunk
// has been written, so truncating the chunk is enough. A row the library refuses
// is caught after that table's whole group has been handed over as one
// transaction, so the group is refused entire — and the daemon has to write it
// again, cut at the bad line, or the records before it would be lost. Nothing
// else in the suite would notice if that second write disappeared.
//
// The stream interleaves two tables and puts the *other* table first, which is
// what makes the documented residue reachable: a group already written cannot be
// unwritten, so lines of that table from after the bad line may be durable too.
func TestAStreamedBadLineWritesTheGoodPrefixOfItsChunk(t *testing.T) {
	dir := t.TempDir()
	env := append(os.Environ(),
		envDataDir.name+"="+dir,
		envSchemaFile.name+"="+writeDocument(t, dir, twoTableDocument),
		envLocalOnly.name+"=true",
	)

	// The events line comes first so that its group is written before the fills
	// group that carries the bad row; with fills first there is no residue to
	// observe at all, because the failure sets the cut before the second group
	// is reached.
	const pairs = 6
	const badAt = 2*pairs + 1 // one-based, and the events/fills pairs above it

	var input strings.Builder
	for i := range pairs {
		input.WriteString(eventLine(i) + "\n")
		input.WriteString(fillLine(i) + "\n")
	}
	// The bad line: a well-formed envelope for a declared table whose row
	// carries a key the table does not have, which only the library can refuse.
	input.WriteString(`{"table":"market.fills","row":{"symbol":"X","price":"1.0","ts_ms":1,"typo":true}}` + "\n")
	for i := pairs; i < pairs+3; i++ {
		input.WriteString(eventLine(i) + "\n")
		input.WriteString(fillLine(i) + "\n")
	}

	code, out := pipeInto(t, env, input.String())
	if code != exitRuntime {
		t.Fatalf("exit code = %d, want %d\n%s", code, exitRuntime, out)
	}
	if !strings.Contains(out, fmt.Sprintf("line %d", badAt)) {
		t.Errorf("the failure does not name line %d:\n%s", badAt, out)
	}
	// The library's row index is deliberately not in the message: it counts from
	// the start of a slice this command built, and an operator counts from the
	// start of their file.
	if strings.Contains(out, "of the batch was refused") {
		t.Errorf("the failure leaks the library's own row index beside the line number:\n%s", out)
	}

	// A second run with nothing to read replays what the first accepted, which
	// is what puts it in the cache where DuckDB can count it.
	if code, out := pipeInto(t, env, ""); code != exitOK {
		t.Fatalf("the second run exited %d, want %d\n%s", code, exitOK, out)
	}

	duck := openDuckDB(t)
	fills := countRows(t, duck, filepath.Join(dir, "cache", "market", "fills", "data", "*.parquet"))
	events := countRows(t, duck, filepath.Join(dir, "cache", "archive", "events", "data", "*.parquet"))

	// The load-bearing assertion, and the one that fails if the truncated
	// rewrite is ever dropped: every fills line before the bad one is durable,
	// and none from the bad one on is.
	if fills != pairs {
		t.Errorf("%d fills rows are durable, want the %d written before the bad line", fills, pairs)
	}

	// And the residue, stated as the two outcomes the design permits rather than
	// as one, because which of them happens depends on where the chunk boundary
	// fell, and that is a property of how the producer wrote rather than of this
	// command. Either the whole stream arrived as one chunk, in which case the
	// events group was written before the fills group failed and carries its
	// later lines too; or it was split, in which case the events lines after the
	// cut were never read. Anything else means the cut was not applied per
	// table.
	switch events {
	case pairs + 3:
		t.Logf("one chunk: the events group was already written, so its %d lines after the bad one are durable too", 3)
	case pairs:
		t.Logf("the stream was split into chunks, so no events line after the bad one was ever read")
	default:
		t.Errorf("%d events rows are durable, want either %d (one chunk, the documented residue) or %d (a split stream)",
			events, pairs+3, pairs)
	}
}

// streamPause is how long the streaming tests wait between writes. It only has
// to be longer than the daemon takes to drain one line and go back to waiting,
// which is microseconds; it is this long so the test is not sensitive to a busy
// machine.
const streamPause = 300 * time.Millisecond

// runStreaming starts the built binary with a real pipe on stdin, so a test can
// write to it over time rather than handing it everything at once. The returned
// function closes the process's copy of the pipe and waits for it.
//
// Handing the daemon a string is what every other test in this file does and is
// the wrong shape for anything about *when* input arrives: a strings.Reader is
// never empty until it is finished, so the daemon never waits, and the whole
// class of defect that only appears when it does becomes unreachable.
func runStreaming(tb testing.TB, env []string) (io.WriteCloser, func() (int, string)) {
	tb.Helper()

	stdin, producer, err := os.Pipe()
	if err != nil {
		tb.Fatalf("opening a pipe: %v", err)
	}

	var printed lockedBuffer
	cmd := exec.Command(binary(tb), "run")
	cmd.Env = env
	cmd.Stdin = stdin
	cmd.Stdout = &printed
	cmd.Stderr = &printed
	if err := cmd.Start(); err != nil {
		tb.Fatalf("starting the command: %v", err)
	}
	tb.Cleanup(func() {
		_ = producer.Close()
		_ = cmd.Process.Kill()
	})
	// The parent's copy of the read end goes now, or closing the write end
	// below would never reach the child as an end of input.
	_ = stdin.Close()

	return producer, func() (int, string) {
		err := cmd.Wait()
		out := printed.String()
		if err == nil {
			return 0, out
		}

		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			tb.Fatalf("running the command: %v\n%s", err, out)
		}

		return exit.ExitCode(), out
	}
}

// writeLines writes each line, with its terminator, to a live pipe.
func writeLines(tb testing.TB, w io.Writer, lines ...string) {
	tb.Helper()

	for _, line := range lines {
		if _, err := fmt.Fprintln(w, line); err != nil {
			tb.Fatalf("writing to the pipe: %v", err)
		}
	}
}

// countRows counts what DuckDB finds under a cache glob, answering zero when the
// table has no files at all rather than failing: "nothing was written" is one of
// the outcomes a test here may legitimately expect.
func countRows(tb testing.TB, duck *sql.DB, glob string) int {
	tb.Helper()

	matches, err := filepath.Glob(glob)
	if err != nil {
		tb.Fatalf("globbing %s: %v", glob, err)
	}
	if len(matches) == 0 {
		return 0
	}

	var total int
	queryRow(tb, duck, fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", glob), &total)

	return total
}

// cborFillItem and cborEventItem render one record of each table as a CBOR
// envelope, in the spellings the manual's CBOR section describes: a decimal as
// digits in a text string, a timestamp as an integer, and a payload as a byte
// string with no base64 anywhere.
func cborFillItem(tb testing.TB, i int) []byte {
	tb.Helper()

	return cborItem(tb, "market.fills", map[string]any{
		"symbol": fmt.Sprintf("SYM%d", i%7),
		"price":  fmt.Sprintf("%d.500000000", i),
		"ts_ms":  1_700_000_000_000 + int64(i)*1000,
	})
}

func cborEventItem(tb testing.TB, i int) []byte {
	tb.Helper()

	return cborItem(tb, "archive.events", map[string]any{
		"source_id":   fmt.Sprintf("stream-%d", i%3),
		"seq":         int64(i),
		"observed_at": 1_700_000_000_000 + int64(i)*1000,
		"payload":     []byte{0x01, 0x02, 0x03},
	})
}

// cborItem marshals one envelope.
func cborItem(tb testing.TB, table string, row map[string]any) []byte {
	tb.Helper()

	item, err := cbor.Marshal(map[string]any{"table": table, "row": row})
	if err != nil {
		tb.Fatalf("rendering a CBOR envelope: %v", err)
	}

	return item
}

// TestACBORStreamIsWrittenLikeAJSONOne is scenario 15's clause about the
// command: the binary an operator installs, a real pipe carrying CBOR records
// with no newlines in it, **nothing configured at all**, and Parquet at the
// other end that DuckDB reads back at exact values.
//
// "Nothing configured" is the claim. The command has no input-format setting and
// no code that decides one, because the library's grammar decides each record's
// spelling from that record's own first byte — so what this test proves about the
// command is that it hands the pipe over and stays out of the way. The grammar's
// own rules are held at library level, where every row of the coercion table is
// reachable without starting a process.
//
// Two tables interleaved, because a CBOR sequence has no separators at all: if
// the framing were wrong by one byte in either direction, the second record
// would not decode, and a single-table stream of identical shapes is the one
// input where that could go unnoticed.
func TestACBORStreamIsWrittenLikeAJSONOne(t *testing.T) {
	dir := t.TempDir()
	env := append(os.Environ(),
		envDataDir.name+"="+dir,
		envSchemaFile.name+"="+writeDocument(t, dir, twoTableDocument),
		envLocalOnly.name+"=true",
		envZSTDLevel.name+"=1",
	)

	const records = 40
	var input bytes.Buffer
	for i := range records {
		input.Write(cborFillItem(t, i))
		input.Write(cborEventItem(t, i))
	}
	if bytes.ContainsRune(input.Bytes(), '\n') {
		t.Logf("the sequence happens to contain a 0x0a byte inside a value, which is exactly why it is not line-framed")
	}

	if code, out := pipeInto(t, env, input.String()); code != exitOK {
		t.Fatalf("exit code = %d, want %d\n%s", code, exitOK, out)
	}

	duck := openDuckDB(t)
	fills := fmt.Sprintf("read_parquet('%s')", filepath.Join(dir, "cache", "market", "fills", "data", "*.parquet"))
	events := fmt.Sprintf("read_parquet('%s')", filepath.Join(dir, "cache", "archive", "events", "data", "*.parquet"))

	var fillCount, eventCount int
	queryRow(t, duck, fmt.Sprintf("SELECT count(*) FROM %s", fills), &fillCount)
	queryRow(t, duck, fmt.Sprintf("SELECT count(*) FROM %s", events), &eventCount)
	if fillCount != records || eventCount != records {
		t.Fatalf("the cache holds %d fills and %d events, want %d of each", fillCount, eventCount, records)
	}

	// The three columns whose CBOR spelling differs from their JSON one, read
	// back at their exact values: the decimal at its digits rather than a float,
	// the timestamp as a real timestamp, and the payload as the bytes that were
	// sent rather than as the base64 a JSON producer would have had to type.
	var price string
	queryRow(t, duck, fmt.Sprintf("SELECT CAST(price AS VARCHAR) FROM %s ORDER BY ts_ms LIMIT 1 OFFSET 7", fills), &price)
	if price != "7.500000000" {
		t.Errorf("price = %q, want %q", price, "7.500000000")
	}

	var payload []byte
	queryRow(t, duck, fmt.Sprintf("SELECT payload FROM %s LIMIT 1", events), &payload)
	if !bytes.Equal(payload, []byte{0x01, 0x02, 0x03}) {
		t.Errorf("payload = %v, want the three bytes that were sent", payload)
	}

	// And a bad record in this format names a *record* number rather than a line
	// number, which is the one piece of this command's printed contract the
	// format changes.
	bad := cborItem(t, "market.fills", map[string]any{"symbol": "X", "price": "1.0", "ts_ms": int64(1), "typo": true})
	var stream bytes.Buffer
	stream.Write(cborFillItem(t, 0))
	stream.Write(cborFillItem(t, 1))
	stream.Write(bad)

	code, out := pipeInto(t, append(env, envDataDir.name+"="+t.TempDir()), stream.String())
	if code != exitRuntime {
		t.Fatalf("exit code = %d, want %d\n%s", code, exitRuntime, out)
	}
	if !strings.Contains(out, "record 3") {
		t.Errorf("the failure does not name record 3:\n%s", out)
	}
	if strings.Contains(out, "line ") {
		t.Errorf("the failure calls a CBOR record a line, which is a thing this spelling does not have:\n%s", out)
	}
}

// TestOneStreamCarriesBothSpellings is the requirement the grammar exists for,
// through the binary: two producers writing into one pipe, one of them sending
// JSON lines and the other sending CBOR items, interleaved, with nothing
// configured anywhere and no restart between them.
//
// It is here rather than only at library level because "no configuration" is a
// claim about this program: a version of this feature with an environment
// variable in it would pass every library test and fail this one.
func TestOneStreamCarriesBothSpellings(t *testing.T) {
	dir := t.TempDir()
	env := append(os.Environ(),
		envDataDir.name+"="+dir,
		envSchemaFile.name+"="+writeDocument(t, dir, twoTableDocument),
		envLocalOnly.name+"=true",
		envZSTDLevel.name+"=1",
	)

	const pairs = 20
	var input bytes.Buffer
	for i := range pairs {
		// The same record shapes in both spellings, alternating, with a blank
		// line thrown in to prove the JSON side still skips those.
		input.WriteString(fillLine(2*i) + "\n")
		input.Write(cborFillItem(t, 2*i+1))
		input.WriteString("\n")
		input.Write(cborEventItem(t, i))
		input.WriteString(eventLine(i) + "\n")
	}

	if code, out := pipeInto(t, env, input.String()); code != exitOK {
		t.Fatalf("exit code = %d, want %d\n%s", code, exitOK, out)
	}

	duck := openDuckDB(t)
	fills := fmt.Sprintf("read_parquet('%s')", filepath.Join(dir, "cache", "market", "fills", "data", "*.parquet"))
	events := fmt.Sprintf("read_parquet('%s')", filepath.Join(dir, "cache", "archive", "events", "data", "*.parquet"))

	var fillCount, eventCount, distinctPrices int
	queryRow(t, duck, fmt.Sprintf("SELECT count(*) FROM %s", fills), &fillCount)
	queryRow(t, duck, fmt.Sprintf("SELECT count(*) FROM %s", events), &eventCount)
	queryRow(t, duck, fmt.Sprintf("SELECT count(DISTINCT price) FROM %s", fills), &distinctPrices)

	if fillCount != 2*pairs || eventCount != 2*pairs {
		t.Fatalf("the cache holds %d fills and %d events, want %d of each", fillCount, eventCount, 2*pairs)
	}
	// Every fills record carried a distinct price, half of them written as JSON
	// digits and half as a CBOR text string, so a spelling that had been mangled
	// rather than refused would show up as a collision here.
	if distinctPrices != 2*pairs {
		t.Errorf("%d distinct prices, want %d: the two spellings must reach the column at the same values",
			distinctPrices, 2*pairs)
	}
}
