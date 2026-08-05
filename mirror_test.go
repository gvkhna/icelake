package icelake_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// This file is the shared machinery for scenarios 16 and 17: how a test points
// icelake's mirror at the container, and how it reads the mirror back.
//
// The reads go through the driver directly rather than through icelake, for the
// reason the suite reads the staging file directly and validates the lake with
// DuckDB: a test that asked the library whether the library's own write worked
// would be believing the thing under test.

// mirrorFor returns the ClickHouse configuration a store is opened with.
func mirrorFor(ch *testsubstrate.ClickHouse) *icelake.ClickHouseConfig {
	return &icelake.ClickHouseConfig{
		Addr:     ch.Addr,
		Username: ch.Username,
		Password: ch.Password,
	}
}

// chConn opens a driver connection for a test's own assertions, closed when the
// test ends.
func chConn(tb testing.TB, ch *testsubstrate.ClickHouse) driver.Conn {
	tb.Helper()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{ch.Addr},
		Auth: clickhouse.Auth{Database: "default", Username: ch.Username, Password: ch.Password},
	})
	if err != nil {
		tb.Fatalf("opening a ClickHouse connection: %v", err)
	}
	tb.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		tb.Fatalf("pinging ClickHouse: %v", err)
	}

	return conn
}

// chExec runs one statement, failing the test if it does not.
func chExec(tb testing.TB, conn driver.Conn, query string) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := conn.Exec(ctx, query); err != nil {
		tb.Fatalf("running %q: %v", query, err)
	}
}

// chCount reads one count.
func chCount(tb testing.TB, conn driver.Conn, query string) uint64 {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var n uint64
	if err := conn.QueryRow(ctx, query).Scan(&n); err != nil {
		tb.Fatalf("running %q: %v", query, err)
	}

	return n
}

// chStrings reads one column of strings, in the query's own order.
func chStrings(tb testing.TB, conn driver.Conn, query string) []string {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	rows, err := conn.Query(ctx, query)
	if err != nil {
		tb.Fatalf("running %q: %v", query, err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			tb.Fatalf("scanning %q: %v", query, err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		tb.Fatalf("reading %q: %v", query, err)
	}

	return out
}

// chColumnTypes reads a table's columns as the server itself describes them,
// which is what the mirror's own reconciliation diffs against.
func chColumnTypes(tb testing.TB, conn driver.Conn, database, table string) map[string]string {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	rows, err := conn.Query(ctx,
		"SELECT name, type FROM system.columns WHERE database = ? AND table = ?", database, table)
	if err != nil {
		tb.Fatalf("reading %s.%s columns: %v", database, table, err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[string]string)
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			tb.Fatalf("scanning columns: %v", err)
		}
		out[name] = typ
	}
	if err := rows.Err(); err != nil {
		tb.Fatalf("reading columns: %v", err)
	}

	return out
}

// sortedCopy returns a sorted copy, so two lists of names compare as sets.
func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)

	return out
}
