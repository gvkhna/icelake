package staging

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/schemamap"
)

// The benchmarks TESTING.md's scenario 14 describes, for the layer the M15
// decisions are about.
//
// They assert nothing and are not part of any gate — a `testing.B` does not run
// under `go test` without `-bench`, which is what keeps `mise run check` as fast
// as it was. `mise run bench` runs them, and the section named "How the
// benchmarks are run and read" in TESTING.md says what the figures mean together.
// The short form is that BenchmarkDiskFsync is the floor every other figure here
// is bounded by, BenchmarkStagingAppend should land within noise of it because
// one record per transaction is one fsync per record, and
// BenchmarkStagingAppendBatch is the same work with the fsync amortized across a
// thousand records.
//
// The single-record benchmark is kept deliberately, even though nothing in the
// library now has to insert one record at a time to go fast: it is the M4
// behaviour, and having it beside the batch figure is what makes the comparison
// in ARCHITECTURE.md a measurement anyone can re-run rather than a number in a
// document.

// benchBatch is the batch size the grouped benchmark uses. A thousand is where
// the measured curve has stopped being steep — the step from a hundred to a
// thousand is worth several times the step from a thousand to ten thousand — so
// it is the size that represents the amortization rather than the extreme of it.
const benchBatch = 1000

// BenchmarkDiskFsync measures the machine's own fsync rate: a small write and a
// sync, and nothing else. It is the physical floor under every other number in
// this file, and it is what makes those numbers comparable across machines,
// because the only thing that changes between two disks is this figure.
func BenchmarkDiskFsync(b *testing.B) {
	f, err := os.Create(filepath.Join(b.TempDir(), "fsync.bin"))
	if err != nil {
		b.Fatalf("creating the file: %v", err)
	}
	b.Cleanup(func() { _ = f.Close() })

	// A payload the size of a staged record's, so the write itself is as
	// representative as a write that small can be. It is not what costs anything
	// here: the sync is.
	buf := make([]byte, 256)

	b.ResetTimer()
	for range b.N {
		if _, err := f.Write(buf); err != nil {
			b.Fatalf("writing: %v", err)
		}
		if err := f.Sync(); err != nil {
			b.Fatalf("syncing: %v", err)
		}
	}
}

// BenchmarkStagingAppend is one record per transaction: the behaviour measured
// at M4 and superseded at M15, kept so the comparison stays measured.
func BenchmarkStagingAppend(b *testing.B) {
	ctx := context.Background()
	s := benchStore(b)
	row := benchRow(b)

	b.ResetTimer()
	for i := range b.N {
		row.CreatedAt = benchStagedAt.Add(time.Duration(i) * time.Millisecond)
		if _, err := s.Append(ctx, row); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}

// BenchmarkStagingAppendBatch is the same records through the batch door, at
// each of the transaction sizes ARCHITECTURE.md's M15 table reports. The ns/op
// each sub-benchmark reports is per record, so every one of them is directly
// comparable with the single-record figure above, and the table in that document
// is a thing anyone can re-run rather than a number to be believed.
func BenchmarkStagingAppendBatch(b *testing.B) {
	for _, size := range []int{100, benchBatch, 10_000, 50_000} {
		b.Run(fmt.Sprintf("rows=%d", size), func(b *testing.B) {
			ctx := context.Background()
			s := benchStore(b)

			rows := make([]AppendRow, size)
			row := benchRow(b)
			for i := range rows {
				rows[i] = row
			}

			b.ResetTimer()
			for i := 0; i < b.N; i += size {
				n := min(size, b.N-i)
				if _, err := s.AppendBatch(ctx, rows[:n]); err != nil {
					b.Fatalf("AppendBatch: %v", err)
				}
			}
		})
	}
}

// benchStagedAt is a fixed instant the benchmarks stamp their rows with, so that
// nothing in them reads a clock the measurement would then include.
var benchStagedAt = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

// benchStore opens a real staging database in the benchmark's own temporary
// directory, with the real pragmas. Nothing is stood in for: the whole point of
// these figures is the cost of a real transaction on a real disk.
func benchStore(b *testing.B) *Store {
	b.Helper()

	s, err := Open(context.Background(), filepath.Join(b.TempDir(), "staging.db"))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })

	return s
}

// benchRow builds one staged record through the real declaration and the real
// encoder, exactly as the tests in this package build theirs.
func benchRow(b *testing.B) AppendRow {
	b.Helper()

	decl, err := schemamap.Declare[fill](fillNamespace, fillTable)
	if err != nil {
		b.Fatalf("Declare: %v", err)
	}
	d, err := canon.Describe(decl.Table(), decl.IcebergSchema())
	if err != nil {
		b.Fatalf("Describe: %v", err)
	}
	payload, err := canon.Encode(fillTable, d, fillValues(1))
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}

	return AppendRow{
		Namespace:  fillNamespace,
		Table:      fillTable,
		Descriptor: d,
		Payload:    payload,
		CreatedAt:  benchStagedAt,
	}
}
