package e2e

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/internal/canon"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// This file is TESTING.md scenario 7, "Fail-loudly guardrails": every claim in
// ARCHITECTURE.md and SCHEMA.md that something is refused, asserted as
// behaviour rather than left as prose. Each assertion goes through errors.Is or
// errors.As against a typed error and its exported fields; none of them reads
// message text, which is the same policy that forced the taxonomy to exist.
//
// The scenario's seven clauses are proven here except where an earlier
// milestone already proved exactly that clause, in which case this file names
// where rather than asserting it twice:
//
//   - Config validation — below, one case per rule plus the multi-violation
//     case that proves the joined error rather than first-error-wins.
//   - Namespace/table name validation — below, all seven rejections through
//     OpenWriter, in both name positions.
//   - Staging ceiling — below, including the recovery half: the writer accepts
//     records again once the store answers and the backlog commits. A second
//     ceiling test below holds the process-wide half of the same claim, across
//     two tables; it was added at the post-M9 completeness audit and says at its
//     own definition why the arc test alone could not hold it.
//   - Poison records — below, the two cases the scenario names.
//   - Two-derivation cross-check — below, both shapes of disagreement. The
//     alias half of the same claim (that the error a caller catches is the
//     public type) is asserted in errors_test.go.
//   - Nested struct rejection — below, through OpenWriter. The derivation-level
//     half is in internal/schemamap.
//   - Per-table reconciliation isolation — already proven exactly as the
//     scenario words it by TestReconcileRefusals in reconcile_test.go: one
//     store, a first table whose declaration adds a required field to a table
//     that already has rows (refused with a ReconcileError), and a second table
//     that opens, writes and commits normally, checked with DuckDB. Repeating
//     it here would be a second copy of a passing test, not a second claim.
//
// Two refusals below go beyond the scenario's own text, added at the post-M9
// completeness audit because SCHEMA.md asserted them as established fact and
// nothing held them: a slice of structs, which SCHEMA.md records as a stronger
// limit than the nested-struct clause above and which was never declared
// anywhere in the suite, and a decimal past the precision ceiling, which is a
// cap that lives entirely at the declaration seam and would be invisible
// everywhere else if it moved.

// -- Config validation.

// TestConfigValidationRefusesEveryBadField walks the matrix scenario 7 names,
// one rule per case, and then asserts the joined-error decision: a Config with
// several things wrong reports all of them in one run.
//
// It needs no substrate, and that is itself part of the claim — an invalid
// configuration is refused before a file is touched or a request is made, which
// the last case checks by looking for the staging database on disk.
func TestConfigValidationRefusesEveryBadField(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name   string
		field  string
		mutate func(*icelake.Config)
	}{
		{"a zero record threshold", "Config.FlushMaxRecords", func(c *icelake.Config) {
			c.FlushMaxRecords = 0
		}},
		{"a negative flush interval", "Config.FlushInterval", func(c *icelake.Config) {
			c.FlushInterval = -time.Second
		}},
		{"a zstd level below the range", "Config.ZSTDLevel", func(c *icelake.Config) {
			c.ZSTDLevel = 0
		}},
		{"a zstd level above the range", "Config.ZSTDLevel", func(c *icelake.Config) {
			c.ZSTDLevel = 23
		}},
		{"both credential forms", "Credentials", func(c *icelake.Config) {
			c.Credentials = staticCredentials(c.AccessKeyID, c.SecretAccessKey)
		}},
		{"neither credential form", "Credentials", func(c *icelake.Config) {
			c.AccessKeyID, c.SecretAccessKey = "", ""
		}},
		// A ceiling under the flush threshold is nonsensical rather than merely
		// small: the batch could never fill before the ceiling refused it, so
		// the writer would be dead on arrival.
		{"a staging ceiling below the record threshold", "Config.FlushMaxRecords", func(c *icelake.Config) {
			c.StagingMaxRecords = int64(c.FlushMaxRecords) - 1
		}},
		// The failure-handling knobs, which are optional-and-defaulted rather
		// than required: zero means the documented default, so what is refused
		// is a negative value and a schedule that contradicts itself.
		{"a negative attempt budget", "FlushMaxAttempts", func(c *icelake.Config) {
			c.FlushMaxAttempts = -1
		}},
		{"a negative retry base delay", "RetryBaseDelay", func(c *icelake.Config) {
			c.RetryBaseDelay = -time.Second
		}},
		{"a negative retry max delay", "RetryMaxDelay", func(c *icelake.Config) {
			c.RetryMaxDelay = -time.Second
		}},
		{"a retry base delay above the cap", "RetryBaseDelay", func(c *icelake.Config) {
			c.RetryBaseDelay, c.RetryMaxDelay = time.Minute, time.Second
		}},
		// The same rule against the resolved values rather than the literal
		// ones: an unset base is one second, so a cap below that is refused
		// rather than silently flattening the schedule to the cap.
		{"a retry cap below the default base delay", "RetryBaseDelay", func(c *icelake.Config) {
			c.RetryBaseDelay, c.RetryMaxDelay = 0, time.Millisecond
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := offlineConfig(t)
			c.mutate(&cfg)

			store, err := icelake.Open(ctx, cfg)
			if store != nil {
				t.Error("a refused Open returned a usable store")
			}

			var cfgErr icelake.ConfigError
			if !errors.As(err, &cfgErr) {
				t.Fatalf("Open returned %v (%T), want an icelake.ConfigError", err, err)
			}
			if !hasFieldViolation(err, c.field) {
				t.Errorf("no violation reported for %s; got %v", c.field, err)
			}
		})
	}

	// The decision this case exists for: violations are collected, not returned
	// first-one-wins, so a caller fixing a configuration file learns everything
	// wrong with it in one run instead of one problem per run.
	t.Run("every violation at once", func(t *testing.T) {
		cfg := offlineConfig(t)
		cfg.Bucket = ""
		cfg.FlushMaxRecords = 0
		cfg.FlushInterval = -time.Second
		cfg.ZSTDLevel = 23
		cfg.AccessKeyID, cfg.SecretAccessKey = "", ""
		cfg.FlushMaxAttempts = -1

		store, err := icelake.Open(ctx, cfg)
		if store != nil {
			t.Error("a refused Open returned a usable store")
		}

		var cfgErr icelake.ConfigError
		if !errors.As(err, &cfgErr) {
			t.Fatalf("Open returned %v (%T), want an icelake.ConfigError", err, err)
		}
		for _, field := range []string{
			"Bucket",
			"Config.FlushMaxRecords",
			"Config.FlushInterval",
			"Config.ZSTDLevel",
			"Credentials",
			"FlushMaxAttempts",
		} {
			if !hasFieldViolation(err, field) {
				t.Errorf("no violation reported for %s; every problem must be reported in one run", field)
			}
		}

		// Nothing was opened, created or contacted on the way to that error.
		if _, err := os.Stat(cfg.StagingPath); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the staging database exists after a refused Open (stat error %v); validation must precede every side effect", err)
		}
	})
}

// TestTableFlushPolicyIsRefusedByTheSameRules walks the other half of the
// configuration surface: a per-table override, checked at OpenWriter rather
// than at Open.
//
// The rules are the same rules — one resolver, one validator, one joined
// ConfigError — and that is exactly why this needs its own test rather than
// being assumed from the Config matrix above. An override is not defaulted the
// way a Config field is: a nil Flush inherits the store's thresholds, but a
// non-nil one is taken whole, so every field it leaves at zero is a violation
// rather than an inherited value. A caller who overrides one threshold and
// expects the rest to be inherited is therefore refused, loudly, at the call
// that opens the table — and the field names in the violations say
// TableConfig.Flush rather than Config, which is the only thing telling that
// caller which of the two places to go and fix.
//
// It needs no substrate for the same reason the Config matrix does not:
// validateTable runs after the store's own availability check and before the
// declaration, the catalog and the first request.
func TestTableFlushPolicyIsRefusedByTheSameRules(t *testing.T) {
	ctx := context.Background()
	store := offlineStore(t)

	// A policy that satisfies every rule, which each case below breaks in one
	// place. Its thresholds sit under offlineConfig's staging ceiling, since a
	// per-table threshold above the process-wide ceiling is itself a violation.
	valid := func() *icelake.FlushPolicy {
		return &icelake.FlushPolicy{
			FlushMaxRecords:  10,
			FlushMaxBytes:    1 << 16,
			FlushInterval:    time.Minute,
			ZSTDLevel:        1,
			MinFlushInterval: time.Nanosecond,
		}
	}

	cases := []struct {
		name   string
		field  string
		mutate func(*icelake.FlushPolicy)
	}{
		{"a zero record threshold", "TableConfig.Flush.FlushMaxRecords", func(p *icelake.FlushPolicy) {
			p.FlushMaxRecords = 0
		}},
		{"a zero byte threshold", "TableConfig.Flush.FlushMaxBytes", func(p *icelake.FlushPolicy) {
			p.FlushMaxBytes = 0
		}},
		{"a negative flush interval", "TableConfig.Flush.FlushInterval", func(p *icelake.FlushPolicy) {
			p.FlushInterval = -time.Second
		}},
		{"a zstd level below the range", "TableConfig.Flush.ZSTDLevel", func(p *icelake.FlushPolicy) {
			p.ZSTDLevel = 0
		}},
		{"a zstd level above the range", "TableConfig.Flush.ZSTDLevel", func(p *icelake.FlushPolicy) {
			p.ZSTDLevel = 23
		}},
		{"a negative flush floor", "TableConfig.Flush.MinFlushInterval", func(p *icelake.FlushPolicy) {
			p.MinFlushInterval = -time.Second
		}},
		// The cross-check against the store's ceiling, which is the one rule
		// that reads a Config field while reporting a TableConfig one: a table
		// allowed to batch more records than the process may ever stage could
		// never fill a batch before the ceiling refused it.
		{"a record threshold above the store's staging ceiling", "TableConfig.Flush.FlushMaxRecords", func(p *icelake.FlushPolicy) {
			p.FlushMaxRecords = 1_000_001
		}},
		{"a byte threshold above the store's staging ceiling", "TableConfig.Flush.FlushMaxBytes", func(p *icelake.FlushPolicy) {
			p.FlushMaxBytes = 1 << 40
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			policy := valid()
			c.mutate(policy)

			writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
				Namespace: "market", Table: "fills", Flush: policy,
			})
			if writer != nil {
				t.Error("a refused OpenWriter returned a usable writer")
			}

			var cfgErr icelake.ConfigError
			if !errors.As(err, &cfgErr) {
				t.Fatalf("OpenWriter returned %v (%T), want an icelake.ConfigError", err, err)
			}
			if !hasFieldViolation(err, c.field) {
				t.Errorf("no violation reported for %s; got %v", c.field, err)
			}
		})
	}

	// The joined-error decision again, on this side of the surface: an override
	// with several things wrong reports all of them in one run.
	t.Run("every violation at once", func(t *testing.T) {
		writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
			Namespace: "market", Table: "fills",
			Flush: &icelake.FlushPolicy{ZSTDLevel: 23, MinFlushInterval: -time.Second},
		})
		if writer != nil {
			t.Error("a refused OpenWriter returned a usable writer")
		}

		var cfgErr icelake.ConfigError
		if !errors.As(err, &cfgErr) {
			t.Fatalf("OpenWriter returned %v (%T), want an icelake.ConfigError", err, err)
		}
		for _, field := range []string{
			"TableConfig.Flush.FlushMaxRecords",
			"TableConfig.Flush.FlushMaxBytes",
			"TableConfig.Flush.FlushInterval",
			"TableConfig.Flush.ZSTDLevel",
			"TableConfig.Flush.MinFlushInterval",
		} {
			if !hasFieldViolation(err, field) {
				t.Errorf("no violation reported for %s; every problem must be reported in one run", field)
			}
		}
	})

	// The control the cases above need, so that what they prove is about the
	// values rather than about a per-table override being refused at all. This
	// store's endpoint answers nothing, so the open goes on to fail at the
	// bucket; what is asserted is that it got that far — that the same policy
	// with one field changed passes the validation the cases above fail. A
	// valid override opening a real table is scenario 1's business, and it does
	// it there against the substrate.
	t.Run("a valid override is not refused by validation", func(t *testing.T) {
		_, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{
			Namespace: "market", Table: "accepted", Flush: valid(),
		})

		var cfgErr icelake.ConfigError
		if errors.As(err, &cfgErr) {
			t.Fatalf("OpenWriter refused a valid per-table policy: %v", err)
		}
	})
}

// -- Namespace and table name validation.

// TestOpenWriterRefusesUnusableNames asserts the seven rejections scenario 7
// names, in both name positions, through the public call a caller actually
// makes.
//
// The slash case is the load-bearing one. Names become the two path segments of
// the warehouse key layout, and catalog rebuild discovers tables by parsing
// those segments back out — so a name containing a slash would produce a table
// rebuild either cannot see or would rediscover under a namespace nobody
// declared. That is not left as prose here: TestRebuildRefusesAPrefixItCannotExplain
// in scenario3_test.go drives the other end of the same rule, putting objects
// under prefixes this validation would never have produced and asserting that
// rebuild refuses them by name rather than skipping them. This test is why such
// a prefix cannot come from icelake; that one is what happens when one exists
// anyway. The others each close a smaller hole: a dot for the same layout
// reason, a hyphen because it is not a valid unquoted SQL identifier in the
// engines that will read these tables, case because object keys are
// case-sensitive while much around them is not, and a leading underscore, an
// empty string and an over-long name because none of them names a table an
// operator can work with.
//
// A bad name is refused and never normalized: quietly rewriting a caller's
// table name is how two services end up sharing a table.
func TestOpenWriterRefusesUnusableNames(t *testing.T) {
	ctx := context.Background()
	store := offlineStore(t)

	cases := []struct {
		name  string
		value string
	}{
		{"a slash", "market/fills"},
		{"a dot", "market.fills"},
		{"a hyphen", "market-fills"},
		{"an uppercase letter", "Fills"},
		{"a leading underscore", "_fills"},
		{"an empty name", ""},
		{"a name over 63 bytes", strings.Repeat("f", 64)},
	}

	check := func(t *testing.T, w *icelake.Writer[badNameDecl], err error, kind icelake.NameKind, value string) {
		t.Helper()

		if w != nil {
			t.Error("OpenWriter returned a writer for a name it refused")
		}

		var ne icelake.NameError
		if !errors.As(err, &ne) {
			t.Fatalf("OpenWriter returned %v (%T), want an icelake.NameError", err, err)
		}
		if ne.Kind != kind {
			t.Errorf("NameError.Kind = %q, want %q", ne.Kind, kind)
		}
		if ne.Value != value {
			t.Errorf("NameError.Value = %q, want the offending name %q, unnormalized", ne.Value, value)
		}
	}

	for _, c := range cases {
		t.Run(c.name+" in the namespace", func(t *testing.T) {
			w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[badNameDecl]{Namespace: c.value, Table: "fills"})
			check(t, w, err, icelake.NameKindNamespace, c.value)
		})

		t.Run(c.name+" in the table name", func(t *testing.T) {
			w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[badNameDecl]{Namespace: "market", Table: c.value})
			check(t, w, err, icelake.NameKindTable, c.value)
		})
	}
}

// -- The staging ceiling.

// TestStagingCeilingRefusesThenRecovers is the ceiling's whole arc: a tiny
// ceiling, a store that cannot be reached, inserts until the ceiling refuses —
// and then the store answering again, the backlog committing, and the writer
// accepting records once more.
//
// Both halves matter. The refusal is what stops a table that cannot flush from
// consuming unbounded local disk and memory in silence, and it is deliberately
// a refusal rather than a block: blocking would turn a storage problem into an
// invisible latency problem in the caller's own hot path. The recovery is what
// makes the ceiling a backstop rather than a wedge — a writer that refused once
// and never accepted again would have turned a passing outage into an outage
// for the rest of the process's life.
//
// The store is made unreachable with the same proxy the other failure tests use
// rather than by stopping the container: what reaches icelake is identical
// (connections that fail immediately), the substrate on the other side is the
// same real MinIO, and the endpoint comes back without a restart the writer
// would have no way to observe.
func TestStagingCeilingRefusesThenRecovers(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	// Deliberately tiny, per TESTING.md's configuration section: two records
	// fill a batch and three fill the whole process's staging store.
	cfg.FlushMaxRecords = 2
	cfg.StagingMaxRecords = 3

	store := openStore(t, cfg)
	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	// Nothing can leave the process from here until the proxy is opened again,
	// so every accepted record stays in staging and counts against the ceiling.
	proxy.SetReject(true)

	const accepted = 3
	for i := range accepted {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	refused := makeFill(accepted)
	err = writer.Insert(ctx, refused)
	if !errors.Is(err, icelake.ErrStagingFull) {
		t.Fatalf("Insert at the ceiling returned %v (%T), want ErrStagingFull", err, err)
	}

	// The refused record is not in staging, asserted by identity rather than by
	// count: the claim is that this record is still the caller's, and a count
	// alone would also be satisfied by a store holding some other three records.
	staged := stagedSequenceIDs(t, cfg.StagingPath, "market", "fills")
	if !slices.Equal(staged, []int64{0, 1, 2}) {
		t.Errorf("staging holds records %v, want exactly the three that were accepted", staged)
	}
	if slices.Contains(staged, refused.SequenceID) {
		t.Errorf("the refused record (sequence_id %d) is in staging; a record Insert refused must never be there",
			refused.SequenceID)
	}
	if pending, _ := writer.Pending(); pending != accepted {
		t.Errorf("Pending() = %d, want %d", pending, accepted)
	}

	// The store answers again. The backlog commits on the next trigger, the
	// ceiling counters come back down with it, and the writer accepts the very
	// record it refused.
	proxy.SetReject(false)
	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := writer.Flush(drain); err != nil {
		t.Fatalf("Flush once the store answered again: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Fatalf("staging holds %d rows after the backlog committed, want 0", got)
	}

	if err := writer.Insert(ctx, refused); err != nil {
		t.Fatalf("Insert of the previously refused record after recovery: %v", err)
	}
	if err := writer.Flush(drain); err != nil {
		t.Fatalf("Flush after recovery: %v", err)
	}
	if err := writer.Close(drain); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Every record the writer ever accepted is committed exactly once — the
	// three from before the refusal and the one that only made it in afterwards.
	duck := openDuckDB(t, bucket)
	var total, distinct int
	duck.queryRow(t, "SELECT count(*), count(DISTINCT sequence_id) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &total, &distinct)
	if total != accepted+1 || distinct != accepted+1 {
		t.Errorf("committed %d rows with %d distinct ids, want %d of each", total, distinct, accepted+1)
	}
}

// fillSequenceIDFieldID is the permanent field id of Fill's sequence_id column,
// which is what makes a staged payload identifiable without asking icelake
// which records it thinks it holds.
const fillSequenceIDFieldID = 5

// stagedSequenceIDs reads one table's staged rows straight out of the staging
// database and decodes each one far enough to say which record it is.
//
// Every step is the one an outside reader would take: the payloads come from
// the real file, each is decoded by the schema descriptor recorded alongside it
// rather than by anything the running writer holds, and the value is found by
// permanent field id rather than by position. That is what turns "three rows
// are there" into "these three records are there, and the refused one is not".
func stagedSequenceIDs(tb testing.TB, stagingPath, namespace, table string) []int64 {
	tb.Helper()

	rows := stagedRowsFor(tb, stagingPath, namespace, table)
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		desc := stagedDescriptor(tb, stagingPath, r.schemaFP)
		i, ok := desc.IndexOfFieldID(fillSequenceIDFieldID)
		if !ok {
			tb.Fatalf("the recorded shape %s has no field id %d", r.schemaFP, fillSequenceIDFieldID)
		}

		row, err := canon.Decode(table, desc, r.payload)
		if err != nil {
			tb.Fatalf("decoding staged row %d: %v", r.id, err)
		}
		id, ok := row[i].(int64)
		if !ok {
			tb.Fatalf("staged row %d holds %T in sequence_id, want an int64", r.id, row[i])
		}
		out = append(out, id)
	}

	return out
}

// TestStagingCeilingIsProcessWideAcrossTables holds the half of the ceiling's
// definition that its arc test above does not: the bound is one budget for the
// whole process, not one budget per table. One store, two writers on two
// different tables, a ceiling filled entirely by the first table — and the
// second table, which has staged nothing at all, refused.
//
// It was added at the post-M9 completeness audit (2026-08-01) because nothing
// held that claim. ARCHITECTURE.md states it ("bound the total staged-but-not
// yet-committed volume across all tables in the process"), Config's own
// documentation states it, and Store.accept has always charged a single pair of
// counters rather than a map keyed by table — but every test that had ever
// reached ErrStagingFull opened exactly one writer on exactly one table, and a
// process-wide bound and a per-table bound are indistinguishable when only one
// table exists. A change that keyed those counters by table would therefore
// have passed the entire suite while multiplying the real bound by the number
// of open tables, which turns a stated limit on total local disk and total
// memory into no limit at all for a process that opens tables in a loop. The
// audit confirmed the gap by making exactly that change and watching everything
// still pass; this test is what fails under it now.
//
// The mechanism is the one the arc test uses, deliberately rather than a new
// one: the proxy rejects, so nothing can leave the process and every accepted
// record stays in staging where it counts. The recovery direction is asserted
// too, and it is the same claim read backwards — the headroom the second table
// gets back is headroom the *first* table released, which no per-table
// accounting would ever hand over.
func TestStagingCeilingIsProcessWideAcrossTables(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	proxy := startSlowProxy(t, bucket.Endpoint, 0)
	cfg := testConfig(t, bucket)
	cfg.Endpoint = proxy.Endpoint
	// The same tiny thresholds as the arc test: two records fill a batch and
	// three fill the whole process's staging store — the whole process's, which
	// is exactly what is under test.
	cfg.FlushMaxRecords = 2
	cfg.StagingMaxRecords = 3

	store := openStore(t, cfg)
	fills, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "fills"})
	if err != nil {
		t.Fatalf("OpenWriter for market.fills: %v", err)
	}
	events, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[EventArchive]{Namespace: "archive", Table: "events"})
	if err != nil {
		t.Fatalf("OpenWriter for archive.events: %v", err)
	}

	// Nothing can leave the process from here, so every record below stays in
	// staging and counts against the ceiling.
	proxy.SetReject(true)

	// One table fills the ceiling by itself. The other is not touched.
	const accepted = 3
	for i := range accepted {
		if err := fills.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d into market.fills: %v", i, err)
		}
	}

	refused := makeEvent(0)
	if err := events.Insert(ctx, refused); !errors.Is(err, icelake.ErrStagingFull) {
		t.Fatalf("Insert into archive.events, which has staged nothing, while market.fills holds the whole ceiling returned %v (%T), want ErrStagingFull",
			err, err)
	}

	// The second table is still empty on disk: the refusal did not stage the
	// record on the way to declining it, and the writer is not holding it.
	if staged := stagedRowsFor(t, cfg.StagingPath, "archive", "events"); len(staged) != 0 {
		t.Errorf("staging holds %d rows for archive.events, want 0; the refused record must still be the caller's", len(staged))
	}
	if pending, _ := events.Pending(); pending != 0 {
		t.Errorf("archive.events Pending() = %d, want 0", pending)
	}

	// The store answers again and the first table's backlog commits. The
	// headroom that releases belongs to the process, so the second table — which
	// did nothing to earn it — can insert the very record it was refused.
	proxy.SetReject(false)
	drain, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if err := fills.Flush(drain); err != nil {
		t.Fatalf("Flush of market.fills once the store answered again: %v", err)
	}
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Fatalf("staging holds %d rows after market.fills committed, want 0", got)
	}

	if err := events.Insert(ctx, refused); err != nil {
		t.Fatalf("Insert into archive.events after the other table drained: %v", err)
	}
	if err := events.Flush(drain); err != nil {
		t.Fatalf("Flush of archive.events: %v", err)
	}
	if err := events.Close(drain); err != nil {
		t.Fatalf("Close of archive.events: %v", err)
	}
	if err := fills.Close(drain); err != nil {
		t.Fatalf("Close of market.fills: %v", err)
	}

	// Both tables hold exactly what each was allowed to accept, read back the
	// way any other reader would read them.
	duck := openDuckDB(t, bucket)
	var events0, fills0 int
	duck.queryRow(t, "SELECT count(*) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "archive", "events")), &events0)
	if events0 != 1 {
		t.Errorf("archive.events committed %d rows, want the single record that was refused and then accepted", events0)
	}
	duck.queryRow(t, "SELECT count(*) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "fills")), &fills0)
	if fills0 != accepted {
		t.Errorf("market.fills committed %d rows, want %d", fills0, accepted)
	}
}

// -- Poison records.

// poisonRecord is a minimal declaration with one string column and one decimal
// column narrow enough that a test can overflow it with a readable number.
//
// DECIMAL(5, 2) holds unscaled values from -99999 to 99999 — five digits — so
// 100000 is the first value that does not fit, which is what makes the refusal
// below about the declared precision rather than about int64's own range.
type poisonRecord struct {
	Note   string `parquet:"name=note, logical=String, fieldid=1" arrow:"note"`
	Amount int64  `parquet:"name=amount, logical=decimal, precision=5, scale=2, fieldid=2" arrow:"amount"`
}

// TestInsertRefusesPoisonRecords proves the poison check: a value that is legal
// Go but not representable in the column it is declared as is refused by Insert
// itself, naming the field, with nothing entering staging.
//
// The alternative is what this exists to prevent. Accepted, such a record would
// be durable, would join a batch, and would fail that batch's encode or misread
// on the way out — at flush time, on a background goroutine, with no caller left
// to hand the error to and no way to tell which of the batch's records was at
// fault. Refusing at the boundary keeps the problem where the context to fix it
// is: the caller still holds the record.
func TestInsertRefusesPoisonRecords(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)

	// This test is also the one place the provider form of credentials is
	// driven end to end — opened, signed with, uploaded and committed through —
	// per TESTING.md's configuration section: it is a supported input shape,
	// every other test uses the two key strings, and an input shape nothing
	// exercises is one that rots. It rides along here rather than in a test of
	// its own because the claim is only worth anything against a real write.
	cfg.Credentials = staticCredentials(cfg.AccessKeyID, cfg.SecretAccessKey)
	cfg.AccessKeyID, cfg.SecretAccessKey = "", ""

	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[poisonRecord]{Namespace: "market", Table: "poison"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	cases := []struct {
		name   string
		record poisonRecord
		field  string
		reason icelake.PoisonReason
	}{
		{
			// Parquet's String logical type is defined as UTF-8, so these bytes
			// have no honest representation in the column at all. They are a
			// lone continuation byte and a truncated two-byte sequence — what a
			// caller gets from slicing a UTF-8 string by bytes.
			name:   "invalid UTF-8 in a string column",
			record: poisonRecord{Note: string([]byte{0x80, 0xc3}), Amount: 1},
			field:  "note",
			reason: icelake.PoisonReasonInvalidUTF8,
		},
		{
			name:   "a decimal that does not fit its declared precision",
			record: poisonRecord{Note: "ok", Amount: 100_000},
			field:  "amount",
			reason: icelake.PoisonReasonDecimalRange,
		},
		{
			// The negative direction too: sign extension into the data file
			// would carry it through just as quietly.
			name:   "a negative decimal that does not fit its declared precision",
			record: poisonRecord{Note: "ok", Amount: -100_000},
			field:  "amount",
			reason: icelake.PoisonReasonDecimalRange,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := writer.Insert(ctx, c.record)

			var pe icelake.PoisonError
			if !errors.As(err, &pe) {
				t.Fatalf("Insert returned %v (%T), want an icelake.PoisonError", err, err)
			}
			if pe.Field != c.field {
				t.Errorf("PoisonError.Field = %q, want the offending column %q", pe.Field, c.field)
			}
			if pe.Table != "poison" {
				t.Errorf("PoisonError.Table = %q, want %q", pe.Table, "poison")
			}
			if pe.Reason != c.reason {
				t.Errorf("PoisonError.Reason = %q, want %q", pe.Reason, c.reason)
			}

			// Read the real staging database rather than asking icelake: the
			// claim is about what is on disk, and nothing refused may be there.
			if got := stagedRows(t, cfg.StagingPath); got != 0 {
				t.Errorf("staging holds %d rows after a refused record, want 0", got)
			}
			if pending, _ := writer.Pending(); pending != 0 {
				t.Errorf("Pending() = %d after a refused record, want 0", pending)
			}
		})
	}

	// The writer is not poisoned by having refused: it still accepts, flushes
	// and commits, which is the difference between rejecting a record and
	// failing a table. The values here sit at the edges the cases above stepped
	// over, so the check is refusing what does not fit rather than everything.
	good := []poisonRecord{
		{Note: "edge", Amount: 99_999},
		{Note: "edge", Amount: -99_999},
	}
	for i, r := range good {
		if err := writer.Insert(ctx, r); err != nil {
			t.Fatalf("Insert of an in-range record %d after the refusals: %v", i, err)
		}
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Insert's refusals are ordered, and the order is documented rather than
	// incidental: a closed writer answers with the sentinel that says so even
	// when the record it was handed is one it would otherwise have refused,
	// because the caller's next move is about the writer and not about that
	// record.
	err = writer.Insert(ctx, poisonRecord{Note: string([]byte{0x80}), Amount: 1})
	if !errors.Is(err, icelake.ErrClosed) {
		t.Errorf("Insert of a poison record on a closed writer returned %v (%T), want ErrClosed", err, err)
	}

	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, "SELECT count(*) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "poison")), &total)
	if total != len(good) {
		t.Errorf("committed %d rows, want the %d records that were accepted", total, len(good))
	}
}

// TestInsertRefusesARecordItCannotStage proves the durability layer's own
// refusal, which is the one the poison cases above step past: a record whose
// staging write fails is not accepted, and the caller is told so with an error
// it can classify.
//
// This is the promise Store.Append's documentation makes in as many words —
// there is no in-memory fallback buffer for "the durability layer is broken",
// because such a buffer would silently downgrade the exact guarantee this layer
// exists to give. Nothing anywhere held it. Every other refusal in this file
// happens before the write is attempted, so what a caller gets when the write
// itself fails was untested from end to end.
//
// The failure is a real one and needs nothing stood in for. Insert consumes its
// context nowhere before the staging write — the closed check, the binding, the
// poison check and the encode are all context-free — so a context the caller
// has already cancelled reaches the transaction unchanged, and the transaction
// refuses to begin. That is a caller's own mistake rather than a hardware
// fault, and it takes the identical path through the identical code.
//
// Four things are asserted beyond the type, and each is a separate way the
// promise could be broken while the error still looked right: nothing is in the
// staging file, nothing is pending in the writer, the ceiling charge the attempt
// made was given back, and the writer goes on working.
//
// The charge is the one of those four that needs the test to be built around it,
// because the writer's own counters cannot see it — a refused record never joins
// the batch, so Pending() reads zero whether the charge was released or not. The
// counters that would be wrong are the store's, which are process-wide and
// visible only as headroom. So the ceiling here is set to exactly the number of
// records the test goes on to insert, and every one of them has to be accepted:
// a charge left behind by the refusal is one record of headroom that is gone for
// the life of the process, so the last insert would come back ErrStagingFull
// against a staging database with room to spare. That is the failure this shape
// is built to catch, and nothing smaller than a real ceiling can see it.
func TestInsertRefusesARecordItCannotStage(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx := context.Background()
	cfg := testConfig(t, bucket)
	// As small as the ceiling can be and still be filled: one leaked charge is
	// then the whole difference between the second record fitting and being
	// refused. The batch size has to come down with it, because a batch that
	// could never fill before the ceiling refused it is a configuration icelake
	// declines to run, and it is set as high as that rule allows: the flush it
	// triggers is triggered by the last of these records, after that record has
	// already been accepted, so no headroom is handed back mid-count.
	const ceiling = 2
	cfg.StagingMaxRecords = ceiling
	cfg.FlushMaxRecords = ceiling
	store := openStore(t, cfg)

	writer, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[Fill]{Namespace: "market", Table: "unstageable"})
	if err != nil {
		t.Fatalf("OpenWriter: %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	err = writer.Insert(cancelled, makeFill(1))

	var se icelake.StagingError
	if !errors.As(err, &se) {
		t.Fatalf("Insert returned %v (%T), want an icelake.StagingError", err, err)
	}
	if se.Kind != icelake.StagingKindWrite {
		t.Errorf("StagingError.Kind = %q, want %q", se.Kind, icelake.StagingKindWrite)
	}
	if se.Path != cfg.StagingPath {
		t.Errorf("StagingError.Path = %q, want %q", se.Path, cfg.StagingPath)
	}
	if !errors.Is(err, context.Canceled) {
		t.Error("the cause is not reachable through the typed error; a caller cannot tell a cancelled write from a broken disk")
	}

	// Read the file rather than asking icelake: the claim is about what is on
	// disk, and a record the caller was told was refused must not be there.
	if got := stagedRows(t, cfg.StagingPath); got != 0 {
		t.Errorf("staging holds %d rows after a refused record, want 0", got)
	}
	if pending, bytes := writer.Pending(); pending != 0 || bytes != 0 {
		t.Errorf("Pending() = (%d, %d) after a refused record, want (0, 0)", pending, bytes)
	}

	// The writer is not poisoned by having refused, and the records really do
	// commit when the write can be made. Exactly the ceiling's worth of them go
	// in: the last one fits only if the refused attempt gave its charge back.
	for i := range ceiling {
		if err := writer.Insert(ctx, makeFill(i)); err != nil {
			t.Fatalf("Insert %d of %d after a refused staging write: %v; a charge the refusal never gave back looks exactly like this",
				i+1, ceiling, err)
		}
	}

	if err := writer.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	duck := openDuckDB(t, bucket)
	var total int
	duck.queryRow(t, "SELECT count(*) FROM "+
		duck.scan(latestMetadata(t, bucket, cfg.WarehousePrefix, "market", "unstageable")), &total)
	if total != ceiling {
		t.Errorf("committed %d rows, want the %d records that were accepted", total, ceiling)
	}
}

// -- The two-derivation cross-check and the nested-group rejection.

// crossCheckSkipDecl keeps a column on the declared side that the row-data side
// is told to skip. The two derivations then describe tables of different widths
// from one struct, which is exactly the class of disagreement the cross-check
// exists to catch: nothing else downstream compares them.
type crossCheckSkipDecl struct {
	A string `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	B string `parquet:"name=b, logical=String, fieldid=2" arrow:"-"`
}

// nestedInnerGroup is the sub-struct nestedDecl declares. Its field IDs
// continue the pre-order numbering, so the declaration is refused for being
// nested rather than for a numbering mistake that would mask it.
type nestedInnerGroup struct {
	X int64 `parquet:"name=x, fieldid=3" arrow:"x"`
	Y int64 `parquet:"name=y, fieldid=4" arrow:"y"`
}

// nestedDecl declares a nested group, which the flat-only scoping decision does
// not support. It derives through the whole three-call chain intact, which is
// precisely why it has to be refused deliberately.
type nestedDecl struct {
	A   string           `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	Grp nestedInnerGroup `parquet:"name=grp, fieldid=2" arrow:"grp"`
}

// listOfGroupsElement is the element struct listOfGroupsDecl's slice field
// carries. Its own field IDs continue the pre-order numbering for the same
// reason nestedInnerGroup's do: the declaration must be refused for being a
// list of groups, not for a numbering mistake that would mask it.
type listOfGroupsElement struct {
	X int64 `parquet:"name=x, fieldid=3" arrow:"x"`
	Y int64 `parquet:"name=y, fieldid=4" arrow:"y"`
}

// listOfGroupsDecl declares a slice of structs, which SCHEMA.md records as a
// stronger limit than the flat-only scoping decision that covers the plain
// sub-struct above — though the refusal a caller receives is the same one.
// Both shapes derive through the first two library calls and are then stopped
// by icelake's own flat-only check, which refuses any nested top-level column:
// a STRUCT for the sub-struct, a LIST here. What differs is what would happen
// if that check were lifted. The sub-struct derives cleanly to the end and
// would simply be supported; the slice of structs would die one call later
// anyway, because arrow-go gives the list's synthesized element node no field
// ID and there is no tag that supplies one (a fieldid on the slice field names
// the list, not its element). Either way the caller must be told at OpenWriter
// rather than discovering it when the first batch fails.
type listOfGroupsDecl struct {
	A    string                `parquet:"name=a, logical=String, fieldid=1" arrow:"a"`
	Rows []listOfGroupsElement `parquet:"name=rows, fieldid=2" arrow:"rows"`
}

// overPrecisionDecl asks for one more decimal digit than the declaration seam
// allows on an INT64-backed column: precision 19 where arrow-go's own
// applicability check permits 1-18.
//
// The value of refusing it is not obvious from the declaration, which is why it
// has to be asserted. The file icelake actually writes is built from Arrow
// Decimal128 columns and would carry a 19-digit decimal perfectly well — the
// cap lives entirely at this seam, in the declared, INT64-backed schema, and
// nothing further down the write path has any reason to object.
type overPrecisionDecl struct {
	Symbol string `parquet:"name=symbol, logical=String, fieldid=1" arrow:"symbol"`
	Price  int64  `parquet:"name=price, logical=decimal, precision=19, scale=9, fieldid=2" arrow:"price"`
}

// TestOpenWriterRefusesDeclarationsTheSchemaLayerCannotHonour covers scenario
// 7's last two declaration clauses at the call a caller makes, rather than at
// the derivation seam underneath it.
//
// Both refusals happen before the catalog is consulted, which is why this test
// needs no substrate — and that ordering is part of the claim: a declaration
// icelake will not accept must never create a table first.
func TestOpenWriterRefusesDeclarationsTheSchemaLayerCannotHonour(t *testing.T) {
	ctx := context.Background()
	store := offlineStore(t)

	// The cross-check is what keeps SCHEMA.md's adapter honest: two independent
	// reflection layers read the same struct through different tag namespaces,
	// and nothing but this comparison stands between a disagreement and a table
	// that fails on its first batch.
	t.Run("a column the row-data side skips", func(t *testing.T) {
		w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[crossCheckSkipDecl]{Namespace: "market", Table: "skipped"})
		if w != nil {
			t.Error("OpenWriter returned a writer for a declaration it refused")
		}

		var ce icelake.CrossCheckError
		if !errors.As(err, &ce) {
			t.Fatalf("OpenWriter returned %v (%T), want an icelake.CrossCheckError", err, err)
		}
		if ce.Table != "skipped" || ce.Column != "b" {
			t.Errorf("CrossCheckError = {Table:%q Column:%q}, want {skipped b}", ce.Table, ce.Column)
		}
	})

	// Scenario 7's other cross-check example — the two tag namespaces naming
	// the same column differently — is proven through this same OpenWriter path
	// by TestSchemaErrorsMatchThePublicNames/CrossCheckError in errors_test.go,
	// which also owns the alias half of that claim. One clause, one test.

	// Flat-only scoping is a decision, and an undefended decision is a comment.
	// The reconciliation loop diffs single-segment field paths only, so a nested
	// column would either silently do nothing or produce a wrong AddColumn
	// against a top-level path.
	t.Run("a nested group", func(t *testing.T) {
		w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[nestedDecl]{Namespace: "market", Table: "nested"})
		if w != nil {
			t.Error("OpenWriter returned a writer for a nested declaration")
		}

		var ce icelake.CrossCheckError
		if !errors.As(err, &ce) {
			t.Fatalf("OpenWriter returned %v (%T), want an icelake.CrossCheckError", err, err)
		}
		if ce.Column != "grp" {
			t.Errorf("CrossCheckError.Column = %q, want the nested column %q", ce.Column, "grp")
		}
	})

	// A slice of structs is the other half of SCHEMA.md's nesting paragraph.
	// The refusal asserted here is icelake's own flat-only rejection — the same
	// check that refuses the sub-struct above, reached for an arrow.LIST rather
	// than an arrow.STRUCT — which is why this asserts a CrossCheckError naming
	// the list column, exactly as that case does. The difference between the
	// two claims is in what survives if that check is ever lifted: the
	// sub-struct would be supported, while this declaration would still be
	// refused one call later, because arrow-go gives the list's synthesized
	// element node no field ID and no tag supplies one. The plain sub-struct
	// case was tested from the beginning and this one was not, which left the
	// caller-facing half of the claim resting entirely on prose.
	t.Run("a list of groups", func(t *testing.T) {
		w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[listOfGroupsDecl]{Namespace: "market", Table: "listed"})
		if w != nil {
			t.Error("OpenWriter returned a writer for a list-of-groups declaration")
		}

		var ce icelake.CrossCheckError
		if !errors.As(err, &ce) {
			t.Fatalf("OpenWriter returned %v (%T), want an icelake.CrossCheckError", err, err)
		}
		if ce.Table != "listed" || ce.Column != "rows" {
			t.Errorf("CrossCheckError = {Table:%q Column:%q}, want {listed rows}", ce.Table, ce.Column)
		}
	})

	// The decimal precision ceiling. SCHEMA.md states the cap is 18 and that it
	// lives at this seam rather than in the bucket, which is precisely what
	// makes it worth asserting: the data files icelake writes are Arrow
	// Decimal128 columns and would carry a nineteenth digit happily, so if the
	// declaration stopped refusing it nothing downstream would notice. The
	// ceiling itself — that 18 is still accepted, and that an int32-backed
	// decimal is refused at any precision — is pinned from both sides in
	// internal/schemamap, which can declare a good schema without a substrate to
	// open it against. This is the caller-facing half: the refusal arrives as a
	// typed error from OpenWriter, before any table is created.
	t.Run("a decimal past the precision ceiling", func(t *testing.T) {
		w, err := icelake.OpenWriter(ctx, store, icelake.TableConfig[overPrecisionDecl]{Namespace: "market", Table: "overprecise"})
		if w != nil {
			t.Error("OpenWriter returned a writer for a declaration it refused")
		}

		var de icelake.DeclarationError
		if !errors.As(err, &de) {
			t.Fatalf("OpenWriter returned %v (%T), want an icelake.DeclarationError", err, err)
		}
		if de.Table != "overprecise" {
			t.Errorf("DeclarationError.Table = %q, want %q", de.Table, "overprecise")
		}
		// The refusal comes out of the first of the three derivation calls, so
		// it arrives as a derivation failure carrying the library's own error
		// rather than as one of icelake's structural kinds.
		if de.Kind != icelake.DeclarationKindDerivation {
			t.Errorf("DeclarationError.Kind = %q, want %q", de.Kind, icelake.DeclarationKindDerivation)
		}
		if errors.Unwrap(de) == nil {
			t.Error("DeclarationError carries no cause; a derivation failure must keep the library's own error reachable")
		}
	})
}
