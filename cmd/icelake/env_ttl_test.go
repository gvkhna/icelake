package main

import (
	"errors"
	"testing"
	"time"

	"github.com/gvkhna/icelake"
)

// TestClickHouseTTLEntriesParseAndRefuse pins the one syntax the mirror-expiry
// variable accepts — ns.table=DURATION@COLUMN, comma-separated — and that
// every malformed entry is a usage refusal naming the variable, not a value
// quietly dropped or half-read.
func TestClickHouseTTLEntriesParseAndRefuse(t *testing.T) {
	t.Run("entries parse to per-table expiries", func(t *testing.T) {
		t.Setenv("ICELAKE_CLICKHOUSE_TTL", "market.fills=720h@ts_ms, archive.events=24h@seen_at")
		got, err := parseClickHouseTTL(envClickHouseTTL)
		if err != nil {
			t.Fatalf("parseClickHouseTTL: %v", err)
		}
		want := map[string]icelake.MirrorTTL{
			"market.fills":   {Column: "ts_ms", After: 720 * time.Hour},
			"archive.events": {Column: "seen_at", After: 24 * time.Hour},
		}
		if len(got) != len(want) {
			t.Fatalf("parsed %d entries, want %d", len(got), len(want))
		}
		for k, w := range want {
			if got[k] != w {
				t.Errorf("entry %q parsed to %+v, want %+v", k, got[k], w)
			}
		}
	})

	t.Run("unset means none", func(t *testing.T) {
		t.Setenv("ICELAKE_CLICKHOUSE_TTL", "")
		got, err := parseClickHouseTTL(envClickHouseTTL)
		if err != nil || got != nil {
			t.Fatalf("an unset variable parsed to (%v, %v), want (nil, nil)", got, err)
		}
	})

	refusals := []struct{ name, value string }{
		{"no separator at all", "market.fills"},
		{"no column", "market.fills=720h"},
		{"an empty column", "market.fills=720h@"},
		{"not a duration", "market.fills=monthly@ts_ms"},
		{"under one second", "market.fills=500ms@ts_ms"},
		{"fractional seconds", "market.fills=1.5s@ts_ms"},
		{"a table twice", "market.fills=720h@ts_ms,market.fills=24h@ts_ms"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ICELAKE_CLICKHOUSE_TTL", tc.value)
			if _, err := parseClickHouseTTL(envClickHouseTTL); !errors.Is(err, errUsage) {
				t.Fatalf("parseClickHouseTTL(%q) returned %v, want a usage refusal", tc.value, err)
			}
		})
	}
}
