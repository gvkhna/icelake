package main

import (
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
)

// Every log line is logfmt: one line per event, `time= level= msg=` and then
// the event's own keys, written by the standard library's text handler. The
// message is a plain sentence and the attributes carry the numbers, so a
// person can read it and a grep can parse it, and the two never disagree
// because they are the same line. The rule (`AGENTS.md`, Logging) has no
// exceptions: the starter's terminal lines, the final report on the way to
// an exit code, and the two operator commands' own error lines are all this
// same shape, because they all report on a run. The only text that is not a
// log line is a command's product — the manual, the help table, the version
// string, a report — which is what the person asked for, not a report on
// asking.
func newLogger(w io.Writer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level}))
}

// logAttrs is the resolved configuration as log attributes: everything this
// command was configured with, one key per setting, with the two secrets
// reduced to whether there is one — a redaction that shows a prefix leaks a
// prefix. It is the one source for describe() below, so the startup log line
// and any rendered form can never disagree.
func (s settings) logAttrs() []any {
	credential := "unset"
	if s.secretAccessKey != "" {
		credential = "set"
	}

	attrs := []any{"staging", s.stagingPath, "cache", s.cacheDir}
	if s.localOnly {
		attrs = append(attrs, "local-only", true)
	} else {
		attrs = append(attrs,
			"catalog", s.catalogPath, "endpoint", s.endpoint, "bucket", s.bucket,
			"prefix", s.prefix, "region", s.region, "access-key-id", s.accessKeyID,
			"secret-access-key", credential,
			"cache-max-age", s.cacheMaxAge.String(), "cache-max-bytes", s.cacheMaxBytes)
	}
	attrs = append(attrs,
		"flush-interval", s.flushInterval.String(), "flush-max-bytes", s.flushMaxBytes,
		"flush-max-records", s.flushMaxRecords,
		"staging-max-records", s.stagingMaxRecord, "staging-max-bytes", s.stagingMaxBytes,
		"zstd-level", s.zstdLevel,
		"shutdown-timeout", s.shutdownTimeout.String(), "max-line-bytes", s.maxLineBytes)

	logDestination := "stderr"
	if s.logFileSet {
		logDestination = s.logFile
	}
	attrs = append(attrs, "log", logDestination, "log-level", s.logLevel.String(), "pid-file", s.pidFile)

	if s.clickHouseAddr != "" {
		clickHouseSecret := "unset"
		if s.clickHousePassword != "" {
			clickHouseSecret = "set"
		}
		attrs = append(attrs,
			"clickhouse-addr", s.clickHouseAddr, "clickhouse-database", s.clickHouseDatabase,
			"clickhouse-username", s.clickHouseUsername, "clickhouse-password", clickHouseSecret,
			"clickhouse-tls", s.clickHouseTLS)
		if len(s.clickHouseTTL) > 0 {
			// Sorted, so the line is the same line for the same configuration.
			keys := make([]string, 0, len(s.clickHouseTTL))
			for k := range s.clickHouseTTL {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			parts := make([]string, 0, len(keys))
			for _, k := range keys {
				ttl := s.clickHouseTTL[k]
				parts = append(parts, fmt.Sprintf("%s=%s@%s", k, ttl.After, ttl.Column))
			}
			attrs = append(attrs, "clickhouse-ttl", strings.Join(parts, ","))
		}
	} else {
		attrs = append(attrs, "clickhouse", "off")
	}

	return attrs
}

// describe renders the same attributes as one `k=v` line, for anything that
// wants the configuration as a string rather than as a log record.
func (s settings) describe() string {
	attrs := s.logAttrs()
	var b strings.Builder
	for i := 0; i+1 < len(attrs); i += 2 {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%v=%v", attrs[i], attrs[i+1])
	}

	return b.String()
}
