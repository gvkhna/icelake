package main

// This file is "icelake check": the setup validated and the current state
// reported, writing nothing anywhere. It is a thin wrapper over
// [inspect.Inspect] plus the two facts only this process can supply — whether
// the configuration resolves, which is readSettings and loadTables exactly as
// run resolves it, and whether a daemon holds the pid lock, which is process
// management and therefore this command's own sanctioned work.
//
// What it prints is a report an operator asked for: a product on stdout, like
// the rebuild report, not log lines. Its failures still exit through main's
// one logfmt line on stderr.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gvkhna/icelake"
	"github.com/gvkhna/icelake/inspect"
)

// checkLine is one row of the report: a name, a judgement, and one plain
// sentence.
type checkLine struct {
	name   string
	ok     bool
	detail string
}

// runCheck is "icelake check": every probe run, one line per check, a verdict,
// and an exit code a script can act on — 0 all passed, 1 something failed,
// 2 the configuration was refused before checks could start.
func runCheck(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 {
		fmt.Fprint(stderr, shortUsage())

		return fmt.Errorf("%w: check takes no arguments (%s); it is configured by environment variables", errUsage, strings.Join(args, " "))
	}

	// The same resolution the daemon makes, refused the same way: a bad
	// configuration exits 2 with the violations on stderr, before any report.
	cfg, err := readSettings(forRun)
	if err != nil {
		return err
	}
	tables, err := loadTables(cfg)
	if err != nil {
		return err
	}

	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.Namespace + "." + t.Table
	}
	lines := []checkLine{{
		name: "configuration",
		ok:   true,
		detail: fmt.Sprintf("schema declares %d %s: %s",
			len(tables), plural(int64(len(tables)), "table"), strings.Join(names, ", ")),
	}}

	lines = append(lines, daemonLine(cfg.pidFile))

	report, err := inspect.Inspect(ctx, inspect.Options{
		StagingPath:     cfg.stagingPath,
		LocalOnly:       cfg.localOnly,
		Endpoint:        cfg.endpoint,
		Bucket:          cfg.bucket,
		WarehousePrefix: cfg.prefix,
		Region:          cfg.region,
		AccessKeyID:     cfg.accessKeyID,
		SecretAccessKey: cfg.secretAccessKey,
		Tables:          tableRefs(tables),
		ClickHouse:      clickHouseOptions(cfg),
	})
	if err != nil {
		// Unreachable while readSettings enforces the same matrix, and still a
		// configuration refusal if the two ever disagree.
		return fmt.Errorf("%w: %w", errUsage, err)
	}

	lines = append(lines, stagingLine(report.Staging, cfg.localOnly))
	lines = append(lines, bucketLine(report.Bucket, cfg.localOnly))
	for _, t := range report.Tables {
		lines = append(lines, tableLine(t))
	}
	if report.ClickHouse != nil {
		lines = append(lines, clickHouseLine(*report.ClickHouse))
	}

	render(stdout, lines, verdict(lines, report.Staging, daemonRunning(lines)))

	if failed := countFailed(lines); failed > 0 {
		return fmt.Errorf("%d of %d checks failed", failed, len(lines))
	}

	return nil
}

// daemonLine asks the kernel, not the file: a shared non-blocking flock on the
// pid file fails exactly when a daemon holds its exclusive lock. The file is
// opened read-only and never created — a check that left a pid file behind
// would be a check that wrote something.
func daemonLine(pidFile string) checkLine {
	line := checkLine{name: "daemon", ok: true}

	f, err := os.Open(pidFile)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		line.detail = "not running, nothing holds " + pidFile

		return line
	case err != nil:
		line.ok = false
		line.detail = "could not read " + pidFile + ": " + err.Error()

		return line
	}
	defer func() { _ = f.Close() }()

	err = syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
	switch {
	case err == nil:
		// The lock is free, so whatever wrote this file is gone; the lock and
		// not the file is the single-instance test, and a stale file blocks
		// nothing. The shared lock is dropped immediately rather than ridden
		// to the deferred close, because for as long as this process holds it
		// a daemon starting right now would be refused — that is also why the
		// daemon's own acquisition retries briefly before believing a refusal.
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		line.detail = "not running, nothing holds " + pidFile

		return line
	case errors.Is(err, syscall.EWOULDBLOCK):
		held, _ := io.ReadAll(io.LimitReader(f, 64))
		pid := strings.TrimSpace(string(held))
		if pid == "" {
			pid = "an unknown pid"
		} else {
			pid = "pid " + pid
		}
		line.detail = "running as " + pid + ", holding " + pidFile

		return line
	default:
		line.ok = false
		line.detail = "could not probe the lock on " + pidFile + ": " + err.Error()

		return line
	}
}

// daemonRunning reads the daemon line's answer back out of the report, so the
// verdict does not probe a second time and cannot disagree with the line.
func daemonRunning(lines []checkLine) bool {
	for _, l := range lines {
		if l.name == "daemon" {
			return strings.HasPrefix(l.detail, "running")
		}
	}

	return false
}

// stagingLine renders the staging summary.
func stagingLine(s inspect.Staging, localOnly bool) checkLine {
	line := checkLine{name: "staging", ok: true}

	switch {
	case s.Err != nil:
		line.ok = false
		line.detail = oneLine(s.Err.Error())
	case !s.Exists:
		line.detail = "no staging database yet at " + s.Path + "; nothing has ever been accepted"
	default:
		line.detail = fmt.Sprintf("%s %s waiting, %s quarantined, in %s",
			group(s.WaitingRecords), plural(s.WaitingRecords, "record"), group(s.QuarantinedRecords), s.Path)
		// A backlog is only worth a clause where an upload is owed: in bucket
		// mode these are files the next writer open or drain will move.
		if !localOnly && s.SpoolBacklogFiles > 0 {
			line.detail += fmt.Sprintf(", %s spool %s not yet uploaded",
				group(s.SpoolBacklogFiles), plural(s.SpoolBacklogFiles, "file"))
		}
	}

	return line
}

// bucketLine renders the reachability probe, or says why there was none.
func bucketLine(b *inspect.Bucket, localOnly bool) checkLine {
	line := checkLine{name: "bucket", ok: true}

	switch {
	case localOnly:
		line.detail = "local-only mode; there is no bucket to check"
	case b.Err != nil:
		line.ok = false
		line.detail = oneLine(b.Err.Error())
	default:
		line.detail = fmt.Sprintf("%s answered a read in %s, credentials accepted", b.Bucket, roundTrip(b.RoundTrip))
	}

	return line
}

// tableLine renders one declared table's state in the bucket.
func tableLine(t inspect.Table) checkLine {
	line := checkLine{name: t.Namespace + "." + t.Name, ok: true}

	switch {
	case errors.Is(t.Err, inspect.ErrNotChecked):
		line.ok = false
		line.detail = t.Err.Error()
	case t.Err != nil:
		line.ok = false
		line.detail = oneLine(t.Err.Error())
	case !t.Exists:
		line.detail = "new table, nothing written yet, the first commit will create it"
	default:
		line.detail = fmt.Sprintf("table exists, last commit %s, %s %s",
			t.LastCommitAt.UTC().Format("2006-01-02 15:04:05 MST"),
			group(int64(t.Snapshots)), plural(int64(t.Snapshots), "snapshot"))
	}

	return line
}

// clickHouseLine renders the mirror's ping.
func clickHouseLine(ch inspect.ClickHouse) checkLine {
	line := checkLine{name: "clickhouse", ok: true}

	if ch.Err != nil {
		line.ok = false
		line.detail = oneLine(ch.Err.Error())

		return line
	}
	line.detail = "mirror answered in " + roundTrip(ch.RoundTrip)

	return line
}

// verdict is the last line: how many passed, and the one thing the operator
// most needs to know about their data given how it went.
func verdict(lines []checkLine, s inspect.Staging, running bool) string {
	failed := countFailed(lines)
	passed := len(lines) - failed

	var b strings.Builder
	fmt.Fprintf(&b, "%d of %d passed.", passed, len(lines))

	waiting := group(s.WaitingRecords) + " waiting " + plural(s.WaitingRecords, "record")
	switch {
	// A staging summary that could not be read leaves every claim about the
	// data unearned, and a verdict that said "nothing is waiting" would be
	// asserting the exact fact the check failed to observe.
	case s.Err != nil:
		b.WriteString(" Where the data stands is unknown until the staging check passes; fix that first.")
	case failed > 0 && s.WaitingRecords > 0:
		fmt.Fprintf(&b, " The %s are safe on disk and will upload once every check passes.", waiting)
	case failed > 0:
		b.WriteString(" Nothing is waiting. Fix what failed and run this again.")
	case running && s.WaitingRecords > 0:
		fmt.Fprintf(&b, " The daemon is running and the %s will upload as it drains.", waiting)
	case running:
		b.WriteString(" The daemon is running and nothing is waiting.")
	case s.WaitingRecords > 0:
		fmt.Fprintf(&b, " The daemon is not running; the %s will upload when it next runs.", waiting)
	default:
		b.WriteString(" The daemon is not running and nothing is waiting.")
	}

	if s.QuarantinedRecords > 0 {
		fmt.Fprintf(&b, " The %s quarantined %s will never upload; inspect the staging database and remove them.",
			group(s.QuarantinedRecords), plural(s.QuarantinedRecords, "record"))
	}

	return b.String()
}

// render prints the whole report: header, one aligned line per check, verdict.
func render(w io.Writer, lines []checkLine, verdict string) {
	fmt.Fprintf(w, "icelake %s checking the setup\n\n", versionString())

	width := 0
	for _, l := range lines {
		width = max(width, len(l.name))
	}
	for _, l := range lines {
		status := "ok"
		if !l.ok {
			status = "failed"
		}
		fmt.Fprintf(w, "%-*s  %-6s  %s\n", width, l.name, status, l.detail)
	}

	fmt.Fprintf(w, "\n%s\n", verdict)
}

// countFailed counts the lines that failed.
func countFailed(lines []checkLine) int {
	n := 0
	for _, l := range lines {
		if !l.ok {
			n++
		}
	}

	return n
}

// tableRefs reduces the schema document's declarations to the identities the
// inspection needs.
func tableRefs(tables []icelake.DynamicTableConfig) []inspect.TableRef {
	refs := make([]inspect.TableRef, len(tables))
	for i, t := range tables {
		refs[i] = inspect.TableRef{Namespace: t.Namespace, Table: t.Table}
	}

	return refs
}

// clickHouseOptions carries the mirror settings across, or nil when no mirror
// is configured.
func clickHouseOptions(cfg settings) *inspect.ClickHouseOptions {
	if cfg.clickHouseAddr == "" {
		return nil
	}

	return &inspect.ClickHouseOptions{
		Addr:     cfg.clickHouseAddr,
		Database: cfg.clickHouseDatabase,
		Username: cfg.clickHouseUsername,
		Password: cfg.clickHousePassword,
		TLS:      cfg.clickHouseTLS,
	}
}

// group renders a count with thousands separators, because 31204 and 31,204
// are read at different speeds and this report exists to be read.
func group(n int64) string {
	s := strconv.FormatInt(n, 10)
	if n < 0 {
		return s
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}

	return b.String()
}

// plural returns the word, pluralized the regular way when the count is not
// one.
func plural(n int64, word string) string {
	if n == 1 {
		return word
	}

	return word + "s"
}

// roundTrip renders a probe's duration at the resolution an operator compares:
// whole milliseconds.
func roundTrip(d time.Duration) string {
	ms := d.Milliseconds()
	if ms < 1 {
		return "<1ms"
	}

	return strconv.FormatInt(ms, 10) + "ms"
}

// oneLine flattens an error onto the single line the report gives it.
func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
