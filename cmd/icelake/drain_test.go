package main

// The drain's clock rule and its narration, tested from outside the process
// the way an operator meets them. The rule under test is the one `AGENTS.md`
// dates to 2026-08-05: end of input drains to completion with no clock, a
// signal's drain is bounded, and either way the log's last word carries the
// account — staged, staging, lost, action — instead of a mechanism phrase.

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestEndOfInputDrainOutlivesTheShutdownTimeout is the rule's sharp edge: the
// timeout is set three orders of magnitude below what the drain needs, and the
// run must still exit zero with everything committed. Before the rule, this
// exact shape — a bulk load whose pipe closes with a backlog staged — died on
// "context deadline exceeded" after minutes of clean work; the 1ms setting is
// what makes this test impossible to pass by merely having a generous default.
func TestEndOfInputDrainOutlivesTheShutdownTimeout(t *testing.T) {
	dir := t.TempDir()
	env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)
	env = append(env, envShutdownTimeout.name+"=1ms")

	var input strings.Builder
	for i := range 150_000 {
		input.WriteString(fillLine(i) + "\n")
	}

	code, out := pipeInto(t, env, input.String())
	if code != 0 {
		t.Fatalf("exit code = %d, want 0\n%s", code, out)
	}
	if !strings.Contains(out, "drained; every accepted record is committed") {
		t.Errorf("the log does not end with the drained line:\n%s", tail(out))
	}
	if strings.Contains(out, "context deadline exceeded") {
		t.Errorf("the mechanism phrase is back:\n%s", tail(out))
	}
}

// TestASignalCutShortDrainSaysWhereTheData is the other half: a signal's drain
// is bounded, and when the budget ends it, the run exits 1 and the last words
// are the account — the count, the file, lost=0, and what happens next — with
// the environment variable named so the operator knows which knob this was.
// The restart then commits everything, which is the claim the account makes.
func TestASignalCutShortDrainSaysWhereTheData(t *testing.T) {
	dir := t.TempDir()
	env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)
	env = append(env, envShutdownTimeout.name+"=1ms")

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()

	cmd := exec.Command(binary(t), "run", "-f")
	cmd.Env = env
	cmd.Stdin = stdinR
	out := &lockedBuffer{}
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	// Enough records that some are still uncommitted when the signal lands a
	// moment later — the writes below take real seconds to accept, and the
	// signal arrives in the middle of them. The pipe stays open, so this is
	// the signal path and not end of input.
	go func() {
		for i := range 400_000 {
			if _, err := stdinW.WriteString(fillLine(i) + "\n"); err != nil {
				return
			}
		}
	}()

	waitFor(t, "the daemon to start reading", func() bool {
		return strings.Contains(out.String(), "reading records")
	})
	time.Sleep(1500 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	err = cmd.Wait()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("a cut-short drain: err %v, want exit 1\n%s", err, tail(out.String()))
	}
	for _, want := range []string{
		envShutdownTimeout.name, "staged=", "lost=0",
		"the next run replays and commits them",
		"still staged",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the account is missing %q:\n%s", want, tail(out.String()))
		}
	}

	// The account's claim, kept: a restart commits everything and says so.
	second := exec.Command(binary(t), "run", "-f")
	second.Env = append(os.Environ(), localOnlyEnvPairs(t, dir)...)
	second.Stdin = strings.NewReader("")
	secondOut, err := second.CombinedOutput()
	if err != nil {
		t.Fatalf("the restart did not exit cleanly: %v\n%s", err, secondOut)
	}
	if !strings.Contains(string(secondOut), "drained; every accepted record is committed") {
		t.Errorf("the restart does not report the commit:\n%s", tail(string(secondOut)))
	}
}

// TestLogLevelIsParsedAndRefused pins the level variable to slog's own
// spellings: the standard four are taken in any case, anything else is a
// configuration refusal naming the variable, and the resolved level appears
// in the startup line so a debug run says it is one.
func TestLogLevelIsParsedAndRefused(t *testing.T) {
	dir := t.TempDir()
	env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)
	env = append(env, envLogLevel.name+"=DEBUG")
	code, out := pipeInto(t, env, fillLine(1)+"\n")
	if code != 0 {
		t.Fatalf("DEBUG level refused: exit %d\n%s", code, out)
	}
	if !strings.Contains(out, "log-level=DEBUG") {
		t.Errorf("the startup line does not report the level:\n%s", tail(out))
	}

	env = append(os.Environ(), localOnlyEnvPairs(t, dir)...)
	env = append(env, envLogLevel.name+"=chatty")
	code, out = pipeInto(t, env, "")
	if code != 2 {
		t.Fatalf("a level slog cannot parse: exit %d, want 2\n%s", code, out)
	}
	if !strings.Contains(out, envLogLevel.name) {
		t.Errorf("the refusal does not name the variable:\n%s", out)
	}
}

// tail keeps a failure message readable when the run's log is long.
func tail(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > 12 {
		lines = lines[len(lines)-12:]
	}

	return strings.Join(lines, "\n")
}
