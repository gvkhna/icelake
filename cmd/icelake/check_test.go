package main

// The check subcommand's tests: the report against a fresh deployment, against
// a live daemon, and against the wreckage of a kill -9 — because "your records
// are safe on disk" is exactly the sentence that must be earned against a
// process that really was killed with records really staged.

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// runCheckInProcess drives the subcommand the way main does and returns what
// it printed and whether it failed.
func runCheckInProcess(t *testing.T) (string, error) {
	t.Helper()

	var out, errOut bytes.Buffer
	err := run(t.Context(), []string{"check"}, strings.NewReader(""), &out, &errOut)

	return out.String(), err
}

// TestCheckReportsAFreshLocalOnlySetupAndWritesNothing is the first run an
// operator makes: everything passes, every line says why, and the check left
// no file behind — a validation that created state would be a validation that
// changed what it validated.
func TestCheckReportsAFreshLocalOnlySetupAndWritesNothing(t *testing.T) {
	dir := localOnlyEnv(t)

	out, err := runCheckInProcess(t)
	if err != nil {
		t.Fatalf("check on a valid fresh setup failed: %v\n%s", err, out)
	}

	for _, want := range []string{
		"checking the setup",
		"configuration",
		"schema declares 1 table: market.fills",
		"not running, nothing holds",
		"no staging database yet",
		"local-only mode; there is no bucket to check",
		"4 of 4 passed.",
		"The daemon is not running and nothing is waiting.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the report does not say %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "failed") {
		t.Errorf("a passing report says failed:\n%s", out)
	}

	for _, name := range []string{"staging.db", "icelake.pid", "icelake.log"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("check created %s", name)
		}
	}
}

// TestCheckSeesTheDaemonAndTheRecordsAKillLeftBehind is the operational story
// end to end: check says "running" while the daemon runs with records staged,
// and after a kill -9 it says "not running", counts the survivors, and
// promises them to the next run — all with exit 0, because a stopped daemon
// with safe data is a state, not a failure.
func TestCheckSeesTheDaemonAndTheRecordsAKillLeftBehind(t *testing.T) {
	dir := t.TempDir()
	env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)
	// Thresholds nothing in this test can trip, so every accepted record stays
	// staged until something drains — and nothing here drains.
	env = append(env, envFlushInterval.name+"=1h")
	for _, kv := range env[len(os.Environ()):] {
		name, value, _ := strings.Cut(kv, "=")
		t.Setenv(name, value)
	}

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()

	daemon := exec.Command(binary(t), "run", "-f")
	daemon.Env = env
	daemon.Stdin = stdinR
	if err := daemon.Start(); err != nil {
		t.Fatalf("starting the daemon: %v", err)
	}
	killed := false
	defer func() {
		if !killed {
			_ = daemon.Process.Kill()
			_, _ = daemon.Process.Wait()
		}
	}()

	for i := 1; i <= 10; i++ {
		if _, err := stdinW.WriteString(fillLine(i) + "\n"); err != nil {
			t.Fatalf("writing record %d: %v", i, err)
		}
	}

	// The daemon accepts asynchronously; the report is re-asked until it has
	// seen both facts it must agree with the kernel and the database about.
	var out string
	deadline := time.Now().Add(30 * time.Second)
	for {
		out, err = runCheckInProcess(t)
		if err != nil {
			t.Fatalf("check beside the running daemon failed: %v\n%s", err, out)
		}
		if strings.Contains(out, "running as pid") && strings.Contains(out, "10 records waiting") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the report never showed a running daemon with 10 records waiting:\n%s", out)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(out, "The daemon is running and the 10 waiting records will upload as it drains.") {
		t.Errorf("the live verdict does not promise the drain:\n%s", out)
	}

	// kill -9: no drain, no pid-file cleanup, records still staged. The one
	// ending the daemon cannot soften is the one the report has to be honest
	// about.
	if err := daemon.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_, _ = daemon.Process.Wait()
	killed = true

	out, err = runCheckInProcess(t)
	if err != nil {
		t.Fatalf("check after the kill failed: %v\n%s", err, out)
	}
	for _, want := range []string{
		"not running, nothing holds",
		"10 records waiting",
		"The daemon is not running; the 10 waiting records will upload when it next runs.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the post-kill report does not say %q:\n%s", want, out)
		}
	}
}

// TestCheckRefusalsAreUsageRefusals pins the exit-2 boundary: arguments it
// does not take, and a configuration that cannot resolve, are both refused
// before any probe runs and without a report.
func TestCheckRefusalsAreUsageRefusals(t *testing.T) {
	t.Run("an argument", func(t *testing.T) {
		localOnlyEnv(t)

		var out, errOut bytes.Buffer
		err := run(t.Context(), []string{"check", "--verbose"}, strings.NewReader(""), &out, &errOut)
		if !errors.Is(err, errUsage) {
			t.Fatalf("check --verbose returned %v, want a refusal wrapping errUsage", err)
		}
		if out.Len() != 0 {
			t.Errorf("a refused check wrote a report:\n%s", out.String())
		}
	})

	t.Run("a missing schema file", func(t *testing.T) {
		t.Setenv(envDataDir.name, t.TempDir())
		t.Setenv(envLocalOnly.name, "true")

		var out, errOut bytes.Buffer
		err := run(t.Context(), []string{"check"}, strings.NewReader(""), &out, &errOut)
		if !errors.Is(err, errUsage) {
			t.Fatalf("check with no schema file returned %v, want a refusal wrapping errUsage", err)
		}
		if out.Len() != 0 {
			t.Errorf("a refused check wrote a report:\n%s", out.String())
		}
	})
}
