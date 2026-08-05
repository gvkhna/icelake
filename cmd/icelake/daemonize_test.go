package main

// The process-management layer's tests: the default backgrounding, the pid
// lock, the log file, and the flags. They drive the built binary the way an
// operator does, because what they assert — a process that outlives its
// starter, a lock the kernel holds, a file another program reads a pid from —
// only exists between real processes.

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestVersionFlagsAnswerLikeTheSubcommand pins the aliases: three spellings,
// one answer, no configuration required.
func TestVersionFlagsAnswerLikeTheSubcommand(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"-v"}, {"--version"}} {
		var out, errOut bytes.Buffer
		if err := run(t.Context(), args, strings.NewReader(""), &out, &errOut); err != nil {
			t.Errorf("run(%q) returned %v, want nil", args, err)
		}
		if strings.TrimSpace(out.String()) == "" {
			t.Errorf("run(%q) printed no version", args)
		}
	}
}

// TestBackgroundRunDetachesLocksLogsAndDrains is the default mode end to end:
// one daemon started the way an operator starts it, with every promise the
// manual makes about it asserted on the way through — the starter exits zero
// only after readiness, the pid file names a live process, the lock refuses a
// second start in either mode, the log file carries the startup line, and end
// of input still drains the pipe into real Parquet before the process goes.
func TestBackgroundRunDetachesLocksLogsAndDrains(t *testing.T) {
	dir := t.TempDir()
	env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)

	// Real descriptors, because the whole point is that the daemon inherits
	// the read end and keeps it after the starter is gone.
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()

	var starterOut bytes.Buffer
	starter := exec.Command(binary(t), "run")
	starter.Env = env
	starter.Stdin = stdinR
	starter.Stdout = &starterOut
	starter.Stderr = &starterOut
	if err := starter.Run(); err != nil {
		t.Fatalf("the starter did not exit cleanly: %v\n%s", err, starterOut.String())
	}
	if !strings.Contains(starterOut.String(), "started in the background: pid ") {
		t.Fatalf("the starter did not report the daemon:\n%s", starterOut.String())
	}

	pidPath := filepath.Join(dir, "icelake.pid")
	raw, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatalf("no pid file after a successful start: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("the pid file holds %q, not a pid", raw)
	}
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("the pid file names %d, which is not running: %v", pid, err)
	}

	// The lock, not the file, is the single-instance test — and it answers a
	// second start the same way whichever mode that start asked for.
	for _, args := range [][]string{{"run"}, {"run", "-f"}} {
		second := exec.Command(binary(t), args...)
		second.Env = env
		second.Stdin = strings.NewReader("")
		out, err := second.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 {
			t.Fatalf("a second %q against a running daemon: err %v, want exit 1\n%s", args, err, out)
		}
		if !strings.Contains(string(out), "already running") {
			t.Errorf("the second start's refusal does not say already running:\n%s", out)
		}
	}

	// The pipe still feeds the daemon after its starter is gone.
	if _, err := stdinW.WriteString(fillLine(1) + "\n" + fillLine(2) + "\n"); err != nil {
		t.Fatalf("writing to the detached daemon's stdin: %v", err)
	}
	_ = stdinW.Close()

	// End of input drains and exits, exactly as in the foreground.
	deadline := time.Now().Add(30 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("daemon %d did not exit after end of input", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if _, err := os.Stat(pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the pid file survived a clean exit: %v", err)
	}

	logRaw, err := os.ReadFile(filepath.Join(dir, "icelake.log"))
	if err != nil {
		t.Fatalf("no log file after a backgrounded run: %v", err)
	}
	if !strings.Contains(string(logRaw), "staging=") {
		t.Errorf("the log does not carry the startup configuration line:\n%s", logRaw)
	}

	// And the records are really on disk, which is what the drain was for.
	files, err := filepath.Glob(filepath.Join(dir, "cache", "market", "fills", "data", "*.parquet"))
	if err != nil || len(files) == 0 {
		t.Errorf("no Parquet in the cache after the drain (%v): %v", err, files)
	}
}

// TestBackgroundRefusalsLandOnTheCallersTerminal pins the ordering the manual
// promises: everything that can be refused is refused before anything
// detaches, so the message arrives where the command was typed and no pid
// file is left pointing at nothing.
func TestBackgroundRefusalsLandOnTheCallersTerminal(t *testing.T) {
	t.Run("a bad configuration exits two and leaves no pid file", func(t *testing.T) {
		dir := t.TempDir()
		cmd := exec.Command(binary(t), "run")
		cmd.Env = append(os.Environ(), envDataDir.name+"="+dir)
		cmd.Stdin = strings.NewReader("")
		out, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 2 {
			t.Fatalf("err %v, want exit 2\n%s", err, out)
		}
		if !strings.Contains(string(out), envSchemaFile.name) {
			t.Errorf("the refusal does not name the missing schema file:\n%s", out)
		}
		if _, err := os.Stat(filepath.Join(dir, "icelake.pid")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("a refused start left a pid file behind")
		}
	})

	t.Run("a terminal on stdin is refused", func(t *testing.T) {
		// A pty master answers termios ioctls, which is the definition the
		// check uses; where the environment has no pty to offer there is
		// nothing to prove.
		master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
		if err != nil {
			t.Skipf("no pty available: %v", err)
		}
		defer master.Close()

		dir := t.TempDir()
		cmd := exec.Command(binary(t), "run")
		cmd.Env = append(os.Environ(), localOnlyEnvPairs(t, dir)...)
		cmd.Stdin = master
		out, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 2 {
			t.Fatalf("err %v, want exit 2\n%s", err, out)
		}
		if !strings.Contains(string(out), "stdin is a terminal") {
			t.Errorf("the refusal does not explain itself:\n%s", out)
		}
	})
}

// TestLogFileSendsForegroundLogsToTheFile pins the override half of the log
// rule: a foreground run logs to stderr until the operator names a file, and
// then everything — the startup line first — goes there instead.
func TestLogFileSendsForegroundLogsToTheFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "elsewhere.log")
	env := append(os.Environ(), localOnlyEnvPairs(t, dir)...)
	env = append(env, envLogFile.name+"="+logPath)

	code, out := pipeInto(t, env, fillLine(1)+"\n")
	if code != 0 {
		t.Fatalf("exit code = %d, want 0\n%s", code, out)
	}
	if strings.Contains(out, "staging=") {
		t.Errorf("the startup line still went to stderr with a log file configured:\n%s", out)
	}
	logRaw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("the configured log file was not written: %v", err)
	}
	if !strings.Contains(string(logRaw), "staging=") {
		t.Errorf("the configured log file does not carry the startup line:\n%s", logRaw)
	}
}
