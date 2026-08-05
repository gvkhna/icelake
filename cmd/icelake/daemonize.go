package main

// This file is process management: how this command runs, not what it does.
// `AGENTS.md` sanctions it as the one category of work a command here may
// contain beside the four translations, and the same decision forbids it in
// the library — a library never manages the process that embeds it. Nothing in
// this file touches a record, and nothing in it is behaviour a crash or
// refusal test on data would assert on: delete it and the daemon still does
// everything it does, only attached to your shell.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gvkhna/icelake"

	"golang.org/x/sys/unix"
)

// daemonMarker tells a copy of this binary that it is the backgrounded child.
// It is private protocol between one exec and the next, not configuration:
// nothing documents it, and an operator who sets it gets what they asked for.
const daemonMarker = "_ICELAKE_DAEMONIZED"

// The two descriptors the parent passes the child beyond the standard three:
// the locked pid file, and the pipe whose one byte means "ready". They are
// positions in ExtraFiles, which the kernel numbers from three.
const (
	pidFileFD   = 3
	readinessFD = 4
)

// launch is everything between "the operator typed run" and runDaemon.
//
// The configuration and the schema document are resolved here, first, whatever
// the mode: every refusal either can make lands on the terminal of whoever
// typed the command, not in a log file they have not found yet, and the
// backgrounded child re-resolves the same environment to the same answer.
func launch(ctx context.Context, foreground bool, stdin io.Reader, stderr io.Writer) error {
	cfg, err := readSettings(forRun)
	if err != nil {
		return err
	}
	tables, err := loadTables(cfg)
	if err != nil {
		return err
	}

	switch {
	case os.Getenv(daemonMarker) != "":
		return runChild(ctx, cfg, tables, stdin, stderr)
	case foreground:
		return runForeground(ctx, cfg, tables, stdin, stderr)
	default:
		return daemonize(cfg, stderr)
	}
}

// daemonize starts this same binary over as a detached `run -f`, hands it
// stdin, the log file, the locked pid file and a readiness pipe, and exits
// the way the operator wants a starter to exit: zero once the daemon is
// actually ready, the child's own code if it died first.
func daemonize(cfg settings, stderr io.Writer) error {
	// The starter reports on the run it is starting, and a report on a run is
	// a log line — the rule has no exceptions — so even these terminal-facing
	// lines are logfmt records over the operator's stderr.
	log := newLogger(stderr, cfg.logLevel)

	// A terminal on stdin means nobody piped anything: a background daemon
	// cannot read a keyboard it just detached from, and backgrounding it
	// anyway would start a daemon whose whole input is the terminal's EOF.
	if stdinIsTerminal() {
		return fmt.Errorf("%w: stdin is a terminal; pipe records in, or run with -f to stay in the foreground", errUsage)
	}

	lock, err := acquirePidLock(cfg.pidFile)
	if err != nil {
		return err
	}
	// Every refusal between here and a started child abandons the lock the
	// same way runForeground's error path does: remove while still holding it,
	// then close. The lock and not the file is the single-instance test, but a
	// file this process created and then walked away from would still be a
	// file somebody reads a pid out of.
	abandon := func() {
		_ = os.Remove(cfg.pidFile)
		_ = lock.Close()
	}

	logFile, err := os.OpenFile(cfg.logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		abandon()

		return fmt.Errorf("%w: %s: %w", errUsage, envLogFile.name, err)
	}
	defer logFile.Close()

	readyR, readyW, err := os.Pipe()
	if err != nil {
		abandon()

		return err
	}
	defer readyR.Close()

	exe, err := os.Executable()
	if err != nil {
		abandon()

		return err
	}

	cmd := exec.Command(exe, "run", "-f")
	// The resolved log path rides along explicitly so the child's startup line
	// names the file it is actually writing to, even when the path was this
	// process's derived default rather than something the operator set.
	cmd.Env = append(os.Environ(), daemonMarker+"=1", envLogFile.name+"="+cfg.logFile)
	cmd.Stdin = os.Stdin
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.ExtraFiles = []*os.File{lock, readyW}
	// A session of its own: no controlling terminal, no membership in the
	// shell's job, no SIGHUP when the terminal goes away.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		_ = readyW.Close()
		abandon()

		return err
	}
	// Only the child's copy of the write end may exist now: the read below
	// must return EOF when the child dies, not wait on a descriptor this
	// process forgot it was holding.
	_ = readyW.Close()

	// The flock rides into the child on its inherited descriptor — the lock
	// belongs to the open file description both processes now share, so it
	// lives exactly as long as the child does, whatever this process closes.
	// The pid the file reports is therefore the child's.
	if err := writePid(lock, cmd.Process.Pid); err != nil {
		log.Warn("could not write the pid file; the lock still holds", "pid-file", cfg.pidFile, "err", err)
	}

	buf := make([]byte, 1)
	if n, _ := readyR.Read(buf); n == 0 {
		// EOF without the ready byte: the child died during startup. Its
		// message is in the log; its exit code is relayed so a configuration
		// refusal stays exit 2 end to end.
		werr := cmd.Wait()
		var exit *exec.ExitError
		if errors.As(werr, &exit) && exit.ExitCode() == exitUsage {
			return fmt.Errorf("%w: the daemon refused its configuration during startup; its log is %s", errUsage, cfg.logFile)
		}

		return fmt.Errorf("the daemon exited during startup; its log is %s", cfg.logFile)
	}

	log.Info("started in the background", "pid", cmd.Process.Pid, "log", cfg.logFile, "pid-file", cfg.pidFile)

	return nil
}

// runChild is the backgrounded daemon itself: the plain foreground run, plus
// the two descriptors its parent left it — the lock it must keep holding and
// the pipe it answers on when the writers are open.
func runChild(ctx context.Context, cfg settings, tables []icelake.DynamicTableConfig, stdin io.Reader, stderr io.Writer) error {
	// Holding the *os.File is holding the lock. It is deliberately kept in a
	// variable that outlives runDaemon rather than closed after start: closing
	// the last descriptor on the open file description would release the flock
	// and let a second daemon in while this one is still writing.
	lock := os.NewFile(uintptr(pidFileFD), cfg.pidFile)

	readyPipe := os.NewFile(uintptr(readinessFD), "readiness")
	ready := func() {
		if readyPipe != nil {
			_, _ = readyPipe.Write([]byte{'1'})
			_ = readyPipe.Close()
		}
	}

	// This process's stderr is the log file the parent redirected, so the
	// logger over it is the daemon's log.
	err := runDaemon(ctx, cfg, tables, stdin, newLogger(stderr, cfg.logLevel), ready)

	// Best-effort tidiness on the way out; the lock, not the file's existence,
	// is what a second start actually checks, so a hard kill that skips this
	// leaves nothing worse than a stale file a later start overwrites.
	_ = os.Remove(cfg.pidFile)
	if lock != nil {
		_ = lock.Close()
	}

	return err
}

// runForeground is `run -f` typed by an operator: attached, logging to stderr
// (or the configured log file), and still holding the pid lock — two daemons
// against one staging.db is the same mistake in either mode, and it is caught
// here by the lock rather than later by whoever loses the race.
func runForeground(ctx context.Context, cfg settings, tables []icelake.DynamicTableConfig, stdin io.Reader, stderr io.Writer) error {
	// The log destination is resolved before anything else so that from the
	// first event, everything this process says as a daemon says it through
	// the one logger — the pid-file warning below included.
	logW := stderr
	var logFile *os.File
	if cfg.logFileSet {
		var err error
		logFile, err = os.OpenFile(cfg.logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("%w: %s: %w", errUsage, envLogFile.name, err)
		}
		logW = logFile
	}
	log := newLogger(logW, cfg.logLevel)

	lock, err := acquirePidLock(cfg.pidFile)
	if err != nil {
		if logFile != nil {
			_ = logFile.Close()
		}

		return err
	}
	if err := writePid(lock, os.Getpid()); err != nil {
		log.Warn("could not write the pid file; the lock still holds", "pid-file", cfg.pidFile, "err", err)
	}

	err = runDaemon(ctx, cfg, tables, stdin, log, nil)

	if logFile != nil {
		_ = logFile.Close()
	}
	_ = os.Remove(cfg.pidFile)
	_ = lock.Close()

	return err
}

// acquirePidLock opens the pid file, creating it if needed, and takes the
// non-blocking exclusive flock that makes "is one already running" a question
// the kernel answers instead of a pid the file merely remembers.
//
// After the lock is won it re-checks that the path still names the inode it
// locked, and starts over if not. That closes the window a clean shutdown
// opens: an exiting daemon removes the file it holds locked, so a start that
// opened the old inode just before the removal wins a lock on a ghost — a
// file no path names — while a third start could create and lock a fresh one.
// Without the check, that is two daemons each holding "the" lock. The loop is
// bounded because each retry needs another daemon to be mid-exit in the same
// few instructions, and a start that loses five of those races in a row has
// earned an error naming the file instead of a sixth spin.
func acquirePidLock(path string) (*os.File, error) {
	for attempt := 0; attempt < 5; attempt++ {
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return nil, fmt.Errorf("pid file %s: %w", path, err)
		}
		// The acquisition itself retries briefly before believing a refusal:
		// `icelake check` probes this same file with a momentary shared lock,
		// and a start that raced those few microseconds would otherwise refuse
		// naming a pid that is not running. A real daemon's lock is not
		// momentary, so three failures a pause apart mean what one used to.
		flockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		for tries := 0; flockErr != nil && errors.Is(flockErr, syscall.EWOULDBLOCK) && tries < 2; tries++ {
			time.Sleep(25 * time.Millisecond)
			flockErr = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		}
		if flockErr != nil {
			held, _ := io.ReadAll(io.LimitReader(f, 64))
			_ = f.Close()
			holder := strings.TrimSpace(string(held))
			if holder == "" {
				holder = "another process"
			} else {
				holder = "pid " + holder
			}

			return nil, fmt.Errorf("already running: %s holds %s", holder, path)
		}

		locked, err := f.Stat()
		if err != nil {
			_ = f.Close()

			return nil, fmt.Errorf("pid file %s: %w", path, err)
		}
		named, err := os.Stat(path)
		if err == nil && os.SameFile(locked, named) {
			return f, nil
		}
		// The file this lock is on is no longer the file the path names:
		// the holder removed it on the way out. Drop the ghost and try the
		// real path again.
		_ = f.Close()
	}

	return nil, fmt.Errorf("pid file %s: kept vanishing mid-acquisition; is a daemon crash-looping?", path)
}

// writePid replaces the file's content with one pid and a newline, the whole
// of the format every init script and `kill $(cat …)` expects.
func writePid(f *os.File, pid int) error {
	if err := f.Truncate(0); err != nil {
		return err
	}
	if _, err := f.WriteAt([]byte(strconv.Itoa(pid)+"\n"), 0); err != nil {
		return err
	}

	return f.Sync()
}

// stdinIsTerminal reports whether fd 0 is an interactive terminal.
//
// The question is asked with a termios read rather than a mode check because
// the mode check cannot tell a terminal from /dev/null — both are character
// devices — and /dev/null is a stdin this daemon accepts: it reads EOF,
// drains nothing, and exits cleanly, which is honest. Only a terminal is
// refused, because only a terminal means a person who expected something else.
func stdinIsTerminal() bool {
	_, err := unix.IoctlGetTermios(int(os.Stdin.Fd()), termiosRequest)

	return err == nil
}
