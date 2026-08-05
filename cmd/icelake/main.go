// Command icelake writes records from a pipe into Apache Iceberg tables.
//
// It is a thin wrapper with no logic of its own: it reads its configuration from
// the environment, loads a schema document, opens one writer per table it
// declares, and hands stdin to the library's [icelake.IngestStream]. Everything a
// reader might take for the daemon's own behaviour — batching, chunked reading,
// the local Parquet cache and its retention, local-only mode, replay after a
// crash, backpressure when staging fills — is a library decision this program
// passes along.
//
// It exists so that a program which is not Go, or a person who would rather not
// write any, can use all of that: a producer that can write a line to a pipe
// needs no compiler and no dependency on this project.
//
// Run "icelake usage" for the manual.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// errUsage marks a refusal decided on the configuration alone, before a file was
// opened or a request was made.
//
// It is a sentinel because it decides the exit code, and the exit code is the
// one part of this program a supervisor reads: a configuration that will never
// work must not look like a bucket that is briefly unreachable. Every refusal
// this command makes about its own environment wraps it.
var errUsage = errors.New("invalid configuration")

// The exit codes, which are part of this command's contract with whatever
// started it.
const (
	// exitOK is a clean end of input, or a signal that drained in time.
	exitOK = 0
	// exitRuntime is a failure that a restart might survive: a bucket that was
	// unreachable, a disk that filled, a line that could not be written.
	exitRuntime = 1
	// exitUsage is a refusal about the configuration or the arguments. A
	// supervisor that restarts on it restarts forever, which is why it is
	// distinct: this is the code that means restarting will not help.
	//
	// It deviates from the flat exit-1 of this repo's two other commands, and
	// deliberately: those are operator tools run by hand, where a human reads the
	// message. This one is a daemon under a process manager, where nobody reads
	// anything until something has been restarting for an hour.
	exitUsage = 2
)

func main() {
	// A signal ends the run by cancelling the context, which stops the daemon
	// reading and starts its drain. It is not a kill: records already accepted
	// are already durable, and the drain is what gets the batch in memory into
	// the table rather than leaving it for the next start to replay.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Once a signal has cancelled the run the signals go back to the runtime, so
	// a second one ends the process. The drain has its own budget and an
	// operator who has decided not to wait for it must be able to say so; a
	// handler still registered would swallow the second signal and make a
	// working drain look like a hang.
	go func() {
		<-ctx.Done()
		stop()
	}()

	err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	switch {
	case err == nil:
		os.Exit(exitOK)
	case errors.Is(err, errUsage):
		report(err)
		os.Exit(exitUsage)
	default:
		report(err)
		os.Exit(exitRuntime)
	}
}

// report logs the error that is about to become the exit code.
//
// It is a log line like every other — the run is reporting on itself, and the
// logging rule in `AGENTS.md` has no exceptions — so it is one logfmt record,
// with the message saying which kind of ending this is and the error riding
// in its key. The library's errors introduce themselves with "icelake:", and
// a wrapped one carries that prefix at every layer it passed through; the
// line's own shape already says whose message this is, so every copy comes
// out rather than only the first.
func report(err error) {
	detail := strings.ReplaceAll(err.Error(), "icelake: ", "")
	log := newLogger(os.Stderr, slog.LevelInfo)
	if errors.Is(err, errUsage) {
		log.Error("configuration refused; nothing that was not already running was started", "err", detail)

		return
	}
	log.Error("the run ended in failure", "err", detail)
}

// run dispatches a subcommand.
//
// Every stream it touches is a parameter so that a test can drive the whole
// program without a process, which is the same shape the other two commands in
// this repo use. Dispatch is written out by hand rather than with a flag package
// because there is nothing to parse: the daemon takes no flags at all, and the
// one subcommand that does defines its own.
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, shortUsage())

		return fmt.Errorf("%w: no subcommand", errUsage)
	}

	switch args[0] {
	case "run":
		// Deliberately one flag, and it carries no value. Configuration stays
		// in the environment — one channel means the resolved configuration
		// has one source to print and one table to generate the help from,
		// and it means a credential can never become a flag's default value,
		// which is how the flag package comes to print a secret back in the
		// block it produces for -h and for every parse error. What -f selects
		// is not configuration: it is how the process runs, which is the one
		// category of decision `AGENTS.md` places in the command itself.
		foreground := false
		for _, arg := range args[1:] {
			switch arg {
			case "-f", "--foreground":
				foreground = true
			default:
				fmt.Fprint(stderr, shortUsage())

				return fmt.Errorf("%w: run takes no argument but -f or --foreground (%s); it is configured by environment variables", errUsage, strings.Join(args[1:], " "))
			}
		}

		return launch(ctx, foreground, stdin, stderr)

	case "check":
		return runCheck(ctx, args[1:], stdout, stderr)

	case "rebuild":
		return runRebuild(ctx, args[1:], stdout, stderr)

	case "usage":
		fmt.Fprint(stdout, manual())

		return nil

	case "version", "-v", "--version":
		fmt.Fprintln(stdout, versionString())

		return nil

	case "help", "-h", "--help":
		fmt.Fprint(stdout, help())

		return nil

	default:
		fmt.Fprint(stderr, shortUsage())

		return fmt.Errorf("%w: unknown subcommand %q", errUsage, args[0])
	}
}
