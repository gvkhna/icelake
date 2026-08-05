// Command icelake-rebuild reconstructs a local Iceberg catalog database from a
// warehouse bucket alone.
//
// It is a thin wrapper with no logic of its own: it parses flags and calls
// rebuild.Rebuild. Run it with the writer stopped.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/gvkhna/icelake/rebuild"
)

// accessKeyEnv and secretKeyEnv are the two environment variables this command
// reads credentials from when the matching flags are not given.
//
// They are deliberately icelake's own names rather than the AWS ones. icelake
// never falls back to the ambient AWS credential chain, and a recovery tool that
// quietly picked up whatever happened to be in the environment would be the same
// mistake with worse consequences — so supplying credentials here stays an
// explicit act, whether it is a flag or one of these.
const (
	accessKeyEnv = "ICELAKE_ACCESS_KEY_ID"
	secretKeyEnv = "ICELAKE_SECRET_ACCESS_KEY"
)

func main() {
	// Ctrl-C and a shutdown signal cancel the run rather than killing it
	// mid-write. The library builds its catalog in a temporary file and renames
	// it into place, so an interrupted rebuild leaves nothing behind either
	// way — but a cancelled context stops the bucket scan promptly instead of
	// waiting out whatever request is in flight.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		// A report on the run is a log line, and every log line in this
		// repository is one logfmt record — the rule in `AGENTS.md` has no
		// exceptions, operator tools included. The library's errors introduce
		// themselves with "icelake:", and a wrapped one carries the prefix at
		// every layer it passed through; the line's shape already says whose
		// message this is, so every copy comes out rather than just the first.
		slog.New(slog.NewTextHandler(os.Stderr, nil)).
			Error("the rebuild failed", "err", strings.ReplaceAll(err.Error(), "icelake: ", ""))
		os.Exit(1)
	}
}

// run translates arguments into rebuild options, calls the library, and prints
// what it reported.
//
// Every decision it might be tempted to make — what counts as a table, what to
// do about a prefix it cannot parse, how to render the result — belongs to the
// library and is made there. An operator recovering a host by hand and a service
// self-healing on startup must get identical behaviour, and they only do if
// there is one implementation to get it from.
func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("icelake-rebuild", flag.ContinueOnError)
	fs.Usage = func() {
		out := fs.Output()
		fmt.Fprintln(out, "icelake-rebuild rebuilds a local Iceberg catalog database from a warehouse bucket.")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "It discovers namespaces and tables by listing the bucket — it is never told")
		fmt.Fprintln(out, "which tables should exist — and writes one catalog row per table it finds.")
		fmt.Fprintln(out, "Nothing in the bucket is modified. Run it with the writer stopped.")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "Usage:")
		fmt.Fprintln(out, "  icelake-rebuild -endpoint URL -bucket NAME -prefix PREFIX -catalog PATH [flags]")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "Flags:")
		fs.PrintDefaults()
		fmt.Fprintln(out, "")
		fmt.Fprintf(out, "Credentials may be given as flags or in %s and %s.\n", accessKeyEnv, secretKeyEnv)
		fmt.Fprintln(out, "Prefer the environment: a secret on a command line is visible in the process list.")
	}

	var opts rebuild.RebuildOptions
	fs.StringVar(&opts.Endpoint, "endpoint", "", "S3-compatible endpoint base URL, including scheme (required)")
	fs.StringVar(&opts.Bucket, "bucket", "", "bucket the warehouse lives in (required)")
	fs.StringVar(&opts.WarehousePrefix, "prefix", "", "warehouse key prefix to discover tables under (required)")
	fs.StringVar(&opts.CatalogPath, "catalog", "", "path of the catalog database to write (required)")
	fs.StringVar(&opts.Region, "region", "", `region to sign requests for (default "auto")`)
	// Both credential flags default to empty and take their environment value
	// only after parsing. Binding os.Getenv as the flag's *default* would put
	// the secret in the usage text, which the flag package prints for -h and for
	// every parse error — so a typo'd flag would echo the operator's secret key
	// to the terminal, and into whatever captured it.
	fs.StringVar(&opts.AccessKeyID, "access-key-id", "", "access key id (default "+accessKeyEnv+")")
	fs.StringVar(&opts.SecretAccessKey, "secret-access-key", "", "secret access key (default "+secretKeyEnv+")")
	fs.BoolVar(&opts.Replace, "replace", false, "replace an existing catalog database instead of refusing")
	fs.BoolVar(&opts.DryRun, "dry-run", false, "discover and report without writing anything")

	if err := fs.Parse(args); err != nil {
		// -h and --help are a request for the usage text, which has already
		// been printed by now, and answering a question is not a failure.
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}

		return err
	}

	if opts.AccessKeyID == "" {
		opts.AccessKeyID = os.Getenv(accessKeyEnv)
	}
	if opts.SecretAccessKey == "" {
		opts.SecretAccessKey = os.Getenv(secretKeyEnv)
	}

	report, err := rebuild.Rebuild(ctx, opts)
	if err != nil {
		return err
	}
	fmt.Print(report)

	return nil
}
