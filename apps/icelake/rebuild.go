package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/gvkhna/icelake/rebuild"
)

// runRebuild is "icelake rebuild": the catalog database rebuilt from the bucket
// alone, using the same environment the daemon runs on.
//
// It exists here rather than only as the separate icelake-rebuild binary for one
// reason, and it is an operational one: the moment a catalog is lost is the
// moment an operator is under pressure, and "the daemon you already have,
// configured the way it already is" is a shorter path than finding a second
// binary and retyping an endpoint, a bucket, a prefix and a credential into
// flags. Disaster recovery with zero new configuration.
//
// The two flags are the two decisions the environment cannot express, because
// they are about this run rather than about this deployment: whether to replace
// a catalog that is already there, and whether to write anything at all.
func runRebuild(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("icelake rebuild", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		out := fs.Output()
		fmt.Fprintln(out, "icelake rebuild reconstructs the local catalog database from the warehouse bucket.")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "It discovers namespaces and tables by listing the bucket — it is never told which")
		fmt.Fprintln(out, "tables should exist — and writes one catalog row per table it finds. Nothing in")
		fmt.Fprintln(out, "the bucket is modified. Run it with the daemon stopped.")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "Usage:")
		fmt.Fprintln(out, "  icelake rebuild [-replace] [-dry-run]")
		fmt.Fprintln(out, "")
		fmt.Fprintln(out, "Flags:")
		fs.PrintDefaults()
		fmt.Fprintln(out, "")
		fmt.Fprintf(out, "Everything else comes from the same environment %s reads: the endpoint, the\n", "icelake run")
		fmt.Fprintln(out, "bucket, the prefix, the region, the credentials and the catalog path.")
	}

	var replace, dryRun bool
	fs.BoolVar(&replace, "replace", false, "replace an existing catalog database instead of refusing")
	fs.BoolVar(&dryRun, "dry-run", false, "discover and report without writing anything")

	if err := fs.Parse(args); err != nil {
		// -h and --help are a request for the usage text, which the flag package
		// has already printed. Answering a question is not a failure, and a
		// command that exited non-zero here would tell a script that a rebuild it
		// never attempted had failed.
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}

		return fmt.Errorf("%w: %w", errUsage, err)
	}

	cfg, err := readSettings(forRebuild)
	if err != nil {
		return err
	}

	// The one refusal this command owns rather than passes along. A local-only
	// store has no catalog — it opens none, by design — so there is nothing here
	// to rebuild, and the honest answer comes from this program's own
	// configuration rather than from a library that would have to invent an
	// error path for a question it is never asked.
	if cfg.localOnly {
		return fmt.Errorf("%w: there is no catalog to rebuild when %s is true; a local-only store opens none",
			errUsage, envLocalOnly.name)
	}

	report, err := rebuild.Rebuild(ctx, rebuild.RebuildOptions{
		Endpoint:        cfg.endpoint,
		Bucket:          cfg.bucket,
		WarehousePrefix: cfg.prefix,
		Region:          cfg.region,
		AccessKeyID:     cfg.accessKeyID,
		SecretAccessKey: cfg.secretAccessKey,
		CatalogPath:     cfg.catalogPath,
		Replace:         replace,
		DryRun:          dryRun,
	})
	if err != nil {
		return err
	}
	fmt.Fprint(stdout, report)

	return nil
}
