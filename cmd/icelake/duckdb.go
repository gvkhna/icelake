package main

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/gvkhna/icelake"
)

// runDuckDBInit translates the command environment and schema into the one
// library operation that emits a DuckDB setup script. The script is product
// output on stdout; this command never starts DuckDB or changes icelake state.
func runDuckDBInit(ctx context.Context, args []string, stdout io.Writer) error {
	cfg, err := readSettings(forRun)
	if err != nil {
		return err
	}
	tables, err := loadTables(cfg)
	if err != nil {
		return err
	}
	selected, err := selectDuckDBTables(args, tables)
	if err != nil {
		return err
	}

	script, err := icelake.DuckDBInit(ctx, icelake.DuckDBInitOptions{
		CacheDir: cfg.cacheDir, CatalogPath: cfg.catalogPath, LocalOnly: cfg.localOnly,
		Endpoint: cfg.endpoint, Bucket: cfg.bucket, Prefix: cfg.prefix, Region: cfg.region, Tables: selected,
	})
	if err != nil {
		return err
	}
	_, err = io.WriteString(stdout, script)
	return err
}

func selectDuckDBTables(args []string, declared []icelake.DynamicTableConfig) ([]icelake.DuckDBInitTable, error) {
	all := make(map[string]icelake.DuckDBInitTable, len(declared))
	for _, table := range declared {
		ref := table.Namespace + "." + table.Table
		all[ref] = icelake.DuckDBInitTable{Namespace: table.Namespace, Name: table.Table}
	}
	if len(args) == 0 {
		out := make([]icelake.DuckDBInitTable, 0, len(declared))
		for _, table := range declared {
			out = append(out, all[table.Namespace+"."+table.Table])
		}
		return out, nil
	}
	out := make([]icelake.DuckDBInitTable, 0, len(args))
	seen := make(map[string]bool, len(args))
	for _, arg := range args {
		if strings.Count(arg, ".") != 1 {
			return nil, fmt.Errorf("%w: duckdb-init table %q must be namespace.table", errUsage, arg)
		}
		table, ok := all[arg]
		if !ok {
			return nil, fmt.Errorf("%w: duckdb-init table %q is not declared by %s", errUsage, arg, envSchemaFile.name)
		}
		if seen[arg] {
			return nil, fmt.Errorf("%w: duckdb-init table %q was named more than once", errUsage, arg)
		}
		seen[arg] = true
		out = append(out, table)
	}
	return out, nil
}
