package main

import (
	_ "embed"
	"fmt"
	"strings"
)

// usageDoc is the manual "icelake usage" prints.
//
// It is embedded rather than fetched or generated, so a binary an operator
// installed from a release carries its own documentation and needs nothing else
// to explain itself. The environment table inside it is checked against the
// table in env.go by this command's own tests, which is what stops the two
// drifting apart.
//
//go:embed usage.md
var usageDoc string

// manual returns the embedded manual.
func manual() string { return usageDoc }

// shortUsage is what a wrong invocation gets: the subcommands and where the
// rest is, on stderr, in a few lines somebody will actually read.
func shortUsage() string {
	var b strings.Builder

	fmt.Fprintln(&b, "icelake writes records from stdin into Apache Iceberg tables.")
	fmt.Fprintln(&b, "")
	fmt.Fprintln(&b, "Usage:")
	fmt.Fprintln(&b, "  icelake run [-f]     read records on stdin and write them (configured by environment;")
	fmt.Fprintln(&b, "                       backgrounds itself by default, -f/--foreground stays attached)")
	fmt.Fprintln(&b, "  icelake check        validate the setup and report the current state, writing nothing")
	fmt.Fprintln(&b, "  icelake duckdb-init [namespace.table ...]  print DuckDB setup SQL, writing nothing")
	fmt.Fprintln(&b, "  icelake rebuild      rebuild the local catalog database from the bucket")
	fmt.Fprintln(&b, "  icelake usage        print the full manual")
	fmt.Fprintln(&b, "  icelake version      print the version (also -v, --version)")
	fmt.Fprintln(&b, "  icelake help         print the environment variables and their defaults (also -h, --help)")
	fmt.Fprintln(&b, "")
	fmt.Fprintln(&b, "Exit codes: 0 clean, 1 runtime failure, 2 configuration or usage refusal.")

	return b.String()
}

// help is the long help: the subcommands, and every environment variable with
// its default and its meaning.
//
// The variable list is generated from the one table in env.go rather than
// retyped, which is the whole reason that table exists. A variable added to the
// configuration appears here the moment it is added, and a test requires it to
// appear in the manual too.
func help() string {
	var b strings.Builder

	b.WriteString(shortUsage())
	fmt.Fprintln(&b, "")
	fmt.Fprintln(&b, "Environment:")

	width := 0
	for _, v := range envVars {
		width = max(width, len(v.name))
	}
	for _, v := range envVars {
		fmt.Fprintf(&b, "  %-*s  %s\n", width, v.name, v.doc)
		fmt.Fprintf(&b, "  %-*s  %s\n", width, "", requirementText(v))
	}

	fmt.Fprintln(&b, "")
	fmt.Fprintln(&b, "Sizes are powers of 1024 with no fractional part: 512, 64KB, 128MB, 4GB.")
	fmt.Fprintln(&b, "Durations are Go durations: 250ms, 15m, 720h.")
	fmt.Fprintln(&b, "No credential is ever printed; the startup line reports the secret as set or unset.")
	fmt.Fprintln(&b, "")
	fmt.Fprintln(&b, "Run 'icelake usage' for the manual: the schema document, the two input")
	fmt.Fprintln(&b, "formats, the cache layout, and the local-only to bucket transition.")

	return b.String()
}

// requirementText renders one variable's default and when it is required, in the
// same words the manual uses.
func requirementText(v envVar) string {
	switch {
	case v.need == always:
		return "(required)"
	case v.need == bucketOnly && v.def == "":
		return "(required unless " + envLocalOnly.name + " is true; must be unset when it is)"
	case v.need == bucketOnly:
		return "(default " + v.def + "; must be unset when " + envLocalOnly.name + " is true)"
	case v.def == "":
		return "(optional)"
	default:
		return "(default " + v.def + ")"
	}
}
