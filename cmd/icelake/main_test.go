package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// secretForHelp is a value that appears nowhere else, so finding it in any
// output can only mean that output printed the credential it was given.
const secretForHelp = "s3cr3t-value-that-must-never-be-printed"

// fillsDocument is a minimal schema document: one table, one of each of the
// three column kinds a line in this test file writes.
const fillsDocument = `{"tables": [
  {"namespace": "market", "table": "fills", "fields": [
    {"name": "symbol", "type": "string",        "fieldid": 1},
    {"name": "price",  "type": "decimal(18,9)", "fieldid": 2},
    {"name": "ts_ms",  "type": "timestamptz",   "fieldid": 3},
    {"name": "note",   "type": "string",        "fieldid": 4, "optional": true}
  ]}
]}`

// builtBinary builds the command once per test run and returns its path.
//
// Once, because every test that needs it needs the same one and `go build` is
// the slowest thing in this file by an order of magnitude. The path carries
// the process id so two test processes sharing a temp directory — concurrent
// go test invocations, a shared CI runner — can never build over each other's
// binary mid-run.
var builtBinary = sync.OnceValues(func() (string, error) {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("icelake-under-test-%d", os.Getpid()))
	if out, err := exec.Command("go", "build", "-o", path, ".").CombinedOutput(); err != nil {
		return "", errors.New(string(out))
	}

	return path, nil
})

// binary returns the built command, failing the test if it will not build.
func binary(tb testing.TB) string {
	tb.Helper()

	path, err := builtBinary()
	if err != nil {
		tb.Fatalf("building the command: %v", err)
	}

	return path
}

// writeDocument puts a schema document in the test's own directory and returns
// its path.
func writeDocument(tb testing.TB, dir, doc string) string {
	tb.Helper()

	path := filepath.Join(dir, "schema.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		tb.Fatalf("writing the schema document: %v", err)
	}

	return path
}

// localOnlyEnv sets a complete, valid local-only configuration for the calling
// test, and returns the data directory it points at.
func localOnlyEnv(tb testing.TB) string {
	tb.Helper()

	dir := tb.TempDir()
	for _, kv := range localOnlyEnvPairs(tb, dir) {
		name, value, _ := strings.Cut(kv, "=")
		tb.Setenv(name, value)
	}

	return dir
}

// localOnlyEnvPairs is [localOnlyEnv] as name=value strings, for the tests that
// run the built binary in a child process rather than calling run directly.
func localOnlyEnvPairs(tb testing.TB, dir string) []string {
	tb.Helper()

	return []string{
		envDataDir.name + "=" + dir,
		envSchemaFile.name + "=" + writeDocument(tb, dir, fillsDocument),
		envLocalOnly.name + "=true",
	}
}

// TestBinaryBuildsAndDocumentsItsConfiguration is scenario 13's first hermetic
// clause: the command builds, its manual is reachable, and its documentation
// cannot drift from its configuration.
//
// The variable names are iterated from the command's own table rather than
// retyped here, which is the whole point: a variable added to the configuration
// without being documented fails this test the moment it is added, and no
// reviewer has to notice.
func TestBinaryBuildsAndDocumentsItsConfiguration(t *testing.T) {
	icelake := binary(t)

	for _, subcommand := range []string{"usage", "help"} {
		t.Run(subcommand, func(t *testing.T) {
			cmd := exec.Command(icelake, subcommand)
			cmd.Env = append(os.Environ(),
				envAccessKeyID.name+"=an-access-key-id",
				envSecretAccessKey.name+"="+secretForHelp,
			)

			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("running %s: %v\n%s", subcommand, err, out)
			}

			for _, v := range envVars {
				if !bytes.Contains(out, []byte(v.name)) {
					t.Errorf("%s does not mention %s", subcommand, v.name)
				}
			}

			// Nothing printed for a human ever carries a secret. This is an
			// assertion about output bytes rather than about an error, and it is
			// here because the mistake it guards is quiet: a credential bound as
			// a flag's default value is printed back in the block the flag
			// package produces for -h and for every parse error. This command
			// takes no flags for exactly that reason, and this is what checks it.
			if bytes.Contains(out, []byte(secretForHelp)) {
				t.Errorf("%s printed the secret access key it was given in the environment", subcommand)
			}
		})
	}
}

// TestStartupLineReportsTheConfigurationWithoutTheSecret covers the other place
// this command prints for a human: the resolved-configuration line the daemon
// writes at startup.
//
// It is the whole of this command's observability and the answer to "what is
// this daemon actually configured with", so it has to carry enough to be worth
// printing — and never the credential. The secret is reported as set or unset
// rather than truncated, because a redaction that shows a prefix leaks a prefix.
func TestStartupLineReportsTheConfigurationWithoutTheSecret(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envDataDir.name, dir)
	t.Setenv(envSchemaFile.name, writeDocument(t, dir, fillsDocument))
	t.Setenv(envEndpoint.name, "http://127.0.0.1:9")
	t.Setenv(envBucket.name, "a-bucket")
	t.Setenv(envPrefix.name, "warehouse")
	t.Setenv(envAccessKeyID.name, "an-access-key-id")
	t.Setenv(envSecretAccessKey.name, secretForHelp)

	cfg, err := readSettings(forRun)
	if err != nil {
		t.Fatalf("readSettings: %v", err)
	}

	line := cfg.describe()
	if strings.Contains(line, secretForHelp) {
		t.Error("the startup line printed the secret access key")
	}
	if !strings.Contains(line, "secret-access-key=set") {
		t.Errorf("the startup line does not report the secret as set: %s", line)
	}
	for _, want := range []string{"a-bucket", "warehouse", "an-access-key-id", filepath.Join(dir, "staging.db")} {
		if !strings.Contains(line, want) {
			t.Errorf("the startup line does not mention %q: %s", want, line)
		}
	}
}

// TestConfigurationRefusalMatrix is scenario 13's refusal matrix.
//
// Every case is matched against this command's own sentinel rather than merely
// being non-nil, for the reason the R2 check command's argument test gives: a
// network failure satisfies a non-nil check while proving the opposite of what
// the test is for. A configuration refusal must be decided on the configuration
// alone, before anything is opened.
func TestConfigurationRefusalMatrix(t *testing.T) {
	cases := []struct {
		name string
		env  func(tb testing.TB, dir string)
		// mentions are the variables the refusal has to name. A refusal that
		// reports a different problem than the one the case made would pass a
		// bare sentinel check.
		mentions []string
	}{
		{"a missing data directory", func(tb testing.TB, dir string) {
			tb.Setenv(envSchemaFile.name, writeDocument(tb, dir, fillsDocument))
			tb.Setenv(envLocalOnly.name, "true")
		}, []string{envDataDir.name}},

		{"a missing schema file", func(tb testing.TB, dir string) {
			tb.Setenv(envDataDir.name, dir)
			tb.Setenv(envLocalOnly.name, "true")
		}, []string{envSchemaFile.name}},

		{"local-only alongside a bucket", func(tb testing.TB, dir string) {
			localOnlyEnv(tb)
			tb.Setenv(envEndpoint.name, "http://127.0.0.1:9")
			tb.Setenv(envBucket.name, "a-bucket")
		}, []string{envEndpoint.name, envBucket.name}},

		{"local-only alongside a catalog path", func(tb testing.TB, dir string) {
			localOnlyEnv(tb)
			tb.Setenv(envCatalogPath.name, filepath.Join(dir, "catalog.db"))
		}, []string{envCatalogPath.name}},

		{"local-only alongside a retention bound", func(tb testing.TB, dir string) {
			localOnlyEnv(tb)
			tb.Setenv(envCacheMaxBytes.name, "1GB")
		}, []string{envCacheMaxBytes.name}},

		{"neither local-only nor a bucket", func(tb testing.TB, dir string) {
			tb.Setenv(envDataDir.name, dir)
			tb.Setenv(envSchemaFile.name, writeDocument(tb, dir, fillsDocument))
		}, []string{envEndpoint.name, envBucket.name, envPrefix.name, envAccessKeyID.name, envSecretAccessKey.name}},

		{"a partial bucket configuration", func(tb testing.TB, dir string) {
			tb.Setenv(envDataDir.name, dir)
			tb.Setenv(envSchemaFile.name, writeDocument(tb, dir, fillsDocument))
			tb.Setenv(envEndpoint.name, "http://127.0.0.1:9")
			tb.Setenv(envBucket.name, "a-bucket")
		}, []string{envPrefix.name, envAccessKeyID.name, envSecretAccessKey.name}},

		{"a malformed boolean", func(tb testing.TB, dir string) {
			localOnlyEnv(tb)
			tb.Setenv(envLocalOnly.name, "yes-please")
		}, []string{envLocalOnly.name}},

		{"a malformed duration", func(tb testing.TB, dir string) {
			localOnlyEnv(tb)
			tb.Setenv(envFlushInterval.name, "15 minutes")
		}, []string{envFlushInterval.name}},

		{"a malformed byte size", func(tb testing.TB, dir string) {
			localOnlyEnv(tb)
			tb.Setenv(envFlushMaxBytes.name, "1.5GB")
		}, []string{envFlushMaxBytes.name}},

		{"a zstd level below the range", func(tb testing.TB, dir string) {
			localOnlyEnv(tb)
			tb.Setenv(envZSTDLevel.name, "0")
		}, []string{envZSTDLevel.name}},

		{"a zstd level above the range", func(tb testing.TB, dir string) {
			localOnlyEnv(tb)
			tb.Setenv(envZSTDLevel.name, "23")
		}, []string{envZSTDLevel.name}},

		// The case the joined refusal exists for: everything wrong at once, and
		// every one of them reported in a single run. An operator fixing a unit
		// file should learn all of it now rather than one problem per restart.
		{"several violations at once", func(tb testing.TB, dir string) {
			tb.Setenv(envSchemaFile.name, "")
			tb.Setenv(envDataDir.name, "")
			tb.Setenv(envZSTDLevel.name, "99")
			tb.Setenv(envFlushInterval.name, "soon")
			tb.Setenv(envMaxLineBytes.name, "lots")
		}, []string{
			envDataDir.name, envSchemaFile.name, envZSTDLevel.name,
			envFlushInterval.name, envMaxLineBytes.name,
			envEndpoint.name, envBucket.name,
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.env(t, t.TempDir())

			_, err := readSettings(forRun)
			if !errors.Is(err, errUsage) {
				t.Fatalf("readSettings returned %v, want a refusal wrapping errUsage", err)
			}
			for _, name := range c.mentions {
				if !strings.Contains(err.Error(), name) {
					t.Errorf("the refusal does not mention %s:\n%v", name, err)
				}
			}
		})
	}
}

// TestByteSizeParserIsExact is scenario 13's byte-size clause.
//
// The expected values are written as the arithmetic that produces them rather
// than as literals, so the test states the rule — powers of 1024 — instead of
// restating whatever the code happens to compute.
func TestByteSizeParserIsExact(t *testing.T) {
	accepted := []struct {
		text string
		want int64
	}{
		{"0", 0},
		{"512", 512},
		{"1B", 1},
		{"1KB", 1024},
		{"64KB", 64 * 1024},
		{"128MB", 128 * 1024 * 1024},
		{"4GB", 4 * 1024 * 1024 * 1024},
		{"1TB", 1024 * 1024 * 1024 * 1024},
		{"10GB", 10 * 1024 * 1024 * 1024},
		{"16MB", 16 * 1024 * 1024},
		// The suffix is read case-insensitively, which is kindness to a human
		// editing a unit file and costs nothing: there is still exactly one
		// value each spelling can mean.
		{"128mb", 128 * 1024 * 1024},
	}

	for _, c := range accepted {
		t.Run(c.text, func(t *testing.T) {
			t.Setenv(envFlushMaxBytes.name, c.text)

			got, err := parseByteSize(envFlushMaxBytes)
			if err != nil {
				t.Fatalf("parseByteSize(%q): %v", c.text, err)
			}
			if got != c.want {
				t.Errorf("parseByteSize(%q) = %d, want %d", c.text, got, c.want)
			}
		})
	}

	refused := []string{
		"1.5GB", // no fractions: an exact number of bytes or nothing
		"1,024", // no thousands separators
		"MB",    // a unit with no number
		"-1",    // no negative sizes
		"-1GB",  //
		"1 GB",  // no internal spaces
		"1GiB",  // one spelling per unit
		"1PB",   // no unit this program does not define
		"nonsense",
	}

	for _, text := range refused {
		t.Run("refuses "+text, func(t *testing.T) {
			t.Setenv(envFlushMaxBytes.name, text)

			if got, err := parseByteSize(envFlushMaxBytes); err == nil {
				t.Errorf("parseByteSize(%q) = %d, want a refusal", text, got)
			}
		})
	}

	// A variable set to nothing is a variable nobody meant to set, so it takes
	// the default rather than being refused as an empty size. That is what makes
	// "unset it to go back to the default" true of every variable here, and it
	// is asserted rather than assumed because the alternative reading — an empty
	// value is a value — would turn a stray line in an env file into a refusal
	// nobody could explain.
	t.Run("an empty value takes the default", func(t *testing.T) {
		t.Setenv(envFlushMaxBytes.name, "")

		got, err := parseByteSize(envFlushMaxBytes)
		if err != nil {
			t.Fatalf("parseByteSize with the variable set to nothing: %v", err)
		}
		if want := int64(128 * 1024 * 1024); got != want {
			t.Errorf("parseByteSize with the variable set to nothing = %d, want the default %d", got, want)
		}
	})
}

// TestRebuildTellsAHelpRequestFromAParseError pins the one branch this
// command's argument handling decides, the same seam scenario 3 names for the
// standalone rebuild CLI: the flag package reports -h as an error like any
// other, and the whole difference between "answered a question" and "was given
// something it does not understand" is that one comparison.
func TestRebuildTellsAHelpRequestFromAParseError(t *testing.T) {
	var out, errOut bytes.Buffer

	if err := runRebuild(t.Context(), []string{"-h"}, &out, &errOut); err != nil {
		t.Errorf("asking for help returned %v, want nil: a question answered is not a failure", err)
	}
	if errOut.Len() == 0 {
		t.Error("asking for help printed nothing")
	}

	err := runRebuild(t.Context(), []string{"-replcae"}, &out, &errOut)
	if err == nil {
		t.Fatal("a flag this command does not define was accepted, want a refusal")
	}
	if errors.Is(err, flag.ErrHelp) {
		t.Error("a mistyped flag was reported as a request for help, which exits zero")
	}
	if !errors.Is(err, errUsage) {
		t.Errorf("a mistyped flag returned %v, want a refusal wrapping errUsage", err)
	}
}

// TestRebuildRefusesInLocalOnlyMode is the one refusal this command owns rather
// than passes along.
//
// A local-only store opens no catalog, so there is nothing to rebuild — and the
// command decides that from its own environment rather than calling a library
// that would have to invent an error path for a question it is never asked.
func TestRebuildRefusesInLocalOnlyMode(t *testing.T) {
	localOnlyEnv(t)

	var out, errOut bytes.Buffer
	err := runRebuild(t.Context(), nil, &out, &errOut)
	if !errors.Is(err, errUsage) {
		t.Fatalf("rebuild in local-only mode returned %v, want a refusal wrapping errUsage", err)
	}
	if !strings.Contains(err.Error(), envLocalOnly.name) {
		t.Errorf("the refusal does not name %s: %v", envLocalOnly.name, err)
	}
	if out.Len() != 0 {
		t.Errorf("a refused rebuild wrote a report: %s", out.String())
	}
}

// TestRebuildDoesNotRequireASchemaFile records a deliberate difference between
// the two subcommands' configurations.
//
// A rebuild opens no schema document — the bucket is the only thing it is told
// about — so requiring the file would be requiring one the operation never
// reads, at the exact moment an operator is recovering a host and least wants to
// be assembling configuration. The daemon still requires it, which the refusal
// matrix above checks.
func TestRebuildDoesNotRequireASchemaFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envDataDir.name, dir)
	t.Setenv(envEndpoint.name, "http://127.0.0.1:9")
	t.Setenv(envBucket.name, "a-bucket")
	t.Setenv(envPrefix.name, "warehouse")
	t.Setenv(envAccessKeyID.name, "an-access-key-id")
	t.Setenv(envSecretAccessKey.name, "a-secret")

	if _, err := readSettings(forRebuild); err != nil {
		t.Errorf("a rebuild configuration without a schema file was refused: %v", err)
	}
	if _, err := readSettings(forRun); !errors.Is(err, errUsage) {
		t.Errorf("the daemon accepted a configuration with no schema file: %v", err)
	}
}

// TestUnknownSubcommandsAreRefused covers the dispatch itself: a subcommand this
// command does not have, and the daemon being handed arguments it does not take.
//
// The second is worth a case of its own because "run takes no flags" is a
// decision rather than an omission: one configuration channel is what keeps a
// credential out of a usage block, so a flag quietly ignored would be the
// beginning of a second channel.
func TestUnknownSubcommandsAreRefused(t *testing.T) {
	var out, errOut bytes.Buffer

	for _, args := range [][]string{nil, {"runn"}, {"run", "-v"}} {
		err := run(t.Context(), args, strings.NewReader(""), &out, &errOut)
		if !errors.Is(err, errUsage) {
			t.Errorf("run(%q) returned %v, want a refusal wrapping errUsage", args, err)
		}
	}
	if errOut.Len() == 0 {
		t.Error("a refused invocation printed no usage")
	}
}

// TestVersionAndUsageAnswerWithoutConfiguration checks that the two informational
// subcommands work with nothing configured at all, which is when somebody
// actually runs them.
func TestVersionAndUsageAnswerWithoutConfiguration(t *testing.T) {
	var out, errOut bytes.Buffer

	if err := run(t.Context(), []string{"version"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatalf("version: %v", err)
	}
	if strings.TrimSpace(out.String()) == "" {
		t.Error("version printed nothing")
	}

	out.Reset()
	if err := run(t.Context(), []string{"usage"}, strings.NewReader(""), &out, &errOut); err != nil {
		t.Fatalf("usage: %v", err)
	}
	if !strings.Contains(out.String(), "icelake run") {
		t.Error("the manual does not describe the run subcommand")
	}
}
