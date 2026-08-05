package main

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// secretForHelp is a value that appears nowhere else, so finding it in the usage
// output can only mean the usage output printed the credential it was given.
const secretForHelp = "s3cr3t-value-that-must-never-be-printed"

// TestBinaryBuildsAndPrintsHelp covers what this command has of its own,
// deliberately little.
//
// The command is a flag parser with no logic of its own — everything it could
// get wrong lives in the rebuild package and is tested there against a real
// bucket. What is left to prove is that the wrapper is a real, buildable program
// whose flags parse and whose usage text is reachable, which is what an operator
// meets first, that asking for that usage text does not print their credentials
// back at them, and that a mistyped flag ends the way a failure ends.
//
// The second half is an assertion about output bytes rather than about an error
// message, and it is here because the mistake it guards is easy and quiet:
// binding os.Getenv as a flag's *default value* puts the secret into the block
// the flag package prints for -h and for every parse error, so a mistyped flag
// would echo a secret key to the terminal and into whatever captured it. The
// only honest way to check that is to look at the bytes.
func TestBinaryBuildsAndPrintsHelp(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "icelake-rebuild")

	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("building the command: %v\n%s", err, out)
	}

	help := exec.Command(binary, "--help")
	help.Env = append(os.Environ(),
		accessKeyEnv+"=an-access-key-id",
		secretKeyEnv+"="+secretForHelp,
	)

	// Asking for help is not a failure, so the exit status must be zero.
	out, err := help.CombinedOutput()
	if err != nil {
		t.Fatalf("running --help: %v\n%s", err, out)
	}
	if bytes.Contains(out, []byte(secretForHelp)) {
		t.Error("--help printed the secret access key it was given in the environment")
	}

	// The other way that usage block gets printed, and the one the comment
	// above is actually about: the flag package prints it for every parse
	// error, not only for -h. So the same binary is run again with a flag it
	// does not define, which is what a typo looks like, and the same two things
	// have to hold — except that this time it is a failure, so the exit status
	// must say so. A command that answered zero here would tell a script that
	// the rebuild it never performed had succeeded.
	mistyped := exec.Command(binary, "-endpiont", "http://127.0.0.1:9")
	mistyped.Env = append(os.Environ(),
		accessKeyEnv+"=an-access-key-id",
		secretKeyEnv+"="+secretForHelp,
	)

	out, err = mistyped.CombinedOutput()
	if err == nil {
		t.Fatalf("a mistyped flag exited zero:\n%s", out)
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("running a mistyped flag: %v\n%s", err, out)
	}
	if exit.ExitCode() != 1 {
		t.Errorf("a mistyped flag exited %d, want 1", exit.ExitCode())
	}
	if bytes.Contains(out, []byte(secretForHelp)) {
		t.Error("a mistyped flag printed the secret access key it was given in the environment")
	}
}

// TestRunTellsAHelpRequestFromAParseError pins the one branch this command's
// argument handling actually decides: the flag package reports -h as an error
// like any other, and the whole of the difference between "answered a question"
// and "was given something it does not understand" is that one comparison.
//
// It matters because the two endings are opposite. A help request exits zero
// with nothing attempted; a parse error must not, or a script would take a run
// that never happened for a rebuild that worked. The exit status itself is
// asserted against the built binary above; this is the seam that decides it,
// and it is asserted here because run is where the decision is and a main has
// no public API to reach it through.
func TestRunTellsAHelpRequestFromAParseError(t *testing.T) {
	if err := run(t.Context(), []string{"-h"}); err != nil {
		t.Errorf("asking for help returned %v, want nil: a question answered is not a failure", err)
	}

	// A flag this command does not define. Nothing is attempted afterwards —
	// the options are never completed from the environment and the library is
	// never called — which is what makes it safe for this test to name an
	// endpoint at all.
	err := run(t.Context(), []string{"-endpiont", "http://127.0.0.1:9"})
	if err == nil {
		t.Fatal("a flag this command does not define was accepted, want a refusal")
	}
	if errors.Is(err, flag.ErrHelp) {
		t.Error("a mistyped flag was reported as a request for help, which exits zero")
	}
}
