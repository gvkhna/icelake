//go:build linux

package icelake_test

import (
	"os/exec"
	"syscall"
)

// reapWithParent asks the kernel to kill the child if this process dies.
//
// A crash child spends most of its life parked, holding staging.db open and
// waiting to be killed. The parent kills it on cleanup — but a parent that dies
// without running cleanup (a `go test` timeout panic, a race-detector abort, a
// killed CI runner) leaves it parked forever, orphaned, still holding the file.
// Pdeathsig makes that unrepresentable rather than something the test has to
// remember, and it is deliberately not a substitute for the explicit kill: it
// is the backstop for the paths where no test code runs at all.
func reapWithParent(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}
