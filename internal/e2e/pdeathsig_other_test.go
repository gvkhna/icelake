//go:build !linux

package e2e

import "os/exec"

// reapWithParent does nothing where the kernel offers no equivalent of Linux's
// parent-death signal.
//
// The explicit kill on cleanup is what actually stops a child everywhere; the
// backstop is simply narrower off Linux, which is where this project's CI runs.
func reapWithParent(*exec.Cmd) {}
