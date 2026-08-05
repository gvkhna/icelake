//go:build darwin

package main

import "golang.org/x/sys/unix"

// termiosRequest is the ioctl that reads a terminal's attributes, which is the
// portable spelling of "is this a terminal". Darwin calls it TIOCGETA.
const termiosRequest = unix.TIOCGETA
