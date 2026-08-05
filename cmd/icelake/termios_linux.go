//go:build linux

package main

import "golang.org/x/sys/unix"

// termiosRequest is the ioctl that reads a terminal's attributes, which is the
// portable spelling of "is this a terminal". Linux calls it TCGETS.
const termiosRequest = unix.TCGETS
