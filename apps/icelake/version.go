package main

import "runtime/debug"

// version is stamped in at build time with -ldflags "-X main.version=vX.Y.Z" by
// the release build. It is deliberately not a constant: a binary built from a
// clone has no version to claim, and saying so is better than claiming one.
var version = ""

// versionString reports the version this binary was built as.
//
// The stamped value wins. Otherwise the answer comes from the build information
// Go records inside the binary, which is what an unstamped build actually has to
// offer: "(devel)" for a `go build` from a working tree, and the module version
// for one the module system resolved. There is no third answer to invent — a
// program with no version does not become more informative by making one up —
// so the only case left is a binary carrying no build information at all, which
// is what a linker stripping it produces, and that says so.
func versionString() string {
	if version != "" {
		return version
	}

	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}

	return info.Main.Version
}
