# Releasing icelake

This is the operating procedure for cutting a release. The decisions behind it
live in `PLAN.md` ("Distribution"); this file only sequences them, the same
relationship `PLAN.md` has to the design documents. If the two disagree,
`PLAN.md` wins and this file has a bug.

One release train. A release is a `vX.Y.Z` tag on the root module: the same tag
publishes the Go module (any public repo is a module host) and triggers the
workflow that builds the `icelake` binaries and attaches them to a draft GitHub
release. The CLI's version is the library's version, always.

**A tag is irreversible.** The moment any version is fetched through the Go
module proxy it is cached there permanently — deleting the tag, or the whole
repo, does not unpublish it. Everything below exists to make sure the thing
being made permanent is right.

## Every release

From a clean tree on `main`, with everything committed:

    GOFLAGS=-count=1 mise run check     # every CI gate, unpiped
    mise run release-check              # full snapshot release, publish skipped

Both must pass. `release-check` is the one that exercises the archive shape
nothing else touches between tags.

Then the tag, and the push that makes it real:

    git tag vX.Y.Z
    git push origin main
    git push origin vX.Y.Z

The tag push triggers `.github/workflows/release.yml`, which runs the same
pinned goreleaser through mise and stops at a **draft** release with the four
archives and `checksums.txt` attached. A pre-release tag (`v1.0.0-rc1`) is
marked as a pre-release on the release page automatically.

A human reviews the draft — the asset list, the checksums, the notes they write
themselves — and presses publish. That last click is deliberately manual: the
irreversibility above is a property of the tag, but the release page is the
front door, and the last step before something becomes visible should be
somebody's decision.

After publishing, verify the install path does what the README promises:

    mise use -g github:gvkhna/icelake
    icelake version

## What gates what

- **Before any tag at all:** the scan-and-sanitize pass over every file, and
  fresh git history (the public repo starts from a curated sequence of logical
  commits rebuilt from the final tree; the private ancestor is never pushed).
  Both are one-time acts, done before `v1.0.0-rc1`, and recorded in `PLAN.md`.
- **Before `v1.0.0` final:** the S3-only file IO replacing `io/gocloud`, so the
  first non-rc version's dependency graph — which the proxy caches forever — is
  the slim one. An rc's heavier graph is cosmetic: Go never selects a
  pre-release unless asked for it by exact name.
- **Version discipline** is `PLAN.md`'s ("Distribution", tagging bullet): the
  rc series is where the 1.0 shape gets proven in the field; `v1.0.0` when it
  has been.

## The repository side, once

The remote is `github.com/gvkhna/icelake`. Before the first tag the repo is
flipped public — a release on a private repo is not a release, and the mise
install above only works tokenless against a public one. The flip happens after
the sanitize pass and the history rewrite, immediately before `v1.0.0-rc1`.
