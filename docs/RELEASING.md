# Releasing icelake

This is the operating procedure for cutting a release. The decisions behind it
live in `PLAN.md` ("Distribution"); this file only sequences them, the same
relationship `PLAN.md` has to the design documents. If the two disagree,
`PLAN.md` wins and this file has a bug.

One release train. A release is a `vX.Y.Z` tag on the root module: the same tag
publishes the Go module (any public repo is a module host) and triggers the
workflow that builds the `icelake` binaries and publishes the GitHub release.
The CLI's version is the library's version, always.

**A tag is irreversible, and the tag is the decision.** The moment any version
is fetched through the Go module proxy it is cached there permanently —
deleting the tag, or the whole repo, does not unpublish it. Everything below
exists to make sure the thing being made permanent is right *before* the tag
is pushed. (Amended 2026-08-04: releases were originally drafted for a human
to publish by hand. The release page gates nothing the tag has not already
decided — the proxy caches the tag, not the page — so the workflow now
publishes directly, and the human act this file protects is pushing the tag.)

## Every release — including a small fix

From a clean tree on `main`, with everything committed and pushed:

    GOFLAGS=-count=1 mise run check     # every CI gate, unpiped
    mise run release-check              # full snapshot release, publish skipped

Both must pass. `release-check` is the one that exercises the archive shape
nothing else touches between tags. For a fix, that is the whole ceremony:
commit, push, let CI agree, then tag the next version and push it —

    git tag vX.Y.Z
    git push origin vX.Y.Z

The tag push triggers `.github/workflows/release.yml`, which runs the same
pinned goreleaser through mise and **publishes** the release with the four
archives and `checksums.txt` attached. A pre-release tag (`v1.0.0-rc1`) is
marked as a pre-release automatically and can never be "latest"; a stable tag
becomes the latest release, which is what installers resolve when asked for no
version in particular.

**Version arithmetic:** a fix is the next patch (`v1.0.1`), new behaviour in
the library or the command is the next minor (`v1.1.0`), and a breaking
change to the public API after 1.0 means a `/v2` module path, per Go's rule —
so in practice it means don't.

After the workflow finishes, verify the install path does what the README
promises — `mise` resolves the latest stable release:

    mise use -g github:gvkhna/icelake
    icelake version

An existing install picks the new version up with `mise up`. Two things
learned by doing this the first time (2026-08-04, v1.0.0 → v1.0.1): mise
caches a repository's version list briefly, so a just-published release may
need `mise cache clear` before it resolves; and mise verifies GitHub artifact
attestations before installing, which is why the release workflow signs them —
v1.0.0 shipped unattested and mise refused it, which is exactly the failure
this verify step exists to catch.

## What gated the first release (done, kept as the record)

- **Before any tag at all:** the scan-and-sanitize pass over every file, and
  fresh git history (the public repo starts from a curated sequence of logical
  commits rebuilt from the final tree; the private ancestor is never pushed).
  Both were one-time acts, done 2026-08-04 before `v1.0.0-rc1`, and recorded
  in `PLAN.md`. The repo was flipped public the same day — a release on a
  private repo is not a release, and the mise install above only works
  tokenless against a public one.
- **The S3-only file IO replacing `io/gocloud`** was originally the gate on
  `v1.0.0` final. **Amended 2026-08-04, owner's call: `v1.0.0` ships without
  it.** What the swap buys — a smaller dependency graph for library consumers,
  and the seam where storage-call metrics belong — is real and stays on the
  roadmap (`PLAN.md`, "Distribution"), but it changes no behaviour and no API,
  so holding the release for it protected nobody. The cost accepted: the
  1.0-line dependency graph the proxy caches includes the multi-cloud file IO
  until the swap lands in a later minor.
- **Version discipline** is `PLAN.md`'s ("Distribution", tagging bullet) plus
  the arithmetic above.
