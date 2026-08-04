// Package testsubstrate starts the real object storage icelake's tests run
// against.
//
// TESTING.md's philosophy is binding and it rules out mocking the storage
// substrate: no mocked S3 client, no fake catalog, no stubbed Iceberg layer,
// because a test against a mock proves the mock. This package is what makes
// that affordable — a MinIO container started and stopped by the test binary
// itself, standing in for Cloudflare R2, so the whole suite runs with no cloud
// account, no credentials and no bucket anyone had to create.
//
// The image is Chainguard's MinIO, pinned by digest ([MinIOImage]), started
// through testcontainers-go's own MinIO module. MinIO speaks the same S3 API R2
// does, so everything icelake itself does — upload, listing, catalog rebuild
// discovery — is exercised here exactly as it would be against R2. The two
// behaviours that are genuinely R2-specific, and that MinIO passing proves
// nothing about, are deliberately out of scope for the automated suite and are
// covered by the manual procedure TESTING.md describes instead.
//
// A test that needs the substrate calls [RequireDocker] and then [StartMinIO]:
//
//	testsubstrate.RequireDocker(t)
//	bucket := testsubstrate.StartMinIO(t)
//
// [StartMinIO] returns a [Bucket] — an endpoint, a region, a bucket name and a
// pair of static credentials — which is the same shape a caller puts into
// icelake's Config, so a test wires the substrate up exactly as a production
// caller wires up R2. Cleanup is registered on the test, so nothing is left
// running.
//
// This package imports testing and is only ever imported by tests.
package testsubstrate
