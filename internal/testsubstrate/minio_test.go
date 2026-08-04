package testsubstrate_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// TestMinIORoundTripsAnObject verifies the harness, and nothing else.
//
// It is deliberately not a test of any icelake behaviour: it starts the
// container, puts one object and gets it back, and asserts the bytes are the
// bytes. That is the whole claim — that this package hands a test a working,
// reachable, S3-speaking bucket — and it is worth one test precisely because
// every storage-facing scenario from here on is built on it, so a harness that
// silently did not work would look like a library bug in every one of them.
func TestMinIORoundTripsAnObject(t *testing.T) {
	t.Parallel()
	testsubstrate.RequireDocker(t)

	bucket := testsubstrate.StartMinIO(t)
	if err := testsubstrate.Reachable(t.Context(), bucket); err != nil {
		t.Fatalf("substrate is not reachable: %v", err)
	}

	client := testsubstrate.NewS3Client(bucket)
	const key = "smoke/one-object"
	body := []byte("icelake test substrate")

	if _, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	got, err := client.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String(bucket.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer func() { _ = got.Body.Close() }()

	read, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatalf("reading the object body: %v", err)
	}
	if !bytes.Equal(read, body) {
		t.Errorf("object body = %q, want %q", read, body)
	}
}
