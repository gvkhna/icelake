package testsubstrate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

// MinIOImage is the container image the test substrate runs, referenced by
// digest and never by tag.
//
// It is the Chainguard MinIO image for the usual reasons Chainguard images are
// chosen: a minimal attack surface with no shell or package manager, which is
// what makes it appropriate to pull into CI. The digest is the multi-arch index
// covering linux/amd64 and linux/arm64, resolved from the registry on
// 2026-07-31. By digest rather than by tag because "latest" is not a version: a
// tag that moves underneath a test suite turns an unrelated upstream release
// into a mystery CI failure.
const MinIOImage = "cgr.dev/chainguard/minio@sha256:d386393960d3126c034d6597b752bc33a02a2c237069788aabcf6d035c166b27"

// DefaultRegion is the region the substrate advertises.
//
// It is deliberately not R2's "auto". MinIO accepts any region, and icelake's
// Region field exists as configuration precisely so the test substrate can pass
// its own value while production defaults to what R2 requires — a test that
// silently ran under the production default would not be exercising that seam
// at all.
const DefaultRegion = "us-east-1"

// startTimeout bounds pulling and starting the container, which on a cold cache
// is a real image pull over the network.
const startTimeout = 5 * time.Minute

// Bucket describes a running S3-compatible object store and one bucket inside
// it, ready for a test to write to.
//
// Everything in it is what a caller would put in icelake's own Config: an
// endpoint, a region, a bucket name and a pair of static credentials. That is
// the point — the substrate hands back configuration, not a client or a
// container, so a test wires it up exactly as a production caller would.
type Bucket struct {
	// Endpoint is the object store's base URL, including scheme.
	Endpoint string
	// Region is the region to sign requests for.
	Region string
	// Bucket is a bucket that already exists and is empty.
	Bucket string
	// AccessKeyID and SecretAccessKey are static credentials for the store.
	AccessKeyID     string
	SecretAccessKey string
}

// StartMinIO starts a MinIO container, creates an empty bucket in it, and
// returns how to reach it. The container is stopped when the test finishes.
//
// One container per call, started and torn down by the test binary itself
// rather than by a docker-compose file a developer has to remember to run.
// Tests that need Docker to be present must call [RequireDocker] first; this
// function assumes it is there and fails the test if the container will not
// start.
func StartMinIO(tb testing.TB) Bucket {
	tb.Helper()

	if err := useDetectedRuntime(); err != nil {
		tb.Fatalf("starting the MinIO container: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	container, err := minio.Run(ctx, MinIOImage)
	if container != nil {
		tb.Cleanup(func() {
			stop, cancelStop := context.WithTimeout(context.Background(), time.Minute)
			defer cancelStop()
			if err := container.Terminate(stop); err != nil {
				tb.Errorf("terminating the MinIO container: %v", err)
			}
		})
	}
	if err != nil {
		tb.Fatalf("starting the MinIO container: %v", err)
	}

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		tb.Fatalf("reading the MinIO endpoint: %v", err)
	}

	b := Bucket{
		Endpoint:        "http://" + endpoint,
		Region:          DefaultRegion,
		Bucket:          randomBucketName(tb),
		AccessKeyID:     container.Username,
		SecretAccessKey: container.Password,
	}

	if _, err := NewS3Client(b).CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b.Bucket)}); err != nil {
		tb.Fatalf("creating bucket %s: %v", b.Bucket, err)
	}
	return b
}

// NewS3Client builds an S3 client for a bucket, constructed the way icelake
// constructs its own.
//
// Two settings are not incidental. Checksum calculation and validation are both
// set to "when required" rather than the SDK's newer "when supported" default,
// because R2 answers the checksum headers that default sends with 501 Not
// Implemented; keeping the test substrate on the same construction is what makes
// the substrate a stand-in for R2 rather than merely an S3 server that happens
// to work. Path-style addressing is used because the endpoint is a host and
// port with no wildcard DNS behind it, so virtual-host-style bucket addressing
// has nowhere to resolve.
func NewS3Client(b Bucket) *s3.Client {
	cfg := aws.Config{
		Region:                     b.Region,
		Credentials:                credentials.NewStaticCredentialsProvider(b.AccessKeyID, b.SecretAccessKey, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(b.Endpoint)
		o.UsePathStyle = true
	})
}

// RequireDocker makes the container runtime usable, or skips the calling test
// saying plainly that there is none.
//
// CI is required to provide a container runtime — the substrate is not optional
// there, and a suite that silently skipped its own storage tests on a
// misconfigured runner would be worse than one that failed. The skip exists for
// the developer machine and the sandbox that genuinely has no runtime, where the
// alternative is a hard failure that says nothing about what to install.
func RequireDocker(tb testing.TB) {
	tb.Helper()

	if err := useDetectedRuntime(); err != nil {
		tb.Skipf("no container runtime available: %v", err)
	}
}

// errNoRuntime reports that nothing on this machine looks like a container
// runtime.
var errNoRuntime = errors.New("no DOCKER_HOST is set and no container runtime socket was found")

// DockerRuntime returns the container-runtime endpoint the substrate will use,
// or an error explaining why there is none.
//
// It looks for a configured endpoint or a runtime socket rather than for the
// docker command: the CLI being installed says nothing about a daemon being up,
// and the socket is what testcontainers actually connects to. A rootless Podman
// socket counts — it serves the same Docker API, testcontainers drives it
// unchanged, and a developer who has Podman rather than Docker should not have
// to discover that themselves.
func DockerRuntime() (string, error) {
	if v := os.Getenv("DOCKER_HOST"); v != "" {
		return v, nil
	}

	candidates := []string{"/var/run/docker.sock", "/run/podman/podman.sock"}
	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		candidates = append(candidates, filepath.Join(dir, "docker.sock"), filepath.Join(dir, "podman", "podman.sock"))
	}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates,
			filepath.Join(home, ".docker", "run", "docker.sock"),
			filepath.Join(home, ".colima", "default", "docker.sock"),
		)
	}
	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return "unix://" + path, nil
		}
	}
	return "", fmt.Errorf("%w; looked at %v", errNoRuntime, candidates)
}

// detected caches the one-time runtime lookup, so parallel tests all see the
// same answer and DOCKER_HOST is written at most once.
var detected struct {
	once     sync.Once
	endpoint string
	err      error
}

// useDetectedRuntime points testcontainers at whatever runtime this machine has.
//
// testcontainers finds a plain Docker socket by itself; it does not go looking
// for a rootless socket under XDG_RUNTIME_DIR, so on such a machine the whole
// suite would skip while a perfectly good runtime sat there. Setting DOCKER_HOST
// is exactly what a developer would be told to do by hand, done once here
// instead. An already-set DOCKER_HOST is never overwritten: an explicit choice
// outranks a guess.
func useDetectedRuntime() error {
	detected.once.Do(func() {
		detected.endpoint, detected.err = DockerRuntime()
		if detected.err != nil || os.Getenv("DOCKER_HOST") != "" {
			return
		}
		if err := os.Setenv("DOCKER_HOST", detected.endpoint); err != nil {
			detected.err = fmt.Errorf("pointing testcontainers at %s: %w", detected.endpoint, err)
		}
	})
	return detected.err
}

// randomBucketName produces a fresh, legal bucket name per call, so two tests
// running in parallel against one container cannot collide.
func randomBucketName(tb testing.TB) string {
	tb.Helper()

	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		tb.Fatalf("generating a bucket name: %v", err)
	}
	return "icelake-test-" + hex.EncodeToString(b[:])
}

// errUnreachable states this package's only network assumption once, rather
// than leaving it scattered: an endpoint it just started answers.
var errUnreachable = errors.New("object store endpoint is unreachable")

// Reachable checks that a bucket's endpoint answers at all, returning
// [errUnreachable] wrapped when it does not. It exists for the case where a
// container starts but its port is not forwarded, which otherwise surfaces as an
// opaque SDK timeout much later.
func Reachable(ctx context.Context, b Bucket) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.Endpoint+"/minio/health/live", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", errUnreachable, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: health check returned %s", errUnreachable, resp.Status)
	}
	return nil
}
