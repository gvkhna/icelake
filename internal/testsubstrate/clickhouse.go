package testsubstrate

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ClickHouseImage is the ClickHouse server image the mirror's tests run,
// referenced by digest and never by tag.
//
// It is the official image at 26.7.2.59, the current stable release, resolved
// from the registry on 2026-08-04 as the multi-arch index covering linux/amd64
// and linux/arm64. By digest for the reason [MinIOImage] is by digest, and the
// version number matters more here than it does there: the mirror's idempotency
// rests on insert_deduplication_token, and the settings around insert
// deduplication have moved repeatedly through the 26.x line — the server's own
// system.settings_changes records changes at 26.1, 26.2, 26.6 and 26.7. What
// was measured on this build, and what a bump has to measure again, is in
// TESTING.md's substrate section.
const ClickHouseImage = "docker.io/clickhouse/clickhouse-server@sha256:c679638e2143af0d36896224b6bd6026f5d7c0ae1fa2fedf512e2c4182575a00"

// The credentials the substrate creates the server with. They are fixed rather
// than random because there is one server per test and nothing shares it, and
// because a test asserting a configuration refusal wants to write a real
// username down.
const (
	clickHouseUser     = "icelake"
	clickHousePassword = "icelake-test-password"
)

// UserFilesDir is where the server will read a file() from. It is the image's
// own user_files path, and it is the directory [ClickHouse.CopyFile] puts a
// file into.
const UserFilesDir = "/var/lib/clickhouse/user_files"

// ClickHouse describes a running ClickHouse server, ready for a test to point
// icelake's mirror at.
//
// Everything in it is what a caller would put in icelake's own
// ClickHouseConfig — an address, a username and a password — for the reason
// [Bucket] hands back configuration rather than a client: a test wires it up
// exactly as a production caller would.
type ClickHouse struct {
	// Addr is the native-protocol address, host:port.
	Addr string
	// Username and Password are the credentials the server was created with.
	Username string
	Password string

	container testcontainers.Container
}

// StartClickHouse starts a ClickHouse server and returns how to reach it. The
// container is stopped when the test finishes.
//
// One container per call, as with MinIO, and over whichever container runtime
// the machine has — the rootless Podman socket included, through the same
// detection [StartMinIO] uses rather than a second copy of it.
func StartClickHouse(tb testing.TB) *ClickHouse {
	tb.Helper()

	if err := useDetectedRuntime(); err != nil {
		tb.Fatalf("starting the ClickHouse container: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        ClickHouseImage,
		ExposedPorts: []string{"9000/tcp", "8123/tcp"},
		Env: map[string]string{
			"CLICKHOUSE_USER":     clickHouseUser,
			"CLICKHOUSE_PASSWORD": clickHousePassword,
			// The mirror creates its own database, so the server needs to let
			// its user do so; the image's default user has that already and
			// this makes it true of the one created here too.
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
		},
		WaitingFor: wait.ForHTTP("/ping").WithPort("8123/tcp").WithStartupTimeout(3 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if container != nil {
		tb.Cleanup(func() {
			stop, cancelStop := context.WithTimeout(context.Background(), time.Minute)
			defer cancelStop()
			// Terminating an already-stopped container is the normal case for
			// the outage test below, and is not an error worth failing on.
			_ = container.Terminate(stop)
		})
	}
	if err != nil {
		tb.Fatalf("starting the ClickHouse container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		tb.Fatalf("reading the ClickHouse host: %v", err)
	}
	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		tb.Fatalf("reading the ClickHouse native port: %v", err)
	}

	return &ClickHouse{
		Addr:      host + ":" + port.Port(),
		Username:  clickHouseUser,
		Password:  clickHousePassword,
		container: container,
	}
}

// Stop stops the server, leaving its address unreachable.
//
// It exists for the one claim that needs a real outage rather than a wrong
// address: that a ClickHouse which goes away mid-run costs the lake nothing.
// A wrong address would prove something weaker, since it never worked.
func (c *ClickHouse) Stop(tb testing.TB) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	timeout := 10 * time.Second
	if err := c.container.Stop(ctx, &timeout); err != nil {
		tb.Fatalf("stopping the ClickHouse container: %v", err)
	}
}

// CopyFile puts a local file into the server's user_files directory under the
// given name, so that a `file()` table function can read it.
//
// It is how the second door of the two-door identity test gets at the very
// Parquet file icelake wrote: the bytes go in through the container runtime
// rather than over a shared mount, so nothing depends on how the host's
// filesystem is mapped into the container.
func (c *ClickHouse) CopyFile(tb testing.TB, localPath, name string) {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := c.container.CopyFileToContainer(ctx, localPath, UserFilesDir+"/"+name, 0o644); err != nil {
		tb.Fatalf("copying %s into the ClickHouse container: %v", localPath, err)
	}
}
