package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/gvkhna/icelake/internal/s3cfg"
	"github.com/gvkhna/icelake/internal/testsubstrate"
)

// secretForHelp is a value that appears nowhere else, so finding it in the
// usage output can only mean the usage output printed the credential it was
// given.
const secretForHelp = "s3cr3t-value-that-must-never-be-printed"

// TestBinaryBuildsAndPrintsHelp is the first of this command's hermetic tests:
// the program builds, its flags parse, and the usage block an operator meets
// first is really printed and really carries no credential.
//
// Two assertions here are about output bytes rather than about a typed error,
// and both are deliberate. The first is that the block was emitted at all and
// names every flag this command defines — the names come from the command's own
// flag set through VisitAll, never from a list retyped here, so this holds "the
// usage text documents this command's arguments" rather than restating a
// sentence. Without it the test passed identically if --help printed nothing,
// which is the one thing it exists to rule out. The second is that the secret
// access key given in the environment is absent, and the mistake it guards is
// easy and quiet: binding os.Getenv as a flag's default value puts the secret
// into the block the flag package prints for -h and for every parse error, so a
// mistyped flag would echo a secret key to the terminal and into whatever
// captured it. TESTING.md's "Real R2 verification" section carries both of these
// as an open question against its own ban on substring assertions, rather than
// quietly rewording the ban to fit them.
func TestBinaryBuildsAndPrintsHelp(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "icelake-r2check")

	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("building the command: %v\n%s", err, out)
	}

	help := exec.Command(binary, "--help")
	help.Env = append(os.Environ(),
		accessKeyEnv+"=an-access-key-id",
		secretKeyEnv+"="+secretForHelp,
	)

	// Asking for help is not a failure, so the exit status must be zero.
	out, err := help.CombinedOutput()
	if err != nil {
		t.Fatalf("running --help: %v\n%s", err, out)
	}
	if len(out) == 0 {
		t.Fatal("--help printed nothing at all; the usage block is what an operator meets first")
	}

	var opts options
	newFlagSet(&opts).VisitAll(func(f *flag.Flag) {
		if !bytes.Contains(out, []byte("-"+f.Name)) {
			t.Errorf("--help did not document the -%s flag this command defines", f.Name)
		}
	})

	if bytes.Contains(out, []byte(secretForHelp)) {
		t.Error("--help printed the secret access key it was given in the environment")
	}
}

// TestRunRefusesBeforeItReachesAnyBucket is the other hermetic half, and it is
// the one that keeps this command safe to have in a repository whose test suite
// runs everywhere.
//
// A run with nothing to go on must fail on its arguments — before a client is
// built, before a request is made, and above all without falling back to
// whatever credentials happen to be in the environment. icelake's own rule is
// that it never uses the ambient AWS chain; a tool that writes throwaway objects
// into whichever bucket it can reach would be that rule's worst possible
// exception.
//
// Both halves of that matter here. The environment is emptied first, because
// this command deliberately reads credentials from it and a developer who has
// theirs exported would otherwise have this test build a client and make real
// requests — the one thing this file exists to guarantee never happens. And the
// error is matched against the sentinel that means "refused on the arguments",
// not merely asserted to be non-nil, because a network failure would satisfy a
// non-nil check while proving the opposite of the claim.
//
// The prefix cases are here for the same reason and are the same claim. The run
// root is built with path.Join, which cleans as it joins, so `-prefix
// warehouse/..` would collapse to nothing and scatter this run's objects across
// the top level of the bucket while every printed line still said PASS. That is
// refused on the arguments, in one place, before a client exists.
func TestRunRefusesBeforeItReachesAnyBucket(t *testing.T) {
	t.Setenv(accessKeyEnv, "")
	t.Setenv(secretKeyEnv, "")

	if err := run(t.Context(), nil, io.Discard); !errors.Is(err, errUsage) {
		t.Fatalf("a run with no endpoint, bucket or credentials returned %v, want a refusal on the arguments", err)
	}

	// An endpoint and a bucket are not enough either: the credentials still have
	// to be given explicitly.
	err := run(t.Context(), []string{"-endpoint", "https://example.invalid", "-bucket", "nothing"}, io.Discard)
	if !errors.Is(err, errUsage) {
		t.Fatalf("a run with no credentials returned %v, want a refusal on the arguments; icelake never falls back to the ambient AWS credential chain", err)
	}

	for _, prefix := range []string{"", "warehouse/..", "warehouse/.", "/warehouse", "warehouse//runs", "warehouse/"} {
		args := []string{
			"-endpoint", "https://example.invalid", "-bucket", "nothing",
			"-access-key-id", "an-access-key-id", "-secret-access-key", "a-secret",
			"-prefix", prefix,
		}
		if err := run(t.Context(), args, io.Discard); !errors.Is(err, errUsage) {
			t.Errorf("a run with -prefix %q returned %v, want a refusal on the arguments", prefix, err)
		}
	}

	// A flag this command does not define, which matters more here than in the
	// sibling command for the reason the credential flags are shaped the way
	// they are: the flag package prints the usage block for every parse error,
	// so this is the path where a secret bound as a flag default would be
	// echoed. It must end the run, and it must end it before anything is
	// written — the report goes to the writer below, so a writer that stayed
	// empty is a run that never reached a bucket.
	var progress bytes.Buffer
	err = run(t.Context(), []string{"-endpiont", "https://example.invalid"}, &progress)
	if err == nil {
		t.Fatal("a flag this command does not define was accepted, want a refusal")
	}
	if errors.Is(err, flag.ErrHelp) {
		t.Error("a mistyped flag was reported as a request for help, which exits zero")
	}
	if progress.Len() != 0 {
		t.Errorf("a mistyped flag still produced a report:\n%s", progress.String())
	}
}

// TestCheckAgainstTheSubstrate runs the whole procedure against the same MinIO
// container every other test in this repository uses.
//
// The claims held here are this program's own: that it refuses a prefix that is
// not disposable, that it refuses a bucket whose versioning would make its
// deletes cosmetic, that a bucket it could not inspect at all is reported as
// that rather than as a judgement about either, and that after a successful run
// the bucket is really empty again. Every one of those is about icelake's code
// rather than R2's, and leaving them untested is what let this program spend a
// milestone printing "nothing left under <root>" without ever looking.
//
// The first subtest runs the whole procedure, which means the three provider
// checks run here too. That is a deliberate, written-down trade rather than an
// accident: it couples this repository's gate to MinIO's list-after-write,
// same-key-overwrite and version-listing behaviour, and TESTING.md's "Real R2
// verification" section states why that is accepted and what to do if it ever
// fires. What none of this is, is evidence about R2 — MinIO answering a question
// about a provider is MinIO answering a question about MinIO, and only the
// manual run against a real bucket goes in the ledger.
func TestCheckAgainstTheSubstrate(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)
	client := testsubstrate.NewS3Client(bucket)

	t.Run("a successful run leaves the bucket as it found it", func(t *testing.T) {
		opts := optionsFor(bucket, "disposable-clean")

		var out bytes.Buffer
		if err := check(t.Context(), opts, &out); err != nil {
			t.Fatalf("check against the substrate: %v\n%s", err, out.String())
		}

		if left := keysUnder(t, client, bucket.Bucket, opts.prefix+"/"); len(left) != 0 {
			t.Errorf("%d objects are still under %s after a successful run: %v", len(left), opts.prefix, left)
		}
	})

	t.Run("a prefix that already holds an object is refused, and nothing is written", func(t *testing.T) {
		opts := optionsFor(bucket, "looks-like-a-warehouse")
		occupied := path.Join(opts.prefix, "market", "fills", "metadata", "00000-x.metadata.json")
		put(t, client, bucket.Bucket, occupied)

		var out bytes.Buffer
		err := check(t.Context(), opts, &out)
		if !errors.Is(err, errRefused) {
			t.Fatalf("a run against a prefix that already holds an object returned %v, want a refusal\n%s", err, out.String())
		}

		// Nothing new: the one object under the prefix is the one that was
		// already there, so the run did not write and then tidy up — it never
		// wrote.
		left := keysUnder(t, client, bucket.Bucket, opts.prefix+"/")
		if len(left) != 1 || left[0] != occupied {
			t.Errorf("the refused run left %v under %s, want only the object that was already there", left, opts.prefix)
		}
	})

	t.Run("a bucket that cannot be inspected is not reported as a judgement about it", func(t *testing.T) {
		// A bucket name that does not exist is the cheapest real version of the
		// most common way a first run against a new account fails: an
		// unreachable endpoint, a typo, or a token without s3:ListBucket. The
		// preflight's first request fails and the run stops — correctly — but
		// what it must not do is close with the advice for the other refusal,
		// which tells the operator to find a disposable, empty prefix in an
		// unversioned bucket and so names a cause this run does not have.
		missing := bucket
		missing.Bucket = bucket.Bucket + "-does-not-exist"
		opts := optionsFor(missing, "disposable-unreachable")

		var out bytes.Buffer
		err := check(t.Context(), opts, &out)
		if !errors.Is(err, errUninspectable) {
			t.Fatalf("a run against a bucket that does not exist returned %v, want the bucket to be reported as uninspectable\n%s", err, out.String())
		}
		if errors.Is(err, errRefused) {
			t.Error("a run that never got an answer was reported as a refusal, which is a judgement about the prefix and the versioning it never learned anything about")
		}
	})

	t.Run("a versioned bucket is refused, and nothing is written", func(t *testing.T) {
		versioned := versionedBucket(t, bucket, client)
		opts := optionsFor(versioned, "disposable-versioned")

		var out bytes.Buffer
		err := check(t.Context(), opts, &out)
		if !errors.Is(err, errRefused) {
			t.Fatalf("a run against a versioning-enabled bucket returned %v, want a refusal\n%s", err, out.String())
		}

		if left := keysUnder(t, client, versioned.Bucket, opts.prefix+"/"); len(left) != 0 {
			t.Errorf("the refused run wrote %v into a versioned bucket", left)
		}
	})
}

// TestACancelledRunIsReportedAsCancelled holds the difference between "this
// bucket failed" and "I stopped asking".
//
// Every step turns a cancelled context into its own FAIL, so a run interrupted
// with Ctrl-C used to end with "N of M checks failed. Do not treat this bucket as
// verified." and nothing anywhere saying it had been cancelled — and this
// program's own last line instructs the operator to record the outcome in
// ARCHITECTURE.md's ledger, which is how a cancelled run becomes a false bucket
// failure written down as fact. The run's own context is consulted before any
// verdict is printed, and this pins that: a cancelled run reports itself as
// cancelled.
func TestACancelledRunIsReportedAsCancelled(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var out bytes.Buffer
	err := check(ctx, optionsFor(bucket, "disposable-cancelled"), &out)
	if !errors.Is(err, errCancelled) {
		t.Fatalf("a cancelled run returned %v, want a cancellation rather than a verdict about the bucket\n%s", err, out.String())
	}
	if errors.Is(err, errRefused) {
		t.Error("a cancelled run was reported as a refusal, which is a statement about the bucket")
	}
}

// TestCleanupFailsWhenTheDeletesLeaveTheObjectsBehind is the check on the check:
// it puts cleanup in the one situation where deleting every key and being left
// with everything are the same thing, and requires it to say so.
//
// A versioned bucket answers an unversioned DeleteObject by writing a delete
// marker and keeping the body as a non-current version. Every delete call
// succeeds, the plain listing goes empty, and the bill does not move. The
// preflight refuses such a bucket before a run starts, so reaching this state
// through the program is not possible — which is exactly why it is worth pinning
// here, at the layer that has to be right the day the preflight cannot tell.
// Under the version of cleanup that printed "nothing left under <root>" from the
// delete count alone, this reported PASS.
func TestCleanupFailsWhenTheDeletesLeaveTheObjectsBehind(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)
	client := testsubstrate.NewS3Client(bucket)
	versioned := versionedBucket(t, bucket, client)

	c := &checker{
		client: testsubstrate.NewS3Client(versioned),
		bucket: versioned.Bucket,
		prefix: "disposable",
		root:   "disposable/run-0123456789abcdef",
	}
	if err := c.put(t.Context(), path.Join(c.root, "checksum", "probe.bin"), []byte("a body worth money")); err != nil {
		t.Fatalf("put: %v", err)
	}

	res := c.cleanup(t.Context())
	if res.err == nil {
		t.Fatalf("cleanup reported success (%q) against a bucket that retained every object it deleted", res.detail)
	}

	// And the failure is a true statement: the body really is still there,
	// hidden behind the delete marker the delete wrote.
	versions, markers, err := c.versions(t.Context(), c.root+"/")
	if err != nil {
		t.Fatalf("listing versions: %v", err)
	}
	if versions == 0 && markers == 0 {
		t.Error("nothing is left under the run root, so cleanup failed for some other reason than the one under test")
	}
}

// TestPutRecordsTheKeyBeforeAnUploadThatFails holds the record-before-write
// invariant the whole cleanup design rests on.
//
// An upload whose response is lost may have stored the object anyway, so a key
// recorded only after a successful PutObject is precisely the key that gets left
// behind. The failure here is a real one from the real store — a bucket that
// does not exist — rather than an injected error, and what matters is that the
// key is on the delete list afterwards regardless.
func TestPutRecordsTheKeyBeforeAnUploadThatFails(t *testing.T) {
	testsubstrate.RequireDocker(t)
	bucket := testsubstrate.StartMinIO(t)

	missing := bucket
	missing.Bucket = bucket.Bucket + "-does-not-exist"

	c := &checker{
		client: testsubstrate.NewS3Client(missing),
		bucket: missing.Bucket,
		prefix: "disposable",
		root:   "disposable/run-0123456789abcdef",
	}
	key := path.Join(c.root, "checksum", "probe.bin")

	if err := c.put(t.Context(), key, []byte("never stored")); err == nil {
		t.Fatal("putting into a bucket that does not exist returned no error")
	}
	if len(c.written) != 1 || c.written[0] != key {
		t.Fatalf("after a failed upload the delete list is %v, want exactly %q", c.written, key)
	}
}

// optionsFor points a run at a substrate bucket, exactly as an operator's flags
// would.
func optionsFor(b testsubstrate.Bucket, prefix string) options {
	return options{
		settings: s3cfg.Settings{
			Endpoint:        b.Endpoint,
			Region:          b.Region,
			AccessKeyID:     b.AccessKeyID,
			SecretAccessKey: b.SecretAccessKey,
		},
		bucket: b.Bucket,
		prefix: prefix,
	}
}

// versionedBucket creates a second bucket on the same substrate with versioning
// switched on, which is the state this program refuses to run against and the
// state its cleanup has to be able to detect.
func versionedBucket(t *testing.T, b testsubstrate.Bucket, client *s3.Client) testsubstrate.Bucket {
	t.Helper()

	versioned := b
	versioned.Bucket = b.Bucket + "-versioned"

	if _, err := client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(versioned.Bucket)}); err != nil {
		t.Fatalf("creating %s: %v", versioned.Bucket, err)
	}
	if _, err := client.PutBucketVersioning(t.Context(), &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(versioned.Bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("enabling versioning on %s: %v", versioned.Bucket, err)
	}

	return versioned
}

// put stores one object, for tests that need something to already be there.
func put(t *testing.T, client *s3.Client, bucket, key string) {
	t.Helper()

	if _, err := client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("{}")),
	}); err != nil {
		t.Fatalf("putting %s: %v", key, err)
	}
}

// keysUnder lists every object key under a prefix, which is how these tests ask
// the bucket what is really there rather than asking the program what it did.
func keysUnder(t *testing.T, client *s3.Client, bucket, prefix string) []string {
	t.Helper()

	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	var keys []string
	for pager.HasMorePages() {
		page, err := pager.NextPage(t.Context())
		if err != nil {
			t.Fatalf("listing %s: %v", prefix, err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}

	return keys
}
