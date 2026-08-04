// Command icelake-r2check verifies the object-storage behaviours icelake's
// design depends on against a real bucket.
//
// Everything in icelake's automated suite runs against a local MinIO container,
// and two of the behaviours this design leans on are specific to the storage
// provider rather than to the S3 API: MinIO passing them proves nothing about
// the bucket a service will actually write to. This command is the manual
// procedure that closes that gap — pointed at a real bucket with real
// credentials and run by hand. That run is never automated, because it needs
// real credentials, costs real (if tiny) money, and its subject is a vendor's
// behaviour rather than icelake's own code.
//
// This program's own code is a different subject and is not exempt. Its
// procedure runs in CI against the same MinIO container the rest of the suite
// uses, which holds the claims that are about the code here — the preflight's
// two refusals, the record-before-write ordering, and a cleanup that measures
// rather than asserts — and which incidentally runs the three checks below
// against a substrate whose answers say nothing about R2. See TESTING.md's
// "Real R2 verification" for what that does and does not buy, including the
// coupling it accepts.
//
// It runs three checks, each of which a real icelake guarantee rests on:
//
//  1. The disabled-checksum workaround holds. An upload through icelake's exact
//     client construction succeeds — R2 answers the checksum headers newer AWS
//     SDKs send by default with 501 Not Implemented, and icelake turns those
//     headers off for that reason.
//  2. List-after-write is consistent enough for catalog rebuild's discovery.
//     Objects are created under fresh <namespace>/<table>/metadata/ prefixes and
//     immediately looked for by the same two-level delimiter listing the rebuild
//     tool performs, which is the only thing rebuild is ever told about a
//     warehouse — followed by the third level rebuild also depends on, the
//     non-delimited listing of one table's metadata/ directory and a read of the
//     object it finds there.
//  3. A same-key overwrite behaves as an overwrite. The same key is uploaded
//     twice with different bytes and comes back as the second body, with the
//     key carrying exactly one version afterwards wherever the endpoint will
//     answer a version listing.
//
// Two further reported steps bracket those three, and they are the reason this
// is safe to point at a bucket someone pays for. Before anything is written, a
// preflight refuses the run unless the prefix it was given is empty and the
// bucket has versioning switched off: an empty prefix is what distinguishes a
// disposable prefix from a warehouse prefix, and an unversioned bucket is what
// makes a plain delete actually remove the body rather than hide it behind a
// delete marker that goes on being billed. Afterwards, cleanup deletes every key
// the run may have created and then *lists the run root to check* — it reports
// what it measured rather than what it attempted.
//
// Credentials and the endpoint reach it only through flags or the environment at
// run time.
//
// Run it on any major AWS SDK bump, before first production use of a new
// account or bucket, and never otherwise — then record the date, the SDK
// version it printed and the outcome in ARCHITECTURE.md's open questions, which
// is the ledger of whether this procedure has been run.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/gvkhna/icelake/internal/s3cfg"
)

// accessKeyEnv and secretKeyEnv are the two environment variables this command
// reads credentials from when the matching flags are not given.
//
// They are deliberately icelake's own names rather than the AWS ones, and are
// the same two the rebuild command reads. icelake never falls back to the
// ambient AWS credential chain, and a program whose whole job is to write
// throwaway objects into a bucket must be the last thing to start guessing
// which bucket: supplying credentials stays an explicit act, whether it is a
// flag or one of these.
const (
	accessKeyEnv = "ICELAKE_ACCESS_KEY_ID"
	secretKeyEnv = "ICELAKE_SECRET_ACCESS_KEY"
)

// defaultPrefix is the key prefix everything this command writes hangs under,
// unless the operator names another. It is deliberately not a warehouse prefix:
// this program writes throwaway objects that must never be mistaken for a
// table's own, and the preflight below enforces that rather than trusting it.
const defaultPrefix = "icelake-r2check"

// cleanupBudget bounds the deletions and the listing that verifies them.
//
// Cleanup runs on a context of its own so that a cancelled run still takes its
// objects with it, which means this is the one stretch of the program that keeps
// working after Ctrl-C. It is printed before it starts, because an unexplained
// silent minute after an interrupt is exactly what makes an operator reach for
// kill -9 — and killing the process here is the one thing that leaves objects
// behind.
const cleanupBudget = 2 * time.Minute

// errUsage marks a refusal that happened on the arguments alone, before a
// client was built or a single request was made.
//
// It is a sentinel rather than prose because the one thing this command's own
// tests must be able to assert is exactly that: that a run with nothing to go
// on stops here, rather than reaching for a bucket with whatever it could
// find.
var errUsage = errors.New("invalid arguments")

// errRefused marks a run stopped by the preflight — the bucket was reachable and
// answered, and what it said was that this is not a bucket or a prefix this
// program may write into.
var errRefused = errors.New("refused before writing anything")

// errUninspectable marks the other way the preflight stops a run: it could not
// reach an answer at all.
//
// It exists because those two endings look identical from outside and mean
// opposite things. A run that stops because the prefix listed non-empty, or
// because versioning came back Enabled, has learned something about the bucket
// and the operator's next move is to point the command somewhere else. A run
// that stops because the endpoint refused the connection, or the bucket name is
// a typo, or the token has no s3:ListBucket has learned nothing about the
// bucket at all — the closing advice that used to be printed for both told that
// operator to find a disposable, empty prefix in an unversioned bucket, which
// names a cause their run does not have and is the most common way a first run
// against a new account fails. errRefused's own sentence above says "the bucket
// was reachable and answered", so returning it here contradicted it as well.
var errUninspectable = errors.New("the bucket could not be inspected")

// errCancelled marks a run that was interrupted rather than one that failed.
//
// The distinction is the whole point of having it: a cancelled run's checks
// report FAIL because they did not finish, which says nothing whatever about the
// bucket, and an operator following this program's own instruction to record the
// outcome must not write that into the ledger.
var errCancelled = errors.New("the run was cancelled")

func main() {
	// Ctrl-C and a shutdown signal cancel the run. Cleanup still happens for
	// whatever the run may already have written: the deletions run under a
	// context of their own, so an interrupted run does not leave its objects
	// behind in someone's bucket. A run interrupted before it wrote anything —
	// during the preflight, which only lists, or before it — has nothing to
	// clean up, does not run the step, and says so rather than claiming it.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Once a signal has cancelled the run, the signals go back to the runtime.
	// Cleanup keeps going for up to its own budget after that, and an operator
	// who presses Ctrl-C again during it has to be able to end the process:
	// leaving the handler registered would swallow the second interrupt, print
	// nothing, and make a working cleanup look like a hang.
	go func() {
		<-ctx.Done()
		stop()
	}()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		// The library's errors introduce themselves with "icelake:", and the
		// command's own name already says whose message this is.
		fmt.Fprintln(os.Stderr, "icelake-r2check:", strings.ReplaceAll(err.Error(), "icelake: ", ""))
		os.Exit(1)
	}
}

// options is what one run needs, and nothing more.
type options struct {
	settings s3cfg.Settings
	bucket   string
	prefix   string
}

// newFlagSet defines this command's whole argument surface.
//
// It is a function of its own rather than inline in [run] so that a test can ask
// the command what flags it has, instead of restating the list and drifting from
// it. The usage text is the operator's first contact with the program, so what
// it must contain is "every flag this command defines" — a claim only the flag
// set itself can state.
func newFlagSet(opts *options) *flag.FlagSet {
	fs := flag.NewFlagSet("icelake-r2check", flag.ContinueOnError)
	fs.Usage = func() {
		u := fs.Output()
		fmt.Fprintln(u, "icelake-r2check verifies, against a real bucket, three storage behaviours")
		fmt.Fprintln(u, "icelake's design depends on and a local MinIO cannot prove:")
		fmt.Fprintln(u, "")
		fmt.Fprintln(u, "  1. an upload through icelake's own client construction succeeds, which is")
		fmt.Fprintln(u, "     the disabled-checksum workaround R2 needs;")
		fmt.Fprintln(u, "  2. objects are visible to the two-level listing catalog rebuild discovers")
		fmt.Fprintln(u, "     tables with, immediately after being written;")
		fmt.Fprintln(u, "  3. writing the same key twice leaves one object carrying the second body,")
		fmt.Fprintln(u, "     which is what makes a retried flush idempotent.")
		fmt.Fprintln(u, "")
		fmt.Fprintln(u, "It is a manual procedure, not a test: run it by hand on a major AWS SDK bump")
		fmt.Fprintln(u, "and before first production use of a new account or bucket. Never in CI.")
		fmt.Fprintln(u, "It writes only under its own disposable prefix and deletes what it wrote.")
		fmt.Fprintln(u, "The prefix must be empty and the bucket must have versioning off; a run that")
		fmt.Fprintln(u, "finds otherwise is refused before anything is written.")
		fmt.Fprintln(u, "")
		fmt.Fprintln(u, "Usage:")
		fmt.Fprintln(u, "  icelake-r2check -endpoint URL -bucket NAME [flags]")
		fmt.Fprintln(u, "")
		fmt.Fprintln(u, "Flags:")
		fs.PrintDefaults()
		fmt.Fprintln(u, "")
		fmt.Fprintf(u, "Credentials may be given as flags or in %s and %s.\n", accessKeyEnv, secretKeyEnv)
		fmt.Fprintln(u, "Prefer the environment: a secret on a command line is visible in the process list.")
	}

	fs.StringVar(&opts.settings.Endpoint, "endpoint", "", "S3-compatible endpoint base URL, including scheme (required)")
	fs.StringVar(&opts.bucket, "bucket", "", "bucket to run the checks against (required)")
	fs.StringVar(&opts.prefix, "prefix", defaultPrefix, "empty key prefix to write the disposable objects under")
	fs.StringVar(&opts.settings.Region, "region", "", `region to sign requests for (default "auto")`)
	// Both credential flags default to empty and take their environment value
	// only after parsing, for the reason the rebuild command spells out: binding
	// os.Getenv as a flag's default would put the secret into the usage text,
	// which the flag package prints for -h and for every parse error.
	fs.StringVar(&opts.settings.AccessKeyID, "access-key-id", "", "access key id (default "+accessKeyEnv+")")
	fs.StringVar(&opts.settings.SecretAccessKey, "secret-access-key", "", "secret access key (default "+secretKeyEnv+")")

	return fs
}

// run parses arguments, performs the checks, and reports them to out. It
// returns an error when any check failed, which is what makes the exit status
// worth reading.
func run(ctx context.Context, args []string, out io.Writer) error {
	var opts options
	fs := newFlagSet(&opts)

	if err := fs.Parse(args); err != nil {
		// -h and --help are a request for the usage text, which has already been
		// printed by now, and answering a question is not a failure.
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}

		return err
	}

	if opts.settings.AccessKeyID == "" {
		opts.settings.AccessKeyID = os.Getenv(accessKeyEnv)
	}
	if opts.settings.SecretAccessKey == "" {
		opts.settings.SecretAccessKey = os.Getenv(secretKeyEnv)
	}
	if err := opts.validate(); err != nil {
		return err
	}

	return check(ctx, opts, out)
}

// validate refuses a run that could not be meaningful, before anything is
// written anywhere.
//
// The credential rule is icelake's own, taken from the same package the library
// validates with rather than restated here: exactly one form, never the ambient
// AWS chain. A checking tool that quietly picked up whatever credentials were
// lying around is how one writes throwaway objects into a stranger's bucket.
func (o options) validate() error {
	var problems []error

	if o.settings.Endpoint == "" {
		problems = append(problems, fmt.Errorf("%w: -endpoint is required", errUsage))
	}
	if o.bucket == "" {
		problems = append(problems, fmt.Errorf("%w: -bucket is required", errUsage))
	}
	if err := validatePrefix(o.prefix); err != nil {
		problems = append(problems, err)
	}
	for _, err := range o.settings.ValidateCredentials() {
		problems = append(problems, fmt.Errorf("%w: credentials: %w", errUsage, err))
	}

	return errors.Join(problems...)
}

// validatePrefix refuses a prefix this program would not write where the
// operator was told it would.
//
// The run root is built with path.Join, which calls path.Clean, so a "." or ".."
// segment does not fail — it silently collapses, and `-prefix warehouse/..`
// would put five objects at the top level of the bucket while the program went
// on reporting PASS. Refusing the segment is the honest form of that: the prefix
// this command writes under is the prefix it was given, spelled out, and
// anything that would be rewritten on the way is a mistake rather than an
// abbreviation. A leading slash and an empty segment go the same way, for the
// same reason — S3 keys have no root and no empty path component, so both are a
// typo that would produce a key nobody meant.
func validatePrefix(prefix string) error {
	if prefix == "" {
		return fmt.Errorf("%w: -prefix must not be empty", errUsage)
	}
	if strings.HasPrefix(prefix, "/") {
		return fmt.Errorf("%w: -prefix must not begin with a slash; object keys have no root", errUsage)
	}
	for _, segment := range strings.Split(prefix, "/") {
		switch segment {
		case "":
			return fmt.Errorf("%w: -prefix must not contain an empty path segment", errUsage)
		case ".", "..":
			return fmt.Errorf("%w: -prefix must not contain a %q segment; it would be collapsed and the run would write somewhere else", errUsage, segment)
		}
	}

	return nil
}

// result is one check's outcome, as it is printed.
type result struct {
	name   string
	detail string
	err    error
}

// check runs the whole procedure: build the client icelake itself would build,
// refuse the run unless the bucket and prefix are safe to write into, run the
// three checks under a prefix nothing else uses, clean up, and report.
func check(ctx context.Context, opts options, out io.Writer) error {
	awsCfg, transport := opts.settings.Build()
	defer transport.CloseIdleConnections()

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) { o.UsePathStyle = true })

	// One prefix per run, so that two runs — or a run against a bucket a
	// previous run left something in — cannot see each other's objects and call
	// it a consistency result.
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return fmt.Errorf("generating a run prefix: %w", err)
	}
	root := path.Join(opts.prefix, "run-"+hex.EncodeToString(suffix))

	fmt.Fprintf(out, "icelake-r2check\n")
	fmt.Fprintf(out, "  date          %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(out, "  sdk           %s %s\n", aws.SDKName, aws.SDKVersion)
	fmt.Fprintf(out, "  endpoint      %s\n", opts.settings.Endpoint)
	fmt.Fprintf(out, "  bucket        %s\n", opts.bucket)
	fmt.Fprintf(out, "  run prefix    %s\n\n", root)

	c := &checker{client: client, bucket: opts.bucket, prefix: opts.prefix, root: root}

	// The preflight is not one FAIL among several. It runs first and, if it
	// refuses, nothing else runs at all: its whole job is to establish that this
	// bucket and this prefix may be written into, and running the checks anyway
	// would be writing into a bucket that just said no.
	results := []result{c.preflight(ctx)}
	refused := results[0].err != nil

	if !refused {
		results = append(results,
			c.checksumWorkaround(ctx),
			c.listAfterWrite(ctx),
			c.sameKeyOverwrite(ctx),
		)
	}

	// Cleanup runs whatever the checks did, and under a context of its own: a
	// run cancelled with Ctrl-C must still take its objects with it. It is
	// announced before it starts so that the pause after an interrupt is a
	// stated budget rather than an unexplained silence. It is skipped in the one
	// case where it has nothing to do, and whether it ran is tracked rather than
	// assumed, because the verdict below says so out loud.
	//
	// Corrected at the completeness audit's fifth round (2026-08-01): this
	// step used to sit inside the `if !refused` branch above, while the
	// cancelled verdict below told the operator "cleanup still ran, on its own
	// budget" with nothing conditioning it. Those two disagree on a path that is
	// not exotic: a context already cancelled at the preflight makes its
	// ListObjectsV2 fail, the preflight reports that as errUninspectable, and a
	// refusal skips everything here — so cleanup never ran and the program said
	// it had. The effect was harmless, because the preflight only lists and so
	// there was nothing to clean, but the program did not know that when it said
	// it, and a verification tool that states an unverified fact about an
	// operator's bucket is the same class of defect the round before this one
	// found in this file's cleanup step. Gating on the recorded key list rather
	// than on `refused` is what makes the sentence provable instead of merely
	// true: put records a key before it uploads it and is the only thing that
	// appends to that list, so an empty list is exactly "nothing was written and
	// nothing needed cleaning", whatever stopped the run and wherever it stopped.
	cleanedUp := false
	if len(c.written) > 0 {
		fmt.Fprintf(out, "cleaning up %d keys under %s, with %s to do it\n\n", len(c.written), c.root, cleanupBudget)
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupBudget)
		defer cancel()
		results = append(results, c.cleanup(cleanupCtx))
		cleanedUp = true
	}

	failed := 0
	for _, r := range results {
		status, detail := "PASS", r.detail
		if r.err != nil {
			failed++
			status, detail = "FAIL", r.err.Error()
		}
		fmt.Fprintf(out, "%s  %-20s %s\n", status, r.name, detail)
	}
	fmt.Fprintln(out)

	switch {
	case ctx.Err() != nil:
		// A cancelled run's checks fail because they were interrupted, which is
		// not a statement about the bucket. Saying "do not treat this bucket as
		// verified" here would be true but useless, and an operator following
		// this program's instruction to record the outcome would write a bucket
		// failure into the ledger that never happened.
		fmt.Fprintf(out, "The run was cancelled. Any FAIL above is this run being interrupted, not a\n")
		fmt.Fprintf(out, "result about the bucket.\n")
		if cleanedUp {
			fmt.Fprintf(out, "Cleanup still ran, on its own budget, and its own line above is what it\n")
			fmt.Fprintf(out, "measured rather than what it attempted.\n")
		} else {
			fmt.Fprintf(out, "Nothing had been written when it stopped, so there was nothing to clean up\n")
			fmt.Fprintf(out, "and no cleanup step ran.\n")
		}
		fmt.Fprintf(out, "Do not record a cancelled run in the ledger; run it again.\n")

		return fmt.Errorf("%w: %w", errCancelled, ctx.Err())

	case refused && errors.Is(results[0].err, errUninspectable):
		// The preflight never got an answer, so it has no opinion about the
		// prefix or the versioning and must not print one. Nothing was written
		// for the same reason nothing was checked: the run stopped at the first
		// request it made.
		fmt.Fprintf(out, "The bucket could not be inspected, so nothing was written and nothing was\n")
		fmt.Fprintf(out, "checked, and the error above is why. This is not a result about the bucket —\n")
		fmt.Fprintf(out, "the run stopped before it could form one. Check the endpoint, the bucket name\n")
		fmt.Fprintf(out, "and that the credentials may list it, and run it again.\n")

		return results[0].err

	case refused:
		fmt.Fprintf(out, "The run was refused before anything was written, so there is nothing to clean\n")
		fmt.Fprintf(out, "up and nothing to record. Point it at a disposable, empty prefix in a bucket\n")
		fmt.Fprintf(out, "with versioning switched off, and run it again.\n")

		return fmt.Errorf("%w: %w", errRefused, results[0].err)

	case failed > 0:
		fmt.Fprintf(out, "%d of %d checks failed. Do not treat this bucket as verified.\n", failed, len(results))

		return fmt.Errorf("%d of %d checks failed", failed, len(results))
	}

	fmt.Fprintf(out, "All %d checks passed. Record the date, the SDK version above and this outcome\n", len(results))
	fmt.Fprintf(out, "in ARCHITECTURE.md's open questions, which is the ledger for this procedure.\n")

	return nil
}

// checker holds what every check needs: the client icelake would use, the
// bucket, the prefix the operator named, and the run's own prefix under it.
type checker struct {
	client *s3.Client
	bucket string
	prefix string
	root   string
	// written is every key this run created, so cleanup deletes exactly what it
	// made rather than everything it can see under the prefix.
	written []string
}

// preflight is the step that runs before anything is written, and it is the only
// one that can stop the run.
//
// It answers two questions, and both of them are about not doing damage rather
// than about the S3 API.
//
// The first is whether the prefix is really disposable. The doc comment on
// defaultPrefix has always promised that it is "deliberately not a warehouse
// prefix", and until this check existed nothing enforced it: -prefix was taken
// verbatim from the operator and validated only as non-empty, while the sibling
// icelake-rebuild command has an identically named, required -prefix flag that
// means the warehouse prefix, and both commands read the same two ICELAKE_
// credential variables. Pasting the wrong one in is an ordinary mistake with an
// unpleasant ending: if any object then survives — a cleanup failure, a SIGKILL
// — rebuild's discovery walks the warehouse's children and refuses "run-<hex>"
// as an invalid table name, aborting the whole rebuild before it appends a
// single table. One leaked run would disarm disaster recovery for every table in
// the warehouse. An empty listing is the difference: a warehouse prefix always
// has objects under it, a disposable one never does.
//
// The second is whether a delete will actually delete. On a bucket with
// versioning enabled — or suspended, which still retains what is already there —
// an unversioned DeleteObject writes a delete marker and keeps the body as a
// non-current version, billed forever. Cleanup would then report success while
// leaving everything it wrote in place, which is the exact opposite of what this
// program promises. Refusing at the start is better than discovering it at the
// end, because at the start nothing has been written yet.
//
// An endpoint that will not answer GetBucketVersioning at all is not refused:
// the answer is recorded as unknown and the cleanup verification below falls
// back to counting versions itself, which measures the same thing one step later
// and without trusting the endpoint's word for it.
//
// A listing that will not answer at all is a third outcome rather than a
// variation on the first, and it is marked with its own sentinel. An unreachable
// endpoint, a mistyped bucket name and a token without s3:ListBucket all land on
// that first return, and none of them is a judgement about the prefix or the
// bucket's versioning — the run stopped before it could form one. Telling that
// operator to find an empty prefix in an unversioned bucket sends them to fix
// something that is not broken.
func (c *checker) preflight(ctx context.Context) result {
	const name = "preflight"

	existing, err := c.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(c.bucket),
		Prefix:  aws.String(c.prefix + "/"),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return result{name: name, err: fmt.Errorf("%w: listing %s to check it is empty: %w", errUninspectable, c.prefix, err)}
	}
	if len(existing.Contents) > 0 {
		return result{name: name, err: fmt.Errorf(
			"%s already holds at least one object (%s), so it is not a disposable prefix; this program must not write into a prefix that holds anything, least of all a warehouse",
			c.prefix, aws.ToString(existing.Contents[0].Key))}
	}

	versioning := "off"
	status, err := c.client.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(c.bucket)})
	switch {
	case err != nil:
		versioning = "unreadable"
	case status.Status == types.BucketVersioningStatusEnabled || status.Status == types.BucketVersioningStatusSuspended:
		return result{name: name, err: fmt.Errorf(
			"the bucket has versioning %s, and this program deletes without a version id: every object it wrote would be kept as a non-current version and go on being billed. Point it at a bucket with versioning off",
			status.Status)}
	}

	return result{name: name, detail: fmt.Sprintf("%s holds no objects and the bucket's versioning is %s", c.prefix, versioning)}
}

// checksumWorkaround is check 1: an upload through icelake's exact client
// construction succeeds, and the object reads back byte for byte.
//
// The client was built by internal/s3cfg — the same code the library and the
// rebuild tool build theirs with — so this is not a re-creation of icelake's
// settings that could drift from them. What it proves is that the storage
// provider accepts what icelake actually sends: R2 answers the checksum headers
// newer SDKs send by default with 501 Not Implemented, and those headers are
// turned off in exactly one place, which is the place this exercises.
func (c *checker) checksumWorkaround(ctx context.Context) result {
	const name = "checksum workaround"

	key := path.Join(c.root, "checksum", "probe.bin")
	body := make([]byte, 4096)
	if _, err := rand.Read(body); err != nil {
		return result{name: name, err: fmt.Errorf("generating a body: %w", err)}
	}

	if err := c.put(ctx, key, body); err != nil {
		return result{name: name, err: fmt.Errorf("the upload icelake's own client construction makes was rejected: %w", err)}
	}
	got, err := c.get(ctx, key)
	if err != nil {
		return result{name: name, err: fmt.Errorf("reading the uploaded object back: %w", err)}
	}
	if !bytes.Equal(got, body) {
		return result{name: name, err: fmt.Errorf("the object read back as %d bytes, not the %d that were uploaded", len(got), len(body))}
	}

	return result{name: name, detail: fmt.Sprintf("uploaded and read back %d bytes through icelake's own client construction", len(body))}
}

// metadataBody is what this command writes at the metadata keys it creates. It
// is a syntactically valid, semantically empty metadata document: nothing here
// parses it, and nothing should be able to mistake it for a real table's.
var metadataBody = []byte("{}")

// listAfterWrite is check 2: objects created under fresh table prefixes are
// immediately visible to the listings catalog rebuild discovers and loads tables
// with.
//
// This is the property auto-discovery rests on, and it is a real question about
// a real provider rather than about the S3 API: rebuild is never told which
// tables exist, so a listing that has not caught up with a write is a table
// silently missing from a rebuilt catalog. The prefixes are fresh on every run,
// which is the case a bucket is least likely to answer for immediately.
//
// All three levels rebuild uses are exercised, and the third is not decoration.
// The two delimiter listings yield namespaces and tables as CommonPrefixes,
// which some providers can materialise ahead of the object underneath them; the
// third is the non-delimited listing of one table's metadata/ directory, and its
// failure mode is the severe one — rebuild treats an empty metadata directory as
// a fatal error for the whole warehouse rather than as one table to skip. The
// object is then read back, because a key that lists and cannot be fetched is
// the same missing table one step later.
func (c *checker) listAfterWrite(ctx context.Context) result {
	const name = "list-after-write"

	// Two namespaces, three tables, shaped exactly like a warehouse: the
	// listing under test walks <root>/<namespace>/<table>/ and nothing else.
	tables := [][2]string{{"ns_one", "table_a"}, {"ns_one", "table_b"}, {"ns_two", "table_a"}}
	for _, t := range tables {
		key := path.Join(c.root, t[0], t[1], "metadata", "00000-r2check.metadata.json")
		if err := c.put(ctx, key, metadataBody); err != nil {
			return result{name: name, err: fmt.Errorf("creating %s: %w", key, err)}
		}
	}

	namespaces, err := c.children(ctx, c.root+"/")
	if err != nil {
		return result{name: name, err: fmt.Errorf("listing namespaces: %w", err)}
	}

	found := make(map[string]bool)
	for _, ns := range namespaces {
		names, err := c.children(ctx, c.root+"/"+ns+"/")
		if err != nil {
			return result{name: name, err: fmt.Errorf("listing tables under %s: %w", ns, err)}
		}
		for _, n := range names {
			found[ns+"/"+n] = true
		}
	}

	var missing []string
	for _, t := range tables {
		if !found[t[0]+"/"+t[1]] {
			missing = append(missing, t[0]+"/"+t[1])
		}
	}
	if len(missing) > 0 {
		return result{name: name, err: fmt.Errorf(
			"%d of %d just-created table prefixes were not returned by the listing rebuild discovers with (%s); rebuild would silently miss those tables",
			len(missing), len(tables), strings.Join(missing, ", "))}
	}

	// The third level, on one of the three tables: the directory rebuild loads a
	// table from, listed without a delimiter, and the object it finds there,
	// fetched.
	sample := tables[0]
	dir := path.Join(c.root, sample[0], sample[1], "metadata") + "/"
	keys, err := c.objects(ctx, dir)
	if err != nil {
		return result{name: name, err: fmt.Errorf("listing %s: %w", dir, err)}
	}
	if len(keys) != 1 {
		return result{name: name, err: fmt.Errorf(
			"%s listed %d objects immediately after one was written there; rebuild treats an empty metadata directory as a fatal error for the whole warehouse, not as one table to skip",
			dir, len(keys))}
	}
	body, err := c.get(ctx, keys[0])
	if err != nil {
		return result{name: name, err: fmt.Errorf("reading %s straight after writing it: %w", keys[0], err)}
	}
	if !bytes.Equal(body, metadataBody) {
		return result{name: name, err: fmt.Errorf("%s read back as %d bytes, not the %d that were written", keys[0], len(body), len(metadataBody))}
	}

	return result{name: name, detail: fmt.Sprintf(
		"all %d just-created table prefixes were discovered by the two-level listing, and %s listed and read back its metadata object immediately",
		len(tables), sample[0]+"/"+sample[1])}
}

// sameKeyOverwrite is check 3: writing the same key twice leaves the second
// body, and one version of it.
//
// Flush retry idempotency rests on this. A batch's object key is a content hash
// over its records, so a retried upload writes the same key again on purpose —
// which is only safe if the provider treats that as an overwrite rather than as
// a second object the table would then see twice.
//
// The count that proves it is a version count, not an object count, and the
// distinction is the whole difference between measuring and asserting. An
// earlier version of this check listed the key with ListObjectsV2 and required
// exactly one entry, which cannot fail: that call returns at most one entry per
// key by construction, so a provider that had retained the first body would have
// been reported as passing. ListObjectVersions is the listing that can tell the
// two apart. Where an endpoint will not answer it, this check says so and claims
// only the read-back, which is what it actually proved.
func (c *checker) sameKeyOverwrite(ctx context.Context) result {
	const name = "same-key overwrite"

	key := path.Join(c.root, "overwrite", "batch.parquet")
	first := []byte(strings.Repeat("first", 200))
	second := []byte(strings.Repeat("second", 100))

	if err := c.put(ctx, key, first); err != nil {
		return result{name: name, err: fmt.Errorf("the first upload: %w", err)}
	}
	if err := c.put(ctx, key, second); err != nil {
		return result{name: name, err: fmt.Errorf("the second upload to the same key: %w", err)}
	}

	got, err := c.get(ctx, key)
	if err != nil {
		return result{name: name, err: fmt.Errorf("reading the key back: %w", err)}
	}
	if !bytes.Equal(got, second) {
		return result{name: name, err: errors.New("the key still carries the first body; a retried upload would not replace what it rewrote")}
	}

	versions, markers, err := c.versions(ctx, key)
	if err != nil {
		return result{name: name, detail: "the key read back as the second body; this endpoint does not answer a version listing, so nothing beyond that read-back is proven"}
	}
	if versions != 1 || markers != 0 {
		return result{name: name, err: fmt.Errorf(
			"after two uploads the key has %d versions and %d delete markers, want exactly one version and no marker; the first body was retained rather than replaced",
			versions, markers)}
	}

	return result{name: name, detail: "the key read back as the second body and carries exactly one version"}
}

// cleanup deletes every key this run may have created, and then checks.
//
// It is a reported step rather than a silent one, and a failure in it fails the
// run: objects left behind in an operator's bucket cost them money and look
// exactly like a table nobody remembers creating. It deletes every key the run
// attempted rather than only the ones it saw succeed — a key that was never
// stored deletes cleanly anyway, and an upload whose response was lost is
// precisely the one worth deleting.
//
// The listing afterwards is the part that makes this a check at all. An earlier
// version printed "nothing left under <root>" without ever looking, which is the
// one place in this program where asserting instead of measuring could cost real
// money: on a versioned bucket every delete would have written a marker and kept
// the body, and the run would have reported PASS over a pile of retained
// objects. The preflight above refuses such a bucket, and this counts what is
// actually there so that the refusal is belt as well as braces.
//
// What it reports when it cannot tell is deliberately weaker than what it
// reports when it can. Keys are named as ones to check by hand, not asserted to
// be stranded: put records a key before it uploads it, so a run whose uploads all
// failed — a mistyped bucket, an expired token — has a full written list and has
// stored nothing at all, and telling that operator they leaked objects would send
// them hunting for things that never existed.
func (c *checker) cleanup(ctx context.Context) result {
	const name = "cleanup"

	var failures []error
	deleted := 0
	for _, key := range c.written {
		if _, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(key),
		}); err != nil {
			failures = append(failures, fmt.Errorf("deleting %s: %w", key, err))

			continue
		}
		deleted++
	}

	left, how, err := c.remaining(ctx)
	if err != nil {
		return result{name: name, err: fmt.Errorf(
			"%d of %d deletes returned, but %s could not be listed afterwards, so whether anything is left there is unknown; these are the keys to check by hand: %s: %w",
			deleted, len(c.written), c.root, strings.Join(c.written, ", "), errors.Join(append(failures, err)...))}
	}
	if left > 0 {
		stranded := fmt.Errorf(
			"%d objects are still under %s after %d of %d deletes returned; they will go on being billed until they are removed by hand",
			left, c.root, deleted, len(c.written))

		// errors.Join of nothing is nil, and a nil wrapped with %w prints as
		// gibberish — so the delete failures are joined on only when there were
		// some. Every object surviving while every delete succeeded is the
		// interesting case, and it must not be reported with a mangled tail.
		return result{name: name, err: errors.Join(append([]error{stranded}, failures...)...)}
	}
	if len(failures) > 0 {
		return result{name: name, detail: fmt.Sprintf(
			"%d keys deleted, %d deletes returned errors, and %s is empty afterwards anyway (%s)",
			deleted, len(failures), c.root, how)}
	}

	return result{name: name, detail: fmt.Sprintf("%d keys deleted, and %s is empty afterwards (%s)", deleted, c.root, how)}
}

// remaining counts what is still under the run root, preferring the listing that
// can see everything.
//
// ListObjectsV2 shows current objects only: on a versioned bucket it hides both
// the non-current versions a delete left behind and the delete markers that hid
// them, which is exactly the state this is looking for. ListObjectVersions sees
// all three. An endpoint that will not answer it is not treated as a failure —
// the count falls back to the plain listing and the returned description says
// what was and was not looked at, so a weaker measurement is reported as a
// weaker measurement rather than as the strong one.
func (c *checker) remaining(ctx context.Context) (int, string, error) {
	if versions, markers, err := c.versions(ctx, c.root+"/"); err == nil {
		return versions + markers, fmt.Sprintf("%d versions and %d delete markers", versions, markers), nil
	}

	keys, err := c.objects(ctx, c.root+"/")
	if err != nil {
		return 0, "", err
	}

	return len(keys), fmt.Sprintf("%d keys; this endpoint does not answer a version listing, so non-current versions were not looked at", len(keys)), nil
}

// put uploads one object, having first recorded the key for cleanup.
//
// The order is deliberate and is the whole reason this is a method rather than
// an inline call. An upload whose response is lost — a dropped connection, a
// cancelled context, a timeout — may well have stored the object anyway, so a
// key recorded only after a *successful* PutObject is exactly the key that gets
// left behind in an operator's bucket. Recording first means cleanup tries to
// delete everything this run may have created; deleting a key that was never
// written costs one request and no harm.
//
// Each key is recorded once even when it is written twice.
func (c *checker) put(ctx context.Context, key string, body []byte) error {
	if !slices.Contains(c.written, key) {
		c.written = append(c.written, key)
	}

	_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})

	return err
}

// get fetches one object whole.
func (c *checker) get(ctx context.Context, key string) ([]byte, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = out.Body.Close() }()

	return io.ReadAll(out.Body)
}

// children returns the immediate child prefix names under a prefix, which is
// exactly the delimiter listing catalog rebuild discovers namespaces and tables
// with.
func (c *checker) children(ctx context.Context, prefix string) ([]string, error) {
	pager := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(c.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	})

	var names []string
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, p := range page.CommonPrefixes {
			name := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(p.Prefix), prefix), "/")
			if name != "" {
				names = append(names, name)
			}
		}
	}

	return names, nil
}

// objects lists the object keys under a prefix, with no delimiter — the listing
// rebuild loads one table's metadata directory with.
func (c *checker) objects(ctx context.Context, prefix string) ([]string, error) {
	pager := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})

	var keys []string
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}

	return keys, nil
}

// versions counts the object versions and delete markers under a prefix.
//
// This is the listing that can distinguish "the object is gone" from "the object
// is hidden behind a delete marker and still on the bill", which no
// ListObjectsV2 can. Not every S3-compatible endpoint implements it, so every
// caller here treats an error as "this cannot be measured that way" rather than
// as a failure of the thing being measured.
func (c *checker) versions(ctx context.Context, prefix string) (int, int, error) {
	pager := s3.NewListObjectVersionsPaginator(c.client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})

	objects, markers := 0, 0
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, 0, err
		}
		objects += len(page.Versions)
		markers += len(page.DeleteMarkers)
	}

	return objects, markers, nil
}
