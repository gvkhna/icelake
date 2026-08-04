package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gvkhna/icelake"
)

// requirement says when a variable has to be set, which is the one thing about
// this configuration that is not the same in both modes.
type requirement int

const (
	// optional means the variable has a default, or is genuinely optional.
	optional requirement = iota
	// always means the variable is required whatever else is set.
	always
	// bucketOnly means the variable is required in bucket mode and must be
	// absent in local-only mode. Both halves are refusals: a store told to reach
	// no bucket while holding a bucket's credentials is a caller who thinks they
	// configured something they did not, and the library refuses the same
	// combination for the same reason.
	bucketOnly
)

// envVar is one line of this command's configuration surface.
//
// The table below is the only place a variable's name, default and meaning are
// written down. Reading the environment, refusing a bad configuration, printing
// the help, and the embedded manual all come from it — which is what makes "the
// documentation cannot drift from the configuration" a property of the code
// rather than a promise, and what a test asserts by iterating this table rather
// than retyping the names.
type envVar struct {
	// name is the variable, as an operator spells it.
	name string
	// def is the default, rendered exactly as a value would be written. Empty
	// means there is no default.
	def string
	// need says when the variable must be set.
	need requirement
	// doc is one line about what it does, for the help and the manual.
	doc string
}

// The configuration surface. Order is the order it is printed in, grouped the
// way an operator meets it: where state lives, what shape the data is, where it
// goes, and then the knobs.
var (
	envDataDir = envVar{
		name: "ICELAKE_DATA_DIR", need: always,
		doc: "directory holding staging.db, catalog.db and the Parquet cache",
	}
	envStagingPath = envVar{
		name: "ICELAKE_STAGING_PATH", def: "<data dir>/staging.db",
		doc: "write-ahead staging database; back this up",
	}
	envCatalogPath = envVar{
		name: "ICELAKE_CATALOG_PATH", def: "<data dir>/catalog.db", need: bucketOnly,
		doc: "local Iceberg catalog database; rebuildable from the bucket",
	}
	envCacheDir = envVar{
		name: "ICELAKE_CACHE_DIR", def: "<data dir>/cache",
		doc: "local Parquet cache; DuckDB can read_parquet() it directly",
	}
	envSchemaFile = envVar{
		name: "ICELAKE_SCHEMA_FILE", need: always,
		doc: "JSON schema document declaring the tables",
	}
	envEndpoint = envVar{
		name: "ICELAKE_ENDPOINT", need: bucketOnly,
		doc: "S3-compatible endpoint base URL, including scheme",
	}
	envBucket = envVar{
		name: "ICELAKE_BUCKET", need: bucketOnly,
		doc: "bucket the warehouse lives in",
	}
	envPrefix = envVar{
		name: "ICELAKE_PREFIX", need: bucketOnly,
		doc: "warehouse key prefix each table hangs under",
	}
	envRegion = envVar{
		name: "ICELAKE_REGION", def: "auto",
		doc: "region to sign requests for; R2 wants auto",
	}
	envAccessKeyID = envVar{
		name: "ICELAKE_ACCESS_KEY_ID", need: bucketOnly,
		doc: "access key id",
	}
	envSecretAccessKey = envVar{
		name: "ICELAKE_SECRET_ACCESS_KEY", need: bucketOnly,
		doc: "secret access key",
	}
	envLocalOnly = envVar{
		name: "ICELAKE_LOCAL_ONLY", def: "false",
		doc: "write to the local cache with no bucket at all (true or false)",
	}
	envFlushInterval = envVar{
		name: "ICELAKE_FLUSH_INTERVAL", def: "15m",
		doc: "longest a batch may sit unflushed",
	}
	envFlushMaxBytes = envVar{
		name: "ICELAKE_FLUSH_MAX_BYTES", def: "128MB",
		doc: "bytes a batch may accumulate before it is flushed",
	}
	envFlushMaxRecords = envVar{
		name: "ICELAKE_FLUSH_MAX_RECORDS", def: "100000",
		doc: "records a batch may accumulate before it is flushed",
	}
	envCacheMaxAge = envVar{
		name: "ICELAKE_CACHE_MAX_AGE", def: "720h", need: bucketOnly,
		doc: "how long a committed cache file is kept",
	}
	envCacheMaxBytes = envVar{
		name: "ICELAKE_CACHE_MAX_BYTES", def: "10GB", need: bucketOnly,
		doc: "total size of the committed files in the cache",
	}
	envStagingMaxRecords = envVar{
		name: "ICELAKE_STAGING_MAX_RECORDS", def: "5000000",
		doc: "accepted-but-uncommitted records before Insert refuses",
	}
	envStagingMaxBytes = envVar{
		name: "ICELAKE_STAGING_MAX_BYTES", def: "4GB",
		doc: "accepted-but-uncommitted bytes before Insert refuses",
	}
	envZSTDLevel = envVar{
		name: "ICELAKE_ZSTD_LEVEL", def: "19",
		doc: "Parquet compression level, 1 to 22",
	}
	envShutdownTimeout = envVar{
		name: "ICELAKE_SHUTDOWN_TIMEOUT", def: "30s",
		doc: "how long a drain may take after end of input or a signal",
	}
	envMaxLineBytes = envVar{
		name: "ICELAKE_MAX_LINE_BYTES", def: "16MB",
		doc: "longest input line accepted",
	}
)

// envVars is the whole surface, in the order it is documented.
//
// A variable that is not in this slice is a variable no help text mentions and
// no test checks, so anything this command reads belongs here.
var envVars = []envVar{
	envDataDir, envStagingPath, envCatalogPath, envCacheDir, envSchemaFile,
	envEndpoint, envBucket, envPrefix, envRegion, envAccessKeyID, envSecretAccessKey,
	envLocalOnly,
	envFlushInterval, envFlushMaxBytes, envFlushMaxRecords,
	envCacheMaxAge, envCacheMaxBytes,
	envStagingMaxRecords, envStagingMaxBytes,
	envZSTDLevel, envShutdownTimeout, envMaxLineBytes,
}

// set reports whether an operator supplied this variable, as opposed to it
// taking its default. An empty value counts as absent: a variable set to nothing
// is a variable nobody meant to set, and treating the two alike is what keeps
// "unset it to go back to the default" true.
func (v envVar) set() bool { return os.Getenv(v.name) != "" }

// value returns what the operator supplied, or the default.
func (v envVar) value() string {
	if s := os.Getenv(v.name); s != "" {
		return s
	}

	return v.def
}

// settings is the resolved configuration: everything this command needs, with
// every default filled in and every value already parsed.
type settings struct {
	dataDir     string
	stagingPath string
	catalogPath string
	cacheDir    string
	schemaFile  string

	endpoint        string
	bucket          string
	prefix          string
	region          string
	accessKeyID     string
	secretAccessKey string
	localOnly       bool

	flushInterval    time.Duration
	flushMaxBytes    int64
	flushMaxRecords  int
	cacheMaxAge      time.Duration
	cacheMaxBytes    int64
	stagingMaxRecord int64
	stagingMaxBytes  int64
	zstdLevel        int
	shutdownTimeout  time.Duration
	maxLineBytes     int
}

// mode says which of the two subcommands is asking for the configuration, which
// is the only thing that changes what is required.
type mode int

const (
	// forRun is the daemon: it needs a schema document, because a daemon with no
	// tables declared has nothing to write to.
	forRun mode = iota
	// forRebuild is the catalog rebuild: it needs the bucket and the catalog
	// path and nothing else. Requiring a schema file to rebuild a catalog would
	// be requiring a file the operation never opens, which is exactly the kind
	// of configuration that makes disaster recovery harder than it has to be.
	forRebuild
)

// readSettings resolves the whole configuration and refuses every problem with
// it at once.
//
// Violations are collected rather than returned first-one-wins, matching the
// library's own configuration rule: an operator fixing a unit file should learn
// everything wrong with it in one run. The whole set comes back wrapped in
// [errUsage], which is what makes a configuration refusal exit 2 rather than 1 —
// a supervisor needs to tell "restart me" from "restarting will not help".
func readSettings(m mode) (settings, error) {
	var s settings
	var bad []error

	refuse := func(v envVar, rule string) {
		bad = append(bad, fmt.Errorf("%s %s", v.name, rule))
	}

	localOnly, err := parseBool(envLocalOnly)
	if err != nil {
		bad = append(bad, err)
	}
	s.localOnly = localOnly

	// The required-and-forbidden matrix, which is the same one the library
	// enforces on the Config this builds — checked here so that a mistake in a
	// unit file is a usage refusal naming environment variables rather than a
	// library error naming Go fields.
	for _, v := range envVars {
		// A rebuild opens no schema document. Requiring the file anyway would be
		// requiring one the operation never reads, which is exactly the kind of
		// configuration that makes disaster recovery harder than it has to be.
		if m == forRebuild && v == envSchemaFile {
			continue
		}

		switch v.need {
		case always:
			if !v.set() {
				refuse(v, "is required")
			}
		case bucketOnly:
			switch {
			case s.localOnly && v.set():
				refuse(v, "must not be set when "+envLocalOnly.name+" is true")
			// A variable with a default is never missing; what makes these
			// bucket-only is the other half of the matrix above.
			case !s.localOnly && !v.set() && v.def == "":
				refuse(v, "is required unless "+envLocalOnly.name+" is true")
			}
		case optional:
		}
	}

	s.dataDir = envDataDir.value()
	s.schemaFile = envSchemaFile.value()
	s.stagingPath = derived(envStagingPath, s.dataDir, "staging.db")
	s.cacheDir = derived(envCacheDir, s.dataDir, "cache")
	if !s.localOnly {
		s.catalogPath = derived(envCatalogPath, s.dataDir, "catalog.db")
	}

	s.endpoint = envEndpoint.value()
	s.bucket = envBucket.value()
	s.prefix = envPrefix.value()
	s.region = envRegion.value()
	s.accessKeyID = envAccessKeyID.value()
	s.secretAccessKey = envSecretAccessKey.value()

	collect := func(err error) {
		if err != nil {
			bad = append(bad, err)
		}
	}

	s.flushInterval, err = parseDuration(envFlushInterval)
	collect(err)
	s.flushMaxBytes, err = parseByteSize(envFlushMaxBytes)
	collect(err)
	s.flushMaxRecords, err = parseInt(envFlushMaxRecords)
	collect(err)
	s.stagingMaxRecord, err = parseInt64(envStagingMaxRecords)
	collect(err)
	s.stagingMaxBytes, err = parseByteSize(envStagingMaxBytes)
	collect(err)
	s.zstdLevel, err = parseInt(envZSTDLevel)
	collect(err)
	s.shutdownTimeout, err = parseDuration(envShutdownTimeout)
	collect(err)

	maxLine, err := parseByteSize(envMaxLineBytes)
	collect(err)
	s.maxLineBytes = int(maxLine)

	// Retention bounds exist only in bucket mode. In local-only nothing is ever
	// uploaded, so nothing is ever evictable and a bound is a knob that cannot
	// turn: the library refuses one, and this passes zero rather than quietly
	// dropping a value the operator wrote down.
	if !s.localOnly {
		s.cacheMaxAge, err = parseDuration(envCacheMaxAge)
		collect(err)
		s.cacheMaxBytes, err = parseByteSize(envCacheMaxBytes)
		collect(err)
	}

	if s.zstdLevel < 1 || s.zstdLevel > 22 {
		refuse(envZSTDLevel, "must be between 1 and 22")
	}
	if s.maxLineBytes < 1 {
		refuse(envMaxLineBytes, "must be positive")
	}

	if len(bad) > 0 {
		return settings{}, fmt.Errorf("%w:\n%w", errUsage, errors.Join(bad...))
	}

	return s, nil
}

// derived returns an explicitly configured path, or one under the data
// directory. It is the whole of what ICELAKE_DATA_DIR buys: three paths from
// one, with each still overridable on its own.
func derived(v envVar, dataDir, name string) string {
	if v.set() {
		return v.value()
	}
	if dataDir == "" {
		return ""
	}

	return filepath.Join(dataDir, name)
}

// config turns the resolved settings into the library configuration.
//
// Every value here is passed straight through. The command decides nothing about
// batching, retention, replay or backpressure — those are library decisions it
// carries, which is the rule every app in this tree follows.
func (s settings) config(onFlushError func(icelake.FlushError)) icelake.Config {
	return icelake.Config{
		StagingPath:       s.stagingPath,
		CatalogPath:       s.catalogPath,
		CacheDir:          s.cacheDir,
		CacheMaxAge:       s.cacheMaxAge,
		CacheMaxBytes:     s.cacheMaxBytes,
		LocalOnly:         s.localOnly,
		Endpoint:          s.endpoint,
		Bucket:            s.bucket,
		WarehousePrefix:   s.prefix,
		Region:            s.region,
		AccessKeyID:       s.accessKeyID,
		SecretAccessKey:   s.secretAccessKey,
		FlushMaxRecords:   s.flushMaxRecords,
		FlushMaxBytes:     s.flushMaxBytes,
		FlushInterval:     s.flushInterval,
		StagingMaxRecords: s.stagingMaxRecord,
		StagingMaxBytes:   s.stagingMaxBytes,
		ZSTDLevel:         s.zstdLevel,
		OnFlushError:      onFlushError,
	}
}

// describe renders the resolved configuration for the line this command prints
// at startup, with the secret reduced to whether there is one.
//
// Printing it at all is the whole of this command's observability, and printing
// it once at startup is what makes a support question answerable: what a daemon
// is actually configured with, rather than what a unit file was believed to say.
// The secret is never printed even in part — a redaction that shows a prefix is
// a redaction that leaks a prefix — so it is reported as set or unset, which is
// the only thing an operator debugging a configuration needs from it.
func (s settings) describe() string {
	credential := "unset"
	if s.secretAccessKey != "" {
		credential = "set"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "staging=%s cache=%s", s.stagingPath, s.cacheDir)
	if s.localOnly {
		fmt.Fprintf(&b, " local-only=true")
	} else {
		fmt.Fprintf(&b, " catalog=%s endpoint=%s bucket=%s prefix=%s region=%s access-key-id=%s secret-access-key=%s",
			s.catalogPath, s.endpoint, s.bucket, s.prefix, s.region, s.accessKeyID, credential)
		fmt.Fprintf(&b, " cache-max-age=%s cache-max-bytes=%d", s.cacheMaxAge, s.cacheMaxBytes)
	}
	fmt.Fprintf(&b, " flush-interval=%s flush-max-bytes=%d flush-max-records=%d",
		s.flushInterval, s.flushMaxBytes, s.flushMaxRecords)
	fmt.Fprintf(&b, " staging-max-records=%d staging-max-bytes=%d zstd-level=%d",
		s.stagingMaxRecord, s.stagingMaxBytes, s.zstdLevel)
	fmt.Fprintf(&b, " shutdown-timeout=%s max-line-bytes=%d", s.shutdownTimeout, s.maxLineBytes)

	return b.String()
}

// parseBool reads a strict boolean: exactly true or false, in the spellings
// strconv accepts, and nothing else. A misspelled boolean is refused rather than
// read as false, because a daemon that silently decided it was not local-only
// would go looking for a bucket that was never configured.
func parseBool(v envVar) (bool, error) {
	b, err := strconv.ParseBool(v.value())
	if err != nil {
		return false, fmt.Errorf("%s is %q, which is not a boolean", v.name, v.value())
	}

	return b, nil
}

// parseDuration reads a Go duration and refuses a negative one.
func parseDuration(v envVar) (time.Duration, error) {
	d, err := time.ParseDuration(v.value())
	if err != nil {
		return 0, fmt.Errorf("%s is %q, which is not a duration such as 15m or 720h", v.name, v.value())
	}
	if d < 0 {
		return 0, fmt.Errorf("%s is %q, which is negative", v.name, v.value())
	}

	return d, nil
}

// parseInt and parseInt64 read a plain count.
func parseInt(v envVar) (int, error) {
	n, err := strconv.Atoi(v.value())
	if err != nil {
		return 0, fmt.Errorf("%s is %q, which is not a whole number", v.name, v.value())
	}

	return n, nil
}

func parseInt64(v envVar) (int64, error) {
	n, err := strconv.ParseInt(v.value(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s is %q, which is not a whole number", v.name, v.value())
	}

	return n, nil
}

// byteUnits are the suffixes a size may carry, and every one of them is a power
// of 1024.
//
// That is stated in the manual rather than left to be discovered, because the
// two conventions genuinely disagree — a disk vendor's GB is 10^9 — and a
// configuration that silently meant one when an operator meant the other would
// be wrong by seven percent and rising. There is no fractional form: "1.5GB" is
// refused rather than rounded, so that every size in a unit file is an exact
// number of bytes somebody can compute.
var byteUnits = []struct {
	suffix string
	scale  int64
}{
	{"TB", 1 << 40},
	{"GB", 1 << 30},
	{"MB", 1 << 20},
	{"KB", 1 << 10},
	{"B", 1},
}

// parseByteSize reads a size in bytes, with an optional power-of-1024 suffix.
func parseByteSize(v envVar) (int64, error) {
	text := strings.TrimSpace(v.value())
	refuse := func() (int64, error) {
		return 0, fmt.Errorf("%s is %q, which is not a size such as 512, 64KB, 128MB or 4GB (powers of 1024, no fractions)",
			v.name, v.value())
	}

	upper := strings.ToUpper(text)
	scale := int64(1)
	for _, u := range byteUnits {
		if digits, ok := strings.CutSuffix(upper, u.suffix); ok {
			upper, scale = digits, u.scale

			break
		}
	}
	if upper == "" {
		return refuse()
	}

	n, err := strconv.ParseInt(upper, 10, 64)
	if err != nil || n < 0 {
		return refuse()
	}
	// Multiplying first and checking afterwards would wrap silently, which for a
	// staging ceiling means a bound far smaller than the one that was asked for.
	if n > (1<<62)/scale {
		return 0, fmt.Errorf("%s is %q, which is larger than this program can address", v.name, v.value())
	}

	return n * scale, nil
}
