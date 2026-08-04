// Package s3cfg builds the one AWS configuration every icelake request is
// signed with, and validates the credential form a caller supplied.
//
// It exists because two entry points need identical behaviour here and must not
// drift: the write path's Config, and the catalog-rebuild tool's own options. A
// recovery tool reaching a bucket with subtly different settings than the writer
// that filled it is a worse version of the same problem the settings exist to
// solve, so the construction lives in one place and both callers spell the same
// fields the same way.
package s3cfg

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/gvkhna/icelake/internal/errdef"
)

// DefaultRegion is the region icelake signs requests for when none is given. It
// is what Cloudflare R2 requires; the field stays overridable so a test
// substrate, or any other S3-compatible endpoint, can pass its own.
const DefaultRegion = "auto"

// Settings is the subset of a caller's configuration that decides how icelake
// reaches object storage. The field names are deliberately the ones both public
// option structs use, so a violation reported against one of them names a field
// the caller can find.
type Settings struct {
	// Endpoint is the S3-compatible endpoint's base URL, including scheme.
	Endpoint string
	// Region is the region requests are signed for. Empty means
	// [DefaultRegion].
	Region string
	// AccessKeyID and SecretAccessKey are static credentials.
	AccessKeyID     string
	SecretAccessKey string
	// Credentials is a pre-built credential provider, the alternative to the
	// two key strings.
	Credentials aws.CredentialsProvider
}

// ValidateCredentials returns one [errdef.ConfigFieldError] per problem with the
// credential fields, and nothing at all when exactly one form is supplied.
//
// Exactly one of the two forms is required: two explicit strings, or one
// pre-built provider. Supplying both, or neither, is a configuration error.
// icelake never falls back to the ambient AWS default credential chain, because
// a library quietly picking up whatever credentials happen to be lying around is
// how a service writes to the wrong bucket — and a recovery tool doing it is how
// one rebuilds a catalog from someone else's.
func (s Settings) ValidateCredentials() []error {
	hasKeys := s.AccessKeyID != "" || s.SecretAccessKey != ""

	switch {
	case hasKeys && s.Credentials != nil:
		return []error{errdef.ConfigFieldError{
			Field: "Credentials",
			Rule:  "must not be set alongside AccessKeyID and SecretAccessKey; supply exactly one credential form",
		}}
	case !hasKeys && s.Credentials == nil:
		return []error{errdef.ConfigFieldError{
			Field: "Credentials",
			Rule:  "must be set, or AccessKeyID and SecretAccessKey must be; icelake never falls back to ambient credentials",
		}}
	case hasKeys && (s.AccessKeyID == "" || s.SecretAccessKey == ""):
		return []error{errdef.ConfigFieldError{
			Field: "AccessKeyID",
			Rule:  "must be set together with SecretAccessKey",
		}}
	}

	return nil
}

// Build returns the AWS configuration every request icelake makes is signed
// with, together with the HTTP transport it owns.
//
// Two settings are not incidental. Checksum calculation and validation are both
// pinned to "when required" rather than the SDK's newer "when supported"
// default, because R2 answers the checksum headers that default sends with 501
// Not Implemented. And the credentials are exactly what the caller supplied,
// never the ambient chain.
//
// The transport is icelake's own rather than the SDK's implicit default, so that
// a caller which is finished with an endpoint can hand its sockets back instead
// of leaving them idle for the transport's own keep-alive window. iceberg-go
// reuses whatever HTTP client the configuration carries, so this one transport
// serves every request that library makes as well.
func (s Settings) Build() (aws.Config, *http.Transport) {
	region := s.Region
	if region == "" {
		region = DefaultRegion
	}
	provider := s.Credentials
	if provider == nil {
		provider = credentials.NewStaticCredentialsProvider(s.AccessKeyID, s.SecretAccessKey, "")
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}

	return aws.Config{
		Region:                     region,
		Credentials:                provider,
		BaseEndpoint:               aws.String(s.Endpoint),
		HTTPClient:                 &http.Client{Transport: transport},
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}, transport
}
