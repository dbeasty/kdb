package s3

import (
	"os"
	"strings"
)

// Config configures an S3-compatible replica sink.
type Config struct {
	Bucket       string
	Region       string
	Prefix       string
	Endpoint     string
	PathStyle    bool
	EnsureBucket bool
}

// ArchiveConfigFromEnv builds the *archive* tier's config from environment variables, and
// returns nil when KDB_S3_ARCHIVE_BUCKET is unset.
//
// A separate destination from the replica, deliberately. The two hold different things: a
// replica follows the primary's deletions, an archive keeps what the primary reclaims. Pointing
// them at one location would have the replica's deletions remove exactly what the archive is
// there to retain, so the bucket is its own variable rather than a mode on the replica's.
//
// Everything except the bucket falls back to the replica's setting, since region, endpoint and
// credentials are usually properties of the object store rather than of the copy.
func ArchiveConfigFromEnv() *Config {
	bucket := strings.TrimSpace(os.Getenv("KDB_S3_ARCHIVE_BUCKET"))
	if bucket == "" {
		return nil
	}
	region := firstNonEmpty(os.Getenv("KDB_S3_ARCHIVE_REGION"), os.Getenv("KDB_S3_REGION"), "us-east-1")
	endpoint := firstNonEmpty(os.Getenv("KDB_S3_ARCHIVE_ENDPOINT"), os.Getenv("KDB_S3_ENDPOINT"))
	prefix := firstNonEmpty(os.Getenv("KDB_S3_ARCHIVE_PREFIX"), os.Getenv("KDB_S3_PREFIX"))
	pathStyle := endpoint != "" || strings.EqualFold(os.Getenv("KDB_S3_PATH_STYLE"), "true")
	ensure := strings.EqualFold(os.Getenv("KDB_S3_ENSURE_BUCKET"), "true") || endpoint != ""
	return &Config{
		Bucket:       bucket,
		Region:       region,
		Prefix:       prefix,
		Endpoint:     endpoint,
		PathStyle:    pathStyle,
		EnsureBucket: ensure,
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

// ConfigFromEnv builds config from environment variables.
// Returns nil when KDB_S3_BUCKET is unset (S3 replica disabled).
func ConfigFromEnv() *Config {
	bucket := strings.TrimSpace(os.Getenv("KDB_S3_BUCKET"))
	if bucket == "" {
		return nil
	}
	region := strings.TrimSpace(os.Getenv("KDB_S3_REGION"))
	if region == "" {
		region = "us-east-1"
	}
	endpoint := strings.TrimSpace(os.Getenv("KDB_S3_ENDPOINT"))
	pathStyle := endpoint != "" || strings.EqualFold(os.Getenv("KDB_S3_PATH_STYLE"), "true")
	ensure := strings.EqualFold(os.Getenv("KDB_S3_ENSURE_BUCKET"), "true") || endpoint != ""
	return &Config{
		Bucket:       bucket,
		Region:       region,
		Prefix:       strings.TrimSpace(os.Getenv("KDB_S3_PREFIX")),
		Endpoint:     endpoint,
		PathStyle:    pathStyle,
		EnsureBucket: ensure,
	}
}
