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
