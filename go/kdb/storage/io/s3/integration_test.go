//go:build s3integration

package s3

import (
	"context"
	"os"
	"testing"
)

func TestOpenReplicaSink_localStack(t *testing.T) {
	endpoint := os.Getenv("KDB_S3_ENDPOINT")
	bucket := os.Getenv("KDB_S3_BUCKET")
	if endpoint == "" || bucket == "" {
		t.Skip("set KDB_S3_ENDPOINT and KDB_S3_BUCKET for integration test")
	}
	cfg := Config{
		Bucket:       bucket,
		Region:       "us-east-1",
		Endpoint:     endpoint,
		PathStyle:    true,
		EnsureBucket: true,
	}
	sink, err := OpenReplicaSink(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	seg := "ns/integration/delta/test-seg"
	if err := sink.PutSegment(seg, []byte("integration")); err != nil {
		t.Fatal(err)
	}
	if err := sink.DeleteSegment(seg); err != nil {
		t.Fatal(err)
	}
}
