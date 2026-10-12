//go:build kdb_lite

package s3

import (
	"context"
	"errors"
)

// errUnavailable is what every AWSBlobStore operation reports in a kdb_lite build.
//
// kdb_lite is the in-process engine compiled to WebAssembly (go/wasm/embed): it has no network to
// reach S3 over, and linking the AWS SDK adds tens of megabytes to the binary a browser downloads.
// The type stays so callers compile, but configuring S3 yields an error here, never silence.
var errUnavailable = errors.New("s3: the AWS blob store is not available in this build (kdb_lite)")

// AWSBlobStore is a placeholder in a kdb_lite build.
type AWSBlobStore struct{}

// NewAWSBlobStore always fails in a kdb_lite build.
func NewAWSBlobStore(_ context.Context, _ Config) (*AWSBlobStore, error) { return nil, errUnavailable }

func (*AWSBlobStore) Put(context.Context, string, []byte) error      { return errUnavailable }
func (*AWSBlobStore) Get(context.Context, string) ([]byte, error)    { return nil, errUnavailable }
func (*AWSBlobStore) Delete(context.Context, string) error           { return errUnavailable }
func (*AWSBlobStore) List(context.Context, string) ([]string, error) { return nil, errUnavailable }
