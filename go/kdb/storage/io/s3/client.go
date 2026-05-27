package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// AWSBlobStore implements BlobStore with the AWS SDK v2 S3 client.
type AWSBlobStore struct {
	client *s3.Client
	bucket string
}

// NewAWSBlobStore opens an S3 client from config.
func NewAWSBlobStore(ctx context.Context, cfg Config) (*AWSBlobStore, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3: bucket required")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
	}
	if cfg.Endpoint != "" {
		accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		if accessKey == "" {
			accessKey = "test"
		}
		if secretKey == "" {
			secretKey = "test"
		}
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3: load config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(strings.TrimRight(cfg.Endpoint, "/"))
		}
		if cfg.PathStyle || cfg.Endpoint != "" {
			o.UsePathStyle = true
		}
	})

	store := &AWSBlobStore{client: client, bucket: cfg.Bucket}
	if cfg.EnsureBucket {
		if err := store.ensureBucket(ctx); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func (s *AWSBlobStore) ensureBucket(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)})
	if err == nil {
		return nil
	}
	_, err = s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(s.bucket)})
	if err != nil {
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if errors.As(err, &owned) || errors.As(err, &exists) {
			return nil
		}
		msg := err.Error()
		if strings.Contains(msg, "BucketAlreadyOwnedByYou") || strings.Contains(msg, "BucketAlreadyExists") {
			return nil
		}
		return fmt.Errorf("s3: create bucket %q: %w", s.bucket, err)
	}
	return nil
}

func (s *AWSBlobStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("s3 put %q: %w", key, err)
	}
	return nil
}

func (s *AWSBlobStore) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get %q: %w", key, err)
	}
	defer out.Body.Close()
	return io.ReadAll(out.Body)
}

func (s *AWSBlobStore) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 delete %q: %w", key, err)
	}
	return nil
}

func (s *AWSBlobStore) List(ctx context.Context, prefix string) ([]string, error) {
	var out []string
	var token *string
	for {
		resp, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("s3 list %q: %w", prefix, err)
		}
		for _, obj := range resp.Contents {
			if obj.Key != nil {
				out = append(out, *obj.Key)
			}
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
	}
	return out, nil
}
