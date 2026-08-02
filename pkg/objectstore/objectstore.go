// Package objectstore wraps S3-compatible storage for job payloads and their
// outputs.
//
// It uses the real AWS SDK against MinIO rather than MinIO's own client. That
// is a deliberate choice: the code here is genuine S3 code -- same API, same
// pagination, same presigning -- so moving to actual S3 is a configuration
// change rather than a rewrite, and the skills it demonstrates are the
// transferable ones.
//
// The alternative considered and rejected was a shared Docker volume. It is
// simpler, but it quietly assumes every worker sees the same filesystem, which
// is exactly the assumption a distributed system should not make: the moment
// workers span two hosts, half the derivatives become invisible to the process
// serving them.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// ErrNotFound is returned when an object does not exist.
var ErrNotFound = errors.New("object not found")

// Config describes how to reach the store.
type Config struct {
	// Endpoint is the S3 endpoint. Empty means real AWS S3.
	Endpoint string
	Region   string
	Bucket   string

	AccessKey string
	SecretKey string

	// PathStyle addresses buckets as endpoint/bucket rather than
	// bucket.endpoint. Required for MinIO, which has no wildcard DNS, and
	// harmless against real S3.
	PathStyle bool

	// PublicEndpoint is the address a browser should use, when it differs from
	// the one the workers use.
	//
	// This exists because of a genuinely confusing failure mode. Inside docker
	// compose the workers reach MinIO at http://minio:9000, but a presigned URL
	// built from that host is useless to a browser on the host machine, which
	// cannot resolve "minio". Presigned URLs are signed over the host header,
	// so the URL cannot simply be string-rewritten afterwards -- the signature
	// would no longer match. A separate presigning client bound to the public
	// endpoint is the correct fix.
	PublicEndpoint string
}

// Store is an S3-compatible object store client.
type Store struct {
	client *s3.Client
	// presign is bound to PublicEndpoint so generated URLs are reachable from
	// a browser. Identical to client when no public endpoint is configured.
	presign *s3.PresignClient
	bucket  string
}

// New connects to the store and ensures the bucket exists.
func New(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("objectstore: bucket is required")
	}
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	client, err := buildClient(ctx, cfg, cfg.Endpoint, region)
	if err != nil {
		return nil, err
	}

	presignSource := client
	if cfg.PublicEndpoint != "" && cfg.PublicEndpoint != cfg.Endpoint {
		presignSource, err = buildClient(ctx, cfg, cfg.PublicEndpoint, region)
		if err != nil {
			return nil, fmt.Errorf("build presigning client: %w", err)
		}
	}

	s := &Store{
		client:  client,
		presign: s3.NewPresignClient(presignSource),
		bucket:  cfg.Bucket,
	}

	if err := s.ensureBucket(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func buildClient(ctx context.Context, cfg Config, endpoint, region string) (*s3.Client, error) {
	loaded, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return s3.NewFromConfig(loaded, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = cfg.PathStyle
	}), nil
}

// ensureBucket creates the bucket if it is missing, so a fresh `docker compose
// up` works with no manual setup step.
func (s *Store) ensureBucket(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err == nil {
		return nil
	}

	_, err = s.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(s.bucket),
	})
	if err != nil {
		// Two workers starting together will race here, and the loser sees
		// BucketAlreadyOwnedByYou. That is success, not failure.
		var owned *types.BucketAlreadyOwnedByYou
		var exists *types.BucketAlreadyExists
		if errors.As(err, &owned) || errors.As(err, &exists) {
			return nil
		}
		return fmt.Errorf("create bucket %q: %w", s.bucket, err)
	}
	return nil
}

// Bucket returns the configured bucket name.
func (s *Store) Bucket() string { return s.bucket }

// Put uploads an object and returns its size in bytes.
func (s *Store) Put(
	ctx context.Context,
	key string,
	body io.Reader,
	contentType string,
) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        body,
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("put object %q: %w", key, err)
	}
	return nil
}

// Get downloads an object. The caller must close the returned reader.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("get object %q: %w", key, err)
	}
	return out.Body, nil
}

// GetBytes downloads an object fully into memory.
//
// Bounded by maxBytes so a hostile or accidental multi-gigabyte upload cannot
// exhaust a worker's heap. A worker that OOMs takes every other job it was
// running down with it, and those jobs then have to be reaped and retried, so
// one bad input becomes a fleet-wide event. Failing this one job loudly is much
// cheaper.
func (s *Store) GetBytes(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	body, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = body.Close() }()

	// Read one byte past the limit so exceeding it is detectable rather than
	// silently truncating the image and producing a corrupt derivative.
	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read object %q: %w", key, err)
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("object %q exceeds the %d byte limit", key, maxBytes)
	}
	return data, nil
}

// Exists reports whether an object is present.
func (s *Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, fmt.Errorf("head object %q: %w", key, err)
}

// Delete removes an object. Deleting a missing key is not an error, which keeps
// cleanup paths idempotent.
func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil && !isNotFound(err) {
		return fmt.Errorf("delete object %q: %w", key, err)
	}
	return nil
}

// PresignGet returns a time-limited URL a browser can fetch directly.
//
// Serving images through presigned URLs rather than proxying them keeps the
// Rails process out of the bytes path entirely -- it hands out a URL and the
// browser talks to object storage. Proxying would tie up a Puma thread for the
// duration of every image download.
func (s *Store) PresignGet(ctx context.Context, key string, ttl time.Duration) (string, error) {
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}

	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", fmt.Errorf("presign get for %q: %w", key, err)
	}
	return req.URL, nil
}

// Ping verifies reachability, for readiness checks.
func (s *Store) Ping(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	return err
}

// isNotFound recognises a missing object or bucket.
//
// The SDK reports this inconsistently -- typed NoSuchKey from GetObject, but a
// bare 404 from HeadObject, which has no response body to decode into a typed
// error. Both have to be handled, or Exists returns a spurious failure for
// every absent key.
func isNotFound(err error) bool {
	var noKey *types.NoSuchKey
	if errors.As(err, &noKey) {
		return true
	}
	var noBucket *types.NoSuchBucket
	if errors.As(err, &noBucket) {
		return true
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return true
	}

	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "NoSuchBucket", "404":
			return true
		}
	}
	return strings.Contains(err.Error(), "status code: 404")
}
