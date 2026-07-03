// Package storage owns the S3-compatible artifact adapter (FR-011,
// EW-STORAGE-001..004: MinIO locally; S3/R2/OSS/COS in production) and the
// artifact-manifest builder (FR-012, EW-STORAGE-005/006).
//
// Object idempotency is SHA-256 metadata comparison on
// x-amz-meta-content-sha256 — the S3 ETag is NEVER used as a content hash
// (multipart ETags are not stable; AC-006). Files larger than the 16 MB
// part size upload multipart with the same metadata contract.
package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/textproto"
	"net/url"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ancyloce/anvilkit-export-worker/contracts/events"
	"github.com/ancyloce/anvilkit-export-worker/internal/errclass"
)

// MetaContentSHA256 is the user-metadata key carrying the content hash
// (stored on the wire as x-amz-meta-content-sha256).
const MetaContentSHA256 = "content-sha256"

// PartSize is the multipart threshold/part size: files above it upload
// multipart, preserving the metadata contract (FR-011).
const PartSize = 16 << 20

// SHA256Hex hashes a body for the metadata contract.
func SHA256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// Object is one artifact object to upload.
type Object struct {
	Key          string
	Body         []byte
	ContentType  string
	CacheControl string
	SHA256Hex    string // computed by the caller (also lands in the manifest)
}

// S3Store is the S3-compatible adapter.
type S3Store struct {
	client *minio.Client
	bucket string
}

// NewS3 builds the adapter from the §14 configuration.
func NewS3(endpointURL, region, accessKey, secretKey, bucket string) (*S3Store, error) {
	u, err := url.Parse(endpointURL)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("S3_ENDPOINT is not a valid URL: %q", endpointURL)
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: u.Scheme == "https",
		Region: region,
	})
	if err != nil {
		return nil, fmt.Errorf("storage client: %w", err)
	}
	return &S3Store{client: client, bucket: bucket}, nil
}

// EnsureBucket verifies the artifact bucket exists — a boot-time fail-fast
// dependency check (FR-019).
func (s *S3Store) EnsureBucket(ctx context.Context) error {
	ok, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("storage unreachable at boot: %w", err)
	}
	if !ok {
		return fmt.Errorf("artifact bucket %q does not exist", s.bucket)
	}
	return nil
}

// EnsureUploaded uploads obj unless an object with the same key AND the same
// x-amz-meta-content-sha256 already exists (per-file headObject skip,
// FR-011/AC-006). The decision never reads the ETag.
func (s *S3Store) EnsureUploaded(ctx context.Context, obj Object) (skipped bool, err error) {
	stat, statErr := s.client.StatObject(ctx, s.bucket, obj.Key, minio.StatObjectOptions{})
	if statErr == nil {
		if existingHash(stat) == obj.SHA256Hex {
			return true, nil
		}
		// Same key, different content: overwrite (deployment re-run after a
		// contract-visible change is impossible — keys are deployment-scoped
		// and renders version-pinned — but idempotent overwrite is safe).
	} else {
		var resp minio.ErrorResponse
		if errors.As(statErr, &resp) && resp.StatusCode != 404 {
			return false, classifyStorage(statErr, "stat "+obj.Key)
		}
		if !errors.As(statErr, &resp) {
			return false, classifyStorage(statErr, "stat "+obj.Key)
		}
	}

	_, err = s.client.PutObject(ctx, s.bucket, obj.Key, bytes.NewReader(obj.Body), int64(len(obj.Body)),
		minio.PutObjectOptions{
			ContentType:  obj.ContentType,
			CacheControl: obj.CacheControl,
			UserMetadata: map[string]string{MetaContentSHA256: obj.SHA256Hex},
			PartSize:     PartSize,
		})
	if err != nil {
		return false, classifyStorage(err, "put "+obj.Key)
	}
	return false, nil
}

// existingHash extracts the content hash from object metadata, tolerating
// the client library's MIME-canonical key form. The ETag is deliberately
// never consulted (AC-006).
func existingHash(stat minio.ObjectInfo) string {
	if v, ok := stat.UserMetadata[textproto.CanonicalMIMEHeaderKey(MetaContentSHA256)]; ok {
		return v
	}
	if v, ok := stat.UserMetadata[MetaContentSHA256]; ok {
		return v
	}
	return ""
}

// UploadAll uploads objects with a bounded worker pool (FR-011: 8–16
// concurrent uploads per deployment), stopping at the first error.
func (s *S3Store) UploadAll(ctx context.Context, objs []Object, concurrency int) (uploaded, skipped int, err error) {
	if concurrency < 1 {
		concurrency = 8
	}
	if concurrency > 16 {
		concurrency = 16
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
	)
	work := make(chan Object)
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range work {
				wasSkipped, uerr := s.EnsureUploaded(ctx, obj)
				mu.Lock()
				if uerr != nil {
					if firstErr == nil {
						firstErr = uerr
						cancel()
					}
				} else if wasSkipped {
					skipped++
				} else {
					uploaded++
				}
				mu.Unlock()
			}
		}()
	}
feed:
	for _, obj := range objs {
		select {
		case <-ctx.Done():
			break feed
		case work <- obj:
		}
	}
	close(work)
	wg.Wait()
	return uploaded, skipped, firstErr
}

// FetchIfExists reads one object, reporting absence without error
// (FR-015: the redelivery path checks for the stored manifest).
func (s *S3Store) FetchIfExists(ctx context.Context, key string) ([]byte, bool, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, false, classifyStorage(err, "get "+key)
	}
	defer obj.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(obj); err != nil {
		var resp minio.ErrorResponse
		if errors.As(err, &resp) && resp.StatusCode == 404 {
			return nil, false, nil
		}
		return nil, false, classifyStorage(err, "read "+key)
	}
	return buf.Bytes(), true, nil
}

// Fetch reads one object back (verification tooling and tests).
func (s *S3Store) Fetch(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, classifyStorage(err, "get "+key)
	}
	defer obj.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(obj); err != nil {
		return nil, classifyStorage(err, "read "+key)
	}
	return buf.Bytes(), nil
}

// classifyStorage maps storage failures to the §13 registry: timeouts →
// STORAGE_TIMEOUT, 5xx/transport → STORAGE_5XX (both retryable); unexpected
// 4xx (permissions, missing bucket) → non-retryable VALIDATION_FAILED.
func classifyStorage(err error, op string) error {
	wrapped := fmt.Errorf("storage %s: %w", op, err)
	if errors.Is(err, context.DeadlineExceeded) {
		return errclass.New(events.ErrorCodeStorageTimeout, events.FailedStageUploadArtifacts, wrapped)
	}
	var t interface{ Timeout() bool }
	if errors.As(err, &t) && t.Timeout() {
		return errclass.New(events.ErrorCodeStorageTimeout, events.FailedStageUploadArtifacts, wrapped)
	}
	var resp minio.ErrorResponse
	if errors.As(err, &resp) && resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return errclass.New(events.ErrorCodeValidationFailed, events.FailedStageUploadArtifacts, wrapped)
	}
	return errclass.New(events.ErrorCodeStorage5xx, events.FailedStageUploadArtifacts, wrapped)
}
