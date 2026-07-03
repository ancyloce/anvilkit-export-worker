// Storage adapter integration tests against a real MinIO (skipped without
// S3_TEST_ENDPOINT). Covers EW-STORAGE-002/003/004 and the initial
// T-object-hash-metadata-idempotency (AC-006), including multipart.
package storage_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ancyloce/anvilkit-export-worker/internal/storage"
)

const testBucket = "anvilkit-artifacts-test"

func testStore(t *testing.T) *storage.S3Store {
	t.Helper()
	endpoint := os.Getenv("S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("S3_TEST_ENDPOINT not set; skipping MinIO integration test")
	}
	access := envOr("S3_TEST_ACCESS_KEY", "minioadmin")
	secret := envOr("S3_TEST_SECRET_KEY", "minioadmin")

	// Bootstrap the bucket and wipe this test's prefix with a raw client
	// (the adapter deliberately has neither capability): the assertions
	// below distinguish "uploaded" from "skipped", so they need a clean
	// slate on every run.
	host := strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	raw, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: strings.HasPrefix(endpoint, "https://"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	exists, err := raw.BucketExists(ctx, testBucket)
	if err != nil {
		t.Fatalf("test minio unreachable: %v", err)
	}
	if !exists {
		if err := raw.MakeBucket(ctx, testBucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for object := range raw.ListObjects(ctx, testBucket, minio.ListObjectsOptions{
		Prefix: "sites/site_t/", Recursive: true,
	}) {
		if object.Err != nil {
			t.Fatal(object.Err)
		}
		if err := raw.RemoveObject(ctx, testBucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	store, err := storage.NewS3(endpoint, "us-east-1", access, secret, testBucket)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	return store
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func obj(key string, body []byte) storage.Object {
	return storage.Object{
		Key:          key,
		Body:         body,
		ContentType:  "application/octet-stream",
		CacheControl: storage.CacheControlNonHashed,
		SHA256Hex:    storage.SHA256Hex(body),
	}
}

// TestUploadMetadataAndHashSkip: first upload writes metadata; identical
// re-upload skips on hash; changed content re-uploads.
func TestUploadMetadataAndHashSkip(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	o := obj("sites/site_t/deployments/dep_t/assets/a.bin", []byte("content-v1"))

	skipped, err := store.EnsureUploaded(ctx, o)
	if err != nil || skipped {
		t.Fatalf("first upload: skipped=%v err=%v", skipped, err)
	}
	skipped, err = store.EnsureUploaded(ctx, o)
	if err != nil || !skipped {
		t.Fatalf("identical re-upload must skip on x-amz-meta-content-sha256: skipped=%v err=%v", skipped, err)
	}

	changed := obj(o.Key, []byte("content-v2"))
	skipped, err = store.EnsureUploaded(ctx, changed)
	if err != nil || skipped {
		t.Fatalf("changed content must re-upload: skipped=%v err=%v", skipped, err)
	}

	got, err := store.Fetch(ctx, o.Key)
	if err != nil || string(got) != "content-v2" {
		t.Fatalf("fetch = %q err=%v", got, err)
	}
}

// TestMultipartPreservesMetadataContract is the multipart half of
// T-object-hash-metadata-idempotency: a > 16 MB object uploads multipart,
// carries the same metadata, and skips idempotently — with the skip decision
// never touching the (unstable multipart) ETag.
func TestMultipartPreservesMetadataContract(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	body := bytes.Repeat([]byte("anvilkit-multipart-fixture-"), (storage.PartSize/27)+64) // ≈ 16 MB + slack
	if len(body) <= storage.PartSize {
		t.Fatalf("fixture must exceed the %d-byte part size, got %d", storage.PartSize, len(body))
	}
	o := obj("sites/site_t/deployments/dep_t/assets/huge.bin", body)

	skipped, err := store.EnsureUploaded(ctx, o)
	if err != nil || skipped {
		t.Fatalf("multipart upload: skipped=%v err=%v", skipped, err)
	}
	skipped, err = store.EnsureUploaded(ctx, o)
	if err != nil || !skipped {
		t.Fatalf("multipart re-upload must hash-skip (never ETag): skipped=%v err=%v", skipped, err)
	}
}

// TestUploadAllConcurrent (EW-STORAGE-004): the bounded pool uploads a batch
// and reruns idempotently.
func TestUploadAllConcurrent(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()

	var objs []storage.Object
	for i := range 24 {
		objs = append(objs, obj(
			"sites/site_t/deployments/dep_t/pool/"+string(rune('a'+i))+".bin",
			bytes.Repeat([]byte{byte(i)}, 128),
		))
	}
	uploaded, skipped, err := store.UploadAll(ctx, objs, 8)
	if err != nil || uploaded != len(objs) || skipped != 0 {
		t.Fatalf("first pass: uploaded=%d skipped=%d err=%v", uploaded, skipped, err)
	}
	uploaded, skipped, err = store.UploadAll(ctx, objs, 8)
	if err != nil || uploaded != 0 || skipped != len(objs) {
		t.Fatalf("second pass must skip everything: uploaded=%d skipped=%d err=%v", uploaded, skipped, err)
	}
}
