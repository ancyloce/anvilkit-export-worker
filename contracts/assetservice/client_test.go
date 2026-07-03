// Contract tests (EW-CONTRACT-005): generated asset-service bindings and
// client against the PRD 0010 §8.4 fixture shapes. The worker uses this API
// as a post-render verifier only.
package assetservice_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ancyloce/anvilkit-export-worker/contracts/assetservice"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return raw
}

func jsonEqual(t *testing.T, want, got []byte) {
	t.Helper()
	var w, g any
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("parse want: %v", err)
	}
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("parse got: %v", err)
	}
	if !reflect.DeepEqual(w, g) {
		t.Fatalf("JSON mismatch:\nwant: %s\ngot:  %s", want, got)
	}
}

func TestResolveAssetsBatch(t *testing.T) {
	reqFixture := readFixture(t, "resolve-batch-request.json")
	respFixture := readFixture(t, "resolve-batch-response.json")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/internal/assets/resolve-batch" {
			t.Errorf("path = %s, want /internal/assets/resolve-batch", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q, want Bearer test-token", got)
		}
		body, _ := io.ReadAll(r.Body)
		jsonEqual(t, reqFixture, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(respFixture)
	}))
	defer srv.Close()

	var req assetservice.ResolveBatchRequest
	if err := json.Unmarshal(reqFixture, &req); err != nil {
		t.Fatalf("unmarshal request fixture: %v", err)
	}

	c := &assetservice.Client{BaseURL: srv.URL, Token: "test-token"}
	resp, err := c.ResolveAssetsBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveAssetsBatch: %v", err)
	}
	if len(resp.Assets) != 1 {
		t.Fatalf("len(Assets) = %d, want 1", len(resp.Assets))
	}
	asset := resp.Assets[0]
	if asset.Ref != "asset://img_01" {
		t.Errorf("Ref = %q, want asset://img_01", asset.Ref)
	}
	if asset.MimeType != "image/webp" || asset.SizeBytes != 120000 {
		t.Errorf("MimeType/SizeBytes = %q/%d", asset.MimeType, asset.SizeBytes)
	}
	if asset.ContentHash != "sha256-xxx" {
		t.Errorf("ContentHash = %q — must be a sha256- content hash, never an S3 ETag", asset.ContentHash)
	}

	// Response must round-trip losslessly.
	remarshaled, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	jsonEqual(t, respFixture, remarshaled)
}
