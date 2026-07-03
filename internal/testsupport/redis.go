// Package testsupport provides integration-test infrastructure. It is
// imported only from _test files and never linked into the worker binary.
package testsupport

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis returns a client for the integration-test Redis on the given
// logical DB and flushes that DB. Each test package uses its own DB number
// (queue=1, lock=2, worker=3) because `go test ./...` runs packages in
// parallel and a shared DB would let one package's flush destroy another's
// live state. Tests are skipped when REDIS_TEST_URL is unset (CI provides a
// redis service container; locally e.g. REDIS_TEST_URL=redis://localhost:16379).
//
// DESTRUCTIVE: the selected database is flushed — point it only at a
// dedicated test instance.
func Redis(t *testing.T, db int) redis.UniversalClient {
	t.Helper()
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		t.Skip("REDIS_TEST_URL not set; skipping Redis integration test")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("REDIS_TEST_URL: %v", err)
	}
	opts.DB = db
	rdb := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("test redis unreachable: %v", err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush test db: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}
