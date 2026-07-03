// Package buildinfo carries the service identity (ADR-015 canonical naming)
// and the build-time version stamp.
package buildinfo

// Name is the canonical service name, used identically on every surface:
// WORKER_NAME default, OTel service name, log workerId prefix (ADR-015).
const Name = "anvilkit-export-worker"

// Version is stamped at release build time via:
//
//	go build -ldflags "-X github.com/ancyloce/anvilkit-export-worker/internal/buildinfo.Version=<tag>"
//
// Images are tagged immutably (git SHA on main, semver on releases — ADR-008).
var Version = "dev"
