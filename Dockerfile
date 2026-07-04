# Multi-stage build (EW-K8S-001): static Go binary in a minimal, non-root
# runtime image. Images are tagged immutably in CI (git SHA on main, semver
# on releases — ADR-008); PR builds build but never publish.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build \
    -ldflags "-s -w -X github.com/ancyloce/anvilkit-export-worker/internal/buildinfo.Version=${VERSION}" \
    -o /out/export-worker ./cmd/export-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/export-worker /export-worker
# Internal-only operational ports: health/readiness (8081), metrics (9091).
# No public ports (FR-018).
EXPOSE 8081 9091
ENTRYPOINT ["/export-worker"]
