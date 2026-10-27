FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.27.3@sha256:4d14dcb0974d4d5a608700a6a495e9730813582dadab5ceb4ff4b99d5462f897 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags -extldflags=static ./cmd/scale_test_worker

FROM gke.gcr.io/debian-base:trixie-v1.0.0-gke.4@sha256:37fe6a16aabcbaf0dd2ff4fda39ce97806bec037d64a2a9aa93991c5f335ab36
COPY --from=builder /src/scale_test_worker /
ENTRYPOINT ["/scale_test_worker"]
