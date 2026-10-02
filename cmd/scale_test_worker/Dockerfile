FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.27.1@sha256:839dafae523554ce25f90b48dae988215e8fed7c8a3c6e0577fec4b847d7e41e AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags -extldflags=static ./cmd/scale_test_worker

FROM gke.gcr.io/debian-base:trixie-v1.0.0-gke.2
COPY --from=builder /src/scale_test_worker /
ENTRYPOINT ["/scale_test_worker"]
