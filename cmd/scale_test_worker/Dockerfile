FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.25.5@sha256:b9b992fbef86e816ebac77aa06d28a5c92c6619cc0ed3548df91cccf3328e9ca AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags -extldflags=static ./cmd/scale_test_worker

FROM gke.gcr.io/debian-base:bookworm-v1.0.4-gke.17
COPY --from=builder /src/scale_test_worker /
ENTRYPOINT ["/scale_test_worker"]
