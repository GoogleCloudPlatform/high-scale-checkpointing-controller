FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.25.5@sha256:b9b992fbef86e816ebac77aa06d28a5c92c6619cc0ed3548df91cccf3328e9ca AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    ./cmd/multitier_stub

FROM gke.gcr.io/debian-base:bookworm-v1.0.4-gke.17 AS debian
RUN clean-install \
  bash \
  procps file xxd lsof iputils-ping dnsutils iproute2 iptables # all for debugging

COPY --from=builder /src/multitier_stub /usr/bin/

CMD ["sleep", "36000"]
