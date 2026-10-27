FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.27.3@sha256:4d14dcb0974d4d5a608700a6a495e9730813582dadab5ceb4ff4b99d5462f897 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    ./cmd/multitier_stub

FROM gke.gcr.io/debian-base:trixie-v1.0.0-gke.4@sha256:37fe6a16aabcbaf0dd2ff4fda39ce97806bec037d64a2a9aa93991c5f335ab36 AS debian
RUN clean-install \
  bash \
  procps file xxd lsof iputils-ping dnsutils iproute2 iptables # all for debugging

COPY --from=builder /src/multitier_stub /usr/bin/

CMD ["sleep", "36000"]
