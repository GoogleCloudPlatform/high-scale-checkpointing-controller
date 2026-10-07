FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.27.1@sha256:839dafae523554ce25f90b48dae988215e8fed7c8a3c6e0577fec4b847d7e41e AS builder

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
