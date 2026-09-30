# TODO: get nfs support working with distroless.
#
# Note that it's not enough to just copy over the mount commands,
# there are other system services that must be started as well.
#
# TODO: create a new container to replace the nfs-server container.

ARG VERSION

FROM gke.gcr.io/debian-base:trixie-v1.0.0-gke.2@sha256:9cf5515a7e60a2a0ac33f4de3e43148ed9bdd5c5911f8aa68d0253a8d28e2a68 AS debian
# Note that netbase is needed to get rpcbind working correctly.
RUN clean-install \
  mount bash nfs-common netbase

FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.25.5@sha256:b9b992fbef86e816ebac77aa06d28a5c92c6619cc0ed3548df91cccf3328e9ca AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags "-extldflags=static -X main.driverVersion=$VERSION" \
    ./cmd/multitier_driver

FROM debian AS driver
COPY --from=builder /src/multitier_driver /

COPY --from=builder /src/cmd/multitier_driver/startup.sh /
ENTRYPOINT ["/startup.sh"]
