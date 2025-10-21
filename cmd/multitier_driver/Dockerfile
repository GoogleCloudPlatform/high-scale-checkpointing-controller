# TODO: get nfs support working with distroless.
#
# Note that it's not enough to just copy over the mount commands,
# there are other system services that must be started as well.
#
# TODO: create a new container to replace the nfs-server container.

ARG VERSION

FROM gke.gcr.io/debian-base:bookworm-v1.0.6-gke.1@sha256:1879da117201d67d7e28310f5b4986be8ed5a202a6fdb8a253441f950caedeba AS debian
# Note that netbase is needed to get rpcbind working correctly.
RUN clean-install \
  mount bash nfs-common netbase

FROM google-go.pkg.dev/golang:1.24.3@sha256:5d2e3331f2ea940f316408b516badb01b146c918abe6941e3154c4704ba50140 AS builder
WORKDIR /src
COPY . .
RUN go build -ldflags "-extldflags=static -X main.driverVersion=$VERSION" ./cmd/multitier_driver

FROM debian AS driver
COPY --from=builder /src/multitier_driver /

COPY --from=builder /src/cmd/multitier_driver/startup.sh /
ENTRYPOINT ["/startup.sh"]
