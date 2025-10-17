# Copyright 2023 Google.
#
# This software is provided as-is, without warranty or representation for any
# use or purpose. Your use of it is subject to your agreement with Google.

FROM google-go.pkg.dev/golang:1.24.3@sha256:5d2e3331f2ea940f316408b516badb01b146c918abe6941e3154c4704ba50140 AS builder
WORKDIR /src
COPY . .
RUN go build -ldflags "-extldflags=static" ./cmd/multitier_controller

FROM gcr.io/distroless/base-debian12 AS distroless
COPY --from=builder /src/multitier_controller /

FROM distroless AS check
COPY --from=debian /usr/bin/ldd /usr/bin/find /usr/bin/xargs /usr/bin/
COPY --from=debian /bin/sh /bin/bash /bin/grep /bin/
COPY --from=builder /src/hack/print-missing-deps.sh /print-missing-deps.sh
COPY --from=debian \
    /lib/x86_64-linux-gnu/libtinfo.so.6 \
    /lib/x86_64-linux-gnu/libselinux.so.1 \
    /lib/x86_64-linux-gnu/libpcre2-8.so.0 \
    /lib/x86_64-linux-gnu/

SHELL ["/bin/bash", "-c"]
RUN /print-missing-deps.sh

FROM distroless
ENTRYPOINT ["/multitier_controller"]

