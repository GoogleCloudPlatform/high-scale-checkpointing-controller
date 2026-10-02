# Copyright 2023 Google.
#
# This software is provided as-is, without warranty or representation for any
# use or purpose. Your use of it is subject to your agreement with Google.

FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.27.1@sha256:839dafae523554ce25f90b48dae988215e8fed7c8a3c6e0577fec4b847d7e41e AS builder
ARG TARGETOS
ARG TARGETARCH
ARG CACHEBUST
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags "-extldflags=static" ./cmd/multitier_ranks


FROM marketplace.gcr.io/google/debian12 AS check
COPY --from=builder /src/multitier_ranks /
COPY --from=builder /src/hack/print-missing-deps.sh /print-missing-deps.sh

SHELL ["/bin/bash", "-c"]
COPY --from=builder /src/multitier_ranks /
RUN /print-missing-deps.sh

FROM gcr.io/distroless/base-debian13 AS final
COPY --from=check /multitier_ranks /
ENTRYPOINT ["/multitier_ranks"]

