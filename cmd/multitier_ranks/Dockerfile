# Copyright 2023 Google.
#
# This software is provided as-is, without warranty or representation for any
# use or purpose. Your use of it is subject to your agreement with Google.

FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.25.5@sha256:b9b992fbef86e816ebac77aa06d28a5c92c6619cc0ed3548df91cccf3328e9ca AS builder
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

