# Copyright 2023 Google.
#
# This software is provided as-is, without warranty or representation for any
# use or purpose. Your use of it is subject to your agreement with Google.

FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.25.5@sha256:b9b992fbef86e816ebac77aa06d28a5c92c6619cc0ed3548df91cccf3328e9ca AS builder
WORKDIR /src
COPY . .

ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags "-extldflags=static" \
    -o multitier_controller_executable \
    ./cmd/multitier_controller

FROM marketplace.gcr.io/google/debian12 AS check
WORKDIR /
COPY --from=builder /src/hack/print-missing-deps.sh /print-missing-deps.sh
COPY --from=builder /src/multitier_controller_executable /multitier_controller_executable
RUN /print-missing-deps.sh

FROM gcr.io/distroless/base-debian13 AS distroless
COPY --from=check /multitier_controller_executable /multitier_controller_executable
ENTRYPOINT ["/multitier_controller_executable"]
