# Copyright 2023 Google.
#
# This software is provided as-is, without warranty or representation for any
# use or purpose. Your use of it is subject to your agreement with Google.

FROM --platform=$BUILDPLATFORM google-go.pkg.dev/golang:1.27.3@sha256:4d14dcb0974d4d5a608700a6a495e9730813582dadab5ceb4ff4b99d5462f897 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG CACHEBUST
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags "-extldflags=static" ./cmd/multitier_ranks


FROM gcr.io/distroless/static-debian13 AS final
COPY --from=builder /src/multitier_ranks /
ENTRYPOINT ["/multitier_ranks"]
