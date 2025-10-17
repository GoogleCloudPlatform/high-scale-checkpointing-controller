FROM golang:1.24 as builder
WORKDIR /src
COPY . .
RUN go build -ldflags -extldflags=static ./cmd/scale_test_worker

FROM gke.gcr.io/debian-base:bookworm-v1.0.4-gke.17
COPY --from=builder /src/scale_test_worker /
ENTRYPOINT ["/scale_test_worker"]
