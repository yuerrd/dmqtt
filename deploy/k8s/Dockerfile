# Stage 1: Build
FROM golang:1.25-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -o /dmqtt ./cmd/dmqtt/

# Stage 2: Runtime
FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /dmqtt /usr/local/bin/dmqtt
RUN useradd -r -s /usr/sbin/nologin dmqtt && \
    mkdir -p /data/dmqtt && \
    chown dmqtt:dmqtt /data/dmqtt
USER dmqtt
EXPOSE 1883 9090 7000 8000
ENTRYPOINT ["dmqtt"]
