# DMQTT — Distributed MQTT Broker

**[中文文档](README_zh.md)**

<p align="center">
  <strong>A high-performance, fully decentralized MQTT broker written in Go</strong>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#configuration">Configuration</a> •
  <a href="#cluster-mode">Cluster Mode</a> •
  <a href="#web-dashboard">Web Dashboard</a> •
  <a href="#deployment">Deployment</a>
</p>

---

## Features

- **MQTT 3.1.1 & 5.0** — Full protocol support including QoS 0/1/2, retained messages, will messages, session persistence
- **Distributed Clustering** — Gossip-based decentralized architecture with consistent hash ring, no single point of failure
- **Multiple Transports** — TCP, TLS (TLS 1.2+), WebSocket, WebSocket over TLS
- **Embedded Storage** — Pebble-based local storage, no external database required
- **Plugin System** — Go plugin (.so) support with interceptor chain for connect, publish, subscribe, delivery, disconnect events
- **Rule Engine** — CEL-based rules with republish, webhook, and log actions
- **Multi-Tenancy** — Per-tenant connection limits, message rate limits, and topic isolation
- **Rate Limiting** — Per-client, global, and adaptive rate limiting with backpressure and anomaly detection
- **Circuit Breaker** — Automatic failure isolation for downstream dependencies
- **Audit Logging** — Async audit trail with buffered writes and backup file support
- **Web Dashboard** — Vue.js admin UI with real-time cluster monitoring, device management, topic inspection
- **Observability** — Prometheus metrics, structured JSON logging, health/readiness endpoints
- **Security** — API key authentication, TLS hardening, WebSocket origin validation, plugin path validation
- **Graceful Shutdown** — Configurable drain period for zero-downtime deployments
- **Benchmark Tool** — Built-in load testing for connections, publish, subscribe, and mixed workloads

## Architecture

```
                    ┌─────────────────────┐
                    │    Global L4 LB     │
                    └──────────┬──────────┘
           ┌───────────────────┼───────────────────┐
           ▼                   ▼                   ▼
    ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
    │    Node 1    │   │    Node 2    │   │    Node 3    │
    │  TCP :1883   │   │  TCP :1883   │   │  TCP :1883   │
    │  TLS :8883   │   │  TLS :8883   │   │  TLS :8883   │
    │  WS  :8083   │   │  WS  :8083   │   │  WS  :8083   │
    │  HTTP :9090  │   │  HTTP :9090  │   │  HTTP :9090  │
    │  Gossip:7000 │   │  Gossip:7000 │   │  Gossip:7000 │
    │  ┌────────┐  │   │  ┌────────┐  │   │  ┌────────┐  │
    │  │ Pebble │  │   │  │ Pebble │  │   │  │ Pebble │  │
    │  └────────┘  │   │  └────────┘  │   │  └────────┘  │
    └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
           │       Gossip Protocol        │
           └──────────────────────────────┘
```

Each node is self-contained with embedded storage, communicating via Gossip protocol for membership, consistent hash ring for shard routing, and direct TCP for cross-node message forwarding.

## Quick Start

### Prerequisites

- Go 1.25+ 
- Node.js 18+ (for admin UI build, optional)

### Build

```bash
# Build binary (Go only, without admin UI)
make build-go

# Build with admin dashboard
make build

# Cross-compile
make build-linux    # Linux amd64
make build-darwin   # macOS amd64
make build-windows  # Windows amd64
```

### Run Standalone

```bash
# Run with defaults (TCP :1883, HTTP :9090)
./bin/dmqtt

# With persistent storage
DMQTT_DATA_DIR=/var/lib/dmqtt ./bin/dmqtt

# With TLS
DMQTT_TLS_ADDR=:8883 \
DMQTT_TLS_CERT=/path/to/cert.pem \
DMQTT_TLS_KEY=/path/to/key.pem \
./bin/dmqtt

# With authentication
DMQTT_AUTH_FILE=/etc/dmqtt/auth.json ./bin/dmqtt
```

### Test with MQTT Client

```bash
# Subscribe
mosquitto_sub -h localhost -p 1883 -t "test/topic"

# Publish
mosquitto_pub -h localhost -p 1883 -t "test/topic" -m "Hello DMQTT"
```

## Configuration

All configuration is via environment variables with `DMQTT_` prefix:

| Variable | Default | Description |
|----------|---------|-------------|
| `DMQTT_TCP_ADDR` | `:1883` | MQTT TCP listener address |
| `DMQTT_TLS_ADDR` | _(disabled)_ | MQTT TLS listener address |
| `DMQTT_TLS_CERT` | | TLS certificate file path |
| `DMQTT_TLS_KEY` | | TLS private key file path |
| `DMQTT_WS_ADDR` | _(disabled)_ | WebSocket listener address |
| `DMQTT_HTTP_ADDR` | `:9090` | HTTP API / metrics / admin address |
| `DMQTT_DATA_DIR` | _(in-memory)_ | Pebble storage directory |
| `DMQTT_AUTH_FILE` | _(no auth)_ | JSON credentials file path |
| `DMQTT_LOG_LEVEL` | `info` | Log level: debug, info, warn, error |
| `DMQTT_CLUSTER_ENABLED` | `false` | Enable cluster mode |
| `DMQTT_CLUSTER_NODE_ID` | | Unique node identifier |
| `DMQTT_CLUSTER_HOST` | `127.0.0.1` | Node IP for gossip and transport |
| `DMQTT_CLUSTER_GOSSIP_PORT` | `7000` | Gossip protocol port |
| `DMQTT_CLUSTER_SEEDS` | | Comma-separated seed node addresses |
| `DMQTT_REPLICATION_ENABLED` | `false` | Enable session/message replication |

### Authentication File Format

```json
[
  { "username": "device1", "password_hash": "$2a$10$..." },
  { "username": "admin", "password_hash": "$2a$10$..." }
]
```

Generate password hashes with the built-in tool:

```bash
go build -o bin/dmqtt-passwd ./cmd/dmqtt-passwd/
./bin/dmqtt-passwd mypassword
```

## Cluster Mode

### 3-Node Cluster Example

```bash
# Node 1
DMQTT_TCP_ADDR=:1883 \
DMQTT_HTTP_ADDR=:9090 \
DMQTT_CLUSTER_ENABLED=true \
DMQTT_CLUSTER_NODE_ID=node-1 \
DMQTT_CLUSTER_HOST=192.168.1.10 \
DMQTT_CLUSTER_GOSSIP_PORT=7000 \
./bin/dmqtt

# Node 2
DMQTT_TCP_ADDR=:1884 \
DMQTT_HTTP_ADDR=:9091 \
DMQTT_CLUSTER_ENABLED=true \
DMQTT_CLUSTER_NODE_ID=node-2 \
DMQTT_CLUSTER_HOST=192.168.1.11 \
DMQTT_CLUSTER_GOSSIP_PORT=7001 \
DMQTT_CLUSTER_SEEDS=192.168.1.10:7000 \
./bin/dmqtt

# Node 3
DMQTT_TCP_ADDR=:1885 \
DMQTT_HTTP_ADDR=:9092 \
DMQTT_CLUSTER_ENABLED=true \
DMQTT_CLUSTER_NODE_ID=node-3 \
DMQTT_CLUSTER_HOST=192.168.1.12 \
DMQTT_CLUSTER_GOSSIP_PORT=7002 \
DMQTT_CLUSTER_SEEDS=192.168.1.10:7000 \
./bin/dmqtt
```

### Cluster Features

- **Gossip-based discovery** — Nodes auto-discover via [memberlist](https://github.com/hashicorp/memberlist)
- **Consistent hash ring** — Client-to-shard mapping with configurable virtual nodes (default: 150)
- **Cross-node forwarding** — Messages routed to the correct shard owner via direct TCP
- **Shard migration** — Automatic rebalancing when nodes join or leave
- **Session replication** — Optional session/offline message replication across replicas

## Web Dashboard

The built-in admin dashboard provides real-time cluster monitoring:

- **Dashboard** — Connection count, message throughput, memory usage, cluster node status
- **Device List** — Connected clients with topic subscriptions, node assignment
- **Device Detail** — Per-client subscription list, connection metadata
- **Topic Explorer** — Active topics with subscriber counts

Access at `http://localhost:9090/admin/` after building with `make build`.

## HTTP API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/ready` | GET | Readiness probe |
| `/metrics` | GET | Prometheus metrics |
| `/admin/` | GET | Web dashboard |
| `/api/v1/clients` | GET | List connected clients |
| `/api/v1/clients/:id` | GET | Client details |
| `/api/v1/clients/:id` | DELETE | Disconnect client |
| `/api/v1/topics` | GET | List active topics |
| `/api/v1/stats` | GET | Broker statistics |
| `/api/v1/cluster/stats` | GET | Cluster-wide aggregated stats |

API endpoints under `/api/v1/` support API key authentication:

```bash
DMQTT_API_KEY=my-secret-key ./bin/dmqtt

# Then access with header
curl -H "X-API-Key: my-secret-key" http://localhost:9090/api/v1/stats
```

## Plugin System

DMQTT supports Go plugins (`.so` files) implementing the interceptor interface:

```go
type Interceptor interface {
    Name() string
    Init() error
    Close() error
    OnConnect(ctx context.Context, evt *ConnectEvent) error
    OnPublish(ctx context.Context, evt *PublishEvent) error
    OnSubscribe(ctx context.Context, evt *SubscribeEvent) error
    OnDelivery(ctx context.Context, evt *DeliveryEvent) error
    OnDisconnect(evt *DisconnectEvent)
}
```

Build and load a plugin:

```bash
# Build example plugin
make example-plugin

# Load via environment
DMQTT_PLUGINS='[{"path":"/opt/dmqtt/plugins/log-interceptor.so"}]' ./bin/dmqtt
```

See `examples/plugins/loginterceptor/` for a complete example.

## Rule Engine

Define rules in JSON to trigger actions when messages match:

```json
[
  {
    "id": "high-temp-alert",
    "source": { "topic": "sensors/+/temperature" },
    "filter": "payload.value > 80",
    "actions": [
      { "type": "republish", "topic": "alerts/high-temp", "qos": 1 },
      { "type": "webhook", "url": "https://api.example.com/alerts" }
    ]
  }
]
```

Enable with:
```bash
DMQTT_RULE_ENGINE_ENABLED=true \
DMQTT_RULE_ENGINE_RULES_FILE=/etc/dmqtt/rules.json \
./bin/dmqtt
```

## Benchmark Tool

Built-in load testing tool:

```bash
go build -o bin/dmqtt-bench ./cmd/dmqtt-bench/

# Connection benchmark
./bin/dmqtt-bench conn -c 10000 -addr localhost:1883

# Publish benchmark
./bin/dmqtt-bench pub -c 100 -n 10000 -topic "bench/test" -qos 1

# Subscribe benchmark
./bin/dmqtt-bench sub -c 100 -topic "bench/#"

# Mixed workload
./bin/dmqtt-bench mixed -publishers 50 -subscribers 50 -n 10000
```

## Deployment

### Docker

```bash
make docker-build
docker run -p 1883:1883 -p 9090:9090 dmqtt:latest
```

### Kubernetes

```bash
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/statefulset.yaml
kubectl apply -f deploy/k8s/service-mqtt.yaml
kubectl apply -f deploy/k8s/service-http.yaml
kubectl apply -f deploy/k8s/service-headless.yaml
kubectl apply -f deploy/k8s/pdb.yaml
```

### Bare Metal (systemd)

```bash
make build
sudo make install
sudo systemctl enable dmqtt
sudo systemctl start dmqtt
```

Configuration: `/etc/dmqtt/dmqtt.env`

## Project Structure

```
dmqtt/
├── cmd/
│   ├── dmqtt/           # Main broker binary
│   ├── dmqtt-bench/     # Benchmark tool
│   └── dmqtt-passwd/    # Password hash generator
├── config/              # Configuration types and defaults
├── internal/
│   ├── auth/            # Authentication (file-based, bcrypt)
│   ├── bench/           # Benchmark engine
│   ├── broker/          # Core MQTT broker, client handling, session management
│   ├── circuitbreaker/  # Circuit breaker pattern
│   ├── cluster/         # Gossip membership, hash ring, shard migration
│   ├── codec/           # MQTT 3.1.1/5.0 packet codec
│   ├── httpapi/         # REST API, Prometheus metrics, admin dashboard
│   ├── logging/         # Structured logging setup
│   ├── metrics/         # Prometheus metric definitions
│   ├── plugin/          # Plugin loader and interceptor chain
│   │   └── audit/       # Built-in audit interceptor
│   ├── ratelimit/       # Token bucket rate limiter
│   ├── rule/            # CEL-based rule engine
│   ├── storage/         # Pebble storage backend
│   ├── tenant/          # Multi-tenancy enforcement
│   └── transport/       # TCP, TLS, WebSocket listeners
├── web/admin/           # Vue.js admin dashboard
├── deploy/
│   ├── k8s/             # Kubernetes manifests + Dockerfile
│   └── bare-metal/      # systemd service + install scripts
├── examples/plugins/    # Example interceptor plugins
├── Makefile
└── go.mod
```

## Testing

```bash
# Run all tests with race detector
make test

# Short tests only
make test-short

# Lint
make lint
```

## License

MIT
