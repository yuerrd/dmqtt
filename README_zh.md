# DMQTT — 分布式 MQTT 消息代理

**[English](README.md)**

<p align="center">
  <strong>高性能、完全去中心化的 MQTT Broker，使用 Go 语言编写</strong>
</p>

<p align="center">
  <a href="#功能特性">功能特性</a> •
  <a href="#快速开始">快速开始</a> •
  <a href="#配置说明">配置说明</a> •
  <a href="#集群模式">集群模式</a> •
  <a href="#管理面板">管理面板</a> •
  <a href="#部署方式">部署方式</a>
</p>

---

## 功能特性

- **MQTT 3.1.1 & 5.0** — 完整协议支持，包括 QoS 0/1/2、保留消息、遗嘱消息、会话持久化
- **分布式集群** — 基于 Gossip 协议的去中心化架构，一致性哈希环分片，无单点故障
- **多种传输层** — TCP、TLS（TLS 1.2+）、WebSocket、WebSocket over TLS
- **嵌入式存储** — 基于 Pebble 的本地存储，无需外部数据库
- **插件系统** — Go 插件（.so）支持，拦截器链覆盖连接、发布、订阅、投递、断开事件
- **规则引擎** — 基于 CEL 的规则，支持消息转发、Webhook、日志等动作
- **多租户** — 每租户连接数限制、消息速率限制、Topic 隔离
- **流控** — 客户端级、全局级、自适应速率限制，支持背压和异常检测
- **熔断器** — 下游依赖自动故障隔离
- **审计日志** — 异步审计追踪，缓冲写入，支持备份文件
- **管理面板** — Vue.js 管理界面，实时集群监控、设备管理、Topic 查看
- **可观测性** — Prometheus 指标、结构化 JSON 日志、健康/就绪探针
- **安全** — API Key 认证、TLS 加固、WebSocket 来源验证、插件路径验证
- **优雅关停** — 可配置排空时间，支持零停机部署
- **压测工具** — 内置负载测试，支持连接、发布、订阅、混合工作负载

## 架构概览

```
                    ┌─────────────────────┐
                    │    全局四层负载均衡    │
                    └──────────┬──────────┘
           ┌───────────────────┼───────────────────┐
           ▼                   ▼                   ▼
    ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
    │    节点 1     │   │    节点 2     │   │    节点 3     │
    │  TCP :1883   │   │  TCP :1883   │   │  TCP :1883   │
    │  TLS :8883   │   │  TLS :8883   │   │  TLS :8883   │
    │  WS  :8083   │   │  WS  :8083   │   │  WS  :8083   │
    │  HTTP :9090  │   │  HTTP :9090  │   │  HTTP :9090  │
    │  Gossip:7000 │   │  Gossip:7000 │   │  Gossip:7000 │
    │  ┌────────┐  │   │  ┌────────┐  │   │  ┌────────┐  │
    │  │ Pebble │  │   │  │ Pebble │  │   │  │ Pebble │  │
    │  └────────┘  │   │  └────────┘  │   │  └────────┘  │
    └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
           │       Gossip 协议通信          │
           └──────────────────────────────┘
```

每个节点自包含嵌入式存储，通过 Gossip 协议管理成员关系，一致性哈希环进行分片路由，直接 TCP 连接进行跨节点消息转发。

## 快速开始

### 环境要求

- Go 1.25+
- Node.js 18+（构建管理面板，可选）

### 编译

```bash
# 仅编译 Go 二进制文件（不含管理面板）
make build-go

# 完整编译（含管理面板）
make build

# 交叉编译
make build-linux    # Linux amd64
make build-darwin   # macOS amd64
make build-windows  # Windows amd64
```

### 单节点运行

```bash
# 使用默认配置运行（TCP :1883, HTTP :9090）
./bin/dmqtt

# 启用持久化存储
DMQTT_DATA_DIR=/var/lib/dmqtt ./bin/dmqtt

# 启用 TLS
DMQTT_TLS_ADDR=:8883 \
DMQTT_TLS_CERT=/path/to/cert.pem \
DMQTT_TLS_KEY=/path/to/key.pem \
./bin/dmqtt

# 启用认证
DMQTT_AUTH_FILE=/etc/dmqtt/auth.json ./bin/dmqtt
```

### 使用 MQTT 客户端测试

```bash
# 订阅
mosquitto_sub -h localhost -p 1883 -t "test/topic"

# 发布
mosquitto_pub -h localhost -p 1883 -t "test/topic" -m "Hello DMQTT"
```

## 配置说明

所有配置通过 `DMQTT_` 前缀的环境变量设置：

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `DMQTT_TCP_ADDR` | `:1883` | MQTT TCP 监听地址 |
| `DMQTT_TLS_ADDR` | _（未启用）_ | MQTT TLS 监听地址 |
| `DMQTT_TLS_CERT` | | TLS 证书文件路径 |
| `DMQTT_TLS_KEY` | | TLS 私钥文件路径 |
| `DMQTT_WS_ADDR` | _（未启用）_ | WebSocket 监听地址 |
| `DMQTT_HTTP_ADDR` | `:9090` | HTTP API / 指标 / 管理面板地址 |
| `DMQTT_DATA_DIR` | _（内存模式）_ | Pebble 存储目录 |
| `DMQTT_AUTH_FILE` | _（无认证）_ | JSON 凭证文件路径 |
| `DMQTT_LOG_LEVEL` | `info` | 日志级别：debug, info, warn, error |
| `DMQTT_CLUSTER_ENABLED` | `false` | 启用集群模式 |
| `DMQTT_CLUSTER_NODE_ID` | | 唯一节点标识 |
| `DMQTT_CLUSTER_HOST` | `127.0.0.1` | 节点 IP（Gossip 和传输） |
| `DMQTT_CLUSTER_GOSSIP_PORT` | `7000` | Gossip 协议端口 |
| `DMQTT_CLUSTER_SEEDS` | | 种子节点地址（逗号分隔） |
| `DMQTT_REPLICATION_ENABLED` | `false` | 启用会话/消息副本 |

### 认证文件格式

```json
[
  { "username": "device1", "password_hash": "$2a$10$..." },
  { "username": "admin", "password_hash": "$2a$10$..." }
]
```

使用内置工具生成密码哈希：

```bash
go build -o bin/dmqtt-passwd ./cmd/dmqtt-passwd/
./bin/dmqtt-passwd mypassword
```

## 集群模式

### 三节点集群示例

```bash
# 节点 1
DMQTT_TCP_ADDR=:1883 \
DMQTT_HTTP_ADDR=:9090 \
DMQTT_CLUSTER_ENABLED=true \
DMQTT_CLUSTER_NODE_ID=node-1 \
DMQTT_CLUSTER_HOST=192.168.1.10 \
DMQTT_CLUSTER_GOSSIP_PORT=7000 \
./bin/dmqtt

# 节点 2
DMQTT_TCP_ADDR=:1884 \
DMQTT_HTTP_ADDR=:9091 \
DMQTT_CLUSTER_ENABLED=true \
DMQTT_CLUSTER_NODE_ID=node-2 \
DMQTT_CLUSTER_HOST=192.168.1.11 \
DMQTT_CLUSTER_GOSSIP_PORT=7001 \
DMQTT_CLUSTER_SEEDS=192.168.1.10:7000 \
./bin/dmqtt

# 节点 3
DMQTT_TCP_ADDR=:1885 \
DMQTT_HTTP_ADDR=:9092 \
DMQTT_CLUSTER_ENABLED=true \
DMQTT_CLUSTER_NODE_ID=node-3 \
DMQTT_CLUSTER_HOST=192.168.1.12 \
DMQTT_CLUSTER_GOSSIP_PORT=7002 \
DMQTT_CLUSTER_SEEDS=192.168.1.10:7000 \
./bin/dmqtt
```

### 集群功能

- **Gossip 自发现** — 通过 [memberlist](https://github.com/hashicorp/memberlist) 自动节点发现
- **一致性哈希环** — 客户端到分片的映射，可配置虚拟节点数（默认 150）
- **跨节点转发** — 消息通过直接 TCP 连接路由到正确的分片所有者
- **分片迁移** — 节点加入或离开时自动重新平衡
- **会话副本** — 可选的会话/离线消息跨副本同步

## 管理面板

内置管理面板提供实时集群监控：

- **仪表盘** — 连接数、消息吞吐量、内存使用、集群节点状态
- **设备列表** — 已连接客户端，包含 Topic 订阅、节点归属
- **设备详情** — 单客户端订阅列表、连接元数据
- **Topic 浏览器** — 活跃 Topic 及订阅者数量

使用 `make build` 编译后，访问 `http://localhost:9090/admin/`。

## HTTP API

| 端点 | 方法 | 说明 |
|------|------|------|
| `/health` | GET | 健康检查 |
| `/ready` | GET | 就绪探针 |
| `/metrics` | GET | Prometheus 指标 |
| `/admin/` | GET | 管理面板 |
| `/api/v1/clients` | GET | 客户端列表 |
| `/api/v1/clients/:id` | GET | 客户端详情 |
| `/api/v1/clients/:id` | DELETE | 断开客户端 |
| `/api/v1/topics` | GET | 活跃 Topic 列表 |
| `/api/v1/stats` | GET | Broker 统计 |
| `/api/v1/cluster/stats` | GET | 集群聚合统计 |

`/api/v1/` 下的接口支持 API Key 认证：

```bash
DMQTT_API_KEY=my-secret-key ./bin/dmqtt

# 使用请求头访问
curl -H "X-API-Key: my-secret-key" http://localhost:9090/api/v1/stats
```

## 插件系统

DMQTT 支持 Go 插件（`.so` 文件）实现拦截器接口：

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

编译并加载插件：

```bash
# 编译示例插件
make example-plugin

# 通过环境变量加载
DMQTT_PLUGINS='[{"path":"/opt/dmqtt/plugins/log-interceptor.so"}]' ./bin/dmqtt
```

参见 `examples/plugins/loginterceptor/` 获取完整示例。

## 规则引擎

使用 JSON 定义规则，当消息匹配时触发动作：

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

启用规则引擎：
```bash
DMQTT_RULE_ENGINE_ENABLED=true \
DMQTT_RULE_ENGINE_RULES_FILE=/etc/dmqtt/rules.json \
./bin/dmqtt
```

## 压测工具

内置负载测试工具：

```bash
go build -o bin/dmqtt-bench ./cmd/dmqtt-bench/

# 连接压测
./bin/dmqtt-bench conn -c 10000 -addr localhost:1883

# 发布压测
./bin/dmqtt-bench pub -c 100 -n 10000 -topic "bench/test" -qos 1

# 订阅压测
./bin/dmqtt-bench sub -c 100 -topic "bench/#"

# 混合负载
./bin/dmqtt-bench mixed -publishers 50 -subscribers 50 -n 10000
```

## 部署方式

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

### 裸机部署（systemd）

```bash
make build
sudo make install
sudo systemctl enable dmqtt
sudo systemctl start dmqtt
```

配置文件位置：`/etc/dmqtt/dmqtt.env`

## 项目结构

```
dmqtt/
├── cmd/
│   ├── dmqtt/           # 主 Broker 程序
│   ├── dmqtt-bench/     # 压测工具
│   └── dmqtt-passwd/    # 密码哈希生成器
├── config/              # 配置类型和默认值
├── internal/
│   ├── auth/            # 认证（文件模式，bcrypt）
│   ├── bench/           # 压测引擎
│   ├── broker/          # 核心 MQTT Broker、客户端处理、会话管理
│   ├── circuitbreaker/  # 熔断器模式
│   ├── cluster/         # Gossip 成员管理、哈希环、分片迁移
│   ├── codec/           # MQTT 3.1.1/5.0 编解码器
│   ├── httpapi/         # REST API、Prometheus 指标、管理面板
│   ├── logging/         # 结构化日志配置
│   ├── metrics/         # Prometheus 指标定义
│   ├── plugin/          # 插件加载器和拦截器链
│   │   └── audit/       # 内置审计拦截器
│   ├── ratelimit/       # 令牌桶限流器
│   ├── rule/            # CEL 规则引擎
│   ├── storage/         # Pebble 存储后端
│   ├── tenant/          # 多租户管理
│   └── transport/       # TCP、TLS、WebSocket 监听器
├── web/admin/           # Vue.js 管理面板
├── deploy/
│   ├── k8s/             # Kubernetes 部署清单 + Dockerfile
│   └── bare-metal/      # systemd 服务 + 安装脚本
├── examples/plugins/    # 示例拦截器插件
├── Makefile
└── go.mod
```

## 测试

```bash
# 运行全部测试（带竞态检测）
make test

# 仅运行短测试
make test-short

# 代码检查
make lint
```

## 许可证

MIT
