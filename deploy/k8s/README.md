# DMQTT Kubernetes Deployment

Deploy DMQTT broker as a 3-node clustered StatefulSet on Kubernetes.

## Prerequisites

- Kubernetes cluster (1.26+)
- `kubectl` configured with cluster access
- Docker (for building the image)

## Building the Image

```bash
# From the project root
docker build -f deploy/k8s/Dockerfile -t dmqtt:latest .
```

For a remote registry:
```bash
docker build -f deploy/k8s/Dockerfile -t your-registry/dmqtt:v1.0.0 .
docker push your-registry/dmqtt:v1.0.0
```

Update the `image` field in `statefulset.yaml` to match your registry.

## Deploying

```bash
# Apply all manifests
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/configmap.yaml
kubectl apply -f deploy/k8s/service-headless.yaml
kubectl apply -f deploy/k8s/service-mqtt.yaml
kubectl apply -f deploy/k8s/service-http.yaml
kubectl apply -f deploy/k8s/pdb.yaml
kubectl apply -f deploy/k8s/statefulset.yaml

# Verify pods are running
kubectl -n dmqtt get pods -w
```

## Configuration

Environment variables are managed via the ConfigMap (`configmap.yaml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DMQTT_TCP_ADDR` | `:1883` | MQTT TCP listen address |
| `DMQTT_HTTP_ADDR` | `:9090` | HTTP API listen address |
| `DMQTT_DATA_DIR` | `/data/dmqtt` | Pebble storage directory |
| `DMQTT_LOG_LEVEL` | `info` | Log level: debug, info, warn, error |
| `DMQTT_CLUSTER_ENABLED` | `true` | Enable cluster mode |
| `DMQTT_CLUSTER_GOSSIP_PORT` | `7000` | Gossip protocol port |

Node-specific values (`NODE_ID`, `HOST`, `SEEDS`) are automatically computed by the init container.

To change config:
```bash
kubectl -n dmqtt edit configmap dmqtt-config
kubectl -n dmqtt rollout restart statefulset dmqtt-node
```

## Architecture

```
                    ┌──────────────────┐
                    │  LoadBalancer     │
                    │  dmqtt-mqtt:1883  │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
        ┌──────────┐  ┌──────────┐  ┌──────────┐
        │ dmqtt-   │  │ dmqtt-   │  │ dmqtt-   │
        │ node-0   │  │ node-1   │  │ node-2   │
        │ PVC:10Gi │  │ PVC:10Gi │  │ PVC:10Gi │
        └────┬─────┘  └────┬─────┘  └────┬─────┘
             │              │              │
             └──────────────┼──────────────┘
                            │
                    Gossip (Headless Service)
                    dmqtt-headless:7000
```

## Scaling

```bash
# Scale to 5 nodes
kubectl -n dmqtt scale statefulset dmqtt-node --replicas=5
```

Note: The init container hardcodes `REPLICAS=3` for seed computation. When scaling beyond 3, only the first 3 nodes are used as seeds (this is sufficient — Gossip protocol will discover all nodes).

## Monitoring

The HTTP API is exposed via ClusterIP service `dmqtt-http:9090`:

```bash
# Port-forward for local access
kubectl -n dmqtt port-forward svc/dmqtt-http 9090:9090

# Check health
curl http://localhost:9090/health

# View metrics
curl http://localhost:9090/metrics
```

## Upgrading

```bash
# Build new image
docker build -f deploy/k8s/Dockerfile -t dmqtt:v2.0.0 .

# Update image in statefulset
kubectl -n dmqtt set image statefulset/dmqtt-node dmqtt=dmqtt:v2.0.0

# Watch rollout
kubectl -n dmqtt rollout status statefulset dmqtt-node
```

## Troubleshooting

**Pod stuck in Pending:**
```bash
kubectl -n dmqtt describe pod dmqtt-node-0
```

**Check logs:**
```bash
kubectl -n dmqtt logs dmqtt-node-0 -c dmqtt
kubectl -n dmqtt logs dmqtt-node-0 -c cluster-init  # init container
```

**Check cluster status:**
```bash
kubectl -n dmqtt exec dmqtt-node-0 -- env | grep DMQTT_CLUSTER
```

**Delete and recreate (WARNING: deletes data):**
```bash
kubectl delete -f deploy/k8s/statefulset.yaml
kubectl -n dmqtt delete pvc -l app.kubernetes.io/name=dmqtt
kubectl apply -f deploy/k8s/statefulset.yaml
```
