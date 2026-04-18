# DMQTT Bare Metal Deployment

Deploy DMQTT broker on a Linux server with systemd.

## Prerequisites

- Linux (Ubuntu 20.04+ / Debian 11+ / RHEL 8+ / CentOS 8+)
- Root or sudo access
- Go 1.25+ (for building from source)

## Quick Start

```bash
# 1. Build the binary
make build

# 2. Install (creates user, directories, systemd service)
sudo ./deploy/bare-metal/install.sh

# 3. Configure (optional — defaults work out of the box)
sudo vi /etc/dmqtt/dmqtt.env

# 4. Start the broker
sudo systemctl start dmqtt

# 5. Verify
sudo systemctl status dmqtt
```

## Configuration

The broker reads environment variables from `/etc/dmqtt/dmqtt.env`. All values are optional — the broker uses sensible defaults if not set.

| Variable | Default | Description |
|----------|---------|-------------|
| `DMQTT_TCP_ADDR` | `:1883` | MQTT TCP listen address |
| `DMQTT_TLS_ADDR` | _(disabled)_ | MQTT over TLS listen address |
| `DMQTT_TLS_CERT` | _(none)_ | TLS certificate file path |
| `DMQTT_TLS_KEY` | _(none)_ | TLS private key file path |
| `DMQTT_WS_ADDR` | _(disabled)_ | WebSocket listen address |
| `DMQTT_HTTP_ADDR` | `:9090` | HTTP API (metrics, health, devices) |
| `DMQTT_DATA_DIR` | _(in-memory)_ | Pebble storage directory |
| `DMQTT_AUTH_FILE` | _(no auth)_ | Authentication credentials JSON file |
| `DMQTT_LOG_LEVEL` | `info` | Log level: debug, info, warn, error |

To enable persistence, set `DMQTT_DATA_DIR=/opt/dmqtt/data`.

## Directory Structure

```
/opt/dmqtt/
├── bin/dmqtt          # Broker binary
├── data/              # Pebble data (if DMQTT_DATA_DIR set)
├── logs/              # Log files (if using file logging)
└── certs/             # TLS certificates

/etc/dmqtt/
└── dmqtt.env          # Environment configuration
```

## Operations

```bash
# Start / Stop / Restart
sudo systemctl start dmqtt
sudo systemctl stop dmqtt
sudo systemctl restart dmqtt

# Check status
sudo systemctl status dmqtt

# View logs (real-time)
sudo journalctl -u dmqtt -f

# View recent logs
sudo journalctl -u dmqtt --since "1 hour ago"

# Reload rules and tenants (SIGHUP)
sudo systemctl reload dmqtt
```

## Upgrading

```bash
# 1. Build new binary
make build

# 2. Stop broker
sudo systemctl stop dmqtt

# 3. Replace binary
sudo install -m 0755 bin/dmqtt /opt/dmqtt/bin/dmqtt

# 4. Start broker
sudo systemctl start dmqtt
```

## Uninstalling

```bash
sudo ./deploy/bare-metal/uninstall.sh
```

The script will ask before removing data, config, and the system user.

## Troubleshooting

**Service fails to start:**
```bash
sudo journalctl -u dmqtt -e    # Check recent errors
```

**Port already in use:**
```bash
ss -tlnp | grep 1883           # Check what's using the port
```

**Permission denied on data directory:**
```bash
sudo chown -R dmqtt:dmqtt /opt/dmqtt/data
```

**Check file descriptor limits:**
```bash
cat /proc/$(pgrep dmqtt)/limits | grep "open files"
```