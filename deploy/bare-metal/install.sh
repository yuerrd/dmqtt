#!/usr/bin/env bash
set -euo pipefail

# DMQTT Bare Metal Install Script
# Usage: sudo ./install.sh [path-to-dmqtt-binary]
#
# This script is idempotent — safe to run multiple times.

DMQTT_USER="dmqtt"
DMQTT_GROUP="dmqtt"
INSTALL_DIR="/opt/dmqtt"
CONFIG_DIR="/etc/dmqtt"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BINARY_SRC="${1:-${SCRIPT_DIR}/../../bin/dmqtt}"

# --- Helpers ---
info()  { echo "[INFO]  $*"; }
error() { echo "[ERROR] $*" >&2; }

# --- Pre-flight checks ---
if [ "$(id -u)" -ne 0 ]; then
    error "This script must be run as root (use sudo)"
    exit 1
fi

if [ ! -f "${BINARY_SRC}" ]; then
    error "Binary not found: ${BINARY_SRC}"
    error "Build first with 'make build' or pass the path: sudo ./install.sh /path/to/dmqtt"
    exit 1
fi

# --- Create system user/group ---
if ! getent group "${DMQTT_GROUP}" >/dev/null 2>&1; then
    groupadd --system "${DMQTT_GROUP}"
    info "Created group: ${DMQTT_GROUP}"
else
    info "Group already exists: ${DMQTT_GROUP}"
fi

if ! id "${DMQTT_USER}" >/dev/null 2>&1; then
    useradd --system --no-create-home --shell /usr/sbin/nologin \
        --gid "${DMQTT_GROUP}" "${DMQTT_USER}"
    info "Created user: ${DMQTT_USER}"
else
    info "User already exists: ${DMQTT_USER}"
fi

# --- Create directories ---
mkdir -p "${INSTALL_DIR}/bin"
mkdir -p "${INSTALL_DIR}/data"
mkdir -p "${INSTALL_DIR}/logs"
mkdir -p "${INSTALL_DIR}/certs"
mkdir -p "${CONFIG_DIR}"
info "Directories created"

# --- Install binary ---
install -m 0755 "${BINARY_SRC}" "${INSTALL_DIR}/bin/dmqtt"
info "Binary installed: ${INSTALL_DIR}/bin/dmqtt"

# --- Install environment file (preserve existing) ---
if [ ! -f "${CONFIG_DIR}/dmqtt.env" ]; then
    install -m 0644 "${SCRIPT_DIR}/dmqtt.env" "${CONFIG_DIR}/dmqtt.env"
    info "Environment file installed: ${CONFIG_DIR}/dmqtt.env"
else
    info "Environment file already exists, skipping (preserving customizations)"
fi

# --- Install systemd service ---
install -m 0644 "${SCRIPT_DIR}/dmqtt.service" /etc/systemd/system/dmqtt.service
info "Systemd service installed"

# --- Install logrotate config ---
install -m 0644 "${SCRIPT_DIR}/dmqtt.logrotate" /etc/logrotate.d/dmqtt
info "Logrotate config installed"

# --- Set ownership ---
chown -R "${DMQTT_USER}:${DMQTT_GROUP}" "${INSTALL_DIR}/data"
chown -R "${DMQTT_USER}:${DMQTT_GROUP}" "${INSTALL_DIR}/logs"
chown -R "${DMQTT_USER}:${DMQTT_GROUP}" "${INSTALL_DIR}/certs"
info "Ownership set"

# --- Enable service ---
systemctl daemon-reload
systemctl enable dmqtt
info "Service enabled (not started)"

echo ""
echo "============================================"
echo "  DMQTT installed successfully!"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Review config:    vi ${CONFIG_DIR}/dmqtt.env"
echo "  2. Start service:    sudo systemctl start dmqtt"
echo "  3. Check status:     sudo systemctl status dmqtt"
echo "  4. View logs:        sudo journalctl -u dmqtt -f"
echo ""