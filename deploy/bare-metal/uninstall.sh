#!/usr/bin/env bash
set -euo pipefail

# DMQTT Bare Metal Uninstall Script
# Usage: sudo ./uninstall.sh

DMQTT_USER="dmqtt"
DMQTT_GROUP="dmqtt"
INSTALL_DIR="/opt/dmqtt"
CONFIG_DIR="/etc/dmqtt"

info()  { echo "[INFO]  $*"; }
error() { echo "[ERROR] $*" >&2; }

if [ "$(id -u)" -ne 0 ]; then
    error "This script must be run as root (use sudo)"
    exit 1
fi

# --- Stop and disable service ---
if systemctl is-active --quiet dmqtt 2>/dev/null; then
    systemctl stop dmqtt
    info "Service stopped"
fi
if systemctl is-enabled --quiet dmqtt 2>/dev/null; then
    systemctl disable dmqtt
    info "Service disabled"
fi

# --- Remove systemd unit ---
rm -f /etc/systemd/system/dmqtt.service
systemctl daemon-reload
info "Systemd unit removed"

# --- Remove logrotate config ---
rm -f /etc/logrotate.d/dmqtt
info "Logrotate config removed"

# --- Remove binary ---
rm -f "${INSTALL_DIR}/bin/dmqtt"
info "Binary removed"

# --- Ask about data ---
echo ""
read -r -p "Remove data directory ${INSTALL_DIR}/data? [y/N] " remove_data
if [[ "${remove_data}" =~ ^[Yy]$ ]]; then
    rm -rf "${INSTALL_DIR}/data"
    info "Data directory removed"
else
    info "Data directory preserved"
fi

# --- Ask about config ---
read -r -p "Remove config directory ${CONFIG_DIR}? [y/N] " remove_config
if [[ "${remove_config}" =~ ^[Yy]$ ]]; then
    rm -rf "${CONFIG_DIR}"
    info "Config directory removed"
else
    info "Config directory preserved"
fi

# --- Ask about user ---
read -r -p "Remove system user ${DMQTT_USER}? [y/N] " remove_user
if [[ "${remove_user}" =~ ^[Yy]$ ]]; then
    if id "${DMQTT_USER}" >/dev/null 2>&1; then
        userdel "${DMQTT_USER}"
        info "User removed"
    fi
    if getent group "${DMQTT_GROUP}" >/dev/null 2>&1; then
        groupdel "${DMQTT_GROUP}"
        info "Group removed"
    fi
else
    info "User/group preserved"
fi

# --- Clean up empty dirs ---
rmdir "${INSTALL_DIR}/bin" 2>/dev/null || true
rmdir "${INSTALL_DIR}/logs" 2>/dev/null || true
rmdir "${INSTALL_DIR}/certs" 2>/dev/null || true
rmdir "${INSTALL_DIR}" 2>/dev/null || true

echo ""
echo "DMQTT uninstalled."