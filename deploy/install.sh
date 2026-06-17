#!/bin/bash
# Install mesh-gw and node-dash services on this host.
# Run as root from anywhere: bash /path/to/deploy/install.sh

set -e

MESH_GW_DIR="/usr/share/pac/dev/pio/projects/mt-yagi/mesh-gw"
NODE_DASH_DIR="/usr/share/pac/dev/pio/projects/mt-yagi/node-dash"

echo "=== mesh-gw: creating venv ==="
python3 -m venv "$MESH_GW_DIR/venv"
"$MESH_GW_DIR/venv/bin/pip" install --upgrade pip -q
"$MESH_GW_DIR/venv/bin/pip" install -r "$MESH_GW_DIR/requirements.txt" -q
echo "venv OK"

echo "=== mesh-gw: installing systemd service ==="
cp "$MESH_GW_DIR/deploy/mesh-gw.service" /etc/systemd/system/mesh-gw.service
systemctl daemon-reload
systemctl enable mesh-gw
echo "mesh-gw.service installed"

echo "=== node-dash: installing dependencies ==="
cd "$NODE_DASH_DIR"
pnpm install --frozen-lockfile 2>/dev/null || npm ci
echo "node-dash deps OK"

echo ""
echo "Done. Start services with:"
echo "  systemctl start mesh-gw"
echo ""
echo "Note: add BLE device addresses to:"
echo "  $MESH_GW_DIR/core/bridge_config.yaml"
