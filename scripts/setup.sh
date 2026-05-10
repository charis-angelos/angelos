#!/bin/bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "============================================"
echo " Angelos Setup Script"
echo "============================================"

# ── 1. Build release binary ──
echo ""
echo "[1/5] Building gateway (release)..."
cd "$ROOT/gateway"
cargo build --release 2>&1 | tail -3
cd "$ROOT"

# ── 2. Create .env if missing ──
echo ""
echo "[2/5] Checking configuration..."
if [ ! -f "$ROOT/.env" ]; then
    cp "$ROOT/.env.example" "$ROOT/.env"
    echo "  Created .env from .env.example — please review and adjust"
else
    echo "  .env exists"
fi

if [ ! -f "$ROOT/chain.json" ]; then
    cp "$ROOT/chain.json.example" "$ROOT/chain.json"
    echo "  Created chain.json from chain.json.example — please add your API keys!"
else
    echo "  chain.json exists"
fi

# ── 3. Install Open WebUI ──
echo ""
echo "[3/5] Installing Open WebUI (pip)..."
pip install open-webui 2>&1 | tail -3 || echo "  (if this failed, install manually: pip install open-webui)"

# ── 4. Install systemd user units ──
echo ""
echo "[4/5] Installing systemd user units..."
mkdir -p ~/.config/systemd/user/

cp "$ROOT/scripts/angelos-gateway.service" ~/.config/systemd/user/
cp "$ROOT/scripts/open-webui.service" ~/.config/systemd/user/

systemctl --user daemon-reload
systemctl --user enable --now angelos-gateway
echo "  angelos-gateway enabled and started"

systemctl --user enable --now open-webui 2>/dev/null || {
    echo "  open-webui systemd unit installed (start manually after verifying pip install)"
    echo "  Run: systemctl --user start open-webui"
}
echo "  open-webui service installed"

# Enable lingering for user services to survive logout
if command -v loginctl &>/dev/null; then
    sudo loginctl enable-linger "$USER" 2>/dev/null || echo "  (run 'sudo loginctl enable-linger $USER' manually if needed)"
fi

# ── 5. Set up cron ──
echo ""
echo "[5/5] Setting up cron job..."
CRON_LINE="0 9 * * * cd $ROOT && $ROOT/scripts/cron_daily.sh >> $ROOT/memory/logs/cron.log 2>&1"

if crontab -l 2>/dev/null | grep -F "cron_daily.sh" >/dev/null; then
    echo "  Cron job already exists, skipping"
else
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
    echo "  Daily cron job added (9 AM)"
fi

# ── Done ──
echo ""
echo "============================================"
echo " Setup complete!"
echo "============================================"
echo ""
echo " Services:"
echo "   Gateway:  http://127.0.0.1:8000  (health: /health)"
echo "   Open WebUI: http://127.0.0.1:3000"
echo ""
echo " Check status:"
echo "   systemctl --user status angelos-gateway"
echo "   systemctl --user status open-webui"
echo ""
echo " Cron:"
echo "   crontab -l | grep cron_daily"
echo ""
echo " Test:"
echo "   curl http://127.0.0.1:8000/health"
