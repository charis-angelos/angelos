# Angelos — Personal Assistant

Local AI assistant with Markdown memory, powered by NVIDIA API (rig + Axum + Open WebUI).

## Quick Start

### One-shot setup (recommended)
```bash
./scripts/setup.sh
```
This builds the gateway, creates config files (.env, chain.json, SOUL.md), installs Open WebUI in a local venv, sets up systemd services, and configures cron.

### Manual setup

#### 1. Configure API keys
```bash
cp chain.json.example chain.json
# Edit chain.json with your real API keys
```

#### 2. Set up environment
```bash
cp .env.example .env
cp SOUL.md.example SOUL.md
# Adjust .env and SOUL.md as needed
```

#### 3. Build the gateway
```bash
cd gateway && cargo build --release
```

#### 4. Run CLI mode (test)
```bash
./gateway/target/release/gateway --mode cron --prompt "你好，介绍一下你自己"
```

#### 5. Start HTTP gateway
```bash
./gateway/target/release/gateway --mode http --port 8000
```

#### 6. Install & start Open WebUI
```bash
/usr/bin/python3.12 -m venv --system-site-packages .venv-openwebui
.venv-openwebui/bin/pip install open-webui
.venv-openwebui/bin/open-webui serve --host 127.0.0.1 --port 3000
```

Then connect Open WebUI to `http://127.0.0.1:8000/v1` as a custom OpenAI endpoint.

### 7. Set up cron (daily tasks at 9 AM)
```bash
(crontab -l; echo "0 9 * * * ~/angelos/scripts/cron_daily.sh") | crontab -
```

## Upgrading Open WebUI

```bash
.venv-openwebui/bin/pip install --upgrade open-webui
systemctl --user restart open-webui
```

## Systemd Deployment

```bash
mkdir -p ~/.config/systemd/user/

# Gateway service
cp scripts/angelos-gateway.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now angelos-gateway

# Open WebUI service (uses .venv-openwebui/bin/open-webui)
cp scripts/open-webui.service ~/.config/systemd/user/
systemctl --user enable --now open-webui

# Enable lingering for user services at boot
sudo loginctl enable-linger $USER
```

## API

### POST /v1/chat/completions
OpenAI-compatible chat completions endpoint with SSE streaming.

```bash
curl http://127.0.0.1:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Hello"}],"stream":false}'
```

### GET /health
Returns "OK" when the server is running.

## Architecture

```
Open WebUI → Axum Gateway (Rust) → rig Agent → NVIDIA API (fallback chain)
                    ↓
              ~/memory/*.md (Markdown persistence)
```

## Memory Layout

```
memory/
├── daily/YYYY-MM-DD.md    # Auto-archived daily notes
├── tasks/pending.md       # Task checklist [ ] / [x]
├── knowledge/*.md         # Structured knowledge
└── logs/                  # Cron execution logs
```
