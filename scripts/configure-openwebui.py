#!/usr/bin/env python3
"""Configure Open WebUI to register the Angelos gateway as an OpenAI-compatible API.

This sets up the local Angelos gateway (http://127.0.0.1:8000/v1) as a
model provider in Open WebUI, so that features like auto-title generation
and model selection work correctly.
"""

import json
import os
import sqlite3
import sys
import time


def find_db():
    """Locate the Open WebUI SQLite database."""
    # Try DATA_DIR env var first
    data_dir = os.environ.get("DATA_DIR")
    if data_dir:
        return os.path.join(data_dir, "webui.db")

    # Otherwise derive from the open_webui package location
    try:
        import open_webui
        pkg_dir = os.path.dirname(open_webui.__file__)
        return os.path.join(pkg_dir, "data", "webui.db")
    except ImportError:
        pass

    # Fallback: search common locations
    home = os.path.expanduser("~")
    candidates = [
        os.path.join(home, ".local/lib"),
        os.path.join(home, ".local/share"),
    ]
    for base in candidates:
        for root, dirs, files in os.walk(base):
            if "webui.db" in files and "open_webui" in root:
                return os.path.join(root, "webui.db")

    return None


def main():
    db_path = find_db()

    if db_path is None:
        print("  Warning: Could not find webui.db, skipping model config")
        print("  Configure manually in Open WebUI Admin > Settings > Connections")
        return

    # Wait up to 30s for the database to be created
    for _ in range(30):
        if os.path.exists(db_path):
            break
        time.sleep(1)

    if not os.path.exists(db_path):
        print(f"  Warning: {db_path} not found after waiting, skipping")
        return

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT data FROM config LIMIT 1").fetchone()
        config = json.loads(row[0]) if row else {}
        changed = False

        # --- 1. Enable base models cache ---
        models = config.setdefault("models", {})
        if not models.get("base_models_cache"):
            models["base_models_cache"] = True
            changed = True

        # --- 2. Register Angelos in OpenAI API connections ---
        openai = config.setdefault("openai", {})
        api_configs = openai.setdefault("api_configs", {})
        api_base_urls = openai.setdefault("api_base_urls", [])
        api_keys = openai.setdefault("api_keys", [])

        angelos_url = "http://127.0.0.1:8000/v1"

        # Check if an entry for this URL already exists
        existing_key = None
        for key, cfg in api_configs.items():
            idx = int(key)
            if idx < len(api_base_urls) and api_base_urls[idx] == angelos_url:
                existing_key = key
                break

        if existing_key is not None:
            cfg = api_configs[existing_key]
            if cfg.get("model_ids") != ["angelos"]:
                cfg["model_ids"] = ["angelos"]
                changed = True
                print("  Updated model_ids for existing Angelos connection")
            else:
                print("  Angelos connection already configured")
        else:
            # Add new API connection
            next_key = str(len(api_configs))
            api_base_urls.append(angelos_url)
            api_keys.append("sk-local")
            api_configs[next_key] = {
                "enable": True,
                "tags": [],
                "prefix_id": "",
                "model_ids": ["angelos"],
                "connection_type": "external",
                "auth_type": "bearer",
            }
            changed = True
            print("  Added Angelos API connection to Open WebUI")

        # --- 3. Write back if changed ---
        if changed:
            conn.execute("UPDATE config SET data = ?", (json.dumps(config),))
            conn.commit()
            print("  Open WebUI config updated successfully")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
