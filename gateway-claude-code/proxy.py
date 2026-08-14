#!/usr/bin/env python3
"""
OpenAI-compatible HTTP shim in front of the local `claude` CLI (Claude Code),
so Open WebUI can talk to this machine's Claude Code login (subscription
quota, not a separate API key) as a chat model.

Each Open WebUI conversation is mapped to a Claude Code session via
`--resume`, keyed by a hash of the conversation's first user message, so
multi-turn context (including real tool-call state — files edited, commands
run) actually persists across turns instead of being replayed as text.

Full tool access, no permission prompts (--dangerously-skip-permissions) —
by explicit request. Bound to 127.0.0.1 only: reachable from Open WebUI's
backend on this same host, not directly from other Tailscale devices.
"""

import json
import hashlib
import subprocess
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "127.0.0.1"
PORT = 8100
CLAUDE_BIN = "claude"
CLAUDE_TIMEOUT = 600  # seconds; full-tool-access tasks can run long

SESSIONS_FILE = "/home/seii/angelos/gateway-claude-code/sessions.json"
_sessions_lock = threading.Lock()

MODEL_FLAGS = {
    "angelos-opus": "opus",
    "angelos-sonnet": "sonnet",
}

MODELS_RESPONSE = {
    "object": "list",
    "data": [
        {"id": mid, "object": "model", "created": 0, "owned_by": "claude-code"}
        for mid in MODEL_FLAGS
    ],
}


def load_sessions():
    try:
        with open(SESSIONS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_sessions(data):
    with open(SESSIONS_FILE, "w") as f:
        json.dump(data, f)


def conversation_key(messages, chat_id_header):
    # Prefer Open WebUI's own stable per-chat ID (sent only when
    # ENABLE_FORWARD_USER_INFO_HEADERS=true) — it doesn't collide when two
    # chats happen to open with the same first message, unlike the hash
    # fallback below.
    if chat_id_header:
        return f"chat:{chat_id_header}"
    first_user = next((m for m in messages if m.get("role") == "user"), None)
    content = first_user["content"] if first_user else ""
    if isinstance(content, list):  # OpenAI content-parts form
        content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
    return f"hash:{hashlib.sha256(content.strip().encode('utf-8')).hexdigest()}"


def build_cmd(prompt, resume_session_id, model_flag, system_prompt, extra):
    cmd = [CLAUDE_BIN, "-p", prompt, "--dangerously-skip-permissions"] + extra
    if resume_session_id:
        cmd += ["--resume", resume_session_id]
    if model_flag:
        cmd += ["--model", model_flag]
    if system_prompt:
        cmd += ["--append-system-prompt", system_prompt]
    return cmd


def run_claude(prompt, resume_session_id, model_flag, system_prompt):
    cmd = build_cmd(prompt, resume_session_id, model_flag, system_prompt, ["--output-format", "json"])
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=CLAUDE_TIMEOUT)
    if result.returncode != 0:
        raise RuntimeError(f"claude exited {result.returncode}: {result.stderr[:2000]}")
    return json.loads(result.stdout)


TOOL_SUMMARY_KEYS = ("command", "file_path", "pattern", "url", "path", "query", "description")


def summarize_tool_input(raw_json):
    try:
        data = json.loads(raw_json)
    except (json.JSONDecodeError, TypeError):
        return None
    for k in TOOL_SUMMARY_KEYS:
        if k in data and data[k]:
            val = str(data[k])
            return val if len(val) <= 100 else val[:97] + "..."
    return None


def stream_claude(prompt, resume_session_id, model_flag, system_prompt):
    """Yields ('delta', text), ('tool_start'|'tool_end', name, summary_or_None),
    ('thinking_start', None), ('thinking', text) — the last only if the API ever
    populates it — then finally ('done', session_id, is_error)."""
    cmd = build_cmd(
        prompt, resume_session_id, model_flag, system_prompt,
        ["--output-format", "stream-json", "--include-partial-messages", "--verbose"],
    )
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    deadline = time.monotonic() + CLAUDE_TIMEOUT
    block_types = {}  # content_block index -> type ("text" / "thinking" / "tool_use" / ...)
    block_names = {}  # index -> tool name, for tool_use blocks
    block_json = {}  # index -> accumulated partial_json, for tool_use blocks

    try:
        for line in proc.stdout:
            if time.monotonic() > deadline:
                proc.kill()
                raise RuntimeError("claude timed out")
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            kind = obj.get("type")
            if kind == "stream_event":
                event = obj.get("event", {})
                etype = event.get("type")
                if etype == "content_block_start":
                    idx = event["index"]
                    block = event.get("content_block", {})
                    block_types[idx] = block.get("type")
                    if block.get("type") == "tool_use":
                        block_names[idx] = block.get("name", "tool")
                        block_json[idx] = ""
                        yield ("tool_start", block_names[idx], None)
                    elif block.get("type") == "thinking":
                        # Live "thinking is happening now" marker, timed to the
                        # real thinking block's start. The actual thinking text
                        # isn't exposed by the API by default (comes through
                        # empty today) — real content, if it ever shows up, is
                        # still forwarded below.
                        yield ("thinking_start", None)
                elif etype == "content_block_delta":
                    idx = event["index"]
                    delta = event.get("delta", {})
                    dtype = delta.get("type")
                    btype = block_types.get(idx)
                    if dtype == "text_delta" and btype == "text":
                        yield ("delta", delta.get("text", ""))
                    elif dtype == "thinking_delta" and btype == "thinking":
                        text = delta.get("thinking", "")
                        if text:
                            yield ("thinking", text)
                    elif dtype == "input_json_delta" and btype == "tool_use":
                        block_json[idx] = block_json.get(idx, "") + delta.get("partial_json", "")
                elif etype == "content_block_stop":
                    idx = event["index"]
                    if block_types.get(idx) == "tool_use":
                        summary = summarize_tool_input(block_json.get(idx, ""))
                        yield ("tool_end", block_names.get(idx, "tool"), summary)
            elif kind == "result":
                proc.wait(timeout=10)
                yield ("done", obj.get("session_id"), obj.get("is_error", False))
                return
    finally:
        if proc.poll() is None:
            proc.kill()
    stderr = proc.stderr.read() if proc.stderr else ""
    raise RuntimeError(f"claude stream ended without a result line: {stderr[:2000]}")


def extract_system_and_prompt(messages):
    system_parts = [m["content"] for m in messages if m.get("role") == "system"]
    system_prompt = "\n".join(
        p if isinstance(p, str) else "".join(x.get("text", "") for x in p if isinstance(x, dict))
        for p in system_parts
    )
    last_user = next((m for m in reversed(messages) if m.get("role") == "user"), None)
    content = last_user["content"] if last_user else ""
    if isinstance(content, list):
        content = "".join(p.get("text", "") for p in content if isinstance(p, dict))
    return system_prompt, content


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[{time.strftime('%H:%M:%S')}] {self.address_string()} {fmt % args}")

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.rstrip("/") == "/v1/models":
            self._send_json(200, MODELS_RESPONSE)
        elif self.path == "/health":
            self._send_json(200, {"status": "ok"})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path.rstrip("/") != "/v1/chat/completions":
            self._send_json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length))
        except json.JSONDecodeError:
            self._send_json(400, {"error": "invalid JSON body"})
            return

        messages = body.get("messages", [])
        requested_model = body.get("model", "claude-code")
        model_flag = MODEL_FLAGS.get(requested_model)
        stream = bool(body.get("stream", False))

        system_prompt, prompt = extract_system_and_prompt(messages)
        chat_id_header = self.headers.get("X-OpenWebUI-Chat-Id")
        key = conversation_key(messages, chat_id_header)

        with _sessions_lock:
            sessions = load_sessions()
            resume_id = sessions.get(key)

        completion_id = f"chatcmpl-{uuid.uuid4().hex}"
        created = int(time.time())

        if stream:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()

            def send_chunk(delta, finish_reason=None):
                chunk = {
                    "id": completion_id, "object": "chat.completion.chunk", "created": created,
                    "model": requested_model,
                    "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
                }
                self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
                self.wfile.flush()

            try:
                sent_role = False

                def with_role(delta):
                    nonlocal sent_role
                    if not sent_role:
                        delta["role"] = "assistant"
                        sent_role = True
                    return delta

                for event in stream_claude(prompt, resume_id, model_flag, system_prompt):
                    kind = event[0]
                    if kind == "delta":
                        send_chunk(with_role({"content": event[1]}))
                    elif kind == "thinking_start":
                        send_chunk(with_role({"reasoning_content": "🤔 Thinking…\n"}))
                    elif kind == "thinking":
                        send_chunk(with_role({"reasoning_content": event[1]}))
                    elif kind == "tool_start":
                        _, tool_name, _ = event
                        send_chunk(with_role({"content": f"\n\n🔧 **{tool_name}**\n"}))
                    elif kind == "tool_end":
                        _, tool_name, summary = event
                        if summary:
                            send_chunk(with_role({"content": f"`{summary}`\n\n"}))
                    else:  # ("done", session_id, is_error)
                        _, session_id, _ = event
                        if session_id:
                            with _sessions_lock:
                                sessions = load_sessions()
                                sessions[key] = session_id
                                save_sessions(sessions)
                        send_chunk({}, finish_reason="stop")
            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception as e:
                send_chunk({"content": f"\n[proxy error: {e}]"}, finish_reason="stop")
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            return

        try:
            result = run_claude(prompt, resume_id, model_flag, system_prompt)
        except Exception as e:
            self._send_json(500, {"error": str(e)})
            return

        with _sessions_lock:
            sessions = load_sessions()
            sessions[key] = result["session_id"]
            save_sessions(sessions)

        text = result.get("result", "")
        response = {
            "id": completion_id,
            "object": "chat.completion",
            "created": created,
            "model": requested_model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": result.get("usage", {}).get("input_tokens", 0),
                "completion_tokens": result.get("usage", {}).get("output_tokens", 0),
                "total_tokens": result.get("usage", {}).get("input_tokens", 0)
                + result.get("usage", {}).get("output_tokens", 0),
            },
        }
        self._send_json(200, response)


if __name__ == "__main__":
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"claude-code-proxy listening on http://{HOST}:{PORT}")
    server.serve_forever()
