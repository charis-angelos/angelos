use anyhow::Context;
use futures::StreamExt;
use rig::providers::openai;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;

use crate::tools::{ReadMemory, RunBash, SearchMemory, UpdateTask, WriteMemory};

// ── Provider chain config ──

#[derive(Deserialize, Clone)]
pub struct ProviderEntry {
    pub model: String,
    pub base_url: String,
    pub api_key: String,
}

/// Multi-provider chain with fallback. Iterates through providers in order
/// until one succeeds.
pub struct ModelChain {
    pub providers: Vec<ProviderEntry>,
    http: reqwest::Client,
}

impl ModelChain {
    pub fn from_env() -> anyhow::Result<Self> {
        let path = std::env::var("CHAIN_CONFIG").unwrap_or_else(|_| "./chain.json".to_string());
        let json = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read chain config at {path}"))?;
        let providers: Vec<ProviderEntry> =
            serde_json::from_str(&json).context("Invalid chain.json")?;
        if providers.is_empty() {
            anyhow::bail!("chain.json must contain at least one provider");
        }
        Ok(Self {
            providers,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .build()
                .context("Failed to build HTTP client")?,
        })
    }

    fn make_client(entry: &ProviderEntry) -> openai::Client {
        openai::Client::from_url(&entry.api_key, &entry.base_url)
    }

    fn build_agent(
        entry: &ProviderEntry,
        preamble: &str,
    ) -> rig::agent::Agent<openai::CompletionModel> {
        Self::make_client(entry)
            .agent(&entry.model)
            .preamble(preamble)
            .tool(ReadMemory)
            .tool(SearchMemory)
            .tool(WriteMemory)
            .tool(UpdateTask)
            .tool(RunBash)
            .temperature(0.7)
            .max_tokens(4096)
            .build()
    }

    /// Non-streaming prompt for CLI/cron mode.
    pub async fn prompt(&self, user_prompt: &str, preamble: &str) -> anyhow::Result<String> {
        let mut last_err = None;

        for (i, entry) in self.providers.iter().enumerate() {
            tracing::info!(
                "Trying provider [{}/{}]: {} @ {}",
                i + 1,
                self.providers.len(),
                entry.model,
                entry.base_url
            );

            let agent = Self::build_agent(entry, preamble);

            match Self::run_agent_loop(&agent, user_prompt).await {
                Ok(response) => {
                    tracing::info!("Success via {}/{}", entry.base_url, entry.model);
                    return Ok(response);
                }
                Err(e) => {
                    tracing::warn!("{}/{} failed: {e}", entry.base_url, entry.model);
                    last_err = Some(e);
                    continue;
                }
            }
        }

        Err(anyhow::anyhow!(
            "All {} providers exhausted. Last error: {:?}",
            self.providers.len(),
            last_err.map(|e| e.to_string())
        ))
    }

    /// True streaming prompt for the HTTP API. Single streaming request with
    /// incremental tool-call parsing — text tokens are forwarded to the client
    /// while tool calls are buffered, executed, and results injected into the
    /// next streaming round.
    pub fn prompt_streaming(
        &self,
        user_prompt: &str,
        preamble: &str,
    ) -> impl futures::Stream<Item = anyhow::Result<String>> + use<> {
        let providers = self.providers.clone();
        let http = self.http.clone();
        let user_prompt = user_prompt.to_string();
        let preamble = preamble.to_string();
        let (tx, rx) = mpsc::channel::<anyhow::Result<String>>(64);

        tokio::spawn(async move {
            for (i, entry) in providers.iter().enumerate() {
                tracing::info!(
                    "Streaming [{}/{}]: {} @ {}",
                    i + 1,
                    providers.len(),
                    entry.model,
                    entry.base_url
                );

                match Self::stream_agent_loop(entry, &http, &preamble, &user_prompt, &tx).await {
                    Ok(()) => {
                        tracing::info!("Streaming via {}/{}", entry.base_url, entry.model);
                        return;
                    }
                    Err(e) => {
                        tracing::warn!("{}/{} streaming failed: {e}", entry.base_url, entry.model);
                        continue;
                    }
                }
            }
            let _ = tx
                .send(Err(anyhow::anyhow!(
                    "All {} providers exhausted for streaming",
                    providers.len()
                )))
                .await;
        });

        tokio_stream::wrappers::ReceiverStream::new(rx)
    }

    /// Single-stream agent loop: one HTTP streaming request handles everything.
    ///
    /// Text tokens are forwarded to `tx`. Tool-call deltas are buffered by index.
    /// When `finish_reason: tool_calls` arrives, tools are executed and results
    /// injected into the message history for the next round (up to 8).
    async fn stream_agent_loop(
        entry: &ProviderEntry,
        http: &reqwest::Client,
        preamble: &str,
        user_prompt: &str,
        tx: &mpsc::Sender<anyhow::Result<String>>,
    ) -> anyhow::Result<()> {
        let tools = crate::tools::tool_definitions();
        let mut messages: Vec<serde_json::Value> = vec![
            json!({"role": "system", "content": preamble}),
            json!({"role": "user", "content": user_prompt}),
        ];
        let url = format!("{}/chat/completions", entry.base_url.trim_end_matches('/'));

        for round in 0..8 {
            let body = json!({
                "model": entry.model,
                "messages": messages,
                "tools": tools,
                "temperature": 0.7,
                "max_tokens": 4096,
                "frequency_penalty": 0.3,
                "stream": true,
            });

            let response = http
                .post(&url)
                .header("Authorization", format!("Bearer {}", entry.api_key))
                .json(&body)
                .send()
                .await
                .with_context(|| format!("Streaming request failed to {url}"))?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                anyhow::bail!("Streaming HTTP {status}: {text}");
            }

            let mut stream = response.bytes_stream();
            let mut line_buf = String::new();
            let mut tool_bufs: std::collections::BTreeMap<usize, ToolCallAccum> =
                std::collections::BTreeMap::new();
            let mut finish_reason: Option<String> = None;

            // ── Parse SSE stream ──
            'sse: while let Some(chunk_result) = stream.next().await {
                let chunk = match chunk_result {
                    Ok(c) => c,
                    Err(e) => {
                        if !line_buf.is_empty() || !tool_bufs.is_empty() {
                            tracing::warn!("Stream interrupted, using partial response: {e}");
                            break 'sse;
                        }
                        return Err(anyhow::anyhow!(
                            "Stream read error (provider may have closed the connection): {e}"
                        ));
                    }
                };
                line_buf.push_str(&String::from_utf8_lossy(&chunk));

                while let Some(pos) = line_buf.find('\n') {
                    let line = line_buf[..pos].trim().to_string();
                    line_buf = line_buf[pos + 1..].to_string();

                    if line.is_empty() || line == "data: [DONE]" {
                        continue;
                    }

                    let data = match line.strip_prefix("data: ") {
                        Some(d) => d,
                        None => continue,
                    };

                    let chunk_v: serde_json::Value = match serde_json::from_str(data) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let choices = match chunk_v.get("choices").and_then(|c| c.as_array()) {
                        Some(c) => c,
                        None => continue,
                    };

                    let choice = match choices.first() {
                        Some(c) => c,
                        None => continue,
                    };

                    let delta = match choice.get("delta") {
                        Some(d) => d,
                        None => continue,
                    };

                    // Forward text content to the output stream
                    if let Some(content) = delta.get("content").and_then(|c| c.as_str()) {
                        if !content.is_empty()
                            && tx.send(Ok(content.to_string())).await.is_err()
                        {
                            return Ok(()); // client disconnected
                        }
                    }

                    // Buffer incremental tool-call deltas by index
                    if let Some(tool_calls) = delta.get("tool_calls").and_then(|t| t.as_array()) {
                        for tc_delta in tool_calls {
                            let idx = tc_delta
                                .get("index")
                                .and_then(|i| i.as_u64())
                                .unwrap_or(0) as usize;
                            let accum = tool_bufs.entry(idx).or_insert_with(|| ToolCallAccum {
                                id: String::new(),
                                name: String::new(),
                                arguments: String::new(),
                            });

                            if let Some(id) = tc_delta.get("id").and_then(|i| i.as_str()) {
                                accum.id.push_str(id);
                            }
                            if let Some(func) = tc_delta.get("function") {
                                if let Some(name) = func.get("name").and_then(|n| n.as_str()) {
                                    accum.name.push_str(name);
                                }
                                if let Some(args) = func.get("arguments").and_then(|a| a.as_str())
                                {
                                    accum.arguments.push_str(args);
                                }
                            }
                        }
                    }

                    // finish_reason only appears in the final chunk of a stream
                    if let Some(fr) = choice.get("finish_reason").and_then(|f| f.as_str()) {
                        finish_reason = Some(fr.to_string());
                        break 'sse;
                    }
                }
            }

            // Use partial content if stream was interrupted
            if finish_reason.is_none() && (!line_buf.is_empty() || !tool_bufs.is_empty()) {
                finish_reason = Some("stop".to_string());
            }

            match finish_reason.as_deref() {
                Some("tool_calls") if !tool_bufs.is_empty() => {
                    tracing::info!("[{round}] Executing {} tool(s)", tool_bufs.len());

                    let mut assistant_tool_calls = Vec::new();
                    let mut tool_results: Vec<serde_json::Value> = Vec::new();

                    for tc in tool_bufs.values() {
                        tracing::info!("[{round}] Tool: {}({})", tc.name, tc.arguments);

                        // Stream tool call in Hermes Tool Feed style
                        let icon = tool_icon(&tc.name);
                        let _ = tx
                            .send(Ok(format!(
                                "┊ {icon} `{name}`\n",
                                name = tc.name,
                            )))
                            .await;
                        let _ = tx
                            .send(Ok(format!(
                                "┊   `{}`\n",
                                tc.arguments
                            )))
                            .await;

                        let started = std::time::Instant::now();
                        let result = crate::tools::execute_tool(&tc.name, &tc.arguments)
                            .await
                            .unwrap_or_else(|e| format!("Error: {e}"));
                        let elapsed = started.elapsed().as_secs_f64();

                        // Stream result summary + content preview
                        let lines = result.lines().count();
                        let preview = format_result_preview(&result);
                        let _ = tx
                            .send(Ok(format!(
                                "┊   ✅ {}B, {lines} lines ({elapsed:.1}s){preview}\n\n---\n\n",
                                result.len(),
                            )))
                            .await;

                        let truncated: String = result.chars().take(300).collect();
                        let dots = if result.len() > 300 { "…" } else { "" };
                        tracing::info!("[{round}] Tool result: {truncated}{dots}");

                        assistant_tool_calls.push(json!({
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.name,
                                "arguments": tc.arguments,
                            }
                        }));

                        tool_results.push(json!({
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": result,
                        }));
                    }

                    messages.push(json!({
                        "role": "assistant",
                        "content": null,
                        "tool_calls": assistant_tool_calls,
                    }));
                    messages.extend(tool_results);

                    continue; // next round
                }
                Some("stop") => return Ok(()),
                Some("length") => {
                    let _ = tx
                        .send(Ok("\n\n ⚠️ Response truncated (max tokens)\n".into()))
                        .await;
                    return Ok(());
                }
                Some("content_filter") => {
                    let _ = tx
                        .send(Ok("\n\n 🚫 Content filtered by provider\n".into()))
                        .await;
                    return Ok(());
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "Stream ended without finish_reason (provider may have crashed or connection lost)"
                    ));
                }
                other => {
                    return Err(anyhow::anyhow!(
                        "Unexpected finish_reason: {other:?}"
                    ));
                }
            }
        }

        Err(anyhow::anyhow!("Max tool call rounds (8) exceeded"))
    }

    async fn run_agent_loop(
        agent: &rig::agent::Agent<openai::CompletionModel>,
        user_prompt: &str,
    ) -> anyhow::Result<String> {
        use rig::completion::{Completion, Message, ModelChoice};

        let mut chat_history: Vec<Message> = Vec::new();
        let mut current_prompt = user_prompt.to_string();

        for round in 0..8 {
            let builder = agent
                .completion(&current_prompt, chat_history.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Completion error: {e}"))?;

            let response = builder
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("Send error: {e}"))?;

            match response.choice {
                ModelChoice::Message(msg) => return Ok(msg),
                ModelChoice::ToolCall(toolname, args) => {
                    tracing::info!("[{round}] Tool call: {toolname}({args})");

                    let result = agent
                        .tools
                        .call(&toolname, args.to_string())
                        .await
                        .map_err(|e| anyhow::anyhow!("Tool error: {e}"))?;

                    let truncated: String = result.chars().take(300).collect();
                    let dots = if result.len() > 300 { "…" } else { "" };
                    tracing::info!("[{round}] Tool result: {truncated}{dots}");

                    chat_history.push(Message {
                        role: "assistant".into(),
                        content: format!("I called the tool `{toolname}` with arguments: {args}"),
                    });
                    chat_history.push(Message {
                        role: "user".into(),
                        content: format!("Tool `{toolname}` returned:\n{result}"),
                    });

                    current_prompt =
                        "Based on the tool result above, respond to my original request.".into();
                }
            }
        }

        Err(anyhow::anyhow!("Max tool call rounds (8) exceeded"))
    }
}

/// Map a tool name to its Hermes-style emoji icon.
fn tool_icon(name: &str) -> &str {
    match name {
        "read_memory" => "📖",
        "search_memory" => "🔍",
        "write_memory" => "✏️",
        "update_task" => "✅",
        "run_bash" => "🔧",
        _ => "💻",
    }
}

/// Format a tool result preview in Hermes Tool Feed style.
/// Shows first 5 lines indented under `┊   `.
fn format_result_preview(result: &str) -> String {
    if result.trim().is_empty() {
        return String::new();
    }
    let preview: String = result
        .lines()
        .take(5)
        .map(|l| format!("┊   {l}"))
        .collect::<Vec<_>>()
        .join("\n");
    let dots = if result.lines().count() > 5 { "\n┊   …" } else { "" };
    format!("\n{preview}{dots}")
}

/// Accumulates incremental tool-call deltas during SSE streaming.
struct ToolCallAccum {
    id: String,
    name: String,
    arguments: String,
}

/// CLI/cron mode: single prompt, blocking output
pub async fn run_sync(prompt: String, soul: String) -> anyhow::Result<String> {
    let preamble = build_full_preamble(&soul);
    let chain = ModelChain::from_env()?;
    chain.prompt(&prompt, &preamble).await
}

/// Build the full preamble from SOUL.md plus memory context and skills injection.
pub fn build_full_preamble(soul: &str) -> String {
    let today = chrono::Local::now().format("%Y-%m-%d");
    let pending_tasks = crate::memory::read_memory("tasks/pending.md").unwrap_or_default();
    let daily_note = crate::memory::read_memory(&format!("daily/{today}.md")).unwrap_or_default();
    let skills = crate::skills::discover();
    let skills_catalog = crate::skills::build_catalog(&skills);

    format!(
        "\
{soul}

---

## Current Context (auto-injected)
- Today: {today}
- Memory directory: $MEMORY_DIR

### Today's Note ({today})
{daily_note}

### Pending Tasks
{pending_tasks}
{skills_catalog}

## Tool Usage Guidelines
- Use `read_memory` to retrieve any memory file.
- Use `search_memory` to find information across all memory files.
- Use `write_memory` to create or update any .md file.
- Use `update_task` to mark tasks as done/undo in tasks/pending.md.
- Use `run_bash` to execute shell commands for system operations.
- Be concise. Use tools proactively when the user asks about past notes, tasks, or stored knowledge.
"
    )
}
