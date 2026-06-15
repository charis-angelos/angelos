use anyhow::Context;
use futures::StreamExt;
use rig::providers::openai;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;

use crate::tools::{ReadSelf, RunBash, SearchSelf, UpdateTask, WriteSelf};

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
                // No global timeout — per-operation timeouts are handled
                // via tokio::time::timeout at the call sites (initial
                // connect 15s, per-chunk read 30s). A global reqwest
                // timeout would kill long-running streaming responses
                // mid-flight.
                .connect_timeout(std::time::Duration::from_secs(15))
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
            .tool(ReadSelf)
            .tool(SearchSelf)
            .tool(WriteSelf)
            .tool(UpdateTask)
            .tool(RunBash)
            .temperature(0.7)
            .max_tokens(4096)
            .build()
    }

    /// Non-streaming prompt for CLI/cron mode.
    pub async fn prompt(&self, user_prompt: &str, preamble: &str) -> anyhow::Result<String> {
        use rig::completion::Message;

        let mut chat_history: Vec<Message> = Vec::new();
        let (model, response) =
            Self::run_agent_loop(&self.providers, preamble, user_prompt, &mut chat_history).await?;

        let preview: String = response.chars().take(200).collect();
        if response.is_empty() {
            tracing::warn!("Returned empty response after all providers tried");
        } else {
            tracing::info!(
                "{}: response ({} chars): {}{}",
                model,
                response.len(),
                preview,
                if response.len() > 200 { "…" } else { "" }
            );
        }
        Ok(format!("**Model: {model}**\n\n{response}"))
    }

    /// Lightweight non-streaming prompt for simple API requests (title/tag
    /// generation, suggestions, etc.). No tools, single completion, clean
    /// output — no model-name prefix. With provider failover.
    pub async fn prompt_light(&self, user_prompt: &str, preamble: &str) -> anyhow::Result<String> {
        use rig::completion::{Completion, ModelChoice};

        let mut last_err = None;

        for (i, entry) in self.providers.iter().enumerate() {
            tracing::info!(
                "Light [{}/{}]: {} @ {}",
                i + 1,
                self.providers.len(),
                entry.model,
                entry.base_url
            );

            let agent = Self::make_client(entry)
                .agent(&entry.model)
                .preamble(preamble)
                .temperature(0.3)
                .max_tokens(2048)
                .build();

            let builder = match agent.completion(user_prompt, vec![]).await {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("Light {}/{} completion error: {e}", entry.base_url, entry.model);
                    last_err = Some(e.into());
                    continue;
                }
            };

            let response = match builder.send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("Light {}/{} send error: {e}", entry.base_url, entry.model);
                    last_err = Some(e.into());
                    continue;
                }
            };

            match response.choice {
                ModelChoice::Message(msg) => {
                    let trimmed = msg.trim();
                    if trimmed.is_empty() {
                        tracing::warn!("Light {}/{} returned empty message", entry.base_url, entry.model);
                        last_err = Some(anyhow::anyhow!("empty response"));
                        continue;
                    }
                    tracing::info!("Light via {}/{}: {} chars", entry.base_url, entry.model, msg.len());
                    return Ok(msg);
                }
                _ => {
                    tracing::warn!("Light {}/{} returned unexpected choice", entry.base_url, entry.model);
                    last_err = Some(anyhow::anyhow!("unexpected response type"));
                    continue;
                }
            }
        }

        Err(anyhow::anyhow!(
            "All {} providers exhausted in light mode. Last error: {:?}",
            self.providers.len(),
            last_err.map(|e| e.to_string())
        ))
    }

    /// True streaming prompt for the HTTP API. Provider retry happens at the
    /// API-call level (inside each round) — a transient failure retries the
    /// same round with the next provider, preserving all prior tool results.
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
            let mut messages: Vec<serde_json::Value> = vec![
                json!({"role": "system", "content": preamble}),
                json!({"role": "user", "content": user_prompt}),
            ];

            match Self::stream_agent_loop(&providers, &http, &mut messages, &tx).await {
                Ok(()) => {}
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                }
            }
        });

        tokio_stream::wrappers::ReceiverStream::new(rx)
    }

    /// Agent loop with per-round provider failover. The round loop is outer;
    /// each round iterates the provider chain so a single failed API call
    /// never discards prior tool results.
    async fn stream_agent_loop(
        providers: &[ProviderEntry],
        http: &reqwest::Client,
        messages: &mut Vec<serde_json::Value>,
        tx: &mpsc::Sender<anyhow::Result<String>>,
    ) -> anyhow::Result<()> {
        let tools = crate::tools::tool_definitions();
        let mut last_model: Option<String> = None;

        for round in 0..7 {
            // ── Try each provider until one succeeds for this round ──
            let mut round_success = false;

            for (pi, entry) in providers.iter().enumerate() {
                tracing::info!(
                    "[{round}] Provider [{}/{}]: {} @ {} ({} messages in context)",
                    pi + 1,
                    providers.len(),
                    entry.model,
                    entry.base_url,
                    messages.len()
                );

                // Announce model switch BEFORE the HTTP request so the
                // user sees it even if the provider takes 120s to time out.
                if last_model.as_deref() != Some(&entry.model) {
                    let label = if last_model.is_some() {
                        format!("\n\n🔄 Switched to **Model: {}**\n\n", entry.model)
                    } else {
                        format!("**Model: {}**\n\n", entry.model)
                    };
                    last_model = Some(entry.model.clone());
                    let _ = tx.send(Ok(label)).await;
                }

                let url = format!("{}/chat/completions", entry.base_url.trim_end_matches('/'));
                let body = json!({
                    "model": entry.model,
                    "messages": messages,
                    "tools": tools,
                    "temperature": 0.7,
                    "max_tokens": 4096,
                    "frequency_penalty": 0.3,
                    "stream": true,
                });

                let response = match tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    http
                        .post(&url)
                        .header("Authorization", format!("Bearer {}", entry.api_key))
                        .json(&body)
                        .send(),
                )
                .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => {
                        tracing::warn!("[{round}] {}/{} HTTP send failed: {e}", entry.base_url, entry.model);
                        continue;
                    }
                    Err(_elapsed) => {
                        tracing::warn!("[{round}] {}/{} first-byte timeout (30s)", entry.base_url, entry.model);
                        continue;
                    }
                };

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    tracing::warn!("[{round}] {}/{} HTTP {status}: {text}", entry.base_url, entry.model);
                    continue; // try next provider
                }

                let mut stream = response.bytes_stream();
                let mut line_buf = String::new();
                let mut tool_bufs: std::collections::BTreeMap<usize, ToolCallAccum> =
                    std::collections::BTreeMap::new();
                let mut finish_reason: Option<String> = None;
                let mut content_buf = String::new();
                let mut thinking_open = false;

                // ── Parse SSE stream ──
                'sse: loop {
                    let chunk_result = match tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        stream.next(),
                    )
                    .await
                    {
                        Ok(Some(Ok(c))) => Ok(c),
                        Ok(Some(Err(e))) => Err(e),
                        Ok(None) => break 'sse, // stream ended cleanly
                        Err(_elapsed) => {
                            // No chunk for 30s — treat as interruption.
                            // Only keep partial response if tool_calls are
                            // in-flight; content-only interruption should
                            // still switch provider for a complete answer.
                            if !tool_bufs.is_empty() {
                                tracing::warn!("[{round}] {}/{} chunk timeout (30s), finalizing partial tool calls", entry.base_url, entry.model);
                                if finish_reason.is_none() {
                                    finish_reason = Some("stop".to_string());
                                }
                            } else {
                                tracing::warn!("[{round}] {}/{} chunk timeout (30s), switching provider", entry.base_url, entry.model);
                            }
                            break 'sse;
                        }
                    };

                    let chunk = match chunk_result {
                        Ok(c) => c,
                        Err(e) => {
                            if !line_buf.is_empty() || !tool_bufs.is_empty() {
                                tracing::warn!("[{round}] {}/{} stream interrupted, using partial response: {e}", entry.base_url, entry.model);
                                break 'sse;
                            }
                            tracing::warn!("[{round}] {}/{} stream read error: {e}", entry.base_url, entry.model);
                            break 'sse;
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

                        // Capture reasoning_content first — reasoning models
                        // (deepseek-v4-pro, qwen3-next-80b-a3b-thinking, kimi-k2.6)
                        // emit this before content. Without this, the stream
                        // completes with empty content_buf → fallback cascade.
                        if let Some(rc) = delta.get("reasoning_content").and_then(|c| c.as_str()) {
                            if !rc.is_empty() {
                                if !thinking_open {
                                    thinking_open = true;
                                    let _ = tx.send(Ok("\n💭 **Thinking:**\n> ".into())).await;
                                }
                                // Escape newlines in reasoning to keep blockquote
                                let formatted = rc.replace('\n', "\n> ");
                                content_buf.push_str(&formatted);
                                if tx.send(Ok(formatted)).await.is_err() {
                                    return Ok(()); // client disconnected
                                }
                            }
                        }

                        if let Some(content) = delta.get("content").and_then(|c| c.as_str()) {
                            if !content.is_empty() {
                                if thinking_open {
                                    thinking_open = false;
                                    let _ = tx.send(Ok("\n\n---\n\n".into())).await;
                                }
                                content_buf.push_str(content);
                                if tx.send(Ok(content.to_string())).await.is_err() {
                                    return Ok(()); // client disconnected
                                }
                            }
                        }

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
                                    if let Some(args) = func.get("arguments").and_then(|a| a.as_str()) {
                                        accum.arguments.push_str(args);
                                    }
                                }
                            }
                        }

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

                // If we have no valid finish_reason, this provider failed — try next
                if finish_reason.is_none() {
                    tracing::warn!("[{round}] {}/{} stream ended without finish_reason", entry.base_url, entry.model);
                    continue;
                }

                match finish_reason.as_deref() {
                    Some("tool_calls") if !tool_bufs.is_empty() => {
                        tracing::info!("[{round}] {}/{}: {} tool(s)", entry.base_url, entry.model, tool_bufs.len());

                        let mut assistant_tool_calls = Vec::new();
                        let mut tool_results: Vec<serde_json::Value> = Vec::new();

                        for tc in tool_bufs.values() {
                            tracing::info!("[{round}] Tool: {}({})", tc.name, tc.arguments);

                            let icon = tool_icon(&tc.name);
                            let _ = tx
                                .send(Ok(format!(
                                    "┊ {icon} `{name}`\n",
                                    name = tc.name,
                                )))
                                .await;
                            let _ = tx
                                .send(Ok(format!(
                                    "┊   ```` {} ````\n",
                                    tc.arguments
                                )))
                                .await;

                            let started = std::time::Instant::now();
                            let result = crate::tools::execute_tool(&tc.name, &tc.arguments)
                                .await
                                .unwrap_or_else(|e| format!("Error: {e}"));
                            let elapsed = started.elapsed().as_secs_f64();

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

                        round_success = true;
                        break; // out of provider loop, continue to next round
                    }
                    Some("stop") => {
                        if content_buf.trim().is_empty() {
                            tracing::warn!("[{round}] {}/{} stream produced empty content — trying next provider", entry.base_url, entry.model);
                            continue;
                        }
                        let preview: String = content_buf.chars().take(200).collect();
                        let dots = if content_buf.len() > 200 { "…" } else { "" };
                        tracing::info!(
                            "[{round}] Success via {}/{}: response ({} chars): {}{}",
                            entry.base_url, entry.model, content_buf.len(), preview, dots
                        );
                        return Ok(());
                    }
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
                    Some(other) => {
                        // Unexpected finish_reason — fatal, don't retry
                        return Err(anyhow::anyhow!(
                            "{}/{} unexpected finish_reason: {other:?}",
                            entry.base_url,
                            entry.model
                        ));
                    }
                    None => unreachable!(),
                }
            }

            if !round_success {
                return Err(anyhow::anyhow!(
                    "All {} providers exhausted at round {round}",
                    providers.len()
                ));
            }

            // round_success means we had tool_calls and they've been executed —
            // messages already updated, continue to next round
        }

        tracing::warn!("Max tool call rounds (7) exceeded — attempting final summary");
        let _ = tx.send(Ok("\n\n---\n⚡ Max rounds reached — synthesizing final response...\n\n".into())).await;
        Self::finalize_with_fallback(providers, http, messages, tx).await?;
        Ok(())
    }

    /// Inject a guidance prompt and call stream_one_shot to get a final response.
    async fn finalize_with_fallback(
        providers: &[ProviderEntry],
        http: &reqwest::Client,
        messages: &mut Vec<serde_json::Value>,
        tx: &mpsc::Sender<anyhow::Result<String>>,
    ) -> anyhow::Result<()> {
        messages.push(json!({
            "role": "user",
            "content": "You haven't produced any response yet. Please now synthesize a comprehensive final answer based on everything gathered so far. Always use the same language as the user's question."
        }));
        Self::stream_one_shot(providers, http, messages, tx).await
    }

    /// One-shot streaming request without tools, used as a fallback when
    /// the agent loop produces no text (empty stop or max rounds exceeded).
    /// Iterates providers for the fallback call.
    async fn stream_one_shot(
        providers: &[ProviderEntry],
        http: &reqwest::Client,
        messages: &[serde_json::Value],
        tx: &mpsc::Sender<anyhow::Result<String>>,
    ) -> anyhow::Result<()> {
        let mut last_model: Option<String> = None;

        for (pi, entry) in providers.iter().enumerate() {
            tracing::info!(
                "Fallback [{}/{}]: {} @ {}",
                pi + 1,
                providers.len(),
                entry.model,
                entry.base_url
            );

            // Announce model switch BEFORE the HTTP request.
            if last_model.as_deref() != Some(&entry.model) {
                let label = if last_model.is_some() {
                    format!("\n\n🔄 Switched to **Model: {}**\n\n", entry.model)
                } else {
                    format!("**Model: {}**\n\n", entry.model)
                };
                last_model = Some(entry.model.clone());
                let _ = tx.send(Ok(label)).await;
            }

            let url = format!("{}/chat/completions", entry.base_url.trim_end_matches('/'));
            let body = json!({
                "model": entry.model,
                "messages": messages,
                "temperature": 0.3,
                "max_tokens": 4096,
                "stream": true,
            });

            let response = match tokio::time::timeout(
                std::time::Duration::from_secs(30),
                http
                    .post(&url)
                    .header("Authorization", format!("Bearer {}", entry.api_key))
                    .json(&body)
                    .send(),
            )
            .await
            {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    tracing::warn!("Fallback {}/{} HTTP send failed: {e}", entry.base_url, entry.model);
                    continue;
                }
                Err(_elapsed) => {
                    tracing::warn!("Fallback {}/{} first-byte timeout (30s)", entry.base_url, entry.model);
                    continue;
                }
            };

            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_default();
                tracing::warn!("Fallback {}/{} HTTP {status}: {text}", entry.base_url, entry.model);
                continue;
            }

            let mut stream = response.bytes_stream();
            let mut line_buf = String::new();
            let mut content_buf = String::new();
            let mut thinking_open = false;

            loop {
                let chunk_result = match tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    stream.next(),
                )
                .await
                {
                    Ok(Some(Ok(c))) => Ok(c),
                    Ok(Some(Err(e))) => Err(e),
                    Ok(None) => break, // stream ended cleanly
                    Err(_elapsed) => {
                        tracing::warn!("Fallback {}/{} chunk timeout (30s)", entry.base_url, entry.model);
                        break;
                    }
                };

                let chunk = match chunk_result {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!("Fallback {}/{} stream error: {e}", entry.base_url, entry.model);
                        break;
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
                    let choice = chunk_v
                        .get("choices")
                        .and_then(|c| c.as_array())
                        .and_then(|c| c.first());
                    let delta = match choice.and_then(|c| c.get("delta")) {
                        Some(d) => d,
                        None => continue,
                    };
                    if let Some(rc) = delta.get("reasoning_content").and_then(|c| c.as_str()) {
                        if !rc.is_empty() {
                            if !thinking_open {
                                thinking_open = true;
                                let _ = tx.send(Ok("\n💭 **Thinking:**\n> ".into())).await;
                            }
                            let formatted = rc.replace('\n', "\n> ");
                            content_buf.push_str(&formatted);
                            if tx.send(Ok(formatted)).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    if let Some(content) = delta.get("content").and_then(|c| c.as_str()) {
                        if !content.is_empty() {
                            if thinking_open {
                                thinking_open = false;
                                let _ = tx.send(Ok("\n\n---\n\n".into())).await;
                            }
                            content_buf.push_str(content);
                            if tx.send(Ok(content.to_string())).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                }
            }
            if content_buf.trim().is_empty() {
                tracing::warn!("Fallback {}/{} produced empty content", entry.base_url, entry.model);
                continue;
            }
            let preview: String = content_buf.chars().take(200).collect();
            let dots = if content_buf.len() > 200 { "…" } else { "" };
            tracing::info!(
                "Fallback via {}/{}: response ({} chars): {}{}",
                entry.base_url, entry.model, content_buf.len(), preview, dots
            );
            return Ok(());
        }

        Err(anyhow::anyhow!(
            "All {} providers exhausted in fallback",
            providers.len()
        ))
    }

    async fn run_agent_loop(
        providers: &[ProviderEntry],
        preamble: &str,
        user_prompt: &str,
        chat_history: &mut Vec<rig::completion::Message>,
    ) -> anyhow::Result<(String, String)> {
        // returns (model_name, response_text)
        use rig::completion::{Completion, Message, ModelChoice};

        let mut current_prompt = user_prompt.to_string();

        for round in 0..7 {
            let mut round_success = false;

            for (pi, entry) in providers.iter().enumerate() {
                tracing::info!(
                    "[{round}] Provider [{}/{}]: {} @ {} ({} prior messages)",
                    pi + 1,
                    providers.len(),
                    entry.model,
                    entry.base_url,
                    chat_history.len()
                );

                let agent = Self::build_agent(entry, preamble);

                let builder = match agent
                    .completion(&current_prompt, chat_history.clone())
                    .await
                {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!("[{round}] {}/{} completion error: {e}", entry.base_url, entry.model);
                        continue; // try next provider
                    }
                };

                let response = match builder.send().await {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("[{round}] {}/{} send error: {e}", entry.base_url, entry.model);
                        continue; // try next provider
                    }
                };

                match response.choice {
                    ModelChoice::Message(msg) => {
                        if msg.trim().is_empty() {
                            tracing::warn!("[{round}] {}/{} returned empty message, trying next provider", entry.base_url, entry.model);
                            continue;
                        }
                        tracing::info!("[{round}] Success via {}/{}", entry.base_url, entry.model);
                        return Ok((entry.model.clone(), msg));
                    }
                    ModelChoice::ToolCall(toolname, args) => {
                        tracing::info!("[{round}] {}/{} Tool call: {toolname}({args})", entry.base_url, entry.model);

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

                        current_prompt = format!(
                            "The tool result is above. Continue working on the original request: \"{user_prompt}\""
                        );

                        round_success = true;
                        break; // out of provider loop, continue to next round
                    }
                }
            }

            if !round_success {
                tracing::warn!("[{round}] no provider returned a valid response — attempting final summary");
                break;
            }

            // round_success means we had a tool call and it was executed —
            // chat_history already updated, continue to next round
        }

        tracing::warn!("Max tool call rounds (7) exceeded — attempting final summary");

        for (pi, entry) in providers.iter().enumerate() {
            tracing::info!(
                "Fallback [{}/{}]: {} @ {}",
                pi + 1,
                providers.len(),
                entry.model,
                entry.base_url
            );

            let no_tool_agent = Self::make_client(entry)
                .agent(&entry.model)
                .temperature(0.3)
                .max_tokens(4096)
                .build();

            let builder = match no_tool_agent
                .completion(
                    "Based on the conversation above, produce a final response.",
                    chat_history.clone(),
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!("Fallback {}/{} completion error: {e}", entry.base_url, entry.model);
                    continue;
                }
            };

            let response = match builder.send().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("Fallback {}/{} send error: {e}", entry.base_url, entry.model);
                    continue;
                }
            };

            match response.choice {
                ModelChoice::Message(msg) => {
                    if msg.trim().is_empty() {
                        tracing::warn!("Fallback {}/{} returned empty message", entry.base_url, entry.model);
                        continue;
                    }
                    tracing::info!("Fallback via {}/{}", entry.base_url, entry.model);
                    return Ok((entry.model.clone(), msg));
                }
                _ => {
                    tracing::warn!("Fallback {}/{} returned unexpected choice", entry.base_url, entry.model);
                    continue;
                }
            }
        }

        Err(anyhow::anyhow!(
            "Max tool call rounds (7) exceeded and all final summaries failed"
        ))
    }
}

/// Map a tool name to its Hermes-style emoji icon.
fn tool_icon(name: &str) -> &str {
    match name {
        "read_self" => "📖",
        "search_self" => "🔍",
        "write_self" => "✏️",
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
pub async fn run_sync(prompt: String) -> anyhow::Result<String> {
    let preamble = build_full_preamble();
    let chain = ModelChain::from_env()?;
    chain.prompt(&prompt, &preamble).await
}

/// Build the full preamble from SOUL.md plus memory context and skills injection.
/// SOUL.md is read fresh from disk on every call so edits take effect without restart.
pub fn build_full_preamble() -> String {
    let soul = std::fs::read_to_string(
        std::env::var("SOUL_PATH").unwrap_or_else(|_| "./SOUL.md".to_string()),
    )
    .unwrap_or_else(|_| "You are a helpful personal assistant.".to_string());

    let today = chrono::Local::now().format("%Y-%m-%d");
    let pending_tasks = crate::memory::read_self("memory/tasks/pending.md").unwrap_or_default();
    let daily_note = crate::memory::read_self(&format!("memory/daily/{today}.md")).unwrap_or_default();
    let skills = crate::skills::discover();
    let skills_catalog = crate::skills::build_catalog(&skills);

    format!(
        "\
{soul}

---

## Current Context (auto-injected)
- Today: {today}
- Workspace: repository root (all paths are relative to the repo root)

### Today's Note ({today})
{daily_note}

### Pending Tasks
{pending_tasks}
{skills_catalog}

## Tool Usage Guidelines
- Use `read_self` to read any file (paths relative to repo root, e.g. memory/tasks/pending.md).
- Use `search_self` to search all Markdown files for keywords.
- Use `write_self` to create or update a file atomically.
- Use `update_task` to mark tasks as done/undo in memory/tasks/pending.md.
- Use `run_bash` to execute shell commands for system operations.
- Be concise. Use tools proactively. When you lack a tool, check the Skills catalog above.
"
    )
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    // ── Helper: build a ModelChain pointing at a mock server ──

    fn mock_chain(server: &httpmock::MockServer, model: &str) -> ModelChain {
        ModelChain {
            providers: vec![ProviderEntry {
                model: model.to_string(),
                base_url: server.base_url(),
                api_key: "test-key".to_string(),
            }],
            http: reqwest::Client::builder().build().unwrap(),
        }
    }

    /// OpenAI-format chat completion response with a text message.
    fn openai_text_body(content: &str) -> String {
        format!(
            r#"{{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"m","choices":[{{"index":0,"message":{{"role":"assistant","content":"{content}"}},"finish_reason":"stop"}}],"usage":{{"prompt_tokens":1,"total_tokens":2}}}}"#
        )
    }

    /// OpenAI-format chat completion response with an empty text message.
    fn openai_empty_text_body() -> String {
        r#"{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":""},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"total_tokens":2}}"#.to_string()
    }

    /// OpenAI-format chat completion response with a tool call.
    fn openai_tool_call_body(tool_name: &str, arguments: &str) -> String {
        format!(
            r#"{{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"m","choices":[{{"index":0,"message":{{"role":"assistant","content":null,"tool_calls":[{{"id":"call_1","type":"function","function":{{"name":"{tool_name}","arguments":"{arguments}"}}}}]}},"finish_reason":"tool_calls"}}],"usage":{{"prompt_tokens":1,"total_tokens":2}}}}"#
        )
    }

    // ─────────────────────────────────────────────
    // 1. Pure functions
    // ─────────────────────────────────────────────

    #[test]
    fn tool_icon_known() {
        assert_eq!(tool_icon("read_self"), "📖");
        assert_eq!(tool_icon("search_self"), "🔍");
        assert_eq!(tool_icon("write_self"), "✏️");
        assert_eq!(tool_icon("update_task"), "✅");
        assert_eq!(tool_icon("run_bash"), "🔧");
    }

    #[test]
    fn tool_icon_unknown_is_default() {
        assert_eq!(tool_icon("nonexistent"), "💻");
        assert_eq!(tool_icon(""), "💻");
    }

    #[test]
    fn format_result_preview_empty() {
        assert_eq!(format_result_preview(""), "");
        assert_eq!(format_result_preview("   "), "");
        assert_eq!(format_result_preview("\n  \n"), "");
    }

    #[test]
    fn format_result_preview_fewer_than_5_lines() {
        let result = "line1\nline2\nline3";
        let preview = format_result_preview(result);
        assert!(preview.contains("line1"));
        assert!(preview.contains("line3"));
        assert!(!preview.contains("…"), "no ellipsis for ≤5 lines");
    }

    #[test]
    fn format_result_preview_exactly_5_lines() {
        let result = "a\nb\nc\nd\ne";
        let preview = format_result_preview(result);
        assert!(preview.contains("a"));
        assert!(preview.contains("e"));
        assert!(!preview.contains("…"));
    }

    #[test]
    fn format_result_preview_more_than_5_lines() {
        let result = "1\n2\n3\n4\n5\n6\n7";
        let preview = format_result_preview(result);
        assert!(preview.contains("…"), "ellipsis for >5 lines");
        assert!(preview.contains("1"));
        assert!(preview.contains("5"));
        assert!(!preview.contains("6"), "line 6 should be truncated");
    }

    // ─────────────────────────────────────────────
    // 2. ModelChain::from_env
    // ─────────────────────────────────────────────

    #[test]
    fn from_env_valid_config() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("chain.json");
        std::fs::write(
            &config_path,
            r#"[
                {"model": "test-model", "base_url": "http://example.com/v1", "api_key": "sk-test"}
            ]"#,
        )
        .unwrap();
        std::env::set_var("CHAIN_CONFIG", config_path.to_str().unwrap());

        let chain = ModelChain::from_env().unwrap();
        assert_eq!(chain.providers.len(), 1);
        assert_eq!(chain.providers[0].model, "test-model");
        assert_eq!(chain.providers[0].base_url, "http://example.com/v1");

        std::env::remove_var("CHAIN_CONFIG");
    }

    #[test]
    fn from_env_file_not_found() {
        let _lock = ENV_LOCK.lock().unwrap();
        std::env::set_var("CHAIN_CONFIG", "/nonexistent/path/to/chain.json");
        match ModelChain::from_env() {
            Err(e) => assert!(
                e.to_string().contains("Failed to read chain config"),
                "got: {e}"
            ),
            Ok(_) => panic!("expected error"),
        }
        std::env::remove_var("CHAIN_CONFIG");
    }

    #[test]
    fn from_env_invalid_json() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("bad.json");
        std::fs::write(&config_path, "definitely not json {{{").unwrap();
        std::env::set_var("CHAIN_CONFIG", config_path.to_str().unwrap());

        match ModelChain::from_env() {
            Err(e) => assert!(
                e.to_string().contains("Invalid chain.json"),
                "got: {e}"
            ),
            Ok(_) => panic!("expected error"),
        }
        std::env::remove_var("CHAIN_CONFIG");
    }

    #[test]
    fn from_env_empty_array() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("empty.json");
        std::fs::write(&config_path, "[]").unwrap();
        std::env::set_var("CHAIN_CONFIG", config_path.to_str().unwrap());

        match ModelChain::from_env() {
            Err(e) => assert!(
                e.to_string().contains("at least one provider"),
                "got: {e}"
            ),
            Ok(_) => panic!("expected error"),
        }
        std::env::remove_var("CHAIN_CONFIG");
    }

    // ─────────────────────────────────────────────
    // 3. build_full_preamble
    // ─────────────────────────────────────────────

    #[test]
    fn build_full_preamble_with_custom_soul_path() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let soul_path = dir.path().join("test-soul.md");
        std::fs::write(&soul_path, "Custom AI soul content.").unwrap();
        std::env::set_var("SOUL_PATH", soul_path.to_str().unwrap());

        let preamble = build_full_preamble();
        assert!(preamble.contains("Custom AI soul content."));
        assert!(preamble.contains("Current Context"));
        assert!(preamble.contains("Tool Usage Guidelines"));

        std::env::remove_var("SOUL_PATH");
    }

    #[test]
    fn build_full_preamble_missing_soul_falls_back() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let soul_path = dir.path().join("does-not-exist.md");
        std::env::set_var("SOUL_PATH", soul_path.to_str().unwrap());

        let preamble = build_full_preamble();
        assert!(preamble.contains("You are a helpful personal assistant."));
        assert!(preamble.contains("Current Context"));

        std::env::remove_var("SOUL_PATH");
    }

    // ─────────────────────────────────────────────
    // 4. prompt_light (network via httpmock)
    // ─────────────────────────────────────────────

    #[tokio::test]
    async fn prompt_light_success_first_provider() {
        let server = httpmock::MockServer::start();
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("Hello!"));
        });

        let chain = mock_chain(&server, "test-model");
        let result = chain.prompt_light("hi", "Be helpful.").await.unwrap();
        assert_eq!(result, "Hello!");
    }

    #[tokio::test]
    async fn prompt_light_first_provider_completion_error_second_succeeds() {
        // Provider 1: live server returning invalid JSON → deserialize error
        let bad = httpmock::MockServer::start();
        bad.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body("not json {{");
        });

        // Provider 2: live server with valid response
        let live = httpmock::MockServer::start();
        live.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("fallback win"));
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "bad".into(),
                    base_url: bad.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "live".into(),
                    base_url: live.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let result = chain.prompt_light("hi", "Be helpful.").await.unwrap();
        assert_eq!(result, "fallback win");
    }

    #[tokio::test]
    async fn prompt_light_empty_message_falls_through() {
        let server_empty = httpmock::MockServer::start();
        server_empty.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_empty_text_body());
        });

        let server_ok = httpmock::MockServer::start();
        server_ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("non-empty"));
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "empty".into(),
                    base_url: server_empty.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: server_ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let result = chain.prompt_light("hi", "preamble").await.unwrap();
        assert_eq!(result, "non-empty");
    }

    #[tokio::test]
    async fn prompt_light_unexpected_choice_falls_through() {
        // Tool-call response reaches prompt_light which has no tools ->
        // ModelChoice::ToolCall falls into `_` arm -> continue to next provider.
        let server_bad = httpmock::MockServer::start();
        server_bad.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_tool_call_body("read_self", r#"{\"path\":\"x.md\"}"#));
        });

        let server_ok = httpmock::MockServer::start();
        server_ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("valid"));
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "bad-choice".into(),
                    base_url: server_bad.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: server_ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let result = chain.prompt_light("hi", "preamble").await.unwrap();
        assert_eq!(result, "valid");
    }

    #[tokio::test]
    async fn prompt_light_all_providers_exhausted() {
        // Live servers returning invalid JSON → deserialize error in rig → Err
        let srv1 = httpmock::MockServer::start();
        srv1.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body("not valid json {{");
        });

        let srv2 = httpmock::MockServer::start();
        srv2.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body("also not valid json");
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "d1".into(),
                    base_url: srv1.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "d2".into(),
                    base_url: srv2.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let err = chain.prompt_light("hi", "preamble").await.unwrap_err();
        assert!(
            err.to_string().contains("All 2 providers exhausted"),
            "got: {err}"
        );
    }

    // ─────────────────────────────────────────────
    // 5. prompt / run_agent_loop (network via httpmock)
    // ─────────────────────────────────────────────

    /// Simple text response — should be wrapped with model name.
    #[tokio::test]
    async fn prompt_returns_model_tagged_response() {
        let server = httpmock::MockServer::start();
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("Hello!"));
        });

        let chain = mock_chain(&server, "my-model");
        let result = chain.prompt("hi", "preamble").await.unwrap();
        assert!(result.contains("**Model: my-model**"));
        assert!(result.contains("Hello!"));
    }

    /// When the first provider returns an empty message, the loop tries the next.
    #[tokio::test]
    async fn prompt_empty_message_tries_next() {
        let srv_empty = httpmock::MockServer::start();
        srv_empty.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_empty_text_body());
        });

        let srv_ok = httpmock::MockServer::start();
        srv_ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("non-empty"));
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "empty".into(),
                    base_url: srv_empty.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: srv_ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let result = chain.prompt("hi", "preamble").await.unwrap();
        assert!(result.contains("non-empty"));
    }

    /// Tool call in round 1, text response in round 2.
    #[tokio::test]
    async fn prompt_tool_call_then_text() {
        // Round 1: tool call (read_self on Cargo.toml)
        let srv_tool = httpmock::MockServer::start();
        srv_tool.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_tool_call_body("read_self", r#"{\"path\":\"Cargo.toml\"}"#));
        });

        // Round 2: text response
        let srv_text = httpmock::MockServer::start();
        srv_text.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("Done!"));
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "tool-model".into(),
                    base_url: srv_tool.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "text-model".into(),
                    base_url: srv_text.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let result = chain.prompt("read cargo", "preamble").await.unwrap();
        assert!(result.contains("Done!"));
    }

    /// All providers exhausted in a round → fallback loop runs (one-shot w/o tools).
    #[tokio::test]
    async fn prompt_all_providers_exhausted_then_fallback() {
        // Two servers returning invalid JSON → completion() errors out
        // After all providers fail in round 0, fallback loop also fails.
        let srv1 = httpmock::MockServer::start();
        srv1.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body("not json {{");
        });

        let srv2 = httpmock::MockServer::start();
        srv2.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body("not json");
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "d1".into(),
                    base_url: srv1.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "d2".into(),
                    base_url: srv2.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let err = chain.prompt("hi", "preamble").await.unwrap_err();
        assert!(
            err.to_string().contains("all final summaries failed"),
            "got: {err}"
        );
    }

    /// Max tool-call rounds exhausted → fallback produces text.
    #[tokio::test]
    async fn prompt_max_rounds_fallback_succeeds() {
        // Always return tool_calls — this forces all 7 rounds to be tool calls
        let srv_tool = httpmock::MockServer::start();
        let tool_body = openai_tool_call_body("read_self", r#"{\"path\":\"Cargo.toml\"}"#);
        // One mock handles all calls (reused across rounds)
        srv_tool.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(&tool_body);
        });

        // Fallback: a separate mock server that returns text (no tools)
        let srv_fallback = httpmock::MockServer::start();
        srv_fallback.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("fallback summary"));
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "tool-model".into(),
                    base_url: srv_tool.base_url(),
                    api_key: "k".into(),
                },
                // Fallback provider (used during fallback loop)
                ProviderEntry {
                    model: "fallback-model".into(),
                    base_url: srv_fallback.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let result = chain.prompt("do something", "preamble").await.unwrap();
        assert!(
            result.contains("fallback summary"),
            "got: {result}"
        );
    }

    // ─────────────────────────────────────────────
    // 6. Streaming: prompt_streaming / stream_agent_loop
    // ─────────────────────────────────────────────

    /// Build an SSE body from content chunks. Each chunk is (content, optional finish_reason).
    fn sse_body(chunks: &[(&str, Option<&str>)]) -> String {
        let mut body = String::new();
        for (content, finish) in chunks {
            let fr = match finish {
                Some(f) => format!(r#","finish_reason":"{f}""#),
                None => String::new(),
            };
            body.push_str(&format!(
                "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{content}\"}},\"index\":0{fr}}}]}}\n\n"
            ));
        }
        body.push_str("data: [DONE]\n\n");
        body
    }

    /// SSE body for tool-call response.
    fn sse_tool_call_body(tool_name: &str, arguments: &str) -> String {
        format!(
            "data: {{\"choices\":[{{\"delta\":{{\"tool_calls\":[{{\"index\":0,\"id\":\"call_1\",\"function\":{{\"name\":\"{tool_name}\",\"arguments\":\"{arguments}\"}}}}]}},\"index\":0,\"finish_reason\":\"tool_calls\"}}]}}\n\ndata: [DONE]\n\n"
        )
    }

    /// SSE body for reasoning_content (thinking models).
    fn sse_reasoning_body() -> String {
        concat!(
            "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"Let me think about this.\"},\"index\":0}]}\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"The answer is 42.\"},\"index\":0,\"finish_reason\":\"stop\"}]}\n\n",
            "data: [DONE]\n\n"
        )
        .to_string()
    }

    /// SSE body with finish_reason only (no content delta).
    fn sse_finish_reason_only(reason: &str) -> String {
        format!(
            "data: {{\"choices\":[{{\"delta\":{{}},\"index\":0,\"finish_reason\":\"{reason}\"}}]}}\n\ndata: [DONE]\n\n"
        )
    }

    /// SSE body without finish_reason (stream ends with DONE only).
    fn sse_no_finish_reason() -> String {
        "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"index\":0}]}\n\ndata: [DONE]\n\n".to_string()
    }

    /// Helper: collect all Ok items from a stream, ignoring errors.
    async fn collect_stream(
        stream: impl futures::Stream<Item = anyhow::Result<String>>,
    ) -> Vec<String> {
        use futures::StreamExt;
        stream
            .filter_map(|r| async move { r.ok() })
            .collect()
            .await
    }

    #[tokio::test]
    async fn streaming_text_response() {
        let server = httpmock::MockServer::start();
        let body = sse_body(&[
            ("Hello", None),
            (" world", None),
            ("!", Some("stop")),
        ]);
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = mock_chain(&server, "stream-model");
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;

        let all: String = items.concat();
        assert!(all.contains("**Model: stream-model**"));
        assert!(all.contains("Hello"));
        assert!(all.contains(" world"));
        assert!(all.contains("!"));
    }

    #[tokio::test]
    async fn streaming_model_switch_announcement() {
        // Two providers, different models. First fails, second succeeds.
        let bad = httpmock::MockServer::start();
        bad.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(500);
        });

        let ok = httpmock::MockServer::start();
        let body = sse_body(&[("ok", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "first".into(),
                    base_url: bad.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "second".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;
        let all = items.concat();

        assert!(all.contains("**Model: second**"));
        assert!(all.contains("ok"));
    }

    #[tokio::test]
    async fn streaming_non_200_status_tries_next() {
        let bad = httpmock::MockServer::start();
        bad.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(500);
        });

        let ok = httpmock::MockServer::start();
        let body = sse_body(&[("recovered", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "bad-status".into(),
                    base_url: bad.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;
        assert!(items.concat().contains("recovered"));
    }

    #[tokio::test]
    async fn streaming_empty_content_on_stop_tries_next() {
        let empty = httpmock::MockServer::start();
        let body = sse_finish_reason_only("stop");
        empty.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let ok = httpmock::MockServer::start();
        let body2 = sse_body(&[("non-empty", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body2);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "empty-stop".into(),
                    base_url: empty.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;
        assert!(items.concat().contains("non-empty"));
    }

    #[tokio::test]
    async fn streaming_finish_reason_length() {
        let server = httpmock::MockServer::start();
        let body = sse_finish_reason_only("length");
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = mock_chain(&server, "m");
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;
        assert!(items.concat().contains("truncated"));
    }

    #[tokio::test]
    async fn streaming_finish_reason_content_filter() {
        let server = httpmock::MockServer::start();
        let body = sse_finish_reason_only("content_filter");
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = mock_chain(&server, "m");
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;
        assert!(items.concat().contains("filtered"));
    }

    #[tokio::test]
    async fn streaming_reasoning_content() {
        let server = httpmock::MockServer::start();
        let body = sse_reasoning_body();
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = mock_chain(&server, "thinker");
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;
        let all = items.concat();
        assert!(all.contains("**Thinking:**"));
        assert!(all.contains("Let me think about this"));
        assert!(all.contains("The answer is 42"));
    }

    #[tokio::test]
    async fn streaming_all_providers_exhausted_returns_err() {
        let srv1 = httpmock::MockServer::start();
        srv1.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(500);
        });

        let srv2 = httpmock::MockServer::start();
        srv2.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(500);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "fail1".into(),
                    base_url: srv1.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "fail2".into(),
                    base_url: srv2.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let stream = chain.prompt_streaming("hi", "preamble");
        let items: Vec<_> = tokio_stream::StreamExt::collect(stream).await;
        // Last item should be an error
        assert!(items.iter().any(|r| r.is_err()));
    }

    #[tokio::test]
    async fn streaming_no_finish_reason_tries_next() {
        // Stream ends without finish_reason → content thrown away → try next
        let bad = httpmock::MockServer::start();
        let body = sse_no_finish_reason();
        bad.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let ok = httpmock::MockServer::start();
        let body2 = sse_body(&[("valid", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body2);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "incomplete".into(),
                    base_url: bad.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("hi", "preamble")).await;
        assert!(items.concat().contains("valid"));
    }

    #[tokio::test]
    async fn streaming_tool_call_executes_and_continues() {
        // Round 1: tool call (read_self on Cargo.toml)
        let srv_tool = httpmock::MockServer::start();
        let tool_body = sse_tool_call_body("read_self", r#"{\"path\":\"Cargo.toml\"}"#);
        srv_tool.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&tool_body);
        });

        // Round 2: text response (same provider, reused)
        let srv_text = httpmock::MockServer::start();
        let text_body = sse_body(&[("done", Some("stop"))]);
        srv_text.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&text_body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "tool".into(),
                    base_url: srv_tool.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "text".into(),
                    base_url: srv_text.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("read cargo", "preamble")).await;
        let all = items.concat();
        // Should contain tool execution indicator and final text
        assert!(all.contains("📖"), "tool icon present");
        assert!(all.contains("done"), "final text present");
    }

    #[tokio::test]
    async fn streaming_tool_execution_error() {
        // Tool call with invalid args → execute_tool returns error
        let server = httpmock::MockServer::start();
        // Bad JSON args for read_self
        let tool_body =
            sse_tool_call_body("read_self", r#"not-valid-json"#);
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&tool_body);
        });

        // After tool error, the round continues; second provider returns text
        let srv_text = httpmock::MockServer::start();
        let text_body = sse_body(&[("recovered", Some("stop"))]);
        srv_text.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&text_body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "bad-tool".into(),
                    base_url: server.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "text".into(),
                    base_url: srv_text.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("test", "preamble")).await;
        let all = items.concat();
        // The tool error is caught (unwrap_or_else) and formatted
        assert!(all.contains("Error"), "error was formatted: {all}");
        assert!(all.contains("recovered"), "second provider worked");
    }

    #[tokio::test]
    async fn streaming_client_disconnect_stops_task() {
        let server = httpmock::MockServer::start();
        let body = sse_body(&[
            ("Hello", None),
            (" world", None),
            ("!", Some("stop")),
        ]);
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = mock_chain(&server, "m");
        let mut stream = chain.prompt_streaming("hi", "preamble");

        // Read one item, then drop the stream → tx.send fails in task
        let first: Result<_, _> = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures::StreamExt::next(&mut stream),
        )
        .await;
        assert!(first.is_ok(), "should receive at least one item");
        drop(stream);
        // Task should exit gracefully (no panic) on tx error
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn streaming_max_rounds_fallback() {
        // Tool call that reads Cargo.toml (always succeeds), 7 rounds
        let srv_tool = httpmock::MockServer::start();
        let tool_body = sse_tool_call_body("read_self", r#"{\"path\":\"Cargo.toml\"}"#);
        srv_tool.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&tool_body);
        });

        // Fallback: one-shot text response
        let srv_fb = httpmock::MockServer::start();
        let fb_body = sse_body(&[("final summary", Some("stop"))]);
        srv_fb.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&fb_body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "tool-loop".into(),
                    base_url: srv_tool.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "fallback".into(),
                    base_url: srv_fb.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("test", "preamble")).await;
        let all = items.concat();
        assert!(
            all.contains("Max rounds reached"),
            "should show max rounds warning: {all}"
        );
        assert!(all.contains("final summary"), "fallback text: {all}");
    }

    // ─────────────────────────────────────────────
    // 7. stream_one_shot / finalize_with_fallback
    // ─────────────────────────────────────────────

    #[tokio::test]
    async fn stream_one_shot_success() {
        let server = httpmock::MockServer::start();
        let body = sse_body(&[("one-shot result", Some("stop"))]);
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = mock_chain(&server, "oneshot");
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;
        assert!(items.concat().contains("one-shot result"));
    }

    #[tokio::test]
    async fn stream_one_shot_fallback_between_providers() {
        let bad = httpmock::MockServer::start();
        bad.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(500);
        });

        let ok = httpmock::MockServer::start();
        let body = sse_body(&[("recovered", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "bad".into(),
                    base_url: bad.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "good".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;
        assert!(items.concat().contains("recovered"));
    }

    #[tokio::test]
    async fn stream_one_shot_empty_content_tries_next() {
        let empty = httpmock::MockServer::start();
        let body = sse_finish_reason_only("stop");
        empty.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let ok = httpmock::MockServer::start();
        let body2 = sse_body(&[("non-empty", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body2);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "empty".into(),
                    base_url: empty.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("hi", "preamble")).await;
        assert!(items.concat().contains("non-empty"));
    }

    // ─────────────────────────────────────────────
    // 8. run_sync
    // ─────────────────────────────────────────────

    #[tokio::test]
    async fn run_sync_happy_path() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("chain.json");
        let server = httpmock::MockServer::start();
        let config = format!(
            r#"[{{"model":"sync-model","base_url":"{}","api_key":"k"}}]"#,
            server.base_url()
        );
        std::fs::write(&config_path, &config).unwrap();
        std::env::set_var("CHAIN_CONFIG", config_path.to_str().unwrap());

        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("sync response"));
        });

        let result = run_sync("hello".into()).await.unwrap();
        assert!(result.contains("sync response"));
        std::env::remove_var("CHAIN_CONFIG");
    }

    // ─────────────────────────────────────────────
    // 9. Edge cases: timeout arms, unexpected finish_reason, etc.
    // ─────────────────────────────────────────────

    /// streaming with an unexpected finish_reason returns Err
    /// (not through the channel, but via the spawned task's error path).
    #[tokio::test]
    async fn streaming_unexpected_finish_reason_returns_err() {
        let server = httpmock::MockServer::start();
        let body = sse_finish_reason_only("unknown_reason");
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = mock_chain(&server, "m");
        let stream = chain.prompt_streaming("hi", "preamble");
        let items: Vec<_> = tokio_stream::StreamExt::collect(stream).await;
        // The Err is emitted on the channel after unexpected finish_reason
        assert!(items.iter().any(|r| r.is_err()), "should contain error: {items:?}");
    }

    /// streaming HTTP send error — provider gets `Ok(Err(e))` arm (e.g.
    /// connection refused on a closed port).
    #[tokio::test]
    async fn streaming_http_send_error_tries_next() {
        // Provider 1: port that will refuse connection → send() returns Err
        // Port 1 is privileged (<1024), connect() typically fails with EACCES
        let bad_url = "http://127.0.0.1:1".to_string();

        // Provider 2: working server
        let ok = httpmock::MockServer::start();
        let body = sse_body(&[("recovered", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "bad".into(),
                    base_url: bad_url,
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(2))
                .build()
                .unwrap(),
        };
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;
        // Should get content from the second provider
        assert!(items.concat().contains("recovered"));
    }

    /// stream_one_shot with all providers failing — channel receives Err.
    #[tokio::test]
    async fn stream_one_shot_all_providers_exhausted() {
        let srv1 = httpmock::MockServer::start();
        srv1.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(500);
        });

        let srv2 = httpmock::MockServer::start();
        srv2.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(500);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "f1".into(),
                    base_url: srv1.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "f2".into(),
                    base_url: srv2.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let stream = chain.prompt_streaming("hi", "preamble");
        let items: Vec<_> = tokio_stream::StreamExt::collect(stream).await;
        assert!(items.iter().any(|r| r.is_err()), "should contain error");
    }

    /// stream_one_shot with non-200 status → try next provider.
    #[tokio::test]
    async fn stream_one_shot_non_200_tries_next() {
        let bad = httpmock::MockServer::start();
        bad.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(503);
        });

        let ok = httpmock::MockServer::start();
        let body = sse_body(&[("recovered", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "bad".into(),
                    base_url: bad.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let stream = chain.prompt_streaming("hi", "preamble");
        let items = collect_stream(stream).await;
        assert!(items.concat().contains("recovered"));
    }

    /// SSE stream ends with content in buffer but no finish_reason →
    /// partial fallback assigns "stop". But since we don't have tool buffers,
    /// it still gets assigned stop and proceeds.
    #[tokio::test]
    async fn streaming_partial_fallback_with_line_buffer() {
        // SSE with content delta but NO finish_reason delta at all
        let server = httpmock::MockServer::start();
        let body = "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"index\":0}]}\n\ndata: [DONE]\n\n";
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        });

        // Second provider with proper finish
        let ok = httpmock::MockServer::start();
        let body2 = sse_body(&[("recovered", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body2);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "partial".into(),
                    base_url: server.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("hi", "preamble")).await;
        assert!(items.concat().contains("recovered"));
    }

    /// SSE stream with partial content in line_buf + content_buf populated,
    /// but no finish_reason. The code assigns "stop" as finish_reason.
    #[tokio::test]
    async fn streaming_partial_content_used_when_interrupted() {
        let server = httpmock::MockServer::start();
        // Send content without proper newline-separated finish, then DONE
        let body = "data: {\"choices\":[{\"delta\":{\"content\":\"partial-content\"},\"index\":0}]}\n\n";
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        });

        let ok = httpmock::MockServer::start();
        let body2 = sse_body(&[("final", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body2);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "interrupted".into(),
                    base_url: server.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("hi", "preamble")).await;
        assert!(items.concat().contains("final"));
    }

    /// run_agent_loop: completion error on a provider → tries next.
    /// We test this via prompt() with a bad JSON server.
    #[tokio::test]
    async fn run_agent_loop_completion_error_tries_next() {
        let bad = httpmock::MockServer::start();
        bad.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body("not json");
        });

        let ok = httpmock::MockServer::start();
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("recovered text"));
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "bad-json".into(),
                    base_url: bad.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let result = chain.prompt("hi", "preamble").await.unwrap();
        assert!(result.contains("recovered text"));
    }

    // ─────────────────────────────────────────────
    // 10. stream_one_shot paths via fallback
    // ─────────────────────────────────────────────

    /// Trigger stream_one_shot via max-rounds fallback, with a provider
    /// returning non-200 in the one-shot phase (covers lines 627-630).
    #[tokio::test]
    async fn stream_one_shot_fallback_non_200_tries_next() {
        // Provider 0: returns tool_calls for 7 rounds of stream_agent_loop
        let srv_tool = httpmock::MockServer::start();
        let tool_body = sse_tool_call_body("read_self", r#"{\"path\":\"Cargo.toml\"}"#);
        srv_tool.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&tool_body);
        });

        // Provider 1: returns 500 — hit in stream_one_shot after provider 0
        // yields empty content (SSE has tool_calls but no content delta)
        let srv_500 = httpmock::MockServer::start();
        srv_500.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(500);
        });

        // Provider 2: valid text for stream_one_shot
        let srv_ok = httpmock::MockServer::start();
        let text_body = sse_body(&[("fallback ok", Some("stop"))]);
        srv_ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&text_body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "tool".into(),
                    base_url: srv_tool.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "bad-fallback".into(),
                    base_url: srv_500.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok-fallback".into(),
                    base_url: srv_ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("hi", "preamble")).await;
        // stream_one_shot hits provider 0 (empty content), provider 1 (500),
        // then succeeds on provider 2
        assert!(items.concat().contains("fallback ok"));
    }

    /// Trigger stream_one_shot via max-rounds fallback, with a provider
    /// that has send error in the one-shot phase (covers lines 616-618).
    #[tokio::test]
    async fn stream_one_shot_fallback_send_error_tries_next() {
        // Provider 0: returns tool_calls for 7 rounds
        let srv_tool = httpmock::MockServer::start();
        let tool_body = sse_tool_call_body("read_self", r#"{\"path\":\"Cargo.toml\"}"#);
        srv_tool.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&tool_body);
        });

        // Provider 1: connection refused → send() returns Err → L616-618
        // Port 1 is privileged, connect() fails immediately
        let bad_url = "http://127.0.0.1:1".to_string();

        // Provider 2: valid text
        let srv_ok = httpmock::MockServer::start();
        let text_body = sse_body(&[("recovered", Some("stop"))]);
        srv_ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&text_body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "tool".into(),
                    base_url: srv_tool.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "dead".into(),
                    base_url: bad_url,
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: srv_ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(2))
                .build()
                .unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("hi", "preamble")).await;
        assert!(items.concat().contains("recovered"));
    }

    /// SSE stream ends without trailing newline → line_buf has residual data,
    /// triggering L419 partial fallback (finish_reason set to "stop").
    #[tokio::test]
    async fn streaming_line_buffer_residual_triggers_partial_fallback() {
        // Body ends without \n — the last SSE line stays in line_buf
        let server = httpmock::MockServer::start();
        let body = "data: {\"choices\":[{\"delta\":{\"content\":\"x\"},\"index\":0}]}";
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(body);
        });

        let ok = httpmock::MockServer::start();
        let body2 = sse_body(&[("saved", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body2);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "broken-eof".into(),
                    base_url: server.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("hi", "preamble")).await;
        // First provider gets L419 fallback → empty content on stop → continue
        // Second provider succeeds
        assert!(items.concat().contains("saved"));
    }

    // ─────────────────────────────────────────────
    // 11. Stream chunk read error via raw TCP RST
    // ─────────────────────────────────────────────

    /// Start a raw TCP listener that sends partial SSE data then RSTs the
    /// connection. Returns (port, join_handle).
    fn start_rst_server(partial_body: &'static str) -> (u16, std::thread::JoinHandle<()>) {
        use std::io::Write;
        use std::os::fd::AsRawFd;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                // Send valid HTTP response headers + partial SSE body
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\n\r\n",
                    partial_body.len() + 500 // lie about length → client expects more data
                );
                let _ = stream.write_all(headers.as_bytes());
                let _ = stream.write_all(partial_body.as_bytes());
                let _ = stream.flush();
                // Enable SO_LINGER with timeout 0 → RST on close
                let linger = libc::linger {
                    l_onoff: 1,
                    l_linger: 0,
                };
                unsafe {
                    let _ = libc::setsockopt(
                        stream.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_LINGER,
                        &linger as *const _ as *const libc::c_void,
                        std::mem::size_of::<libc::linger>() as libc::socklen_t,
                    );
                }
                drop(stream);
            }
        });
        (port, handle)
    }

    /// Stream chunk read error (L287, L313-314): connection RSTs after
    /// sending a complete SSE line with NO finish_reason, so the SSE loop
    /// calls stream.next() again and hits the RST error.
    #[tokio::test]
    async fn streaming_chunk_read_error_no_partial_buffer() {
        let (port, _handle) = start_rst_server(
            "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"index\":0}]}\n\n",
        );
        let bad_url = format!("http://127.0.0.1:{port}");

        // Second provider: normal
        let ok = httpmock::MockServer::start();
        let body = sse_body(&[("recovered", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "rst".into(),
                    base_url: bad_url,
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("hi", "preamble")).await;
        // After RST, the next provider takes over
        assert!(items.concat().contains("recovered"));
    }

    /// Stream chunk read error with data in line_buf (L287, L309-311):
    /// connection RSTs mid-line → partial buffer triggers L309-L311.
    #[tokio::test]
    async fn streaming_chunk_read_error_with_partial_buffer() {
        // SSE line with NO trailing newline → stays in line_buf
        let (port, _handle) = start_rst_server(
            "data: {\"choices\":[{\"delta\":{\"content\":\"incomplete\"},\"index\":0}]}",
        );
        let bad_url = format!("http://127.0.0.1:{port}");

        let ok = httpmock::MockServer::start();
        let body = sse_body(&[("saved", Some("stop"))]);
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "rst-partial".into(),
                    base_url: bad_url,
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("hi", "preamble")).await;
        // L309 partial buffer triggers, then L419 partial fallback,
        // then empty content on stop → continue → second provider wins
        assert!(items.concat().contains("saved"));
    }

    /// stream_one_shot with reasoning_content → covers thinking toggle
    /// inside stream_one_shot (lines 686-697).
    #[tokio::test]
    async fn stream_one_shot_fallback_with_reasoning() {
        // Provider 0: tool_calls for 7 rounds
        let srv_tool = httpmock::MockServer::start();
        let tool_body = sse_tool_call_body("read_self", r#"{\"path\":\"Cargo.toml\"}"#);
        srv_tool.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&tool_body);
        });

        // Provider 1: reasoning_content SSE (for stream_one_shot)
        let srv_reason = httpmock::MockServer::start();
        let reason_body = sse_reasoning_body();
        srv_reason.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&reason_body);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "tool".into(),
                    base_url: srv_tool.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "thinker".into(),
                    base_url: srv_reason.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let items = collect_stream(chain.prompt_streaming("hi", "preamble")).await;
        let all = items.concat();
        // stream_one_shot handles reasoning_content → thinking block
        assert!(all.contains("Thinking"));
        assert!(all.contains("The answer is 42"));
    }

    // ─────────────────────────────────────────────
    // 12. Remaining error paths
    // ─────────────────────────────────────────────

    /// run_agent_loop: completion() itself fails (unreachable server).
    /// Covers lines 763-765.
    #[tokio::test]
    async fn run_agent_loop_completion_error_unreachable() {
        // Provider 0: unreachable → completion() fails
        let bad_url = "http://127.0.0.1:1".to_string();

        // Provider 1: working
        let ok = httpmock::MockServer::start();
        ok.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "application/json")
                .body(openai_text_body("recovered"));
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "dead".into(),
                    base_url: bad_url,
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "ok".into(),
                    base_url: ok.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(2))
                .build()
                .unwrap(),
        };
        let result = chain.prompt("hi", "preamble").await.unwrap();
        assert!(result.contains("recovered"));
    }

    /// stream_one_shot: fallback all providers exhausted → Err (L726-729).
    #[tokio::test]
    async fn stream_one_shot_all_providers_exhausted_with_reasoning() {
        // Provider 0: tool_calls for 7 rounds of stream_agent_loop
        let srv_tool = httpmock::MockServer::start();
        let tool_body = sse_tool_call_body("read_self", r#"{\"path\":\"Cargo.toml\"}"#);
        srv_tool.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(200)
                .header("content-type", "text/event-stream")
                .body(&tool_body);
        });

        // Provider 1: returns 500 — fails in stream_one_shot
        let srv_500 = httpmock::MockServer::start();
        srv_500.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/chat/completions");
            then.status(500);
        });

        let chain = ModelChain {
            providers: vec![
                ProviderEntry {
                    model: "tool".into(),
                    base_url: srv_tool.base_url(),
                    api_key: "k".into(),
                },
                ProviderEntry {
                    model: "fail-fallback".into(),
                    base_url: srv_500.base_url(),
                    api_key: "k".into(),
                },
            ],
            http: reqwest::Client::builder().build().unwrap(),
        };
        let stream = chain.prompt_streaming("hi", "preamble");
        let items: Vec<_> = tokio_stream::StreamExt::collect(stream).await;
        // stream_one_shot exhausts both providers → Err on channel
        assert!(items.iter().any(|r| r.is_err()), "expected error in stream");
    }
}
