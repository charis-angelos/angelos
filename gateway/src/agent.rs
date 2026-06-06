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
        let mut model_label_sent = false;

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

                let response = match http
                    .post(&url)
                    .header("Authorization", format!("Bearer {}", entry.api_key))
                    .json(&body)
                    .send()
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("[{round}] {}/{} HTTP send failed: {e}", entry.base_url, entry.model);
                        continue; // try next provider
                    }
                };

                if !response.status().is_success() {
                    let status = response.status();
                    let text = response.text().await.unwrap_or_default();
                    tracing::warn!("[{round}] {}/{} HTTP {status}: {text}", entry.base_url, entry.model);
                    continue; // try next provider
                }

                if !model_label_sent {
                    model_label_sent = true;
                    let _ = tx.send(Ok(format!("**Model: {}**\n\n", entry.model))).await;
                }

                let mut stream = response.bytes_stream();
                let mut line_buf = String::new();
                let mut tool_bufs: std::collections::BTreeMap<usize, ToolCallAccum> =
                    std::collections::BTreeMap::new();
                let mut finish_reason: Option<String> = None;
                let mut content_sent = false;
                let mut content_buf = String::new();

                // ── Parse SSE stream ──
                'sse: while let Some(chunk_result) = stream.next().await {
                    let chunk = match chunk_result {
                        Ok(c) => c,
                        Err(e) => {
                            if !line_buf.is_empty() || !tool_bufs.is_empty() {
                                tracing::warn!("[{round}] {}/{} stream interrupted, using partial response: {e}", entry.base_url, entry.model);
                                break 'sse;
                            }
                            tracing::warn!("[{round}] {}/{} stream read error: {e}", entry.base_url, entry.model);
                            break 'sse; // will fall through to retry next provider if no finish_reason
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

                        if let Some(content) = delta.get("content").and_then(|c| c.as_str()) {
                            if !content.is_empty() {
                                content_sent = true;
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
                        if !content_sent || content_buf.trim().is_empty() {
                            tracing::warn!("[{round}] {}/{} stream produced empty content — retrying without tools", entry.base_url, entry.model);
                            Self::finalize_with_fallback(providers, http, messages, tx).await?;
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
        for (pi, entry) in providers.iter().enumerate() {
            tracing::info!(
                "Fallback [{}/{}]: {} @ {}",
                pi + 1,
                providers.len(),
                entry.model,
                entry.base_url
            );

            let url = format!("{}/chat/completions", entry.base_url.trim_end_matches('/'));
            let body = json!({
                "model": entry.model,
                "messages": messages,
                "temperature": 0.3,
                "max_tokens": 4096,
                "stream": true,
            });

            let response = match http
                .post(&url)
                .header("Authorization", format!("Bearer {}", entry.api_key))
                .json(&body)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("Fallback {}/{} HTTP send failed: {e}", entry.base_url, entry.model);
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

            while let Some(chunk_result) = stream.next().await {
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
                    if let Some(content) = delta.get("content").and_then(|c| c.as_str()) {
                        if !content.is_empty() {
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
