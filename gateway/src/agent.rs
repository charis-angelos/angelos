use anyhow::Context;
use rig::providers::openai;
use serde::Deserialize;

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
        Ok(Self { providers })
    }

    fn make_client(entry: &ProviderEntry) -> openai::Client {
        openai::Client::from_url(&entry.api_key, &entry.base_url)
    }

    /// Build a rig agent for the given provider entry.
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
            .build()
    }

    /// Run a prompt through the chain with multi-round tool calling.
    /// Tries each provider in order. Returns the final text response.
    pub async fn prompt(
        &self,
        user_prompt: &str,
        preamble: &str,
    ) -> anyhow::Result<String> {
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

    /// Multi-round tool calling loop. Executes tools and feeds results back
    /// to the model until a text response is produced.
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
