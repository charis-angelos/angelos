use rig::{
    completion::ToolDefinition,
    tool::Tool,
};
use serde::Deserialize;

use crate::memory;

// ── ReadSelf tool ──

#[derive(Deserialize)]
pub struct ReadArgs {
    path: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Read error: {0}")]
pub struct ReadError(#[from] anyhow::Error);

pub struct ReadSelf;

impl Tool for ReadSelf {
    const NAME: &'static str = "read_self";

    type Error = ReadError;
    type Args = ReadArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Read a file from the workspace. Accepts absolute paths or paths relative to the repository root.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Absolute path, or relative path to the file (e.g. memory/daily/2026-05-09.md)"
                    }
                },
                "required": ["path"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        Ok(memory::read_self(&args.path)?)
    }
}

// ── WriteSelf tool ──

#[derive(Deserialize)]
pub struct WriteArgs {
    path: String,
    content: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Write error: {0}")]
pub struct WriteError(#[from] anyhow::Error);

pub struct WriteSelf;

impl Tool for WriteSelf {
    const NAME: &'static str = "write_self";

    type Error = WriteError;
    type Args = WriteArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Atomically write content to a file. Creates parent directories automatically. Path is relative to the repository root.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Relative path to write, e.g. memory/tasks/pending.md"
                    },
                    "content": {
                        "type": "string",
                        "description": "Full Markdown content to write"
                    }
                },
                "required": ["path", "content"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        memory::write_self(&args.path, &args.content)?;
        Ok(format!("Written to {}", args.path))
    }
}

// ── UpdateTask tool ──

#[derive(Deserialize)]
pub struct UpdateTaskArgs {
    /// Line to find (partial match) and the new status
    search: String,
    status: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Task update error: {0}")]
pub struct UpdateTaskError(#[from] anyhow::Error);

pub struct UpdateTask;

impl Tool for UpdateTask {
    const NAME: &'static str = "update_task";

    type Error = UpdateTaskError;
    type Args = UpdateTaskArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Update a task's status in tasks/pending.md. Searches for a line containing the search text and replaces `[ ]` with `[x]` (or vice versa if status is 'undo'). Status must be 'done' or 'undo'.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "search": {
                        "type": "string",
                        "description": "Text to find in the pending task line"
                    },
                    "status": {
                        "type": "string",
                        "enum": ["done", "undo"],
                        "description": "'done' marks complete, 'undo' unmarks"
                    }
                },
                "required": ["search", "status"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let content = memory::read_self("memory/tasks/pending.md")?;
        let mut updated = content.clone();
        match args.status.as_str() {
            "done" => {
                for line in content.lines() {
                    if line.contains(&args.search) && line.contains("[ ]") {
                        let new_line = line.replacen("[ ]", "[x]", 1);
                        updated = updated.replace(line, &new_line);
                        break;
                    }
                }
            }
            "undo" => {
                for line in content.lines() {
                    if line.contains(&args.search) && line.contains("[x]") {
                        let new_line = line.replacen("[x]", "[ ]", 1);
                        updated = updated.replace(line, &new_line);
                        break;
                    }
                }
            }
            _ => return Err(UpdateTaskError(anyhow::anyhow!("Invalid status: {}", args.status))),
        }
        memory::write_self("memory/tasks/pending.md", &updated)?;
        Ok(format!("Task matching '{}' updated to status '{}'", args.search, args.status))
    }
}

// ── SearchSelf tool ──

#[derive(Deserialize)]
pub struct SearchArgs {
    query: String,
}

#[derive(Debug, thiserror::Error)]
#[error("Search error: {0}")]
pub struct SearchError(#[from] anyhow::Error);

pub struct SearchSelf;

impl Tool for SearchSelf {
    const NAME: &'static str = "search_self";

    type Error = SearchError;
    type Args = SearchArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Search all Markdown files in the workspace for a keyword. Returns matching file paths and content snippets.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Keyword or phrase to search for"
                    }
                },
                "required": ["query"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let results = memory::search_self(&args.query)?;
        if results.is_empty() {
            Ok("No matches found.".into())
        } else {
            let formatted: Vec<String> = results
                .iter()
                .map(|m| format!("## {}\n{}", m.path, m.snippet))
                .collect();
            Ok(formatted.join("\n\n---\n\n"))
        }
    }
}

// ── RunBash tool ──

#[derive(Deserialize)]
pub struct RunBashArgs {
    command: String,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
#[error("Bash error: {0}")]
pub struct BashError(String);

pub struct RunBash;

impl Tool for RunBash {
    const NAME: &'static str = "run_bash";

    type Error = BashError;
    type Args = RunBashArgs;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Execute a bash command and return stdout+stderr. \
                Runs from the workspace directory. Max output 8KB. \
                Use for system operations: check disk, list files, run scripts, etc. \
                Avoid destructive commands (rm -rf, etc)."
                .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The bash command to execute"
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "description": "Optional timeout in seconds (default 30)"
                    }
                },
                "required": ["command"]
            }),
        }
    }

    async fn call(&self, args: Self::Args) -> Result<Self::Output, Self::Error> {
        let timeout = args.timeout_secs.unwrap_or(30).min(120);
        let work_dir = ".".to_string();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(timeout),
            tokio::process::Command::new("bash")
                .args(["-c", &args.command])
                .current_dir(&work_dir)
                .output(),
        )
        .await
        .map_err(|_| BashError("Command timed out".into()));

        match result {
            Ok(Ok(output)) => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                let mut combined = String::new();
                if !stdout.is_empty() {
                    combined.push_str(&stdout);
                }
                if !stderr.is_empty() {
                    if !combined.is_empty() {
                        combined.push('\n');
                    }
                    combined.push_str("--- stderr ---\n");
                    combined.push_str(&stderr);
                }
                if combined.is_empty() {
                    combined = format!("Command exited with code {:?}", output.status.code());
                }
                // Truncate to ~8KB
                if combined.len() > 8192 {
                    let end = combined.floor_char_boundary(8192);
                    combined.truncate(end);
                    combined.push_str("\n... (output truncated)");
                }
                Ok(combined)
            }
            Ok(Err(e)) => Err(BashError(format!("Failed to execute: {e}"))),
            Err(e) => Err(e),
        }
    }
}

// ── Streaming path: raw tool definitions and dispatch ──

/// OpenAI-format tool definitions for the streaming HTTP path.
pub fn tool_definitions() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "type": "function",
            "function": {
                "name": "read_self",
                "description": "Read a file from the workspace. Accepts absolute paths or paths relative to the repository root.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Absolute path, or relative path to the file (e.g. memory/daily/2026-05-09.md)"
                        }
                    },
                    "required": ["path"]
                }
            }
        }),
        serde_json::json!({
            "type": "function",
            "function": {
                "name": "write_self",
                "description": "Atomically write content to a file. Creates parent directories automatically. Path is relative to the repository root.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Relative path to write, e.g. memory/tasks/pending.md"
                        },
                        "content": {
                            "type": "string",
                            "description": "Full Markdown content to write"
                        }
                    },
                    "required": ["path", "content"]
                }
            }
        }),
        serde_json::json!({
            "type": "function",
            "function": {
                "name": "update_task",
                "description": "Update a task's status in memory/tasks/pending.md. Searches for a line containing the search text and replaces `[ ]` with `[x]` (or vice versa if status is 'undo'). Status must be 'done' or 'undo'.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "search": {
                            "type": "string",
                            "description": "Text to find in the pending task line"
                        },
                        "status": {
                            "type": "string",
                            "enum": ["done", "undo"],
                            "description": "'done' marks complete, 'undo' unmarks"
                        }
                    },
                    "required": ["search", "status"]
                }
            }
        }),
        serde_json::json!({
            "type": "function",
            "function": {
                "name": "search_self",
                "description": "Search all Markdown files in the workspace for a keyword. Returns matching file paths and content snippets.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Keyword or phrase to search for"
                        }
                    },
                    "required": ["query"]
                }
            }
        }),
        serde_json::json!({
            "type": "function",
            "function": {
                "name": "run_bash",
                "description": "Execute a bash command and return stdout+stderr. Runs from the workspace directory. Max output 8KB. Use for system operations: check disk, list files, run scripts, etc. Avoid destructive commands (rm -rf, etc).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "The bash command to execute"
                        },
                        "timeout_secs": {
                            "type": "integer",
                            "description": "Optional timeout in seconds (default 30)"
                        }
                    },
                    "required": ["command"]
                }
            }
        }),
    ]
}

/// Execute a tool by name, given raw JSON arguments string.
pub async fn execute_tool(name: &str, args: &str) -> anyhow::Result<String> {
    match name {
        "read_self" => {
            let args: ReadArgs = serde_json::from_str(args)?;
            ReadSelf.call(args).await.map_err(|e| anyhow::anyhow!("{e}"))
        }
        "write_self" => {
            let args: WriteArgs = serde_json::from_str(args)?;
            WriteSelf.call(args).await.map_err(|e| anyhow::anyhow!("{e}"))
        }
        "update_task" => {
            let args: UpdateTaskArgs = serde_json::from_str(args)?;
            UpdateTask.call(args).await.map_err(|e| anyhow::anyhow!("{e}"))
        }
        "search_self" => {
            let args: SearchArgs = serde_json::from_str(args)?;
            SearchSelf.call(args).await.map_err(|e| anyhow::anyhow!("{e}"))
        }
        "run_bash" => {
            let args: RunBashArgs = serde_json::from_str(args)?;
            RunBash.call(args).await.map_err(|e| anyhow::anyhow!("{e}"))
        }
        _ => {
            let defs = tool_definitions();
            let available: Vec<&str> = defs
                .iter()
                .filter_map(|t| t["function"]["name"].as_str())
                .collect();
            anyhow::bail!(
                "Tool '{name}' does not exist. Available tools: {available}.\n\
                 If your task matches a skill in the Available Skills catalog (see system prompt), \
                 load that skill via read_self first and follow its instructions.\n\
                 If no available tool or skill fits the task, explain clearly what you cannot do \
                 rather than guessing tool names.",
                available = available.join(", "),
            )
        },
    }
}
