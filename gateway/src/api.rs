use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive},
        Sse,
    },
    routing::{get, post},
    Json, Router,
};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc, time::Duration};

use crate::agent::ModelChain;

// ── OpenAI-compatible request/response types ──

#[derive(Deserialize)]
pub struct ChatCompletionRequest {
    pub messages: Vec<Message>,
    #[serde(default)]
    #[allow(dead_code)]
    pub stream: bool,
    #[serde(default)]
    #[allow(dead_code)]
    pub model: Option<String>,
}

#[derive(Deserialize)]
pub struct Message {
    pub role: String,
    pub content: Option<String>,
}

#[derive(Serialize)]
struct ChatCompletionChunk {
    id: String,
    object: String,
    created: u64,
    model: String,
    choices: Vec<ChoiceDelta>,
}

#[derive(Serialize)]
struct ChoiceDelta {
    index: u32,
    delta: DeltaContent,
    finish_reason: Option<String>,
}

#[derive(Serialize)]
struct DeltaContent {
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

// ── App state ──

struct AppState {
    soul: String,
    chain: ModelChain,
}

pub fn router(soul: String) -> Router {
    let chain = ModelChain::from_env().expect("Failed to load chain config");
    let state = Arc::new(AppState { soul, chain });

    Router::new()
        .route("/health", get(health))
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .with_state(state)
}

#[derive(Serialize)]
struct ModelEntry {
    id: String,
    object: String,
    created: u64,
    owned_by: String,
}

#[derive(Serialize)]
struct ModelListResponse {
    object: String,
    data: Vec<ModelEntry>,
}

async fn list_models() -> Json<ModelListResponse> {
    Json(ModelListResponse {
        object: "list".into(),
        data: vec![ModelEntry {
            id: "angelos".into(),
            object: "model".into(),
            created: 0,
            owned_by: "angelos".into(),
        }],
    })
}

async fn health() -> &'static str {
    "OK"
}

async fn chat_completions(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ChatCompletionRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    // Extract the latest user message as prompt
    let user_msg = req
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .and_then(|m| m.content.as_deref())
        .unwrap_or("");

    // Build preamble + inject prior chat messages as context
    let preamble = build_preamble(&state.soul, &req.messages);

    let response = match tokio::time::timeout(
        Duration::from_secs(120),
        state.chain.prompt(user_msg, &preamble),
    )
    .await
    {
        Ok(Ok(text)) => text,
        Ok(Err(e)) => {
            tracing::error!("LLM error: {e}");
            return Err(StatusCode::BAD_GATEWAY);
        }
        Err(_) => {
            tracing::error!("LLM timeout");
            return Err(StatusCode::GATEWAY_TIMEOUT);
        }
    };

    let model = state
        .chain
        .providers
        .first()
        .map(|p| p.model.clone())
        .unwrap_or_else(|| "unknown".into());

    let id = format!("chatcmpl-{}", uuid_v4());
    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Build SSE stream: emit content chunks then done
    let stream = sse_stream(response, id, model, created);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn build_preamble(soul: &str, messages: &[Message]) -> String {
    let mut preamble = crate::agent::build_full_preamble(soul);

    // Inject prior conversation as context
    let prior: Vec<String> = messages
        .iter()
        .filter(|m| m.role != "system")
        .filter_map(|m| {
            m.content
                .as_deref()
                .map(|c| format!("[{}]: {c}", m.role))
        })
        .collect();

    if !prior.is_empty() {
        preamble.push_str("\n\n## Conversation History\n");
        preamble.push_str(&prior.join("\n"));
    }

    preamble
}

fn sse_stream(
    text: String,
    id: String,
    model: String,
    created: u64,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let chars: Vec<char> = text.chars().collect();
    let chunk_size = 4usize; // chars per SSE event
    let total = chars.len();

    let events: Vec<Result<Event, Infallible>> = chars
        .chunks(chunk_size)
        .enumerate()
        .map(|(i, chunk)| {
            let content: String = chunk.iter().collect();
            let chunk_data = ChatCompletionChunk {
                id: id.clone(),
                object: "chat.completion.chunk".into(),
                created,
                model: model.clone(),
                choices: vec![ChoiceDelta {
                    index: 0,
                    delta: DeltaContent {
                        content: Some(content),
                    },
                    finish_reason: if i >= total.saturating_sub(1) / chunk_size {
                        Some("stop".into())
                    } else {
                        None
                    },
                }],
            };
            Ok(Event::default()
                .data(serde_json::to_string(&chunk_data).unwrap_or_default()))
        })
        .collect();

    // Append [DONE] marker
    let mut events = events;
    events.push(Ok(Event::default().data("[DONE]")));

    futures::stream::iter(events)
}

fn uuid_v4() -> String {
    use std::fmt::Write;
    let mut buf = String::with_capacity(36);
    for (i, b) in rand_bytes(16).iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            buf.push('-');
        }
        write!(buf, "{b:02x}").unwrap();
    }
    buf
}

fn rand_bytes(n: usize) -> Vec<u8> {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut bytes = Vec::with_capacity(n);
    let mut hasher = RandomState::new().build_hasher();
    while bytes.len() < n {
        hasher.write_u64(bytes.len() as u64);
        bytes.extend_from_slice(&hasher.finish().to_le_bytes());
    }
    bytes.truncate(n);
    bytes
}
