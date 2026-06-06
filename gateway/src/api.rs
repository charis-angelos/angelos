use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive},
        IntoResponse, Sse,
    },
    routing::{get, post},
    Json, Router,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc, time::Duration};

use crate::agent::ModelChain;

// ── OpenAI-compatible request/response types ──

#[derive(Deserialize)]
pub struct ChatCompletionRequest {
    pub messages: Vec<Message>,
    #[serde(default)]
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

// ── Streaming (SSE) response types ──

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

// ── Non-streaming (JSON) response types ──

#[derive(Serialize)]
struct NonStreamingResponse {
    id: String,
    object: String,
    created: u64,
    model: String,
    choices: Vec<NonStreamingChoice>,
}

#[derive(Serialize)]
struct NonStreamingChoice {
    index: u32,
    message: ResponseMessage,
    finish_reason: String,
}

#[derive(Serialize)]
struct ResponseMessage {
    role: String,
    content: String,
}

// ── App state ──

struct AppState {
    chain: ModelChain,
}

pub fn router() -> Router {
    let chain = ModelChain::from_env().expect("Failed to load chain config");
    let state = Arc::new(AppState { chain });

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
) -> Result<axum::response::Response, StatusCode> {
    let user_msg = req
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .and_then(|m| m.content.as_deref())
        .unwrap_or("")
        .trim()
        .to_string();

    if user_msg.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let preamble = build_preamble(&req.messages);

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

    if req.stream {
        let token_stream = state.chain.prompt_streaming(&user_msg, &preamble);

        let stream = token_stream
            .map(move |result| match result {
                Ok(token) => {
                    let chunk = ChatCompletionChunk {
                        id: id.clone(),
                        object: "chat.completion.chunk".into(),
                        created,
                        model: model.clone(),
                        choices: vec![ChoiceDelta {
                            index: 0,
                            delta: DeltaContent {
                                content: Some(token),
                            },
                            finish_reason: None,
                        }],
                    };
                    Ok(Event::default()
                        .data(serde_json::to_string(&chunk).unwrap_or_default()))
                }
                Err(e) => {
                    tracing::error!("Stream error: {e}");
                    let chunk = ChatCompletionChunk {
                        id: id.clone(),
                        object: "chat.completion.chunk".into(),
                        created,
                        model: model.clone(),
                        choices: vec![ChoiceDelta {
                            index: 0,
                            delta: DeltaContent {
                                content: Some(format!("\n\n[Error: {e}]")),
                            },
                            finish_reason: Some("stop".into()),
                        }],
                    };
                    Ok(Event::default()
                        .data(serde_json::to_string(&chunk).unwrap_or_default()))
                }
            })
            .chain({
                let done: Vec<Result<Event, Infallible>> =
                    vec![Ok(Event::default().data("[DONE]"))];
                futures::stream::iter(done)
            });

        Ok(Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response())
    } else {
        let response_text = match tokio::time::timeout(
            Duration::from_secs(120),
            state.chain.prompt_light(&user_msg, &preamble),
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

        Ok(Json(NonStreamingResponse {
            id,
            object: "chat.completion".into(),
            created,
            model,
            choices: vec![NonStreamingChoice {
                index: 0,
                message: ResponseMessage {
                    role: "assistant".into(),
                    content: response_text,
                },
                finish_reason: "stop".into(),
            }],
        })
        .into_response())
    }
}

fn build_preamble(messages: &[Message]) -> String {
    let mut preamble = crate::agent::build_full_preamble();

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
