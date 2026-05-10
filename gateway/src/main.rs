use std::net::SocketAddr;

mod agent;
mod api;
mod memory;
mod skills;
mod tools;

#[derive(Debug)]
enum Mode {
    Http { port: u16 },
    Cron { prompt: String },
}

fn parse_args() -> Mode {
    let args: Vec<String> = std::env::args().collect();
    let mut port = 8000u16;
    let mut mode_flag = "http".to_string();
    let mut prompt = String::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                i += 1;
                if let Some(p) = args.get(i) {
                    port = p.parse().unwrap_or(8000);
                }
            }
            "--mode" => {
                i += 1;
                if let Some(m) = args.get(i) {
                    mode_flag = m.clone();
                }
            }
            "--prompt" => {
                i += 1;
                if let Some(p) = args.get(i) {
                    prompt = p.clone();
                }
            }
            _ => {}
        }
        i += 1;
    }

    match mode_flag.as_str() {
        "cron" => Mode::Cron {
            prompt: if prompt.is_empty() {
                std::io::Read::read_to_string(&mut std::io::stdin(), &mut prompt).unwrap_or(0);
                prompt = prompt.trim().to_string();
                if prompt.is_empty() {
                    prompt = "Generate a daily summary and update tasks".to_string();
                }
                prompt
            } else {
                prompt
            },
        },
        _ => Mode::Http { port },
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let soul = std::fs::read_to_string(
        std::env::var("SOUL_PATH").unwrap_or_else(|_| "./SOUL.md".to_string()),
    )
    .unwrap_or_else(|_| "You are a helpful personal assistant.".to_string());

    let mode = parse_args();
    tracing::info!("Gateway starting in {:?} mode", mode);

    match mode {
        Mode::Http { port } => {
            let addr = SocketAddr::from(([0, 0, 0, 0], port));
            tracing::info!("Listening on {addr}");
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, api::router(soul)).await?;
        }
        Mode::Cron { prompt } => {
            let result = agent::run_sync(prompt, soul).await?;
            println!("{result}");
        }
    }

    Ok(())
}
