use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

fn memory_dir() -> PathBuf {
    std::env::var("MEMORY_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./memory"))
}

/// Resolve a relative path against MEMORY_DIR. Absolute paths pass through.
/// Security: rejects paths containing ".." to prevent directory traversal.
pub fn resolve_path(path: &str) -> Result<PathBuf> {
    if path.contains("..") {
        anyhow::bail!("Path traversal rejected: {path}");
    }
    let p = Path::new(path);
    if p.is_absolute() {
        Ok(p.to_path_buf())
    } else {
        Ok(memory_dir().join(p))
    }
}

pub fn read_memory(path: &str) -> Result<String> {
    let full = resolve_path(path)?;
    Ok(std::fs::read_to_string(&full).unwrap_or_default())
}

pub fn write_memory(path: &str, content: &str) -> Result<()> {
    let full = resolve_path(path)?;
    if let Some(parent) = full.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("mkdir {}", parent.display()))?;
    }
    let dir = full.parent().unwrap_or(Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    std::io::Write::write_all(&mut tmp, content.as_bytes())?;
    tmp.persist(&full)
        .with_context(|| format!("persist {}", full.display()))?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct SearchMatch {
    pub path: String,
    pub snippet: String,
}

pub fn search_memory(query: &str) -> Result<Vec<SearchMatch>> {
    let mut results = Vec::new();
    let base = memory_dir();
    if base.exists() {
        search_dir(&base, query, &mut results)?;
    }
    // Sort by path for deterministic output
    results.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(results)
}

fn search_dir(dir: &Path, query: &str, results: &mut Vec<SearchMatch>) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            search_dir(&path, query, results)?;
        } else if path.extension().is_some_and(|e| e == "md") {
            let content = std::fs::read_to_string(&path)?;
            if content.to_lowercase().contains(&query.to_lowercase()) {
                let rel = path
                    .strip_prefix(memory_dir())
                    .unwrap_or(&path)
                    .display()
                    .to_string();
                let snippet = snippet_around(&content, query, 120);
                results.push(SearchMatch { path: rel, snippet });
            }
        }
    }
    Ok(())
}

fn snippet_around(content: &str, query: &str, radius: usize) -> String {
    let lower = content.to_lowercase();
    if let Some(pos) = lower.find(&query.to_lowercase()) {
        let start = pos.saturating_sub(radius / 2);
        let end = (pos + query.len() + radius / 2).min(content.len());
        let snip = &content[start..end];
        let prefix = if start > 0 { "…" } else { "" };
        let suffix = if end < content.len() { "…" } else { "" };
        format!("{prefix}{snip}{suffix}")
    } else {
        content.chars().take(radius * 2).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_path_rejects_traversal() {
        assert!(resolve_path("../etc/passwd").is_err());
        assert!(resolve_path("foo/../../bar").is_err());
    }

    #[test]
    fn test_snippet_around() {
        let s = snippet_around("The quick brown fox jumps over the lazy dog", "fox", 20);
        assert!(s.contains("fox"));
    }
}
