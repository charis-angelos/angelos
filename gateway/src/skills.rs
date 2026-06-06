use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Parsed frontmatter from a SKILL.md file.
#[derive(Debug, Clone, Deserialize)]
pub struct SkillMeta {
    pub name: String,
    pub description: String,
}

/// A discovered skill with metadata and file location.
#[derive(Debug, Clone)]
pub struct Skill {
    pub name: String,
    pub description: String,
    /// Absolute path to the SKILL.md file.
    pub location: PathBuf,
    /// Absolute path to the skill directory.
    pub dir: PathBuf,
}

/// Extract YAML frontmatter name and description from SKILL.md content.
fn parse_frontmatter(content: &str) -> Result<SkillMeta> {
    let body = content.strip_prefix("---\n").or_else(|| content.strip_prefix("---\r\n"));
    let body = body.unwrap_or(content);

    let end = body.find("\n---").context("No closing --- for frontmatter")?;
    let yaml = &body[..end];

    // Try strict YAML parsing first
    if let Ok(meta) = serde_yaml::from_str::<SkillMeta>(yaml) {
        return Ok(meta);
    }

    // Fallback: lenient line-by-line extraction for malformed YAML
    // (e.g. unquoted values containing colons)
    parse_frontmatter_lenient(yaml).context("Failed to parse SKILL.md frontmatter (strict and lenient)")
}

/// Lenient fallback: extract name and description line-by-line.
/// Handles common YAML issues like unquoted colons in description values.
fn parse_frontmatter_lenient(yaml: &str) -> Option<SkillMeta> {
    let mut name = None;
    let mut description = None;

    for line in yaml.lines() {
        if let Some(v) = line.strip_prefix("name:").or_else(|| line.strip_prefix("name :")) {
            name = Some(v.trim().trim_matches('"').trim_matches('\'').to_string());
        }
    }
    // Description may appear as plain scalar or quoted string
    for line in yaml.lines() {
        let desc = line
            .strip_prefix("description:")
            .or_else(|| line.strip_prefix("description :"));
        if let Some(v) = desc {
            let v = v.trim();
            // Strip surrounding quotes if present
            let v = if (v.starts_with('"') && v.ends_with('"'))
                || (v.starts_with('\'') && v.ends_with('\''))
            {
                &v[1..v.len() - 1]
            } else {
                v
            };
            if !v.is_empty() {
                description = Some(v.to_string());
            }
            break;
        }
    }

    match (name, description) {
        (Some(n), Some(d)) => Some(SkillMeta { name: n, description: d }),
        _ => None,
    }
}

/// Scan a directory for subdirectories containing SKILL.md files.
fn scan_dir(root: &Path) -> Vec<Skill> {
    let mut skills = Vec::new();
    let dirs = match std::fs::read_dir(root) {
        Ok(d) => d,
        Err(_) => return skills,
    };

    for entry in dirs.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let skill_md = path.join("SKILL.md");
        if !skill_md.is_file() {
            continue;
        }
        let content = match std::fs::read_to_string(&skill_md) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let meta = match parse_frontmatter(&content) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("Skipping {}: {e}", skill_md.display());
                continue;
            }
        };
        let location = std::fs::canonicalize(&skill_md).unwrap_or_else(|_| skill_md.clone());
        let dir = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        skills.push(Skill {
            name: meta.name,
            description: meta.description,
            location,
            dir,
        });
    }

    skills
}

/// Discover all skills from configured directories.
/// Returns skills sorted by name (project-level first, then user-level).
pub fn discover() -> Vec<Skill> {
    let mut all = Vec::new();

    // Project-level: ./skills/
    all.extend(scan_dir(Path::new("./skills")));

    // User-level: ~/.agents/skills/
    if let Some(home) = home_dir() {
        all.extend(scan_dir(&home.join(".agents").join("skills")));
    }

    // Deduplicate by name: project-level wins (first-found stays)
    let mut seen = std::collections::HashSet::new();
    all.retain(|s| seen.insert(s.name.clone()));

    all.sort_by(|a, b| a.name.cmp(&b.name));
    all
}

fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME").ok().map(PathBuf::from)
}

/// Build the skill catalog string for injection into the system prompt.
pub fn build_catalog(skills: &[Skill]) -> String {
    if skills.is_empty() {
        return String::new();
    }

    let mut catalog = String::from("\n\n## Available Skills\n\n");
    catalog.push_str(
        "When a task matches a skill's description, use `read_self` with the exact absolute \
         path from _location_ to load the SKILL.md before proceeding. \
         Do not construct a relative path — copy the _location_ value verbatim. \
         When a skill references relative paths, resolve them against the skill's directory \
         (the parent of SKILL.md) and use absolute paths in tool calls.\n\n",
    );

    for s in skills {
        catalog.push_str(&format!(
            "- **{}**: {}\n  _location_: `{}`\n  _dir_: `{}`\n",
            s.name,
            s.description,
            s.location.display(),
            s.dir.display()
        ));
    }

    catalog
}
