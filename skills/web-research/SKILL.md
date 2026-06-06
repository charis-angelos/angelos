---
name: web-research
description: 'Use agent-browser to conduct deep research on the web. This skill is used when the user asks to "research", "investigate", "survey", or "deep dive" into a topic. It involves a multi-step process: searching for sources, opening promising links, extracting text content, and synthesizing findings.'
---

## Navigation Pattern
- **First search only**: Use `agent-browser tab new <url>` to create a fresh browser tab.
- **All subsequent navigations**: Use `agent-browser open <url>` to reuse the current tab.
- Never use `tab new` after the first page — reusing a tab is faster and avoids tab clutter.

## Workflow
1. **Connect & First Search**: 
   - `agent-browser connect 9222`
   - `agent-browser tab new <url>` (e.g., `https://duckduckgo.com/?q=rust+axum+middleware`)
2. **Source Selection**: Use `agent-browser snapshot` to see the page structure and identify high-quality links (official docs, technical blogs, GitHub repos). Snapshot refs (`[ref=eXX]`) must use `@` prefix when clicking: `agent-browser click @eXX`.
3. **Content Extraction**: 
   - Use `agent-browser eval "document.body.innerText"` for quick full-text extraction.
   - Use `agent-browser get text <selector>` for specific elements if available.
4. **Follow Links**: Use `agent-browser open <url>` — never `tab new` — to navigate into detailed documentation or follow search result links.
5. **URL Verification** (if needed): If content looks wrong or page shows 404, fall back to DevTools API:
   ```
   curl -s http://127.0.0.1:9222/json/list | jq -r '.[] | "\(.title) | \(.url)"'
   ```
6. **Synthesis**: Combine information from multiple sources to provide a comprehensive answer.

## Tool Chain

### Primary: agent-browser Commands
- `run_bash` → `agent-browser connect 9222`: Ensure connection to the browser instance.
- `run_bash` → `agent-browser tab new <url>`: Create a new tab for the very first page. Use once per research session.
- `run_bash` → `agent-browser open <url>`: Navigate current tab to a target page. Use for all subsequent navigations.
- `run_bash` → `agent-browser wait --load networkidle`: Ensure the page is fully rendered before extraction.
- `run_bash` → `agent-browser snapshot`: Analyze DOM structure for navigation.
- `run_bash` → `agent-browser click @eXX`: Click a snapshot ref. Ref must be prefixed with `@`.
- `run_bash` → `agent-browser eval <js>`: Extract raw data using JavaScript execution in browser context.

### Secondary: DevTools API (fallback)
- `run_bash` → `curl -s http://127.0.0.1:9222/json/list | jq -r '.[] | "\(.title) | \(.url)"'`
  → Read real URLs directly from Chrome's debugging protocol when snapshots give wrong/empty results or when you need exact URLs.

## Best Practices
- Always use `--load networkidle` after navigation to avoid extracting empty pages during loading.
- Prefer official documentation (`docs.rs`, GitHub) over generic blogs for technical accuracy.
- When researching frameworks/libraries, look for "Architecture", "Principles", and "Quick Start" sections first.
- Search broadly in multiple languages and sources — don't limit to one site or language.
