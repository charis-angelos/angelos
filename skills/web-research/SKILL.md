---
name: web-research
description: 'Use agent-browser to conduct deep research on the web. This skill is used when the user asks to "research", "investigate", "survey", or "deep dive" into a topic. It involves a multi-step process: searching for sources, opening promising links, extracting text content, and synthesizing findings.'
---

## Workflow
1. **Initial Search**: Use `agent-browser open` with a search engine (e.g., DuckDuckGo) or `agent-browser open` directly to a known official site.
2. **Source Selection**: Use `agent-browser snapshot` to see the page structure and identify high-quality links (official docs, technical blogs, GitHub repos).
3. **Content Extraction**: 
   - Use `agent-browser eval "document.body.innerText"` for quick full-text extraction.
   - Use `agent-browser get text <selector>` for specific elements if available.
4. **Deep Dive**: If the initial page is a landing page, use `agent-browser click` or `open` to navigate into detailed documentation or API references.
5. **Synthesis**: Combine information from multiple sources to provide a comprehensive answer, comparing different perspectives if necessary.

## Tool Chain
- `run_bash` $\rightarrow$ `agent-browser connect 9222`: Ensure connection to the browser instance.
- `run_bash` $\rightarrow$ `agent-browser open <url>`: Navigate to a target page.
- `run_bash` $\rightarrow$ `agent-browser wait --load networkidle`: Ensure the page is fully rendered before extraction.
- `run_bash` $\rightarrow$ `agent-browser snapshot`: Analyze DOM structure for navigation.
- `run_bash` $\rightarrow$ `agent-browser eval <js>`: Extract raw data using JavaScript execution in the browser context.

## Best Practices
- Always use `--load networkidle` to avoid extracting empty pages during loading.
- Prefer official documentation (`docs.rs`, GitHub) over generic blogs for technical accuracy.
- When researching frameworks/libraries, look for "Architecture", "Principles", and "Quick Start" sections first.
