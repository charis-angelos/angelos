---
description: Write commit messages — describe the commit as a unit vs its parent, not as a changelog of editing sessions.
---

## Commit Message Discipline

When writing or amending a commit message, the message describes the **net result** of the entire commit relative to its parent. It is NOT a log of what you edited during the session.

### Core Rules

1. **Describe final state, not edit history.** Every bullet must be true when comparing `git diff <parent>..HEAD`. If a line was added then removed within the same amended commit, it never existed — don't mention it.
2. **Verify with `git diff` before finalizing.** Run `git diff <parent>..HEAD` (or `--stat` for scope). Write bullets from the diff, not from memory.
3. **Don't mention things that never shipped.** "Drop X", "remove X", "revert X" are only valid if X exists in the parent commit. If X was introduced and removed within the same amended commit, it was never committed — silence.
4. **Match surrounding style.** Preserve subject line structure, bullet prefix (`-`), indentation (2 spaces), and level of detail. A new bullet in an existing message body should blend in.
5. **Precision over vagueness.** `8→7 across both streaming and non-streaming agent loops` — not `tune tool limits`. Specific numbers, specific locations, specific effects.
6. **State what the code IS, not what you DID to it.** "fallback prompt omits tool-unavailability language" (describes the code) beats "removed 'no more tools'" (describes your editing session).

### Common Mistakes

| Wrong | Right | Why |
|-------|-------|-----|
| "Drop X from fallback prompt" | "Fallback prompt omits X" | X was added in this same commit — it never shipped |
| "Tune tool limits" | "Cap max tool rounds 8→7" | Vague; no way to verify from diff |
| "Remove no-more-tools wording" | (omit the bullet entirely if phrase never in parent) | If intro'd and removed in same amend, it's not a change |
| Describing from memory | Running `git diff <parent>..HEAD` first | Memory of editing session ≠ actual commit content |

### Amend Workflow

```
# 1. See what the commit actually changes vs parent
git diff <parent>..HEAD --stat
git diff <parent>..HEAD -- <files-of-interest>

# 2. Write bullets describing the NET change
# Each bullet: what does this commit do that the parent didn't?

# 3. Amend
git commit --amend -m "subject
- bullet describing final state
- bullet describing final state"

# 4. Verify
git log -1 --format='%B'
```
