---
name: bash-execution
description: Execute bash commands for system operations — check disk, list files, run scripts, read logs, manage processes, and inspect system state.
---

## Bash Execution

You have access to a `run_bash` tool that executes shell commands from the repository root.

### When to Use This Skill

- Check disk usage: `run_bash` with `df -h` or `du -sh /path`
- List files: `run_bash` with `ls -la`
- Read system logs: `run_bash` with `tail -n 50 /var/log/syslog`
- Check running processes: `run_bash` with `ps aux | grep <name>`
- Run scripts in the workspace: `run_bash` with `./scripts/script.sh`
- Check system info: `run_bash` with `uname -a`, `free -h`, `uptime`

### Safety Constraints

- Max timeout: 120 seconds (default 30)
- Max output: ~8KB (truncated beyond that)
- The command runs from the repository root
- Avoid destructive commands (`rm -rf`, `dd`, `mkfs`, etc.)
- Avoid commands that modify system configuration without user confirmation

### Examples

```
# Check disk space
run_bash(command="df -h")

# Check memory usage
run_bash(command="free -h")

# Find large files in home directory
run_bash(command="find ~ -type f -size +100M -exec ls -lh {} \; 2>/dev/null | head -20", timeout_secs=60)
```
