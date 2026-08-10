---
name: agy
description: Quick reference and workflow guide for Antigravity CLI (agy), session history reconstruction, task monitoring, subagent delegation, and workspace compliance. Use when working within Antigravity CLI or executing session management and project verification workflows.
---

# Antigravity CLI (`agy`) Workflow & Reference Skill

This skill provides quick references for operating inside the **Antigravity CLI (`agy`)** harness environment.

## 1. Antigravity Session & History Management

Transcript logs for active and past sessions are stored in the user's brain log directory:
- **Base directory**: `~/.gemini/antigravity-cli/brain/<conversation-id>/.system_generated/logs/`
- **Files**:
  - `transcript.jsonl`: Token-efficient, compact log.
  - `transcript_full.jsonl`: Untruncated full record.

### History Recap Workflow

To safely extract user prompts and assistant outputs without XML metadata or context window pollution:

```bash
python3 - "$TRANSCRIPT_PATH" > /tmp/antigravity-history-recap.txt <<'PY'
import json, re, sys

def clean_content(text):
    if not isinstance(text, str):
        return ""
    req_match = re.findall(r'<USER_REQUEST>(.*?)</USER_REQUEST>', text, re.DOTALL)
    if req_match:
        text = "\n".join(req_match)
    else:
        text = re.sub(
            r'<(ADDITIONAL_METADATA|SYSTEM_MESSAGE|user_rules|user_information|identity|skills|subagents|messaging|conversation_transcript|artifacts|slash_commands|guidelines|communication_style|customizations)\b[^>]*>.*?</\1>',
            '', text, flags=re.DOTALL
        )
        text = re.sub(r'</?[a-zA-Z0-9_\-]+[^>]*>', '', text)
    return text.strip()

source = sys.argv[1]
with open(source, encoding="utf-8", errors="replace") as f:
    for line in f:
        try:
            rec = json.loads(line)
        except Exception:
            continue
        step = rec.get("step_index")
        rec_type = rec.get("type")
        source_type = rec.get("source")
        if rec_type == "USER_INPUT" and source_type == "USER_EXPLICIT":
            cleaned = clean_content(rec.get("content", ""))
            if cleaned:
                print(f"user (step {step}):")
                print(cleaned)
                print("---")
        elif rec_type == "PLANNER_RESPONSE" and source_type == "MODEL":
            cleaned = clean_content(rec.get("content", ""))
            if cleaned:
                print(f"assistant (step {step}):")
                print(cleaned)
                print("---")
PY
```

Measure and view bounded slices:

```bash
wc -l -c /tmp/antigravity-history-recap.txt
tail -200 /tmp/antigravity-history-recap.txt
```

---

## 2. Background Tasks & Subagent Delegation

- **Background Tasks**: Launch long-running builds/tests asynchronously via `run_command` with a short `WaitMsBeforeAsync`. Monitor status using `manage_task` (actions: `status`, `send_input`, `kill`).
- **Subagents**: Invoke subagents using `invoke_subagent`. Pass exact context and evidence. Subagents run background tasks reactively without requiring polling loops.

---

## 3. Project Quality & Anti-Cheating Directives

- **Clippy & Verification**: Always run `cargo clippy --all-targets --all-features -p <crate>` to verify changes.
- **Error Handling**: Do not swallow errors with `let _ =` or empty catch blocks. Use `inspect_err(...)` or explicit error propagation (`?`, `.expect(...)`).
- **VCS Safety**: Never use `git` commands; use `jj` (Jujutsu) for version control.
