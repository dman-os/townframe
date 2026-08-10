---
name: history-recap
description: Safely reconstruct important context from Antigravity CLI JSONL session transcripts while excluding XML boilerplate, tool calls, binary/truncated noise, developer instructions, and oversized outputs. Use after context compaction, when resuming an earlier Antigravity CLI session, or when the user asks to recover decisions, user requests, and issues from conversation history.
---

# Safe Antigravity CLI History Recap

Recover user messages and assistant messages from an Antigravity CLI transcript log without flooding the context window with XML boilerplate tags or raw tool execution output.

## Workflow

1. Identify candidate session transcripts in Antigravity CLI's brain log directory:

   ```bash
   find ~/.gemini/antigravity-cli/brain/ -type f -name 'transcript.jsonl' -printf '%T@ %p\n' \
     | sort -nr | head -10
   ```

2. Extract only clean conversational user inputs and key assistant outputs to a temporary file. Skip malformed lines, tool call details, and system XML metadata blocks (`<USER_REQUEST>`, `<ADDITIONAL_METADATA>`, `<SYSTEM_MESSAGE>`, `<user_rules>`, `<identity>`, etc.):

   ```bash
   python3 - "$transcript_path" > /tmp/antigravity-history-recap.txt <<'PY'
   import json, re, sys

   def clean_content(text):
       if not isinstance(text, str):
           return ""
       # Extract inner content of <USER_REQUEST> if present
       req_match = re.findall(r'<USER_REQUEST>(.*?)</USER_REQUEST>', text, re.DOTALL)
       if req_match:
           text = "\n".join(req_match)
       else:
           # Strip system XML metadata blocks
           text = re.sub(
               r'<(ADDITIONAL_METADATA|SYSTEM_MESSAGE|user_rules|user_information|identity|skills|subagents|messaging|conversation_transcript|artifacts|slash_commands|guidelines|communication_style|customizations)\b[^>]*>.*?</\1>',
               '', text, flags=re.DOTALL
           )
           text = re.sub(r'</?[a-zA-Z0-9_\-]+[^>]*>', '', text)
       return text.strip()

   source = sys.argv[1]
   malformed = 0
   with open(source, encoding="utf-8", errors="replace") as stream:
       for line in stream:
           try:
               record = json.loads(line)
           except json.JSONDecodeError:
               malformed += 1
               continue

           step = record.get("step_index")
           rec_type = record.get("type")
           source_type = record.get("source")

           if rec_type == "USER_INPUT" and source_type == "USER_EXPLICIT":
               cleaned = clean_content(record.get("content", ""))
               if cleaned:
                   print(f"user (step {step}):")
                   print(cleaned)
                   print("---")
           elif rec_type == "PLANNER_RESPONSE" and source_type == "MODEL":
               cleaned = clean_content(record.get("content", ""))
               if cleaned:
                   print(f"assistant (step {step}):")
                   print(cleaned)
                   print("---")

   if malformed:
       print(f"history-recap: skipped {malformed} malformed JSONL record(s)", file=sys.stderr)
   PY
   ```

3. Measure before reading:

   ```bash
   wc -l -c /tmp/antigravity-history-recap.txt
   ```

4. Read bounded slices chosen from the measured size:

   ```bash
   tail -200 /tmp/antigravity-history-recap.txt
   ```

5. Produce a compact operational recap containing user goals, accepted decisions, constraints, code changes, evidence, unresolved questions, and exact next actions. Explicitly remind the next agent to refresh `AGENTS.md` and linked project documents after compaction.

## Guardrails

- Never `cat` or print an entire raw transcript JSONL into context.
- Never emit system XML tags (`<USER_REQUEST>`, `<ADDITIONAL_METADATA>`, `<SYSTEM_MESSAGE>`, etc.) into recap outputs.
- Filter for `USER_INPUT` steps when searching for user prompts or issue lists.
- Skip tool calls (`RUN_COMMAND`, `REPLACE_FILE_CONTENT`, etc.) and system notifications.
