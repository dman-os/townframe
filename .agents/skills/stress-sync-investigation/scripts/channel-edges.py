#!/usr/bin/env python3
"""Summarize named-channel correlation; unresolved observations are not lost-message proof."""

import argparse
from collections import Counter
import json
from pathlib import Path
import re

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("log", type=Path)
parser.add_argument("--limit", type=int, default=12, help="maximum channel rows and unresolved examples")
parser.add_argument("--max-pending", type=int, default=100000, help="fail rather than drop correlation state above this bound")
parser.add_argument("--max-channels", type=int, default=4096)
args = parser.parse_args()
if min(args.limit, args.max_pending, args.max_channels) <= 0:
    parser.error("all bounds must be positive")

ansi = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
edge = re.compile(
    r'actor channel edge process_id=(\d+) channel_id=(\d+) message_id=(\d+) '
    r'from=(.*?) to=(.*?) message_type="([^"]+)" phase="([^"]+)"'
)
phases = {"send_attempt", "send_complete", "receive", "send_cancelled", "send_full", "send_closed"}
channels = {}
pending = {}
totals = Counter()
records = 0
malformed = 0

with args.log.open(errors="replace") as source:
    for line_number, raw in enumerate(source, 1):
        if "actor channel edge" not in raw:
            continue
        match = edge.search(ansi.sub("", raw))
        if match is None:
            malformed += 1
            continue
        process, channel, message = map(int, match.group(1, 2, 3))
        origin, destination, message_type, phase = match.group(4, 5, 6, 7)
        if phase not in phases:
            parser.error(f"line {line_number}: unknown channel phase {phase!r}")
        identity = (process, channel)
        if identity not in channels:
            if len(channels) == args.max_channels:
                parser.error(f"channel budget exceeded at line {line_number}; raise --max-channels explicitly")
            channels[identity] = {
                "from": origin,
                "to": destination,
                "message_type": message_type,
                "phases": Counter(),
            }
        description = channels[identity]
        if (description["from"], description["to"], description["message_type"]) != (origin, destination, message_type):
            parser.error(f"line {line_number}: channel identity reused; analyze separate process runs separately")
        description["phases"][phase] += 1
        totals[phase] += 1
        records += 1
        key = (process, channel, message)
        mask, first_line = pending.get(key, (0, line_number))
        if phase in {"send_cancelled", "send_full", "send_closed"}:
            pending.pop(key, None)
            continue
        mask |= {"send_attempt": 1, "send_complete": 2, "receive": 4}[phase]
        if mask == 7:
            pending.pop(key, None)
        else:
            if key not in pending and len(pending) == args.max_pending:
                parser.error(f"correlation budget exceeded at line {line_number}; raise --max-pending explicitly")
            pending[key] = (mask, first_line)

if malformed:
    parser.error(f"{malformed} channel records lack the supported process/message schema; no partial summary accepted")
if not records:
    parser.error("no channel records; enable big_sync_core/channel-trace and big_sync_core::mpsc=trace")

rows = []
for (process, channel), description in sorted(channels.items())[:args.limit]:
    rows.append({
        "process": process,
        "channel": channel,
        "from": description["from"][:160],
        "to": description["to"][:160],
        "message_type": description["message_type"][:160],
        "phases": description["phases"],
    })
examples = []
for (process, channel, message), (mask, first_line) in sorted(pending.items(), key=lambda entry: entry[1][1])[:args.limit]:
    examples.append({
        "process": process,
        "channel": channel,
        "message": message,
        "first_line": first_line,
        "observed": [name for name, bit in (("send_attempt", 1), ("send_complete", 2), ("receive", 4)) if mask & bit],
    })
print(json.dumps({
    "records": records,
    "channels": len(channels),
    "phases": totals,
    "unresolved_observations": len(pending),
    "channel_rows": rows,
    "unresolved_examples": examples,
    "limits": {"rows": args.limit, "pending": args.max_pending, "channels": args.max_channels},
    "interpretation": "Correlation only: observations can cross send completion; unresolved entries may be queued or outside the capture. No payloads or queue linearization order were recorded. IDs are process-local; do not concatenate unrelated runs with reused PIDs.",
}, indent=2))
