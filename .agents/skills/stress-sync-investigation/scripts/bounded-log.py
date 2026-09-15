#!/usr/bin/env python3
"""Extract bounded, numbered matches or a bounded line range from a large log."""

import argparse
import re
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("log", type=Path)
parser.add_argument("patterns", nargs="*")
parser.add_argument("--start", type=int, default=1)
parser.add_argument("--end", type=int)
parser.add_argument("--lines", type=int, default=20)
parser.add_argument("--line-chars", type=int, default=160)
parser.add_argument("--chars", type=int, default=3000)
parser.add_argument("--last", action="store_true")
parser.add_argument("--ignore-case", action="store_true")
args = parser.parse_args()

flags = re.IGNORECASE if args.ignore_case else 0
patterns = [re.compile(pattern, flags) for pattern in args.patterns]
matches: list[str] = []
with args.log.open(errors="replace") as source:
    for number, raw in enumerate(source, 1):
        if number < args.start:
            continue
        if args.end is not None and number > args.end:
            break
        if patterns and not any(pattern.search(raw) for pattern in patterns):
            continue
        line = f"{number}:{raw.rstrip()}"[: args.line_chars]
        matches.append(line)
        if not args.last and len(matches) >= args.lines:
            break
        if args.last and len(matches) > args.lines:
            matches.pop(0)

used = 0
for line in matches:
    remaining = args.chars - used
    if remaining <= 0:
        break
    rendered = line[:remaining]
    print(rendered)
    used += len(rendered) + 1
