#!/usr/bin/env python3
"""W2 jump tables from w2.jsonl: python3 -I w2_tables.py <w2.jsonl>
Error series e(n) = marker y minus where the script's own scrolling alone would have put it
(y0 - sum of the script's increments since the click): 0 = no visible jump."""
import json, sys
from collections import defaultdict

def series(r, source):
    inc = {s[0]: s[4] for s in r["samples"]}
    cum, acc = {}, 0.0
    for n in range(r["clickN"] + 1, max(inc) + 1):
        acc += inc.get(n, 0.0)
        cum[n] = acc
    out = []
    if source == "dom":
        for s in r["samples"]:
            if s[0] > r["clickN"] and s[3] is not None:
                out.append((s[0], s[3] - (r["y0"] - cum[s[0]])))
    else:
        top = r["crop"]["y0"]
        for p in r["painted"]:
            if p["n"] > r["clickN"] and p["y"] is not None and p["n"] in cum:
                out.append((p["n"], (p["y"] - top) - (r["y0"] - cum[p["n"]])))
    return out

def metrics(r, source):
    delta = r["n"] * 20
    thr = max(5.0, 0.5 * delta)
    e = series(r, source)
    if not e:
        return None
    displaced = [n for n, v in e if abs(v) >= thr]
    steps = [(n, e[i][1] - e[i - 1][1]) for i, (n, _) in enumerate(e) if i and abs(e[i][1] - e[i - 1][1]) >= thr]
    return {
        "frames": len(e),
        "displaced": len(displaced),
        "first": (displaced[0] - r["clickN"]) if displaced else None,
        "steps": len(steps),
        "max": max(abs(v) for _, v in e),
        "last": e[-1][1],
    }

rows = defaultdict(list)
for line in open(sys.argv[1]):
    r = json.loads(line)
    rows[(r["n"], r["case"])].append(r)

def med(v):
    v = sorted(x for x in v if x is not None)
    return v[len(v) // 2] if v else None

for source in ("painted", "dom"):
    print(f"\n#### series from {source} ({'screencast frames, counter-matched' if source == 'painted' else 'per-rAF getBoundingClientRect'})")
    print("| N (px) | case | runs | frames tracked (med) | frames displaced >= max(5px, half the jump) (per-run list) | discontinuities >= thr (per-run list) | max abs error px (med) | error at last tracked frame px (per-run list) |")
    print("|---|---|---|---|---|---|---|---|")
    for (n, case), rs in sorted(rows.items()):
        ms = [metrics(r, source) for r in rs]
        ms = [m for m in ms if m]
        if not ms:
            print(f"| {n} ({n*20}) | {case} | {len(rs)} | no marker frames | | | | |")
            continue
        print(f"| {n} ({n*20}) | {case} | {len(ms)} | {med([m['frames'] for m in ms])} | {[m['displaced'] for m in ms]} | {[m['steps'] for m in ms]} | {med([m['max'] for m in ms]):.1f} | {[round(m['last']) for m in ms]} |")
