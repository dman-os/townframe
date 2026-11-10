#!/usr/bin/env python3
"""X5 tables from results.jsonl. Cells are medians over rounds of the per-round value, [lo-hi] the
spread of the rounds. Runs whose 1-minute load average was above 10 at either end are dropped."""
import json, statistics, sys, collections

path = sys.argv[1]
MAX_LOAD = 10.0
rows = [json.loads(l) for l in open(path) if l.strip()]
kept = [r for r in rows if r["loadStart"][0] <= MAX_LOAD and r["loadEnd"][0] <= MAX_LOAD]
print(f"<!-- {len(kept)} of {len(rows)} runs kept (load <= {MAX_LOAD} at both ends) -->")

ORDER = ["r0", "r3", "r3a", "r3-pierre", "r3a-pierre"]
CELL = lambda r: (r["mode"], r["speed"])
groups = collections.defaultdict(list)
for r in kept:
    groups[(r["variant"], CELL(r))].append(r)

def med(xs):
    return statistics.median(xs) if xs else float("nan")

def fmt(xs, nd=0):
    if not xs:
        return "-"
    f = lambda v: f"{v:.{nd}f}"
    return f"{f(med(xs))} [{f(min(xs))}-{f(max(xs))}]"

def episode_ms(r):
    return sum(e["ms"] for e in r["episodes"])

def longest(r):
    return max([e["ms"] for e in r["episodes"]], default=0)

cells = sorted({c for (_, c) in groups}, key=lambda c: (c[0] != "wheel", c[1]))
print("| variant | input | rounds | frames captured | blank frames | runs with any blank | max band px | blank ms per run (sum of episodes) | longest episode ms | rAF samples uncovered / samples | rAF max uncovered px | measured px/s |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|")
for v in ORDER:
    for c in cells:
        g = groups.get((v, c))
        if not g:
            continue
        anyblank = sum(1 for r in g if r["blankFrames"] > 0)
        print(
            f"| {v} | {c[0]} {c[1]} | {len(g)} | {fmt([r['framesTotal'] for r in g])} | {fmt([r['blankFrames'] for r in g])} | {anyblank}/{len(g)} | {fmt([r['maxBlankPx'] for r in g])} | {fmt([episode_ms(r) for r in g])} | {fmt([longest(r) for r in g])} | {fmt([r['probe']['rafUncoveredSamples'] for r in g])} / {fmt([r['probe']['rafSamples'] for r in g])} | {fmt([r['probe']['rafMaxUncoveredPx'] for r in g])} | {fmt([r['scrollPxPerSecMeasured'] for r in g])} |"
        )

print()
print("| variant | input | rows rendered (first sample) | rows above viewport px (median over frames) | rows below viewport px (median over frames) | row-add commits per run | rows added per commit (median) | rows removed per commit (median) |")
print("|---|---|---|---|---|---|---|---|")
for v in ORDER:
    for c in cells:
        g = groups.get((v, c))
        if not g:
            continue
        print(
            f"| {v} | {c[0]} {c[1]} | {fmt([r['probe']['rowCountFirst'] for r in g])} | {fmt([r['probe']['aboveRowsPx']['median'] for r in g])} | {fmt([r['probe']['belowRowsPx']['median'] for r in g])} | {fmt([r['rowsAdded']['commits'] for r in g])} | {fmt([r['rowsAdded']['addedPerCommit']['median'] or 0 for r in g])} | {fmt([r['rowsAdded']['removedPerCommit']['median'] or 0 for r in g])} |"
        )
