#!/usr/bin/env python3
"""Aggregates results.jsonl (one JSON line per rung x workload x round) into the X1 markdown tables.
Each cell is the median over rounds of the per-round median (spread = min..max of the round
medians), so one noisy round shows up as spread instead of moving the number."""
import json, sys, statistics as st, collections

path = sys.argv[1]
rows = [json.loads(l) for l in open(path) if l.strip()]
import os
if os.environ.get("AGG_CLEAN") == "1":
    # Drop runs whose 1-minute load average was above 10 at either end (shared, loaded host).
    rows = [r for r in rows if max(r["loadStart"][0], r["loadEnd"][0]) <= 10]
order = ["r1-static", "r0", "r1-dom", "r1-html", "r2", "r3"]
by = collections.defaultdict(list)
for r in rows:
    by[(r["rung"], r["workload"])].append(r)

def cell(key, getter, digits=2):
    vals = [getter(r) for r in by.get(key, [])]
    vals = [v for v in vals if v is not None]
    if not vals:
        return "-"
    m = st.median(vals)
    if len(vals) == 1:
        return f"{m:.{digits}f}"
    return f"{m:.{digits}f} ({min(vals):.{digits}f}-{max(vals):.{digits}f})"

def med(r, *path):
    for p in path:
        r = r[p]
    return r

def rounds(key):
    return len(by.get(key, []))


def mval(key, getter):
    vals = [getter(r) for r in by.get(key, [])]
    vals = [v for v in vals if v is not None]
    return st.median(vals) if vals else None

summary = ["#### Ladder summary: per commit (one 50-row shift), main-thread ms, median over rounds; delta = vs the rung named in the last column\n",
           "| workload | rung | script | delta script | style+layout+paint | busy total | delta busy | frames >8.3 ms (fast scroll, of 100) | frames >8.3 ms (slow scroll, of 100) | delta vs |",
           "|---|---|---|---|---|---|---|---|---|---|"]
chain = [("r1-static", None), ("r1-dom", "r1-static"), ("r2", "r1-dom"), ("r3", "r2"), ("r1-html", "r1-dom"), ("r0", "r1-dom")]
for w in ["syn7", "syn18", "corpus", "corpus-rust"]:
    for rung, below in chain:
        key = (rung, w)
        if key not in by:
            continue
        script = mval(key, lambda r: r["trace"]["script"]["median"])
        scp = sum(mval(key, lambda r, k=k: r["trace"][k]["median"]) for k in ["style", "layout", "paint"])
        busy = mval(key, lambda r: r["trace"]["busy"]["median"])
        ref = (below, w) if below else None
        ds = db = "-"
        if ref and ref in by:
            ds = f"{script - mval(ref, lambda r: r['trace']['script']['median']):+.2f}"
            db = f"{busy - mval(ref, lambda r: r['trace']['busy']['median']):+.2f}"
        fast = mval(key, lambda r: r["scroll_fast"]["busyOver8_3"])
        slow = mval(key, lambda r: r["scroll_slow"]["busyOver8_3"])
        summary.append(f"| {w} | {rung} | {script:.2f} | {ds} | {scp:.2f} | {busy:.2f} | {db} | {fast:.0f} | {slow:.0f} | {below or '-'} |")
print("\n".join(summary))
print()
workloads = sorted({w for _, w in by}, key=lambda w: ["syn7", "syn18", "corpus", "corpus-rust"].index(w))
out = []
for w in workloads:
    out.append(f"\n#### Workload `{w}`: per commit (one 50-row chunk shift), main-thread ms from the Chromium trace, median over rounds (min-max of round medians)\n")
    out.append("| rung | rounds | commits/round | script (JS+wasm+DOM calls) | gc | style recalc | layout | paint (PrePaint/Paint/Layerize/Commit) | other in tasks | busy total | wall input->window (ms, frame-quantized) |")
    out.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for rung in order:
        key = (rung, w)
        if key not in by:
            continue
        g = lambda k: (lambda r: med(r, "trace", k, "median"))
        out.append("| " + " | ".join([
            rung, str(rounds(key)), str(by[key][0]["trace"]["commits"]),
            cell(key, g("script")), cell(key, g("gc")), cell(key, g("style")), cell(key, g("layout")),
            cell(key, g("paint")), cell(key, g("other")), cell(key, g("busy")),
            cell(key, lambda r: med(r, "wallToWindowMs", "median"), 1),
        ]) + " |")
    out.append(f"\np95 over commits (median over rounds of the per-round p95): ")
    out.append("")
    out.append("| rung | script p95 | style p95 | layout p95 | paint p95 | busy p95 |")
    out.append("|---|---|---|---|---|---|")
    for rung in order:
        key = (rung, w)
        if key not in by:
            continue
        p = lambda k: (lambda r: med(r, "trace", k, "p95"))
        out.append("| " + " | ".join([rung, cell(key, p("script")), cell(key, p("style")), cell(key, p("layout")), cell(key, p("paint")), cell(key, p("busy"))]) + " |")
    out.append(f"\n#### Workload `{w}`: continuous scroll, {100} frames, busy = main-thread task time per frame (trace)\n")
    out.append("| rung | px/frame | frames busy >8.3 ms | frames busy >16.7 ms | busy median | busy p95 | rAF intervals >16.7 (60 Hz vsync misses) | longtasks (n) | script | style | layout | paint |")
    out.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for rung in order:
        key = (rung, w)
        if key not in by:
            continue
        for name in ["fast", "slow"]:
            s = lambda *p: (lambda r: med(r, f"scroll_{name}", *p))
            out.append("| " + " | ".join([
                rung, "1000" if name == "fast" else "200",
                cell(key, s("busyOver8_3"), 0), cell(key, s("busyOver16_7"), 0),
                cell(key, s("busyMs", "median")), cell(key, s("busyMs", "p95")),
                cell(key, s("rafOver16_7"), 0), cell(key, lambda r: med(r, f"scroll_{name}", "longtasks", "n"), 0),
                cell(key, s("busyByClassMedian", "script")), cell(key, s("busyByClassMedian", "style")),
                cell(key, s("busyByClassMedian", "layout")), cell(key, s("busyByClassMedian", "paint")),
            ]) + " |")
    out.append("\n#### Workload `%s`: DOM size and mutations\n" % w)
    out.append("| rung | window DOM nodes / elements | MutationObserver records, added, removed (one commit) | CPU-profile categories, ms per commit |")
    out.append("|---|---|---|---|")
    for rung in order:
        key = (rung, w)
        if key not in by:
            continue
        r = by[key][-1]
        cats = r.get("cpuProfilePerCommitMs", {}).get("categories", {})
        cats = ", ".join(f"{k.split(' ')[0]} {v}" for k, v in cats.items())
        mo = r["mutationsOneCommit"]
        out.append(f"| {rung} | {r['census']['nodes']} / {r['census']['elements']} | {mo['records']}, {mo['added']}, {mo['removed']} | {cats} |")

out.append("\n#### Rung-specific timers (ms per commit, median over rounds)\n")
out.append("| rung | workload | timers |")
out.append("|---|---|---|")
for (rung, w), rs in sorted(by.items()):
    r = rs[0]
    if "r1PhasesMs" in r:
        k = (rung, w)
        out.append(f"| {rung} | {w} | JS total {cell(k, lambda r: med(r,'r1PhasesMs','total','median'))}, build {cell(k, lambda r: med(r,'r1PhasesMs','build','median'))}, attach {cell(k, lambda r: med(r,'r1PhasesMs','attach','median'))} |")
    if rung == "r0":
        k = (rung, w)
        vals = [st.median(r["rungTimers"]) for r in rs if r.get("rungTimers")]
        out.append(f"| r0 | {w} | `file.render` calls per commit window: median of per-round median {st.median(vals):.2f} ({min(vals):.2f}-{max(vals):.2f}), render calls per commit ~{len(rs[0]['rungTimers'])/rs[0]['commits']:.1f} |")
    if rung == "r3":
        k = (rung, w)
        g = lambda *p: cell(k, lambda r: med(r, *p))
        out.append(f"| r3 | {w} | guest: parse+handler {g('guestPhasesMs','parseAndHandler','median')}, VDOM diff {g('guestPhasesMs','vdomDiff','median')}, take {g('guestPhasesMs','batchTake','median')}, apply call {g('guestPhasesMs','applyCallIncludingBridge','median')}; host: dispatch {g('hostTimingMs','dispatchScroll','median')}, apply {g('hostTimingMs','apply','median')}, render {g('hostTimingMs','render','median')} |")

out.append("\n#### Host load (uptime) per run\n")
out.append("| rung | workload | round | load at start (1/5/15 min) | load at end | note |")
out.append("|---|---|---|---|---|---|")
for r in rows:
    ls, le = r["loadStart"], r["loadEnd"]
    noisy = "NOISY (>10)" if max(ls[0], le[0]) > 10 else ""
    out.append(f"| {r['rung']} | {r['workload']} | {r['round']} | {ls} | {le} | {noisy} |")
print("\n".join(out))
