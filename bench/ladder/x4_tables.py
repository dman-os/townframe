#!/usr/bin/env python3
"""X4 tables from results.jsonl (runs with load <= 10 at both ends). Cells are the median over
rounds of the per-round median; [lo-hi] is the spread of the round medians."""
import json, sys, statistics as st, collections

rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
total = len(rows)
rows = [r for r in rows if max(r["loadStart"][0], r["loadEnd"][0]) <= 10]
print(f"<!-- {len(rows)} of {total} runs kept (load <= 10 at both ends) -->")
by = collections.defaultdict(list)
for r in rows:
    by[(r["rung"], r["workload"])].append(r)
order = ["r1-dom", "r2", "r3", "r3a", "r3b"]
label = {"r1-dom": "R1 vanilla createElement", "r2": "R2 dioxus-web", "r3": "R3 Clay stack (host tree + Dioxus)",
         "r3a": "R3a JS receiver", "r3b": "R3b Rust receiver"}

def vals(key, f):
    out = []
    for r in by[key]:
        try:
            v = f(r)
        except (KeyError, TypeError):
            v = None
        if v is not None:
            out.append(v)
    return out

def cell(key, f, d=1, spread=False):
    v = vals(key, f)
    if not v:
        return "-"
    return f"{st.median(v):.{d}f}" + (f" [{min(v):.{d}f}-{max(v):.{d}f}]" if spread else "")

def med(key, f):
    v = vals(key, f)
    return st.median(v) if v else None

print("\n**Ladder summary (per commit, ms; delta vs the rung in the last column)**\n")
print("| workload | rung | rounds | script | delta script | style+layout+paint | busy total | delta busy | frames >8.3 ms (fast, of 100) | frames >8.3 ms (slow, of 100) | delta vs |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
chain = {"r1-dom": None, "r2": "r1-dom", "r3": "r2", "r3a": "r3", "r3b": "r3"}
for w in ["syn7", "syn18", "corpus"]:
    for rung in order:
        k = (rung, w)
        if k not in by:
            continue
        script = med(k, lambda r: r["trace"]["script"]["median"])
        scp = sum(med(k, lambda r, c=c: r["trace"][c]["median"]) for c in ["style", "layout", "paint"])
        busy = med(k, lambda r: r["trace"]["busy"]["median"])
        below = chain[rung]
        ds = db = "-"
        if below and (below, w) in by:
            ds = f"{script - med((below, w), lambda r: r['trace']['script']['median']):+.2f}"
            db = f"{busy - med((below, w), lambda r: r['trace']['busy']['median']):+.2f}"
        print(f"| {w} | {rung} | {len(by[k])} | {script:.2f} | {ds} | {scp:.2f} | {busy:.2f} | {db} | {cell(k, lambda r: r['scroll_fast']['busyOver8_3'], 0)} | {cell(k, lambda r: r['scroll_slow']['busyOver8_3'], 0)} | {below or '-'} |")

for w in ["syn7", "syn18", "corpus"]:
    print(f"\n**`{w}`: per commit, ms (trace)**\n")
    print("| rung | rounds | script | style | layout | paint | other | busy total [round spread] | busy p95 | wall input->window |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for rung in order:
        k = (rung, w)
        if k not in by:
            continue
        t = lambda c, q="median": (lambda r: r["trace"][c][q])
        print(f"| {label[rung]} | {len(by[k])} | {cell(k, t('script'))} | {cell(k, t('style'))} | {cell(k, t('layout'))} | {cell(k, t('paint'))} | {cell(k, t('other'))} | {cell(k, t('busy'), 1, True)} | {cell(k, t('busy','p95'))} | {cell(k, lambda r: r['wallToWindowMs']['median'])} |")
    print(f"\n**`{w}`: continuous scroll, 100 frames; frames whose main-thread busy time exceeds the budget**\n")
    print("| rung | fast (1000 px/frame): >8.3 / >16.7 ms | fast busy median / p95 | slow (200 px/frame): >8.3 / >16.7 ms | slow busy p95 |")
    print("|---|---|---|---|---|")
    for rung in order:
        k = (rung, w)
        if k not in by:
            continue
        s = lambda n, c, q=None: (lambda r: r[f"scroll_{n}"][c] if q is None else r[f"scroll_{n}"][c][q])
        print(f"| {label[rung]} | {cell(k, s('fast','busyOver8_3'),0,True)} / {cell(k, s('fast','busyOver16_7'),0,True)} | {cell(k, s('fast','busyMs','median'))} / {cell(k, s('fast','busyMs','p95'))} | {cell(k, s('slow','busyOver8_3'),0,True)} / {cell(k, s('slow','busyOver16_7'),0,True)} | {cell(k, s('slow','busyMs','p95'))} |")

print("\n**Where the script time of the three Clay rungs goes (ms per commit, median over rounds; guest timers 0.1 ms clock, host timers are `performance.now` deltas)**\n")
print("| workload | rung | guest VDOM diff | guest `surface.apply` call (wcmp crossing + lift + host sink) | host sink `apply` | of which receiver encode | of which receiver JS call / Rust loop | host render (measured: surface body -> effects) | trace top-level script: scroll EventDispatch | trace top-level script: all other tasks |")
print("|---|---|---|---|---|---|---|---|---|---|")
for w in ["syn7", "syn18", "corpus"]:
    for rung in ["r3", "r3a", "r3b"]:
        k = (rung, w)
        if k not in by:
            continue
        g = lambda *p: (lambda r: r[p[0]][p[1]]["median"] if len(p) == 2 else r[p[0]][p[1]][p[2]])
        hostrender = cell(k, lambda r: r["hostTimingMs"]["hostRender"]["median"], 2)
        disp = lambda r: r["trace"]["topScript"].get("EventDispatch:scroll", {"median": 0})["median"]
        total_top = lambda r: sum(v["median"] for v in r["trace"]["topScript"].values())
        other = lambda r: total_top(r) - disp(r)
        print(f"| {w} | {rung} | {cell(k, lambda r: r['guestPhasesMs']['vdomDiff']['median'], 2)} | {cell(k, lambda r: r['guestPhasesMs']['applyCallIncludingBridge']['median'], 2)} | {cell(k, lambda r: r['hostTimingMs']['apply']['median'], 2)} | {cell(k, lambda r: r['hostTimingMs']['directEncode']['median'], 2)} | {cell(k, lambda r: (r['hostTimingMs']['directJs'] if rung == 'r3a' else r['hostTimingMs']['directRust'])['median'], 2)} | {hostrender} | {cell(k, disp, 2)} | {cell(k, other, 2)} |")

print("\n**Trace: top-level script events per commit (inclusive ms, median over rounds of the per-round median; sum of events with no script ancestor)**\n")
names = sorted({n for r in rows if r["rung"] in ("r3", "r3a", "r3b") for n in r["trace"].get("topScript", {})})
print("| workload | rung | " + " | ".join(names) + " |")
print("|---|---|" + "---|" * len(names))
for w in ["syn7", "syn18", "corpus"]:
    for rung in ["r3", "r3a", "r3b"]:
        k = (rung, w)
        if k not in by:
            continue
        print(f"| {w} | {rung} | " + " | ".join(cell(k, lambda r, n=n: r["trace"]["topScript"].get(n, {"median": 0})["median"], 2) for n in names) + " |")

print("\n**DOM census and mutations of one commit**\n")
print("| workload | rung | window nodes / elements | MutationObserver records, added, removed |")
print("|---|---|---|---|")
for w in ["syn7", "syn18", "corpus"]:
    for rung in order:
        k = (rung, w)
        if k not in by:
            continue
        r = by[k][-1]
        mo = r["mutationsOneCommit"]
        print(f"| {w} | {rung} | {r['census']['nodes']} / {r['census']['elements']} | {mo['records']}, {mo['added']}, {mo['removed']} |")
