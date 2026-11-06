#!/usr/bin/env python3
"""Compact markdown tables for docs/scratch/pierre-spikes.md from results.jsonl (runs with load <= 10
at both ends). Cells are the median over rounds of the per-round median; [lo-hi] is the spread of the
round medians."""
import json, sys, statistics as st, collections

rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
rows = [r for r in rows if max(r["loadStart"][0], r["loadEnd"][0]) <= 10]
by = collections.defaultdict(list)
for r in rows:
    by[(r["rung"], r["workload"])].append(r)
order = ["r1-static", "r1-dom", "r1-html", "r2", "r3", "r0"]
label = {"r1-static": "R1s static control", "r1-dom": "R1 vanilla createElement", "r1-html": "R1 vanilla innerHTML",
         "r2": "R2 dioxus-web", "r3": "R3 Clay stack", "r0": "R0 real Pierre"}

def m(key, f):
    v = [f(r) for r in by[key]]
    return st.median(v), min(v), max(v), len(v)

def cell(key, f, d=1, spread=False):
    med, lo, hi, n = m(key, f)
    return f"{med:.{d}f}" + (f" [{lo:.{d}f}-{hi:.{d}f}]" if spread else "")

for w in ["syn7", "syn18", "corpus", "corpus-rust"]:
    print(f"\n**`{w}`: per commit, ms (trace; the window is the commit's three frames)**\n")
    print("| rung | rounds | script | style | layout | paint | other | busy total [round spread] | busy p95 | wall input->window |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for rung in order:
        k = (rung, w)
        if k not in by:
            continue
        t = lambda c, q="median": (lambda r: r["trace"][c][q])
        print(f"| {label[rung]} | {len(by[k])} | {cell(k, t('script'))} | {cell(k, t('style'))} | {cell(k, t('layout'))} | {cell(k, t('paint'))} | {cell(k, t('other'))} | {cell(k, t('busy'), 1, True)} | {cell(k, t('busy','p95'))} | {cell(k, lambda r: r['wallToWindowMs']['median'])} |")
    print(f"\n**`{w}`: continuous scroll, 100 frames; frames whose main-thread busy time exceeds the budget**\n")
    print("| rung | fast (1000 px/frame): >8.3 / >16.7 ms | fast busy median / p95 | slow (200 px/frame, a commit every 5th frame): >8.3 / >16.7 ms | slow busy p95 |")
    print("|---|---|---|---|---|")
    for rung in order:
        k = (rung, w)
        if k not in by:
            continue
        s = lambda n, c, q=None: (lambda r: r[f"scroll_{n}"][c] if q is None else r[f"scroll_{n}"][c][q])
        print(f"| {label[rung]} | {cell(k, s('fast','busyOver8_3'),0,True)} / {cell(k, s('fast','busyOver16_7'),0,True)} | {cell(k, s('fast','busyMs','median'))} / {cell(k, s('fast','busyMs','p95'))} | {cell(k, s('slow','busyOver8_3'),0,True)} / {cell(k, s('slow','busyOver16_7'),0,True)} | {cell(k, s('slow','busyMs','p95'))} |")
