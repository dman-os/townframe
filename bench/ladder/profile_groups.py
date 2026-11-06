#!/usr/bin/env python3
"""Groups the V8 CPU-profile self times (ms per commit, `cpuProfilePerCommitMs.top`) of r2 / r3 by
what the function is, averaged over rounds. The profiler (100 us sampling) inflates wasm time, so
read the shares, not the absolute ms; the trace table has the unperturbed totals."""
import json, sys, collections, statistics as st

rows = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]

def group(key: str) -> str:
    kind, _, rest = key.partition(":")
    name = rest
    if kind == "program":
        return "program (browser native: style/layout/paint/idle)"
    if kind == "gc":
        return "gc"
    if kind == "native":
        return "DOM natives (appendChild, setAttribute, remove, cloneNode, ...)"
    if kind == "js":
        return "js glue"
    # wasm
    if "micro_pierre_spike_scroll.wasm" in name or "@fac36596" in name or name.startswith("wasm-function["):
        return "guest wasm (micro: app + dioxus-core diff + guest allocator)"
    if "wcmp" in name or "canonical_abi" in name or "clay_rt_wcmp" in name:
        return "wcmp runtime (canonical ABI, host-call plumbing)"
    if "dlmalloc" in name:
        return "host allocator (dlmalloc, dayframe_web_bg.wasm)"
    if "clay_surface" in name or "clay_dioxus" in name or "clay_host" in name:
        return "Clay host (SurfaceTree render/apply, clay_dioxus)"
    if "dioxus_core" in name or "dioxus_web" in name:
        return "host Dioxus (VirtualDom diff of the surface tree, dioxus-web)"
    return "other wasm"

for rung in ["r2", "r3"]:
    for w in ["syn7", "syn18", "corpus"]:
        rs = [r for r in rows if r["rung"] == rung and r["workload"] == w and len(r.get("cpuProfilePerCommitMs", {}).get("top", [])) > 100]  # rounds taken after the top list was widened to 250
        if not rs:
            continue
        acc = collections.defaultdict(list)
        for r in rs:
            g = collections.defaultdict(float)
            for k, v in r["cpuProfilePerCommitMs"]["top"]:
                g[group(k)] += v
            for k, v in g.items():
                acc[k].append(v)
        total = sum(st.median(v) for k, v in acc.items() if not k.startswith("program"))
        print(f"\n{rung} {w} ({len(rs)} rounds): V8 profile self ms per commit, excluding 'program'; total {total:.2f}")
        for k, v in sorted(acc.items(), key=lambda kv: -st.median(kv[1])):
            m = st.median(v)
            share = "" if k.startswith("program") else f" ({100*m/total:.0f}%)"
            print(f"  {k}: {m:.2f}{share}  [{min(v):.2f}-{max(v):.2f}]")
