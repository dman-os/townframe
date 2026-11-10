#!/usr/bin/env python3
"""W1 stage tables from w1.jsonl: python3 -I w1_tables.py <w1.jsonl>"""
import bisect, json, sys, statistics
from collections import defaultdict

def q(v, p):
    s = sorted(v)
    return s[min(len(s) - 1, int(p * len(s)))] if s else None

def row(name, v):
    if not v:
        return f"| {name} | 0 | - | - | - |"
    return f"| {name} | {len(v)} | {q(v,.5):.3f} | {q(v,.95):.3f} | {max(v):.3f} |"

cells = defaultdict(lambda: defaultdict(list))
counts = defaultdict(list)
for line in open(sys.argv[1]):
    r = json.loads(line)
    key = f"{r['profile']} {r['mode']}-{r['speed']}"
    T = r["timing"]
    ins = [x[1] for x in T if x[0] == "listener-in"]
    encs = [x[1] for x in T if x[0] == "encoded"]
    disp = [x for x in T if x[0] == "dispatch:scroll"]
    apps = [x for x in T if x[0] == "apply"]
    hrs = [x for x in T if x[0] == "host-render"]
    # phases[0] is the setup's draining click (it set the phases text); the rest follow the
    # scroll dispatches in order. Check: the dispatches with mutations must be the same ones.
    native, frames, muts, phases = r["native"], r["frames"], r["muts"], r["phases"][1:]
    with_apply = [i for i, dp in enumerate(disp) if any(dp[1] <= a[1] <= dp[2] for a in apps)]
    with_muts = [i for i, p in enumerate(phases) if p[4] > 0]
    assert with_apply == [i for i in with_muts if i < len(disp)], (key, with_apply[:5], with_muts[:5])
    counts[key].append((len(native), len(ins), len(disp), len(phases), len(apps), len(muts)))
    # events: one listener-in per native scroll event (scroll listeners only: nothing else
    # dispatches during a run).
    n = min(len(ins), len(disp), len(phases), len(native))
    d = cells[key]
    fi = lambda t: bisect.bisect_left(frames, t)
    for i in range(n):
        t_in, dp, ph = ins[i], disp[i], phases[i]
        t_enc = next((e for e in encs if e >= t_in), t_in)
        guest = ph[0] + ph[1] + ph[2] + ph[3]
        d["browser event -> host listener"].append(t_in - native[i])
        d["encode data_json"].append(t_enc - t_in)
        d["host pre-dispatch (stale check)"].append(dp[1] - t_enc)
        d["dispatch call total (host-measured)"].append(dp[2] - dp[1])
        d["guest: parse + handler"].append(ph[0])
        d["guest: VirtualDom diff"].append(ph[1])
        d["guest: batch encode (take)"].append(ph[2])
        d["guest total (handler..apply call)"].append(guest)
        d["boundary: dispatch call - guest-measured"].append(dp[2] - dp[1] - guest)
        ap = next((a for a in apps if dp[1] <= a[1] <= dp[2]), None)
        if ap is None:
            d["_no-response events"].append(1)
            continue
        d["_response events"].append(1)
        for name, v in (("browser event -> host listener", t_in - native[i]), ("encode data_json", t_enc - t_in),
                        ("host pre-dispatch (stale check)", dp[1] - t_enc), ("dispatch call total (host-measured)", dp[2] - dp[1]),
                        ("guest: parse + handler", ph[0]), ("guest: VirtualDom diff", ph[1]), ("guest: batch encode (take)", ph[2]),
                        ("guest total (handler..apply call)", guest)):
            d["R " + name].append(v)
        d["R mutations in the batch"].append(ph[4])
        d["guest apply call (host tree update, host-measured)"].append(ap[2] - ap[1])
        entry = ap[1] - (ph[0] + ph[1] + ph[2])
        d["boundary in: dispatch call -> guest handler entered"].append(entry - dp[1])
        d["after apply: return to host"].append(dp[2] - ap[2])
        hr = next((h for h in hrs if h[1] >= dp[2] - 0.05), None)
        if hr:
            d["dispatch return -> host render starts"].append(hr[1] - dp[2])
            d["host render (projection + diff + DOM writes)"].append(hr[2] - hr[1])
        m_i = bisect.bisect_left(muts, ap[2])
        if m_i < len(muts):
            m = muts[m_i]
            d["host render end -> DOM mutation observed"].append(m - (hr[2] if hr else ap[2]))
            fe = frames[fi(m)] if fi(m) < len(frames) else None
            d["native event -> DOM mutation observed"].append(m - native[i])
            if fe is not None:
                d["DOM mutation -> end of its frame"].append(fe - m)
                d["native event -> end of painted frame"].append(fe - native[i])
                d["frames late (frame of mutation - frame of scroll event)"].append(fi(m) - fi(native[i]))
for key, d in cells.items():
    print(f"\n### {key}  (runs: {len(counts[key])}; per-run counts native/listener/dispatch/phases/apply/muts: {counts[key][:5]})")
    print(f"events with a guest response: {sum(d['_response events'])}, without: {sum(d['_no-response events'])}\n")
    print("| stage (ms) | n | median | p95 | max |\n|---|---|---|---|---|")
    for name, v in d.items():
        if name.startswith("_"): continue
        print(row(name, v))
    fl = d.get("frames late (frame of mutation - frame of scroll event)", [])
    if fl:
        from collections import Counter
        print("\nframes late histogram:", dict(sorted(Counter(int(x) for x in fl).items())))
