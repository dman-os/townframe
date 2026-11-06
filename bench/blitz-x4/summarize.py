#!/usr/bin/env python3
"""Medians per mode over every commit of every rep from blitz_x4's stdout (one file per workload)."""
import sys, statistics as st
for path in sys.argv[1:]:
    rows = [l.split() for l in open(path)]
    print(f"{path}")
    for mode in ("direct", "today"):
        rs = [r for r in rows if r[0] == mode]
        col = lambda i: [float(r[i]) for r in rs]
        tree, diff, apply, resolve = (st.median(col(i)) for i in (4, 5, 6, 7))
        recv = tree + diff + apply
        tot = [float(r[4]) + float(r[5]) + float(r[6]) + float(r[7]) for r in rs]
        p95 = sorted(tot)[int(len(tot) * 0.95)]
        print(f"  {mode:7s} n={len(rs):4d} ops={rs[0][3]} tree={tree:.3f} diff={diff:.3f} apply={apply:.3f} receive_total={recv:.3f} resolve={resolve:.3f} commit_total_median={st.median(tot):.3f} p95={p95:.3f}")
