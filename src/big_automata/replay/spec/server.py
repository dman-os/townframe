#!/usr/bin/env python3
"""Export `server.qnt`'s whole reachable state graph as `server.graph.json`.

    python3 server.py regen        # rewrite server.graph.json (run inside `nix develop`)
    python3 server.py regen --check  # fail if the checked-in fixture is stale

How: `quint compile --target=tlaplus` turns the model into TLA+; TLC (bundled in Apalache's
jar, which ships with quint) enumerates it with `VIEW srv`, so a node is the server state alone;
a wrapper module `PrintT`s every (state, event, answer, next state) it evaluates; this script
parses those lines. TLC's own `-dump dot` is not enough: it merges edges between the same two
states and does not carry the event or the answer.
"""

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIXTURE = HERE / "server.graph.json"

WRAPPER = """---- MODULE ServerEdges ----
EXTENDS server, TLC
View == srv
EdgeNext == q_step /\\ PrintT(<<"EDGE", srv, lastEvent', lastAnswer', srv'>>)
====
"""
CFG = "INIT q_init\nNEXT EdgeNext\nVIEW View\n"


def find_jar() -> Path:
    if jar := os.environ.get("APALACHE_JAR"):
        return Path(jar)
    quint = Path(subprocess.check_output(["which", "quint"], text=True).strip()).resolve()
    jars = sorted(quint.parent.parent.glob("share/quint/apalache-dist-*/apalache/lib/apalache.jar"))
    assert jars, "no apalache.jar next to quint; set APALACHE_JAR"
    return jars[-1]


def tlc_edges() -> tuple[list[str], str]:
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        tla = subprocess.run(
            ["quint", "compile", "--target=tlaplus", "--main=server", str(HERE / "server.qnt")],
            check=True, capture_output=True, text=True,
        ).stdout
        # Apalache's `:=` assignment is `=` for TLC.
        (tmp / "server.tla").write_text(tla.replace(":=", "="))
        (tmp / "ServerEdges.tla").write_text(WRAPPER)
        (tmp / "ServerEdges.cfg").write_text(CFG)
        out = subprocess.run(
            ["java", "-Xmx8G", "-Xss515m", "-cp", str(find_jar()), "tlc2.TLC", "-workers", "1",
             "-metadir", str(tmp / "meta"), "ServerEdges.tla"],
            cwd=tmp, check=True, capture_output=True, text=True,
        ).stdout
    assert '<< "EDGE",' in out, "TLC printed no edge:\n" + out[-3000:]
    # TLC pretty-prints each value over several lines; an edge starts at a line-initial `<<`.
    chunks = re.split(r'(?m)^(?=<< "EDGE",)', out)
    return [c for c in chunks if c.startswith('<< "EDGE",')], out


# ---- TLC value syntax -> Python ----
TOKEN = re.compile(r'\s*(<<|>>|\|->|:>|@@|"[^"]*"|-?\d+|[A-Za-z_]\w*|[\[\]{}(),])')


def parse(text: str):
    toks = TOKEN.findall(text)
    pos = 0

    def peek():
        return toks[pos] if pos < len(toks) else None

    def take(expect=None):
        nonlocal pos
        t = toks[pos]
        pos += 1
        assert expect is None or t == expect, (expect, t)
        return t

    def seq(close):
        items = []
        while peek() != close:
            items.append(value())
            if peek() == ",":
                take()
        take(close)
        return items

    def value():
        t = take()
        if t == "<<":
            return ("seq", seq(">>"))
        if t == "{":
            return ("set", seq("}"))
        if t == "[":
            rec = {}
            while peek() != "]":
                k = take()
                take("|->")
                rec[k] = value()
                if peek() == ",":
                    take()
            take("]")
            return ("rec", rec)
        if t == "(":
            fn = {}
            while True:
                k = value()
                take(":>")
                fn[k] = value()
                if peek() == "@@":
                    take()
                    continue
                break
            take(")")
            return ("fn", fn)
        if t.startswith('"'):
            return t[1:-1]
        if t in ("TRUE", "FALSE"):
            return t == "TRUE"
        return int(t)

    # The chunk of the last edge runs on into TLC's summary; the value ends at its own `>>`.
    return value()


def plain(v):
    """Python data with sets as sorted lists, seqs as lists, functions as sorted [k, v] pairs."""
    if isinstance(v, tuple):
        kind, body = v
        if kind == "seq":
            return [plain(x) for x in body]
        if kind == "set":
            return sorted((plain(x) for x in body), key=json.dumps)
        if kind == "rec":
            return {k: plain(x) for k, x in body.items()}
        if kind == "fn":
            return sorted(([plain(k), plain(x)] for k, x in body.items()), key=json.dumps)
    return v


def session(s):
    if not s["held"]:
        return None
    held = [[i, r] for i, r in s["targets"] if r != -1]
    return {"generation": s["generation"], "targets": held, "refused": s["refused"]}


def canon(x) -> str:
    return json.dumps(x, sort_keys=True, separators=(",", ":"))


def build() -> dict:
    edge_lines, log = tlc_edges()
    raw = []
    for line in edge_lines:
        _, src, event, answer, dst = plain(parse(line))
        sessions = lambda srv: [session(s) for _, s in srv]
        ev = {k: v for k, v in event.items() if k in ("kind", "session", "generation",
                                                      "additions", "removals", "admitted")}
        if ev["kind"] != "update":
            ev = {"kind": ev["kind"]} | ({"session": ev["session"]} if ev["kind"] == "close" else {})
        ans = None if answer["kind"] == "none" else answer
        raw.append((sessions(src), ev, ans, sessions(dst)))
    init = [None] * len(raw[0][0])
    nodes = sorted({canon(r[0]) for r in raw} | {canon(r[3]) for r in raw} | {canon(init)})
    nodes.remove(canon(init))
    nodes.insert(0, canon(init))
    events = sorted({canon(r[1]) for r in raw})
    answers = sorted({canon(r[2]) for r in raw})
    ni = {n: i for i, n in enumerate(nodes)}
    ei = {e: i for i, e in enumerate(events)}
    ai = {a: i for i, a in enumerate(answers)}
    rows = [[None] * len(events) for _ in nodes]
    for src, ev, ans, dst in raw:
        cell = rows[ni[canon(src)]]
        slot = ei[canon(ev)]
        assert cell[slot] is None or cell[slot] == (ai[canon(ans)], ni[canon(dst)]), "nondeterministic edge"
        cell[slot] = (ai[canon(ans)], ni[canon(dst)])
    assert all(c is not None for row in rows for c in row), "some (state, event) pair has no edge"
    m = re.search(r"(\d+) states generated, (\d+) distinct states found", log)
    return {
        "generated_by": "server.py regen; see server.md",
        "tlc": {"states_generated": int(m[1]), "distinct_states": int(m[2])},
        "nodes": [json.loads(n) for n in nodes],
        "events": [json.loads(e) for e in events],
        "answers": [json.loads(a) for a in answers],
        # next[node] is flat: for event index e, [answer index, next node] at 2e, 2e+1.
        "next": [[x for c in row for x in c] for row in rows],
    }


def dump(graph: dict) -> str:
    def line(x):
        return json.dumps(x, separators=(",", ":"))
    parts = [f'  "{k}": {line(graph[k])}' for k in ("generated_by", "tlc")]
    for k in ("nodes", "events", "answers", "next"):
        parts.append(f'  "{k}": [\n' + ",\n".join("    " + line(x) for x in graph[k]) + "\n  ]")
    return "{\n" + ",\n".join(parts) + "\n}\n"


def main():
    args = sys.argv[1:]
    if not args or args[0] != "regen":
        sys.exit(__doc__)
    graph = build()
    text = dump(graph)
    n_edges = len(graph["nodes"]) * len(graph["events"])
    print(f"{len(graph['nodes'])} nodes, {len(graph['events'])} events, {n_edges} edges, "
          f"{len(text) / 1e6:.2f} MB; TLC: {graph['tlc']}", file=sys.stderr)
    if "--check" in args:
        if FIXTURE.read_text() != text:
            sys.exit("server.graph.json is stale: run `python3 server.py regen`")
    else:
        FIXTURE.write_text(text)


main()
