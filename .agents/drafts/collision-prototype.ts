// Exploratory allocator, NOT the specified collision algorithm. Kept outside docs/
// because docs/README.md forbids LLM edits there. Run: deno run .agents/drafts/collision-prototype.ts
// Models files and implicit parent directories; not lens-created directories,
// platform-specific equivalence, removal, or allocation across partial arrivals.
type Claim = { id: string; path: string };

function allocate(claims: Claim[]): Map<string, string> {
  const bound = new Map<string, string>();
  const files = new Set<string>();
  const directories = new Set<string>();
  const occupied = (path: string) => files.has(path) || directories.has(path);
  const parent = (path: string) => path.slice(0, path.lastIndexOf("/")) || "/";
  const suffixed = (path: string, suffix: string) => {
    const slash = path.lastIndexOf("/");
    const dot = path.lastIndexOf(".");
    const split = dot > slash + 1 ? dot : path.length;
    return path.slice(0, split) + suffix + path.slice(split);
  };
  for (const { id, path } of claims) {
    if (bound.has(id)) throw new Error(`duplicate id: ${id}`);
    if (
      !path.startsWith("/") ||
      path === "/" ||
      path
        .split("/")
        .slice(1)
        .some((part) => !part || part === "." || part === "..")
    ) {
      throw new Error(`invalid path: ${path}`);
    }
    // Preserve an existing file by spilling the *newcomer's* descendants.
    let desired = path;
    const segments = path.slice(1).split("/");
    let prefix = "";
    for (let i = 0; i < segments.length - 1; i++) {
      prefix += "/" + segments[i];
      if (!files.has(prefix)) continue;
      let spill = prefix + ".d";
      while (occupied(spill)) spill += ".d";
      desired = spill + path.slice(prefix.length);
      break;
    }
    let candidate = desired;
    let counter = 0;
    while (
      occupied(candidate) ||
      [...files].some((file) => candidate.startsWith(file + "/"))
    ) {
      candidate = suffixed(desired, `~${id}${counter ? `-${counter}` : ""}`);
      counter++;
    }
    for (
      let cursor = parent(candidate);
      cursor !== "/";
      cursor = parent(cursor)
    ) {
      if (files.has(cursor)) throw new Error(`unhandled ancestor: ${cursor}`);
      directories.add(cursor);
    }
    files.add(candidate);
    bound.set(id, candidate);
  }
  return bound;
}

const cases: Array<[Claim[], Record<string, string>]> = [
  [
    [
      { id: "A", path: "/x.png" },
      { id: "B", path: "/x.png" },
    ],
    // ~B.png? was that decidedi n the original FDR?
    // we don't have human readable doc ids!
    { A: "/x.png", B: "/x~B.png" },
  ],
  [
    [
      { id: "A", path: "/a" },
      { id: "D", path: "/a.d" },
    ],
    { A: "/a", D: "/a.d" },
  ],
  [
    [
      { id: "A", path: "/a" },
      { id: "C", path: "/a/b" },
    ],
    { A: "/a", C: "/a.d/b" },
  ],
  [
    // THIS IS VERY STUPID
    [
      { id: "A", path: "/a" },
      { id: "D", path: "/a.d" },
      { id: "C", path: "/a/b" },
    ],
    { A: "/a", D: "/a.d", C: "/a.d.d/b" },
  ],
];
for (const [claims, expected] of cases) {
  const got = Object.fromEntries(allocate(claims));
  if (JSON.stringify(got) !== JSON.stringify(expected))
    throw new Error(
      `expected ${JSON.stringify(expected)}; got ${JSON.stringify(got)}`,
    );
  console.log(JSON.stringify(got));
}
