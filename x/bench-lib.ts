// The benchmark record format and the logic on it: parsing what each bench tier prints, the
// comparison between two commits, and the noise thresholds. No I/O here, so it is unit tested
// (`x/bench-lib.test.ts`); `x/bench.ts` is the command line around it and the CI gate runs the
// same `compare`.
//
// One record is one measurement, one JSON object per line:
//
//   bench     crate-qualified name, e.g. `big_automata::scaling::okv_write_n_batches,_one_tx`
//   metric    instructions | allocations | alloc_bytes | sim_time | wall_ns | rounds | ...
//   value     the number
//   unit      count | bytes | s | ns | ...
//   params    the knobs of the run (n, C, M, ...), strings
//   commit, dirty, machine, toolchain, profile, target, ts   where and when it was taken
//
// Tiers print `BENCH_RECORD {bench, metric, unit, value, params}` lines (the *partial* record);
// `x/bench.ts` completes them with the environment. Gungraun's own JSON is converted by
// `fromGungraunSummary`.

export type Target = "native" | "wasm-browser";

export interface Partial {
  bench: string;
  metric: string;
  value: number;
  unit: string;
  params: Record<string, string>;
}

export interface Env {
  commit: string;
  dirty: boolean;
  machine: string;
  toolchain: string;
  profile: string;
  target: Target;
  ts: string;
}

export type BenchRecord = Partial & Env;

/** What a tier prints for one measurement. */
export const RECORD_PREFIX = "BENCH_RECORD ";

/** Whether a larger value of the metric is better. Every metric recorded today is a cost. */
const HIGHER_IS_BETTER = new Set<string>([]);

/**
 * The relative change below which a step is noise, per metric. Counts are deterministic on one
 * machine and toolchain (the slack covers hash seeds and allocator growth), simulated time is
 * deterministic, wall time is not.
 */
export const NOISE: Record<string, number> = {
  instructions: 0.01,
  allocations: 0.01,
  alloc_bytes: 0.01,
  sim_time: 0.01,
  sim_time_high_p50: 0.01,
  sim_time_low_p50: 0.01,
  calls: 0.005,
  rounds: 0.005,
  wire_bytes_up: 0.005,
  wire_bytes_down: 0.005,
  wall_ns: 0.15,
};
const DEFAULT_NOISE = 0.05;

export const noiseOf = (metric: string): number =>
  NOISE[metric] ?? DEFAULT_NOISE;

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Validate a partial record read from a tier's output. Throws on a malformed one. */
export function parsePartial(value: unknown): Partial {
  if (!isObject(value)) {
    throw new Error(`a record is an object, got ${JSON.stringify(value)}`);
  }
  const { bench, metric, unit, params } = value;
  const measured = value.value;
  if (typeof bench !== "string" || bench === "") {
    throw new Error("record.bench must be a name");
  }
  if (typeof metric !== "string" || metric === "") {
    throw new Error("record.metric must be a name");
  }
  if (typeof unit !== "string") throw new Error("record.unit must be a string");
  if (typeof measured !== "number" || !Number.isFinite(measured)) {
    throw new Error(
      `record.value must be a finite number in ${bench} ${metric}`,
    );
  }
  const shaped: Record<string, string> = {};
  if (params !== undefined) {
    if (!isObject(params)) throw new Error("record.params must be an object");
    for (const [key, entry] of Object.entries(params)) {
      if (typeof entry !== "string") {
        throw new Error(`record.params.${key} must be a string`);
      }
      shaped[key] = entry;
    }
  }
  return { bench, metric, unit, value: measured, params: shaped };
}

/** Validate a complete record read back from history. */
export function parseRecord(value: unknown): BenchRecord {
  const partial = parsePartial(value);
  const env = value as Record<string, unknown>;
  for (
    const field of ["commit", "machine", "toolchain", "profile", "ts"] as const
  ) {
    if (typeof env[field] !== "string" || env[field] === "") {
      throw new Error(`record.${field} must be a non-empty string`);
    }
  }
  if (typeof env.dirty !== "boolean") {
    throw new Error("record.dirty must be a boolean");
  }
  if (env.target !== "native" && env.target !== "wasm-browser") {
    throw new Error("record.target must be native or wasm-browser");
  }
  return { ...partial, ...(env as unknown as Env) };
}

/** The partial records among the lines of a run's output; other lines are ignored. */
export function recordsFromOutput(output: string): Partial[] {
  const records: Partial[] = [];
  for (const line of output.split("\n")) {
    const at = line.indexOf(RECORD_PREFIX);
    if (at < 0) continue;
    records.push(
      parsePartial(JSON.parse(line.slice(at + RECORD_PREFIX.length))),
    );
  }
  return records;
}

export const withEnv = (partials: Partial[], env: Env): BenchRecord[] =>
  partials.map((partial) => ({ ...partial, ...env }));

/**
 * Gungraun's `summary.json` of one benchmark as records. Gungraun names the benchmark by module
 * path, function and id; its callgrind summary carries the event totals, of which the
 * instruction count (`Ir`) is the one this repo tracks.
 */
export function fromGungraunSummary(
  summary: unknown,
  crate: string,
): Partial[] {
  if (!isObject(summary)) throw new Error("a gungraun summary is an object");
  const { module_path: modulePath, function_name: fn, id } = summary as Record<
    string,
    unknown
  >;
  if (typeof modulePath !== "string" || typeof fn !== "string") {
    throw new Error("a gungraun summary names its module path and function");
  }
  const profiles = summary.profiles;
  if (!Array.isArray(profiles)) {
    throw new Error("a gungraun summary has profiles");
  }
  const records: Partial[] = [];
  for (const profile of profiles) {
    if (!isObject(profile) || profile.tool !== "Callgrind") continue;
    // profiles[].data.total.metrics.Ir.values.new, as gungraun 0.20 writes it (summary version 7)
    const total = path(
      profile,
      "data",
      "total",
      "metrics",
      "Ir",
      "values",
      "new",
    );
    if (typeof total !== "number") {
      throw new Error(`no Ir total in ${modulePath}::${fn}`);
    }
    const params: Record<string, string> = {};
    if (typeof id === "string") params.id = id;
    records.push({
      // the module path already ends in the function: `instr::layers::okv_write`
      bench: `${crate}::${modulePath}`,
      metric: "instructions",
      value: total,
      unit: "count",
      params,
    });
  }
  return records;
}

const DIVAN_UNITS: Record<string, number> = {
  ps: 1e-3,
  ns: 1,
  "µs": 1e3,
  us: 1e3,
  ms: 1e6,
  s: 1e9,
};

/**
 * The median times of a divan run, as `wall_ns` records. divan has no machine-readable output,
 * so this reads its tree: a function is a node, each of its arguments a child whose row ends
 * `fastest │ slowest │ median │ mean │ samples │ iters`. A function without arguments carries
 * its own row. `bench` is the bench target's name, which divan prints as the tree's root.
 */
export function fromDivanOutput(output: string, crate: string): Partial[] {
  const records: Partial[] = [];
  const names: string[] = [];
  // deno-lint-ignore no-control-regex
  const plain = output.replace(/\u001b\[[0-9;]*m/g, "");
  for (const line of plain.split("\n")) {
    const row = /^((?:[│ ]{3})*)(?:├─|╰─) (.*)$/.exec(line);
    if (!row) continue;
    const depth = row[1].length / 3;
    const cells = row[2].split("│").map((cell) => cell.trim());
    const [name, ...fastest] = cells[0].split(/\s{2,}/);
    names.length = depth;
    names[depth] = name;
    const median = /^([\d.]+)\s*(ps|ns|µs|us|ms|s)$/.exec(cells[2] ?? "");
    if (!median || fastest.length === 0) continue;
    const value = Number(median[1]) * DIVAN_UNITS[median[2]];
    // `fn` then its argument, or the function alone
    const [fn, arg] = names.length >= 2 && /^[\d_]+$/.test(name)
      ? [names[depth - 1], name]
      : [name, undefined];
    records.push({
      bench: `${crate}::wall::${fn}`,
      metric: "wall_ns",
      value,
      unit: "ns",
      params: arg === undefined ? {} : { n: arg.replaceAll("_", "") },
    });
  }
  return records;
}

/** The value at `keys` under nested objects, or undefined where the path breaks. */
const path = (value: unknown, ...keys: string[]): unknown =>
  keys.reduce((node, key) => isObject(node) ? node[key] : undefined, value);

// # Comparison

/** Records are the same measurement when their name, metric and parameters agree. */
export const keyOf = (record: Partial): string =>
  `${record.bench}\u0000${record.metric}\u0000${
    Object.entries(record.params).sort(([a], [b]) => a.localeCompare(b)).map((
      [k, v],
    ) => `${k}=${v}`)
      .join(",")
  }`;

export interface Basis {
  machine: string;
  toolchain: string;
  profile: string;
  target: Target;
}

export const basisOf = (record: BenchRecord): Basis => ({
  machine: record.machine,
  toolchain: record.toolchain,
  profile: record.profile,
  target: record.target,
});

export interface Change {
  key: string;
  bench: string;
  metric: string;
  params: Record<string, string>;
  unit: string;
  before: number;
  after: number;
  /** `(after - before) / before`; positive is a cost increase. */
  relative: number;
  /** The threshold this change was judged against. */
  noise: number;
  verdict: "regression" | "improvement" | "unchanged";
}

export interface Comparison {
  changes: Change[];
  /** Measurements in one side only. */
  onlyBefore: string[];
  onlyAfter: string[];
  /** Set when the two sides differ in machine, toolchain, profile or target and it was forced. */
  forcedAcross: string[];
}

export class CrossBasisError extends Error {}

/**
 * Compare the records of two commits. They must come from the same machine, toolchain, profile
 * and target: instruction counts and times do not transfer between them. `force` compares
 * anyway and reports the differences in `forcedAcross`, for the caller to label loudly.
 *
 * `thresholds` overrides the noise threshold per metric; a change beyond it is a regression
 * (cost up) or an improvement (cost down).
 */
export function compare(
  before: BenchRecord[],
  after: BenchRecord[],
  options: { force?: boolean; thresholds?: Record<string, number> } = {},
): Comparison {
  if (before.length === 0 || after.length === 0) {
    throw new CrossBasisError("one side of the comparison has no records");
  }
  const basisKey = (record: BenchRecord) => JSON.stringify(basisOf(record));
  const afterBases = new Set(after.map(basisKey));
  const sharedBases = new Set(
    before.map(basisKey).filter((basis) => afterBases.has(basis)),
  );
  const forced = sharedBases.size === 0;
  const forcedAcross: string[] = [];
  if (forced) {
    for (
      const field of ["machine", "toolchain", "profile", "target"] as const
    ) {
      for (const a of before) {
        if (a[field] !== after[0][field]) {
          forcedAcross.push(`${field}: ${a[field]} vs ${after[0][field]}`);
        }
      }
    }
    if (!options.force) {
      throw new CrossBasisError(
        `the two sides share no machine, toolchain, profile and target (${
          [...new Set(forcedAcross)].join("; ")
        }); pass --force-cross-machine to compare anyway`,
      );
    }
  }
  const latest = (records: BenchRecord[]) => {
    const byKey = new Map<string, BenchRecord>();
    for (const record of records) {
      if (forced || sharedBases.has(basisKey(record))) {
        byKey.set(
          forced ? keyOf(record) : `${basisKey(record)}\u0001${keyOf(record)}`,
          record,
        );
      }
    }
    return byKey;
  };
  const left = latest(before);
  const right = latest(after);
  const changes: Change[] = [];
  const onlyBefore: string[] = [];
  const onlyAfter: string[] = [];
  for (const [id, a] of left) {
    const b = right.get(id);
    if (!b) {
      onlyBefore.push(keyOf(a));
      continue;
    }
    const noise = options.thresholds?.[a.metric] ?? noiseOf(a.metric);
    const relative = a.value === 0
      ? (b.value === 0 ? 0 : Infinity)
      : (b.value - a.value) / a.value;
    const worse = HIGHER_IS_BETTER.has(a.metric) ? -relative : relative;
    changes.push({
      key: keyOf(a),
      bench: a.bench,
      metric: a.metric,
      params: a.params,
      unit: a.unit,
      before: a.value,
      after: b.value,
      relative,
      noise,
      verdict: worse > noise
        ? "regression"
        : worse < -noise
        ? "improvement"
        : "unchanged",
    });
  }
  for (const [id, b] of right) if (!left.has(id)) onlyAfter.push(keyOf(b));
  return {
    changes,
    onlyBefore,
    onlyAfter,
    forcedAcross: [...new Set(forcedAcross)],
  };
}

/**
 * `Bench-Accept: <glob> -- <reason>` lines of the commit messages between base and head: an
 * intentional regression is accepted by saying so, with a reason, in the commit that causes it.
 * The glob matches a bench name; `*` matches any run of characters.
 */
export function parseAccepts(
  messages: string,
): { pattern: RegExp; glob: string; reason: string }[] {
  const accepts = [];
  for (const line of messages.split("\n")) {
    const found = /^Bench-Accept:\s*(\S+)\s+--\s+(\S.*)$/.exec(line.trim());
    if (!found) continue;
    const glob = found[1];
    const pattern = new RegExp(
      "^" + glob.split("*").map((part) =>
        part.replace(/[.+?^${}()|[\]\\]/g, "\\$&")
      ).join(".*") + "$",
    );
    accepts.push({ pattern, glob, reason: found[2] });
  }
  return accepts;
}

/** The regressions that no accept covers. */
export function failures(
  comparison: Comparison,
  accepts: { pattern: RegExp }[] = [],
): Change[] {
  return comparison.changes.filter((change) =>
    change.verdict === "regression" &&
    !accepts.some((accept) => accept.pattern.test(change.bench))
  );
}

// # Scaling

/** The most a count may grow when its input grows tenfold; a quadratic path grows 100x. */
const SCALING_LIMITS: Record<string, number> = {
  instructions: 14,
  allocations: 14,
  // one hash table or vector growing past a power of two jumps up to 16x in one allocation
  alloc_bytes: 25,
};

export interface Scaling {
  bench: string;
  metric: string;
  small: number;
  large: number;
  /** How many times the value grew for a tenfold input. */
  ratio: number;
  limit: number;
}

/** The input size of a record: `n` as a number, or a gungraun id like `n10k`, `n1638`. */
function sizeOf(params: Record<string, string>): number | undefined {
  const written = params.n ?? params.id;
  const found = /^n?(\d+)(k?)$/.exec(written ?? "");
  return found ? Number(found[1]) * (found[2] ? 1000 : 1) : undefined;
}

/**
 * For every bench and counted metric measured at two sizes ten times apart, how much the value
 * grew. A ratio over the metric's limit is a path that costs more than linear: the check that
 * needs no baseline, so it holds on a commit that has no ancestor to compare with.
 */
export function scalings(records: BenchRecord[]): Scaling[] {
  const bySeries = new Map<string, Map<number, number>>();
  for (const record of records) {
    const size = sizeOf(record.params);
    if (
      size === undefined || !(record.metric in SCALING_LIMITS) || record.dirty
    ) continue;
    const key = `${record.bench}\u0000${record.metric}`;
    const sizes = bySeries.get(key) ?? new Map<number, number>();
    sizes.set(size, record.value);
    bySeries.set(key, sizes);
  }
  const out: Scaling[] = [];
  for (const [key, sizes] of bySeries) {
    const [bench, metric] = key.split("\u0000");
    for (const [small, smallValue] of sizes) {
      const large = [...sizes.keys()].find((size) =>
        Math.abs(size / small - 10) < 0.05
      );
      if (large === undefined) continue;
      out.push({
        bench,
        metric,
        small,
        large,
        ratio: sizes.get(large)! / Math.max(smallValue, 1),
        limit: SCALING_LIMITS[metric],
      });
    }
  }
  return out.sort((a, b) => a.bench.localeCompare(b.bench));
}

export const overScaled = (all: Scaling[]): Scaling[] =>
  all.filter((s) => s.ratio > s.limit);

// # Trend report

export interface Step {
  commit: string;
  value: number;
  /** Relative change from the previous commit's value, once there is one. */
  relative?: number;
  flagged: boolean;
}

export interface Trend {
  key: string;
  bench: string;
  metric: string;
  params: Record<string, string>;
  unit: string;
  steps: Step[];
}

/**
 * Per measurement, the value at each commit in `commits` (oldest first), with the steps
 * beyond the metric's noise threshold flagged. A commit's value is the median of its records
 * (a bench run twice on one commit), and records of other bases than `basis` are left out.
 */
export function trends(
  records: BenchRecord[],
  commits: string[],
  basis: Basis,
): Trend[] {
  const wanted = JSON.stringify(basis);
  const grouped = new Map<string, Map<string, number[]>>();
  const first = new Map<string, BenchRecord>();
  for (const record of records) {
    if (record.dirty || JSON.stringify(basisOf(record)) !== wanted) continue;
    const key = keyOf(record);
    first.set(key, record);
    const byCommit = grouped.get(key) ?? new Map<string, number[]>();
    byCommit.set(record.commit, [
      ...(byCommit.get(record.commit) ?? []),
      record.value,
    ]);
    grouped.set(key, byCommit);
  }
  const median = (values: number[]) => {
    const sorted = [...values].sort((a, b) => a - b);
    return sorted[Math.floor(sorted.length / 2)];
  };
  const out: Trend[] = [];
  for (const [key, byCommit] of grouped) {
    const sample = first.get(key)!;
    const steps: Step[] = [];
    let previous: number | undefined;
    for (const commit of commits) {
      const values = byCommit.get(commit);
      if (!values) continue;
      const value = median(values);
      const relative = previous === undefined || previous === 0
        ? undefined
        : (value - previous) / previous;
      steps.push({
        commit,
        value,
        relative,
        flagged: relative !== undefined &&
          Math.abs(relative) > noiseOf(sample.metric),
      });
      previous = value;
    }
    if (steps.length > 0) {
      out.push({
        key,
        bench: sample.bench,
        metric: sample.metric,
        params: sample.params,
        unit: sample.unit,
        steps,
      });
    }
  }
  return out.sort((a, b) => a.key.localeCompare(b.key));
}

export const formatParams = (params: Record<string, string>): string =>
  Object.entries(params).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) =>
    `${k}=${v}`
  ).join(" ");

export const percent = (relative: number): string =>
  Number.isFinite(relative)
    ? `${relative >= 0 ? "+" : ""}${(relative * 100).toFixed(2)}%`
    : "new";
