// Tests of the record schema, the comparison and the noise logic. Run:
//   deno test x/bench-lib.test.ts

import {
  type BenchRecord,
  compare,
  CrossBasisError,
  failures,
  fromDivanOutput,
  fromGungraunSummary,
  noiseOf,
  overScaled,
  parseAccepts,
  parsePartial,
  parseRecord,
  recordsFromOutput,
  scalings,
  trends,
} from "./bench-lib.ts";

// JSON with keys in order, so two objects are equal whatever order their keys came in.
function canonical(value: unknown): string {
  return JSON.stringify(
    value,
    (_key, entry) =>
      entry && typeof entry === "object" && !Array.isArray(entry)
        ? Object.fromEntries(
          Object.entries(entry).sort(([a], [b]) => a.localeCompare(b)),
        )
        : entry,
  );
}

function eq(actual: unknown, expected: unknown, _what?: string) {
  if (canonical(actual) !== canonical(expected)) {
    throw new Error(
      `expected ${canonical(expected)}, got ${canonical(actual)}`,
    );
  }
}

function ok(condition: boolean, what = "condition") {
  if (!condition) throw new Error(`${what} does not hold`);
}

function throws(
  run: () => unknown,
  expected: RegExp | (new (...args: never[]) => Error) = /./,
) {
  try {
    run();
  } catch (error) {
    const matches = expected instanceof RegExp
      ? expected.test(String((error as Error).message))
      : error instanceof expected;
    if (!matches) throw new Error(`threw the wrong error: ${error}`);
    return;
  }
  throw new Error("expected a throw");
}

const env = {
  commit: "a".repeat(40),
  dirty: false,
  machine: "box (cpu)",
  toolchain: "rustc 1.99.0",
  profile: "dev",
  target: "native" as const,
  ts: "2026-11-25T00:00:00.000Z",
};

const record = (
  overrides: Partial<BenchRecord> & { value: number },
): BenchRecord => ({
  bench: "big_automata::scaling::okv",
  metric: "allocations",
  unit: "count",
  params: { n: "10000" },
  ...env,
  ...overrides,
});

Deno.test("a partial record validates its shape", () => {
  const ok = {
    bench: "c::b",
    metric: "wall_ns",
    unit: "ns",
    value: 5,
    params: { n: "1" },
  };
  eq(parsePartial(ok), ok);
  throws(() => parsePartial({ ...ok, value: "5" }), /finite number/);
  throws(() => parsePartial({ ...ok, value: NaN }), /finite number/);
  throws(() => parsePartial({ ...ok, bench: "" }), /bench/);
  throws(() => parsePartial({ ...ok, params: { n: 1 } }), /params\.n/);
  eq(parsePartial({ ...ok, params: undefined }).params, {});
});

Deno.test("a full record needs its environment", () => {
  const full = record({ value: 1 });
  eq(parseRecord(JSON.parse(JSON.stringify(full))), full);
  throws(() => parseRecord({ ...full, target: "wasm" }), /target/);
  throws(() => parseRecord({ ...full, dirty: "no" }), /dirty/);
  throws(() => parseRecord({ ...full, commit: "" }), /commit/);
});

Deno.test("records are read out of a run's mixed output", () => {
  const output = [
    "running 1 test",
    `        PASS [ 0.1s] c SCALING x BENCH_RECORD {"bench":"c::b","metric":"allocations","unit":"count","value":7,"params":{"n":"10"}}`,
    "BENCH_RECORD " +
    JSON.stringify({
      bench: "c::b",
      metric: "sim_time",
      unit: "s",
      value: 1.5,
      params: {},
    }),
    "unrelated",
  ].join("\n");
  const records = recordsFromOutput(output);
  eq(records.length, 2);
  eq(records[0].value, 7);
  throws(() => recordsFromOutput("BENCH_RECORD {not json"));
});

Deno.test("gungraun's summary becomes an instruction-count record", () => {
  // the shape gungraun 0.20 writes (summary version 7), trimmed to what is read
  const summary = {
    module_path: "instr::layers::okv_write",
    function_name: "okv_write",
    id: "n10k",
    profiles: [{
      tool: "Callgrind",
      data: {
        total: {
          metrics: {
            Ir: { values: { new: 1234 } },
            Dr: { values: { new: 5 } },
          },
        },
      },
    }],
  };
  const [one] = fromGungraunSummary(summary, "big_automata");
  eq(one.bench, "big_automata::instr::layers::okv_write");
  eq(one.metric, "instructions");
  eq(one.value, 1234);
  eq(one.params.id, "n10k");
  throws(
    () =>
      fromGungraunSummary(
        { ...summary, profiles: [{ tool: "Callgrind" }] },
        "c",
      ),
    /Ir/,
  );
  // another tool's profile is not an instruction count
  eq(
    fromGungraunSummary({ ...summary, profiles: [{ tool: "DHAT" }] }, "c"),
    [],
  );
});

Deno.test("divan's tree becomes wall_ns records by median", () => {
  const output = [
    "Timer precision: 10 ns",
    "wall                               fastest       │ slowest       │ median        │ mean          │ samples │ iters",
    "├─ okv_write                                     │               │               │               │         │",
    "│  ├─ 10000                        2.426 ms      │ 13.82 ms      │ 4.444 ms      │ 5.181 ms      │ 100     │ 100",
    "│  ╰─ 100000                       50.7 ms       │ 189.2 ms      │ 58.25 ms      │ 64.08 ms      │ 100     │ 100",
    "├─ plain                           900 ns        │ 2.1 µs        │ 1.5 µs        │ 1.6 µs        │ 100     │ 1000",
    "╰─ whole_sync                                    │               │               │               │         │",
    "   ╰─ 1000                         30.91 ms      │ 124.6 ms      │ 0.5 s         │ 52.02 ms      │ 100     │ 100",
  ].join("\n");
  eq(
    fromDivanOutput(output, "big_automata").map((
      r,
    ) => [r.bench, r.params.n, r.value]),
    [
      ["big_automata::wall::okv_write", "10000", 4_444_000],
      ["big_automata::wall::okv_write", "100000", 58_250_000],
      ["big_automata::wall::plain", undefined, 1500],
      ["big_automata::wall::whole_sync", "1000", 500_000_000],
    ],
  );
});

Deno.test("noise is tight for counts and loose for wall time", () => {
  ok(noiseOf("instructions") < noiseOf("wall_ns"));
  ok(noiseOf("allocations") <= 0.01);
  eq(noiseOf("something_new"), 0.05);
});

Deno.test("compare flags only changes beyond the metric's noise", () => {
  const before = [
    record({ value: 1000 }),
    record({ metric: "wall_ns", unit: "ns", value: 1000 }),
    record({ metric: "alloc_bytes", value: 1000 }),
    record({ bench: "gone", value: 1 }),
  ];
  const after = [
    record({ value: 1005 }), // +0.5% allocations: inside the 1% noise
    record({ metric: "wall_ns", unit: "ns", value: 1100 }), // +10% wall: inside the 15%
    record({ metric: "alloc_bytes", value: 1500 }), // +50%: a regression
    record({ bench: "new", value: 1 }),
  ];
  const result = compare(before, after);
  const verdicts = Object.fromEntries(
    result.changes.map((change) => [change.metric, change.verdict]),
  );
  eq(verdicts, {
    allocations: "unchanged",
    wall_ns: "unchanged",
    alloc_bytes: "regression",
  });
  eq(result.onlyBefore.length, 1);
  eq(result.onlyAfter.length, 1);
  eq(failures(result).length, 1);
  const better = compare(before, [record({ value: 500 })]);
  eq(better.changes[0].verdict, "improvement");
  eq(failures(better).length, 0);
});

Deno.test("a threshold override moves the line", () => {
  const result = compare([record({ value: 100 })], [record({ value: 103 })], {
    thresholds: { allocations: 0.05 },
  });
  eq(result.changes[0].verdict, "unchanged");
  eq(
    compare([record({ value: 100 })], [record({ value: 103 })]).changes[0]
      .verdict,
    "regression",
  );
});

Deno.test("comparison refuses another machine, toolchain or profile unless forced", () => {
  const here = [record({ value: 10 })];
  for (
    const other of [{ machine: "other (cpu)" }, { toolchain: "rustc 2.0" }, {
      profile: "release",
    }]
  ) {
    throws(
      () => compare(here, [record({ value: 10, ...other })]),
      CrossBasisError,
    );
  }
  const forced = compare(
    here,
    [record({ value: 10, machine: "other (cpu)" })],
    { force: true },
  );
  eq(forced.forcedAcross, ["machine: box (cpu) vs other (cpu)"]);
  eq(forced.changes.length, 1);
  throws(() => compare([], here), CrossBasisError);
});

Deno.test("only the basis both sides share is compared", () => {
  const before = [
    record({ value: 10 }),
    record({ value: 5, profile: "release" }),
  ];
  const after = [record({ value: 10 })];
  const result = compare(before, after);
  eq(result.changes.length, 1);
  eq(
    result.onlyBefore.length,
    0,
    "the other profile is out of scope, not missing",
  );
});

Deno.test("the latest of a measurement recorded twice counts", () => {
  const result = compare([record({ value: 10 }), record({ value: 20 })], [
    record({ value: 20 }),
  ]);
  eq(result.changes[0].before, 20);
  eq(result.changes[0].verdict, "unchanged");
});

Deno.test("accepts name a bench glob and a reason", () => {
  const accepts = parseAccepts([
    "feat: slower but right",
    "",
    "Bench-Accept: big_automata::scaling::okv* -- the row now carries a checksum",
    "Bench-Accept: nonsense without a reason",
  ].join("\n"));
  eq(accepts.length, 1);
  ok(accepts[0].pattern.test("big_automata::scaling::okv_write"));
  ok(!accepts[0].pattern.test("big_automata::scaling::rev"));
  const regression = compare([record({ value: 100 })], [
    record({ value: 200 }),
  ]);
  eq(failures(regression).length, 1);
  eq(
    failures(regression, parseAccepts("Bench-Accept: big_automata::* -- why"))
      .length,
    0,
  );
});

Deno.test("scaling ratios catch a tenfold input costing a hundredfold", () => {
  const sized = (
    n: string,
    value: number,
    bench = "b",
    metric = "instructions",
  ) => record({ bench, metric, value, params: { id: n } });
  const found = scalings([
    sized("n10k", 100),
    sized("n100k", 1000), // linear
    sized("n1k", 100, "quadratic"),
    sized("n10k", 10000, "quadratic"), // 100x
    sized("n1638", 10, "odd"),
    sized("n16380", 160, "odd"), // 16x: over the instruction limit
    sized("n10k", 1, "bytes", "alloc_bytes"),
    sized("n100k", 16, "bytes", "alloc_bytes"), // a table doubling: inside the bytes limit
    record({
      bench: "n-param",
      metric: "allocations",
      value: 5,
      params: { n: "100" },
    }),
    record({
      bench: "n-param",
      metric: "allocations",
      value: 50,
      params: { n: "1000" },
    }),
    record({ bench: "time", metric: "sim_time", value: 1, params: { n: "1" } }),
    record({
      bench: "time",
      metric: "sim_time",
      value: 99,
      params: { n: "10" },
    }),
  ]);
  eq(found.map((s) => [s.bench, Math.round(s.ratio)]), [
    ["b", 10],
    ["bytes", 16],
    ["n-param", 10],
    ["odd", 16],
    ["quadratic", 100],
  ]);
  eq(overScaled(found).map((s) => s.bench), ["odd", "quadratic"]);
});

Deno.test("trends flag steps beyond noise and skip dirty and foreign records", () => {
  const sha = (letter: string) => letter.repeat(40);
  const records = [
    record({ value: 100, commit: sha("a") }),
    record({ value: 100, commit: sha("b") }),
    record({ value: 150, commit: sha("c") }),
    record({ value: 999, commit: sha("d"), dirty: true }),
    record({ value: 1, commit: sha("c"), machine: "other (cpu)" }),
  ];
  const [trend] = trends(records, [sha("a"), sha("b"), sha("c"), sha("d")], {
    machine: env.machine,
    toolchain: env.toolchain,
    profile: env.profile,
    target: env.target,
  });
  eq(trend.steps.map((step) => step.flagged), [
    false,
    false,
    true,
  ]);
  eq(trend.steps.at(-1)!.relative, 0.5);
});
