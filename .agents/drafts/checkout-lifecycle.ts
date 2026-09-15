// deno run .agents/drafts/checkout-lifecycle.ts
// A checkout-first model. One output per document, text instead of bytes.
// No lenses, Automerge, naming allocator, or real filesystem persistence.

function eq<T>(actual: T, expected: T): void {
  if (actual !== expected) throw Error(`expected ${String(expected)}, got ${String(actual)}`);
}
function ok(value: unknown): asserts value { if (!value) throw Error("assertion failed"); }

type Entry = { path: string; text: string };
// Backend-defined identity/version. A path-only backend may leave them absent.
type Item = Entry & { key?: string; version?: string };
type Snapshot = { entries: Map<string, Item>; moves: Map<string, string> };
type Offer = { key: string; desired: Entry; expected?: Entry; baseVersion?: string };
type Outcome = { kind: "applied"; actual: Entry } | { kind: "refused"; reason: string };
interface TreeBackend {
  observe(): Snapshot;
  receive(offer: Offer): Outcome;
}
function snapshot(items: Item[], moves: Map<string, string> = new Map()): Snapshot {
  return { entries: new Map(items.map(item => [item.path, item])), moves };
}

class Filesystem implements TreeBackend {
  private readonly files = new Map<string, string>();
  private readonly observedMoves = new Map<string, string>();
  observe(): Snapshot {
    const result = snapshot([...this.files].map(([path, text]) => ({ path, text })),
      new Map(this.observedMoves));
    this.observedMoves.clear();
    return result;
  }
  receive({ desired, expected }: Offer): Outcome {
    const destination = this.files.get(desired.path);
    if (expected?.path !== desired.path && destination !== undefined) {
      return { kind: "refused", reason: "destination occupied" };
    }
    if (expected?.path === desired.path && destination !== expected.text &&
        destination !== desired.text) {
      return { kind: "refused", reason: "destination changed" };
    }
    if (expected && expected.path !== desired.path &&
        this.files.get(expected.path) !== expected.text) {
      return { kind: "refused", reason: "old path changed" };
    }
    this.files.set(desired.path, desired.text); // Atomic replacement in this toy model.
    if (expected && expected.path !== desired.path) this.files.delete(expected.path);
    return { kind: "applied", actual: { ...desired } };
  }
  // Test fixtures: edits come from outside the bridge, not the checkout policy.
  userWrites(path: string, text: string): void { this.files.set(path, text); }
  userRemoves(path: string): void { this.files.delete(path); }
  userMoves(from: string, to: string): void {
    const text = this.files.get(from);
    if (text === undefined || this.files.has(to)) throw Error("invalid move fixture");
    this.files.delete(from);
    this.files.set(to, text);
    this.observedMoves.set(from, to);
  }
}

type Document = { id: string; path: string; text: string; revision: number };
class Daybook implements TreeBackend {
  private readonly current = new Map<string, Document>();
  private readonly history = new Map<string, Document>();
  constructor(doc: Document) { this.save({ ...doc }); }
  private save(doc: Document): void {
    this.current.set(doc.id, doc);
    this.history.set(`${doc.id}@${doc.revision}`, { ...doc });
  }
  observe(): Snapshot {
    return snapshot([...this.current.values()].map(doc => ({
      key: doc.id, version: `${doc.id}@${doc.revision}`, path: doc.path, text: doc.text,
    })));
  }
  describeOutputs(): FileDescription[] {
    return [...this.current.values()].map(doc => ({
      key: doc.id, path: doc.path, version: `${doc.id}@${doc.revision}`,
    }));
  }
  receive({ key, desired, baseVersion }: Offer): Outcome {
    const doc = this.current.get(key);
    const base = this.history.get(baseVersion ?? "");
    if (!doc || !base || base.id !== key) throw Error("invalid output/base token");
    const moved = desired.path !== base.path;
    // Unison-style false conflict: both sides reached the same visible output.
    // This also makes retry safe if a prior ingest landed before its receipt.
    if (doc.path === desired.path && doc.text === desired.text) {
      return { kind: "applied", actual: { path: doc.path, text: doc.text } };
    }
    const edited = desired.text !== base.text;
    if ((moved && doc.path !== base.path) || (edited && doc.text !== base.text)) {
      return { kind: "refused", reason: "concurrent edit from render base" };
    }
    if (moved || edited) {
      this.save({ id: key, revision: doc.revision + 1,
        path: moved ? desired.path : doc.path,
        text: edited ? desired.text : doc.text });
    }
    const accepted = this.current.get(key)!;
    return { kind: "applied", actual: { path: accepted.path, text: accepted.text } };
  }
  remoteChange(id: string, edit: Partial<Pick<Document, "path" | "text">>): void {
    const doc = this.current.get(id)!;
    this.save({ ...doc, ...edit, revision: doc.revision + 1 });
  }
  // Toy source reads a historical output at the mounted version. Real byte
  // ranges must operate on encoded bytes, not JavaScript string indices.
  readVersion(key: string, version: string, offset: number, length: number): string {
    const doc = this.history.get(version);
    if (!doc || doc.id !== key) throw Error("source version unavailable");
    return doc.text.slice(offset, offset + length); // ASCII fixtures only.
  }
}

// Generic pair state. This is where a vtree starts to emerge: latest reported
// trees are separate from the pair's last commonly applied correspondences.
type Correspondence = { key: string; leftVersion: string; right: Entry };
type Receipt = { key: string; acceptedInput: Entry };
type Intent = { key: string; proposed: Item; expected?: Entry };
class PairBridge {
  readonly left: TreeBackend;
  readonly right: TreeBackend;
  readonly lastReported = new Map<TreeBackend, Snapshot>();
  readonly common = new Map<string, Correspondence>();
  readonly receipts = new Map<string, Receipt>();
  readonly intents = new Map<string, Intent>();
  constructor(left: TreeBackend, right: TreeBackend) {
    this.left = left;
    this.right = right;
  }
  report(backend: TreeBackend): Snapshot {
    const observed = backend.observe();
    this.lastReported.set(backend, observed);
    return observed;
  }
  // The receiving backend, not the bridge, decides whether the offer applies.
  offer(receiver: TreeBackend, request: Offer): Outcome {
    return receiver.receive(request);
  }
  // A directional acknowledgement does NOT update the common correspondence.
  acknowledgeIngest(key: string, acceptedInput: Entry): void {
    this.receipts.set(key, { key, acceptedInput });
  }
  recordIntent(key: string, proposed: Item, expected?: Entry): void {
    this.intents.set(key, { key, proposed, expected });
  }
  acknowledgeProjection(key: string, proposed: Item, actual: Entry): void {
    ok(proposed.version);
    this.common.set(key, { key, leftVersion: proposed.version, right: actual });
    this.receipts.delete(key);
    this.intents.delete(key);
  }
}

type Problem = { key: string; path: string; reason: string };
// Checkout-specific policy. It chooses FS → Daybook → FS, knows which side
// produces stable keys, and decides how to treat a missing or moved output.
class Checkout {
  readonly pair: PairBridge;
  readonly problems: Problem[] = [];
  constructor(daybook: TreeBackend, filesystem: TreeBackend) {
    this.pair = new PairBridge(daybook, filesystem);
  }
  private problem(key: string, path: string, reason: string): void {
    this.problems.push({ key, path, reason });
  }
  ingest(crashAfterDaybookApply = false): void {
    const { left: daybook, right: filesystem } = this.pair;
    const fs = this.pair.report(filesystem);
    this.pair.report(daybook);
    for (const base of this.pair.common.values()) {
      const { key } = base;
      const intent = this.pair.intents.get(key);
      if (intent) {
        if (fs.entries.get(intent.proposed.path)?.text !== intent.proposed.text) {
          this.problem(key, intent.proposed.path, "interrupted write changed");
        }
        continue; // Our unacknowledged write is not a user edit.
      }
      const receipt = this.pair.receipts.get(key);
      if (receipt) {
        if (fs.entries.get(receipt.acceptedInput.path)?.text !== receipt.acceptedInput.text) {
          this.problem(key, receipt.acceptedInput.path, "file changed after ingest");
        }
        continue; // The previous FS offer already landed in Daybook.
      }
      const movedTo = fs.moves.get(base.right.path); // Optional evidence, not guaranteed by scans.
      if (movedTo && fs.entries.has(base.right.path)) {
        this.problem(key, base.right.path, "ambiguous move");
        continue;
      }
      const path = movedTo ?? base.right.path;
      const text = fs.entries.get(path)?.text;
      if (text === undefined) {
        this.problem(key, path, "missing output needs lens decision");
        continue;
      }
      if (path === base.right.path && text === base.right.text) continue;
      const proposal = { path, text };
      const result = this.pair.offer(daybook, {
        key, desired: proposal, baseVersion: base.leftVersion,
      });
      if (result.kind === "refused") this.problem(key, path, result.reason);
      else {
        if (crashAfterDaybookApply) throw Error("simulated crash after Daybook ingest");
        this.pair.acknowledgeIngest(key, proposal);
      }
    }
  }
  project(crashAfterWrite = false): void {
    const { left: daybook, right: filesystem } = this.pair;
    const outputs = this.pair.report(daybook);
    this.pair.report(filesystem);
    for (const output of outputs.entries.values()) {
      if (!output.key || !output.version) throw Error("checkout requires stable output keys and versions");
      const key = output.key;
      if (this.problems.some(problem => problem.key === key)) continue;
      const base = this.pair.common.get(key);
      const receipt = this.pair.receipts.get(key);
      const intent = this.pair.intents.get(key);
      if (intent && (intent.proposed.path !== output.path ||
                     intent.proposed.text !== output.text ||
                     intent.proposed.version !== output.version)) {
        this.problem(key, output.path, "output changed during interrupted write");
        continue;
      }
      const expected = base && {
        path: base.right.path,
        text: receipt?.acceptedInput.path === base.right.path
          ? receipt.acceptedInput.text : base.right.text,
      };
      let actual: Entry;
      if (intent) {
        // A receiver may have applied the offer before the acknowledgement was saved.
        const observed = this.pair.report(filesystem).entries;
        if (observed.get(output.path)?.text !== output.text ||
            (base && base.right.path !== output.path && observed.has(base.right.path))) {
          this.problem(key, output.path, "interrupted write cannot be verified");
          continue;
        }
        actual = { path: output.path, text: output.text };
      } else {
        this.pair.recordIntent(key, output, expected); // Before the external write.
        const result = this.pair.offer(filesystem, { key, desired: output, expected });
        if (result.kind === "refused") {
          this.pair.intents.delete(key); // No operation was applied.
          this.problem(key, output.path, result.reason);
          continue;
        }
        actual = result.actual;
        if (crashAfterWrite) throw Error("simulated crash after filesystem apply");
      }
      if (this.pair.report(filesystem).entries.get(actual.path)?.text !== actual.text) {
        this.problem(key, output.path, "applied output cannot be verified");
        continue;
      }
      this.pair.acknowledgeProjection(key, output, actual);
    }
  }
}

function scenario(name: string, test: () => void): void { test(); console.log(`PASS ${name}`); }
function setup(): { checkout: Checkout; daybook: Daybook; fs: Filesystem } {
  const daybook = new Daybook({ id: "note", path: "/a", text: "base", revision: 1 });
  const fs = new Filesystem();
  return { checkout: new Checkout(daybook, fs), daybook, fs };
}
scenario("initial checkout and local edit", () => {
  const { checkout: c, daybook, fs } = setup(); c.project();
  fs.userWrites("/a", "local"); c.ingest();
  eq(c.pair.common.get("note")?.right.text, "base");
  ok(c.pair.receipts.has("note")); c.project();
  eq(daybook.observe().entries.get("/a")?.text, "local");
  eq(c.pair.common.get("note")?.right.text, "local");
});
scenario("simultaneous content changes preserve local bytes", () => {
  const { checkout: c, daybook, fs } = setup(); c.project();
  fs.userWrites("/a", "local"); daybook.remoteChange("note", { text: "remote" });
  c.ingest(); c.project();
  eq(c.problems[0].reason, "concurrent edit from render base");
  eq(fs.observe().entries.get("/a")?.text, "local");
  eq(c.pair.common.get("note")?.right.text, "base");
});
scenario("both sides make the same edit: no conflict or duplicate write", () => {
  const { checkout: c, daybook, fs } = setup(); c.project();
  fs.userWrites("/a", "same"); daybook.remoteChange("note", { text: "same" });
  const revisionBefore = daybook.observe().entries.get("/a")?.version;
  c.ingest(); c.project();
  eq(c.problems.length, 0);
  eq(daybook.observe().entries.get("/a")?.version, revisionBefore);
  eq(c.pair.common.get("note")?.right.text, "same");
});
scenario("Daybook ingest lands, then receipt write is interrupted", () => {
  const { checkout: c, daybook, fs } = setup(); c.project();
  fs.userWrites("/a", "local");
  try { c.ingest(true); } catch (error) {
    eq((error as Error).message, "simulated crash after Daybook ingest");
  }
  eq(c.pair.common.get("note")?.right.text, "base");
  eq(daybook.observe().entries.get("/a")?.text, "local");
  c.ingest(); // Receiver sees it has already applied the proposal.
  c.project();
  eq(c.problems.length, 0);
  eq(c.pair.common.get("note")?.right.text, "local");
});

scenario("two moves from /a remain unresolved", () => {
  const { checkout: c, daybook, fs } = setup(); c.project();
  fs.userMoves("/a", "/c"); daybook.remoteChange("note", { path: "/b" });
  c.ingest(); c.project();
  eq(c.problems[0].reason, "concurrent edit from render base");
  eq(fs.observe().entries.get("/c")?.text, "base");
  eq(c.pair.common.get("note")?.right.path, "/a");
});
scenario("independent text edit and remote move", () => {
  const { checkout: c, daybook, fs } = setup(); c.project();
  fs.userWrites("/a", "local"); daybook.remoteChange("note", { path: "/b" });
  c.ingest(); c.ingest(); c.project();
  eq(fs.observe().entries.get("/a"), undefined);
  eq(fs.observe().entries.get("/b")?.text, "local");
  eq(c.pair.common.get("note")?.right.path, "/b");
});
scenario("crash after write is not a new FS edit", () => {
  const { checkout: c, daybook } = setup(); c.project();
  daybook.remoteChange("note", { text: "new" });
  try { c.project(true); } catch (error) {
    eq((error as Error).message, "simulated crash after filesystem apply");
  }
  eq(c.pair.common.get("note")?.right.text, "base");
  c.ingest(); c.project();
  eq(c.pair.common.get("note")?.right.text, "new");
});
scenario("identical unowned destination is protected", () => {
  const { checkout: c, daybook, fs } = setup(); c.project();
  fs.userWrites("/b", "base"); daybook.remoteChange("note", { path: "/b" });
  c.ingest(); c.project();
  eq(c.problems[0].reason, "destination occupied");
  eq(c.pair.common.get("note")?.right.path, "/a");
});
scenario("receiver can be swapped without changing checkout code", () => {
  class ReadOnlyTree implements TreeBackend {
    observe(): Snapshot { return snapshot([]); }
    receive(_offer: Offer): Outcome { return { kind: "refused", reason: "read-only backend" }; }
  }
  const c = new Checkout(
    new Daybook({ id: "note", path: "/a", text: "base", revision: 1 }),
    new ReadOnlyTree(),
  );
  c.project();
  eq(c.problems[0].reason, "read-only backend");
  eq(c.pair.common.size, 0);
});
// A WASI VFS mounts names and opaque source versions, NOT file bytes.
// The existing Entry{text} API above is too eager for this use case. This is
// the additional capability the bridge would need, not a hidden copy on mount.
type FileDescription = { key: string; path: string; version: string; size?: number };
interface ByteSource {
  describe(): FileDescription[];
  read(key: string, version: string, offset: number, length: number): string;
}
class DaybookByteSource implements ByteSource {
  readonly reads: Array<{ key: string; offset: number; length: number }> = [];
  constructor(private readonly daybook: Daybook) {}
  describe(): FileDescription[] {
    return this.daybook.describeOutputs();
  }
  read(key: string, version: string, offset: number, length: number): string {
    this.reads.push({ key, offset, length });
    return this.daybook.readVersion(key, version, offset, length);
  }
}
// The bridge routes a byte request without teaching WASI about Daybook.
class ByteBridge {
  read(source: ByteSource, file: FileDescription, offset: number, length: number): string {
    if (offset < 0 || length < 0) throw Error("invalid byte range");
    return source.read(file.key, file.version, offset, length);
  }
}
class WasiView {
  private readonly mounted = new Map<string, FileDescription>();
  constructor(private readonly source: ByteSource, private readonly bridge: ByteBridge) {}
  mount(): void {
    for (const file of this.source.describe()) this.mounted.set(file.path, file);
  }
  read(path: string, offset: number, length: number): string {
    const file = this.mounted.get(path);
    if (!file) throw Error(`not mounted: ${path}`);
    return this.bridge.read(this.source, file, offset, length);
  }
}
scenario("WASI mount reads no bytes until opened, then requests only a range", () => {
  const daybook = new Daybook({ id: "note", path: "/a", text: "abcdefgh", revision: 1 });
  const source = new DaybookByteSource(daybook);
  const view = new WasiView(source, new ByteBridge());
  view.mount();
  eq(source.reads.length, 0);
  eq(view.read("/a", 2, 3), "cde");
  eq(source.reads.length, 1);
  eq(source.reads[0].length, 3);
});
scenario("mounted WASI view retains its source version after a document update", () => {
  const daybook = new Daybook({ id: "note", path: "/a", text: "abcdefgh", revision: 1 });
  const view = new WasiView(new DaybookByteSource(daybook), new ByteBridge());
  view.mount();
  daybook.remoteChange("note", { text: "changed" });
  eq(view.read("/a", 1, 3), "bcd");
  eq(daybook.observe().entries.get("/a")?.text, "changed");
});


scenario("missing output does not delete the document", () => {
  const { checkout: c, daybook, fs } = setup(); c.project();
  fs.userRemoves("/a"); c.ingest(); c.project();
  eq(c.problems[0].reason, "missing output needs lens decision");
  eq(daybook.observe().entries.get("/a")?.key, "note");
});
