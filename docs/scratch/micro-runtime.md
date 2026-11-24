# Micro runtime and language (claytxt lane, ADR draft)

Owner: claytxt lane; operator reviews.
Updated: 2026-11-17
Scope: The micro language and runtime choice: TypeScript default, Rust first class, wasm-first placement.

Status: draft for the operator, 2026-11-17. The claytxt lane owns the micro-language decision (operator, 2026-11-17).
The operator's first review (2026-11-17) is recorded under "Operator review" below. In short, TypeScript is the
end-user default, Rust is first class, and the Clay-on-the-node API comes before the TS spike.
Builds on `end-user-programming.md`. Evidence: `.claude/scratch/{ts-in-wasm-survey,spike-js-engine,spike-zena}.md`.

## Context

Micros must be editable by end users in the app, live, sandboxed in wasm, on Android, desktop and the web, with
many active at once. Base features ship as cartridges and are dogfooded in the same language users get. Micros
have more constraints than nodules: they render UI through the presentation wire (a retained DOM-mutation
stream), hold live state across edits through collections, and run under per-call fuel. Nodules may pick a
language separately.

## Decision (proposed)

1. **The micro contract is language-neutral.** A micro is an artifact plus a WIT world: presentation, collections,
   capability bank, lifecycle and the dispatch transport. No engine API is part of the contract. Language support
   ships as a *language cartridge*: a runtime component plus a build routine (`end-user-programming.md` §1;
   nodules' `#hosted` component kind).
2. **The default language is TypeScript, run as JS in wasm**, on one shared QuickJS engine component that is
   pre-initialized with Wizer and whose memory is shared copy-on-write.
   - Spike numbers: about 0.3 ms to instantiate, about 1.2 ms to load source, about 1.7 ms from edit to live; about
     1.45 MB per micro with the framework in the snapshot (about 73 MB for 50 micros); one compiled component for
     all of them.
   - TS types are stripped in the engine (about 2.5 ms for 483 lines). Type checking runs in the editor tooling, not
     on load.
3. **The universal layer is the presentation wire; Solid is only the default JS framework on it** (operator,
   2026-11-17). Any framework can target the wire: Dioxus (Rust), Solid's universal renderer, React through
   `react-reconciler`, Vue through `createRenderer`. In the spike, all three JS frameworks drove the same DOM
   adapter and emitted the same mutations for the same work (create 1,000 rows: 14,000 ops each), except a row
   swap, which React emits as 997 ops against 2.
   - **Why React and Preact are slow here, and Dioxus isn't.** A virtual-DOM framework re-runs components and diffs
     their output on every update. The spike's naive React app spent about 30 µs per vnode on that, so 13–21 ms per
     keystroke at 100 rows, all of it JavaScript running in QuickJS. QuickJS is an interpreter, with no JIT. The
     "30–100×" in the spike notes compares the same React code in QuickJS-in-wasm against Node/V8, so it measures
     the engine, not the wire. Dioxus does the same diffing in compiled Rust, so it is unaffected. Solid needs no
     diff at all (0.045 ms per typing step), which is why it is the interpreter tier's default. A memoized React
     app, or the AOT tier (item 5), narrows the gap; neither was measured.
   - Every JSX framework needs a compile step, not just Solid: React and Preact JSX compiles to
     `createElement`/`jsx()` calls, and Solid's compiles to fine-grained template code (babel-preset-solid,
     `generate: "universal"`). TypeScript also needs its types stripped. The "build routine" is the language
     cartridge's step that turns micro source into runnable JS.
   - **A micro ships both its built JS and its source** (operator, 2026-11-17).
     - The cartridge carries the transpiled, minified bundle, which is what runs. The default path does no build
       on the device: install and run, like any shipped app.
     - It also carries the full source (TS/JSX), which is what the person edits. Editing runs the build routine
       on the host, and its output replaces the bundle for that device.
     - A cartridge without source can run but not be edited in the app. End-user programmability requires the
       source to ship.
     - Because the build routine is deterministic, the registry can rebuild the bundle from the source and check
       that they match. Then the code a person reads is the code that runs.
     - **Deferred (operator, 2026-11-17):** the on-device build routine comes after TS micros run. Start with the
       shipping model only: bundles built ahead of time (as in the spike) plus the source.
4. **Heavy or stable micros stay in Rust** (the diff view, virtualization, highlighting) and are composed into TS
   micros by embedding (`mount-component`), with data exchanged through collections. That is the Emacs C-primitive
   split.
5. **Tiers.** The interpreter tier (QuickJS) serves editing. A settled micro can be promoted by another build
   routine to an AOT tier (StarlingMonkey + weval, scriptc or Porffor) under the same WIT world. Promotion needs a
   conformance run first, and its memory cost depends on shared-engine linking (wit-dylib).
6. **Other languages are more language cartridges**, not host features. Zena (AOT, 9–19× QuickJS on scalar code,
   18–60 KB modules, about 90–165 ms per edit on a laptop) is the strongest second candidate. It is blocked on
   `borrow<resource>` exports and component emission, and has one maintainer. Lua and Hoot remain options.

## Consequences

- **The JS MDK** becomes the main micro SDK and gets the care of a public API: WIT-generated `.d.ts`, typed
  collections, lifecycle hooks, and an embedding helper. The Rust MDK stays for Rust micros.
- **The presentation wire moves to explicit node ids** (claytxt lane's choice, delegated by the operator on
  2026-11-17: "choose whichever is more universal"). Today's ops are a stack machine (push-id,
  append-children(n)) shaped after Dioxus's edit stream. Every JS framework's renderer addresses nodes by handle:
  insert(parent, node, anchor), remove(node), set attribute or property on a node, set text. Explicit ids map onto
  those one to one. Lowering Dioxus onto explicit ids is natural too, because Dioxus already gives each node an
  `ElementId`; the stack is only its encoding, and the Rust MDK translates it. The reverse, making JS renderers
  drive a stack, would put an encoder in every framework adapter. Keep a template op (instantiate a pre-sent
  subtree) as an optimization any client may use; Dioxus templates and Solid's compiled templates both fit it.
- **Engine hardening before relying on it:** build the engine on our toolchain (the generated crate fails on
  wasip3 today); give `load` a `result` so a user syntax error doesn't panic the guest; cap each micro's QuickJS
  heap (a busy micro reached about 35 MB); add per-call fuel accounting for the interpreter.
- **Web:** the same engine component runs through wcmp in the browser. The browser's own JS engine is not used
  for micros, which keeps one sandbox model; revisit if web performance demands it.

## Operator review (2026-11-17)

- **TypeScript is the default** for end-user programmability, and for daytxt, where most end-user malleability is
  expected. **Rust is first class too.** intxt is a separate product and ships in Rust now, so it doesn't wait for
  the TS runtime.
- **The tiers are accepted**, and so is the roughly 1 MB per-micro baseline; memory can be optimized later.
- **Host APIs follow WinterTC** (the former WinterCG, now Ecma TC55: the "Minimum Common Web Platform API" that
  Deno, Cloudflare Workers, Bun and Node align on), wherever possible. That means `fetch`, `URL`, `TextEncoder`
  and `TextDecoder`, streams, `AbortController`, timers, `crypto.getRandomValues`, `structuredClone` and
  `console`. `fetch` is the `wasi:http` transport, so egress is limited to `dispatch.local` as for Rust micros
  (`clay-on-the-node.md` §3). Most of these are pure JS-level APIs with no host I/O (`URL`, `TextEncoder`,
  `structuredClone`, `atob`), so the engine should bring them itself. Only `fetch`, timers and `console` need host
  hooks. Bare QuickJS has none of them. The spike's wasm-rquickjs adds Node and web shims on top (its `fetch` is
  always imported), but which WinterTC APIs those shims cover hasn't been audited. Boa ships them in Rust; see
  open question 6.
- **The DOM layer must outlive any one framework.** The lasting contract is the presentation wire, not Solid. The
  JS MDK exposes a small DOM-like node API over the wire: create, insert before an anchor, remove, set an attribute
  or property, set text, listen. Solid's universal renderer, React's `react-reconciler` host config and Vue's
  `createRenderer` are all thin adapters over that API. A framework that writes straight to `document` (Svelte, lit)
  needs a DOM-subset shim over the same API. Solid is the default only because it skips the virtual-DOM diff, which
  is costly on an interpreter (see item 3 above).
- **The JSX compile step stays.** It happens in the language cartridge's build routine: at publish time for the shipped
  bundle, and on the host when a person edits (see item 3).
- **Open question 1: an edit is a full restart, not a hot swap or hot reload.** The "snapshot" is the engine's
  memory after Wizer has run its setup at build time (QuickJS booted, Solid and the MDK loaded), saved inside the
  component. Each instance starts from that memory instead of redoing the setup, which is why instantiating takes
  about 0.3 ms. Applying an edit throws the micro's instance away and starts a fresh one from the snapshot with the
  new code. Nothing from the old heap carries over: no poisoned state, no stale closures. State that must survive
  an edit lives in collections, which are already the place for live state. Don't call this hot swap or hot
  reload, because those replace code inside a running heap. Keeping the instance and reloading only changed
  modules (hot module reload) is a later optimization, and only if measurements show the restart is too slow and
  it can be done without carrying state over.
- **Open question 2 (npm): a traditional build step first.** npm packages are bundled at build time inside the
  sandboxed build routine. Later this moves to Deno-style no-build imports (`npm:` and `jsr:` specifiers, possibly
  through Deno's crates). That work must not block the first micros.
- **Open question 3 (type checking): set aside.** Fast checkers (TypeScript's Go port, Rust experiments) can come
  later.
- **Order: Clay on the node first** (`clay-on-the-node.md`), so the micro API shape is settled before anything is
  ported to TS. The `micro_counter` TS spike comes after it.

## Open questions

1. Answered: a full restart from the snapshot (see "Operator review"). Hot module reload is later, and only
   after measuring.
2. Answered: a build step first, no-build imports later.
3. Set aside for now.
4. Fuel: QuickJS interpreter steps versus wasm fuel. Which budget does a user see?
5. Answered: explicit node ids, with Dioxus lowered onto them (see "Consequences").
6. The engine: QuickJS (via wasm-rquickjs) or Boa. The operator's interest in Boa (2026-11-17) is its WinterTC
   coverage, not its speed, since both run inside wasm. Boa is pure Rust and embeds directly. `boa_runtime` 0.22
   (2026-10-20; docs.rs read 2026-11-17) and the separate `boa_wintertc` crate implement, in Rust:
   - `console` (behind a `Logger` trait);
   - `URL` (feature `url`, on by default);
   - `TextEncoder` and `TextDecoder`;
   - `structuredClone`, `queueMicrotask` and `atob`/`btoa`;
   - the timers and `AbortController`;
   - `fetch` with `Headers`, `Request` and `Response`. Its JS side is in Rust; the network goes through a host
     `Fetcher` trait, where we would plug in `wasi:http` to `dispatch.local`.

   Not found in Boa: streams, `crypto` and `Blob`. Boa's own (bare) engine also needs no C toolchain, which removes
   the spike's wasip3 build problem (`rquickjs-sys` and wasi-sdk). To decide: whether Boa builds as a p3 component
   with Wizer pre-initialization, and its memory per micro against QuickJS's 1.45 MB.

   **wasm-rquickjs, audited 2026-11-17** (README and GitHub API, golemcloud/wasm-rquickjs):
   - **Age and activity.** Created 2025-06-13, Apache-2.0, last pushed 2026-11-17. v0.5.3 came out on 2026-11-13,
     after 10 releases in five weeks. There are two main maintainers (vigoo, about 1,870 commits; noise64, about
     1,170) and a few others, and much of the recent work is co-authored with an agent account. 23 stars and 5
     forks: its adoption is mostly Golem itself. `main` (0.5.x) targets Golem 2.0; `v0.4.x` is a maintenance line.
   - **What it does.** It generates a Rust crate that wraps the JS on QuickJS-ng 0.16.2 (rquickjs 0.14, with
     Golem's patched fork) and builds it into a component.
   - **Component model: the heavy lifting is done.**
     - WIT imports and exports, exported and imported resources, mapped `result` errors.
     - Preview 3 is the only target (Preview 2 was removed in 0.5.3), including async exports and futures/streams
       (with limits: a synchronous export can't return a `future` or `stream`).
     - `.d.ts` generation from WIT.
     - Wizer pre-initialization of p3 components, made deterministic and refusing external I/O during init.
     - Stated limits: at most 26 parameters, no anonymous interfaces, no resources declared directly in a world.
   - **WinterTC coverage is far beyond Boa's.**
     - `fetch`, `Headers`, `Request`, `Response`, `FormData`, `Blob`, `File`, `URL` and `URLSearchParams`.
     - The full WHATWG streams (readable, writable and transform, BYOB included).
     - Timers, `AbortController`, `DOMException`, `TextEncoder`/`TextDecoder` and their stream forms.
     - `MessageChannel`, `EventTarget`/`Event`, a subset of `Intl`, `crypto.getRandomValues` and `randomUUID`,
       and `performance.now` on the WASI monotonic clock.
     - A large Node 22 compatibility layer, checked against a vendored Node test suite and an npm library tracker.
   - **Risks.** A small team whose roadmap is Golem's. It depends on forks of rquickjs and of wit-bindgen ("repaired
     0.63"). Each JS program becomes a Rust crate build. That doesn't matter for us, because the spike ran one shared
     engine that loads micro source as data, so there's one engine build and no per-micro Rust build. The spike's
     wasip3 C-toolchain workaround still applies.

   **Leaning: wasm-rquickjs.** It has already done the component-model and WinterTC work we would otherwise write
   around Boa. Boa stays the fallback if the forks or the Golem coupling become a problem. Before committing, check
   whether upstream will take our needs (the eval-module shape, `dispatch.local` egress) or whether we would carry a
   fork.

## First spike (proposed)

A `micro_counter` port in TS on the shared engine against the real `micro-client` WIT world in our Wasmtime fork:
- Solid universal rendering to the real presentation wire;
- one collection read and write;
- an edit applied as a full restart from the snapshot, with state kept in a collection;
- mounted in the lab micro next to the Rust counter.

Measure memory per instance, edit-to-live time and fuel use. This settles the hardening list and the wire variant
question.
