# Micro runtime and language (claytxt lane, ADR draft)

Status: draft for the operator, 2026-11-17. The claytxt lane owns the micro-language decision (operator, 2026-11-17).
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
3. **The UI layer is Solid's universal renderer** (`solid-js/universal`) emitting presentation-wire mutations
   directly. It does fine-grained updates with no virtual-DOM diff: in the spike, 0.045 ms per typing step and
   0.5 ms to update 10 of 1,000 rows. Preact and React work but cost 25–100× Node and emit far more mutations; they
   are not defaults. Solid needs its JSX compile step (babel-preset-solid, `generate: "universal"`) inside the
   language cartridge's build routine.
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
- **The presentation wire gets a second client.** The stack-machine ops (push-id, append-children(n)) suit Dioxus.
  Solid wants explicit node ids, so consider a variant with explicit ids once the JS MDK exists.
- **Engine hardening before relying on it:** build the engine on our toolchain (the generated crate fails on
  wasip3 today); give `load` a `result` so a user syntax error doesn't panic the guest; cap each micro's QuickJS
  heap (a busy micro reached about 35 MB); add per-call fuel accounting for the interpreter.
- **Web:** the same engine component runs through wcmp in the browser. The browser's own JS engine is not used
  for micros, which keeps one sandbox model; revisit if web performance demands it.

## Open questions

1. Does a hot swap keep the snapshot's framework heap and reload only user modules, or re-instantiate from the
   snapshot (about 2 ms either way in the spike)?
2. npm dependencies: vendored as datums at build time (bundled), or a curated set inside the snapshot?
3. Type checking on the phone: unmeasured. Is it needed on-device, or only where the editor runs?
4. Fuel: QuickJS interpreter steps versus wasm fuel. Which budget does a user see?

## First spike (proposed)

A `micro_counter` port in TS on the shared engine against the real `micro-client` WIT world in our Wasmtime fork:
- Solid universal rendering to the real presentation wire;
- one collection read and write;
- live source swap with state kept;
- mounted in the lab micro next to the Rust counter.

Measure memory per instance, edit-to-live time and fuel use. This settles the hardening list and the wire variant
question.
