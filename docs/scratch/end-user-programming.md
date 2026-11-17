# End-user programming (claytxt lane, draft)

Status: draft for discussion, 2026-11-17. Written from the operator's direction in the week of 2026-11-16. Nothing
here is decided unless it is marked as decided. The cartridge-identity parts are input to the nodules lane's
cartridge design this week.

## Goal

daytxt is end-user programmable the way Emacs is. Any feature, built-ins included, can be read, changed and
extended from inside the app, by a human with no LLM as much as by an agent. Base features ship as cartridges, so
the path we use ourselves is the path users get. Forks and over-the-air updates come from the same machinery.

## Decided so far (operator)

- **Everything is editable.** We can't know ahead of time what users will want to change.
- **Edits apply live.** A micro restarts without the app restarting, and collections carry its state across.
- **Code runs in wasm**, sandboxed and under budgets.
- **Language:** if we commit to JS, it starts as JS in wasm: a QuickJS engine snapshotted with Wizer, plus Solid
  universal. Heavy micros, and micros nobody expects users to change, can be ported to Rust. Isolates are not
  pursued for now.
  - Spike evidence is in `.claude/scratch/spike-js-engine.md`: about 1.5 MB per micro with the framework baked
    into the snapshot, and about 2 ms from edit to live.
  - Zena stays a candidate (`.claude/scratch/spike-zena.md`). The survey is `.claude/scratch/ts-in-wasm-survey.md`.
- **Source edits are the escape hatch.** Forking a built-in's source is allowed until we have designed good hooks
  for it.

## 1. Language cartridges, not host features

The host doesn't know about toolchains. Support for a language ships as a cartridge:
- a **runtime component**, for example the shared JS engine, which every micro naming it instantiates (one compiled
  component, a heap per instance);
- a **build routine**, a nodule that turns source datums into artifact datums;
- type surfaces for the editor, such as `.d.ts` files generated from our WIT.

A micro's manifest names its runtime the way it names any dependency (for example `<DID>/js@1`). The host's job
is just "instantiate this component with these datums". Zena, Lua or Hoot would be more language cartridges. A
settled micro can be promoted to an AOT tier (StarlingMonkey with weval, Porffor, scriptc) by another build
routine, against the same micro interface (survey, "Tiered JS").

## 2. A datum-backed filesystem over WASI

Toolchains (tsc, esbuild-wasm, the Zena compiler) and agents expect a filesystem. Proposal: `wasi:filesystem`
preopens backed by datums, so a drawer appears as a directory tree. A toolchain then runs unmodified over a
cartridge's source and writes artifacts back as datums. Open points:
- the path-to-datum mapping and how it relates to ADR 035 datum references;
- whether writes are transactional per build;
- watching for changes, so incremental builds and editor reloads work;
- capability scoping: a build routine sees only the drawers it was granted.

## 3. The sandbox (agents, toolchains, builds)

Agents need a POSIX environment, and so do real toolchains. One sandbox serves both. Platform split:
- **Desktop:** a microVM, which smolvm provides: libkrun VMs that boot OCI images in under 200 ms; virtiofs mounts;
  networking off by default with egress allow-lists; branch and checkpoint of a live VM; an embeddable Rust SDK
  (`smolmachines`); Apache-2.0. It needs hardware virtualization on every host (KVM, HVF or WHP), and its docs don't
  mention Android.
- **Android:** ordinary apps don't get hardware virtualization; only some devices expose the Android
  Virtualization Framework. A phone needs a wasm-based POSIX environment, which has yet to be surveyed.
- **What both share:** the datum-backed filesystem (section 2) under both, mounted into the VM through virtiofs on
  desktop. A sandbox's egress follows the same grant model as micros and nodules, which is being designed with
  nodules.

## 4. Forks, overlays and lineage

If people fork rather than hook, forks have to be first-class:
- **Identity** stays the stable `<DID>/<name>`, with substitution (nodules' model): my fork replaces the original
  everywhere it is referenced.
- **Lineage** is metadata on top: a fork is an overlay recorded as "base `X@v` plus patches P" (per file or datum).
  That shows what was overlaid on what.
- **Upstream updates** rebase each overlay with a three-way merge. A conflict halts that micro's update, not the
  app's.
- **Mixing forks (composing overlays): deferred** (operator, 2026-11-17). It's powerful but serious work, and it's
  only easy in a pure-LLM workflow. Until then a fork has one base. Managing and understanding forks goes through
  intxt: an overlay's patches are a change the user can read, annotate and review like any other.
- **Questions for this week, with nodules:** how a substituted identity carries its grants; whether lineage lives
  in the cartridge envelope or beside it; how a user un-forks back to base.

## 5. Live-update semantics

- **Hot swap** is cheap (about 2 ms in the spike). The work is around it:
  - **State migration:** a new version changes its collection row shapes.
  - **Rollback:** go back to the last good version when the new one faults or crash-loops.
- **Safe mode:** a boot with every user change disabled, Emacs's `-q`.
- **Build failures** report diagnostics and leave the running version untouched.

## 6. Seams (hooks without forking)

Elisp's real power is hooks, advice, keymaps and variables, not editing core code. Our equivalents:
- registration collections as extension points;
- the command and shortcut registry (keyboard model, see the kit ADR item);
- user stylesheets in a cascade layer (styling item);
- settings as datums.

Built-ins should expose many more of these over time. Source edits cover the gap until they do.

## 7. Tooling inside the app (human first)

- **Start with:** a code editor (the CM6 port), ACP chat with agents, and the POSIX sandbox.
- **Later:** a full IDE/ADE.
- **Also needed:**
  - a REPL attached to a running micro;
  - an inspector: element to micro to source, its collections, its event and mutation log;
  - a console per micro, with stack traces mapped back to source.

## 8. Permissions and review for changed code

Does an edited built-in keep its grants? What does adding a capability look like to the user? LLM-made changes are
reviewed before they go live through the intxt review loop: agents propose micro edits as reviewable changes.
This depends on nodules' per-routine permission model.

## 9. Public surfaces

The micro WIT surface, collections, the mutation wire and the cartridge manifest become our SDK. Before launch,
audit and evaluate every public surface: versioning, compatibility rules, documentation.

## Order

1. Seams and the cartridge identity/lineage model, now, alongside the cartridge-authoring work with nodules.
2. The datum-backed filesystem and language cartridges: they decide the data model.
3. Live-update semantics.
4. Tooling and the sandbox, once the language is committed. The Android sandbox needs a survey first.
5. Permissions, after nodules' permission model.
6. The public-surface audit, before launch.
