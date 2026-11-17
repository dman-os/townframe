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

## 2. Getting files to toolchains (revised 2026-11-17)

The first draft proposed a full `wasi:filesystem` backed by datums. The operator's framing of sandboxes as local
resources (section 3) makes that unnecessary for toolchains and agents, which work on the sandbox's own disk, with
source imported in and artifacts exported out. What's left is the live working copy: an editor and an agent should
see the same files while a micro is being edited. That can be one mounted directory backed by the working drawer,
not a general datum filesystem. Small build routines that run as plain wasm guests (TS type stripping in the
engine) need no filesystem at all.

## 3. The sandbox (agents, toolchains, builds)

A sandbox is a **resource, like local state**: a container with its own disk, local to one device, not synced,
created and destroyed as a unit (operator, 2026-11-17). Agents and toolchains get an ordinary POSIX filesystem
inside it, at native speed. Their scratch files, `node_modules` and build caches never become datums.
- **Crossing the datum boundary** is explicit: source is imported into the sandbox, and artifacts and patches are
  exported back as datums. smolvm's "staged" mounts work this way: copy in on first boot, sync back on stop.
- **Platforms:**
  - Desktop: a microVM through smolvm (libkrun; boots OCI images in under 200 ms; virtiofs mounts; networking off
    by default, with egress allow-lists; branch and checkpoint; an embeddable Rust SDK; Apache-2.0). It needs
    hardware virtualization on every host (KVM, HVF or WHP).
  - Android: ordinary apps get no hardware virtualization, so the phone needs Linux or a POSIX userland inside
    wasm. Survey in progress: `.claude/scratch/linux-in-wasm-survey.md`.
- **Egress** follows the same grant model as micros and nodules.

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

## Nodules' first positions (2026-11-17, not yet settled with the operator)

- **Language runtimes:** an ADR 047 amendment adding a hosted component kind,
  `#hosted { runtime: <cartridge>/js@1#component, sources: [datum pins], entry }`. The runtime cartridge is a
  dependency with its own permbind. The build routine is an ordinary nodule routine. claytxt asked for a
  working-copy state, so live edits don't need a new envelope each time: `sources` refers to a mutable drawer
  until publish pins it.
- **Datum-backed wasi:filesystem:** owned by nodules, on the cabinet/docs layer. It's the same seam as R19
  (granted host directories for nodules), with one implementation serving both guest kinds.
- **Forks:** identity per ADR 028; grants never widen silently. A substitution starts with the original's grants
  narrowed to what the fork's manifest asks for; anything wider needs fresh consent. Lineage is a signed `lineage`
  datum beside the envelope, pinned by it. Un-forking means substituting the base again.
- **Sandbox egress:** uses the same grant vocabulary as micro and nodule egress.
- **Working copy, agreed with nodules:** `sources` is either a pinned set (published) or a reference to a working
  drawer (live). Grants stay bound to the manifest's permbinds and never to the sources. A working-copy reference is
  admitted only on nodes where the editing user has write access to that drawer, i.e. their own devices. A cartridge
  installed elsewhere, or published, carries pinned sources, so nobody runs code that changes under them.
  Publishing pins the drawer's heads into a new version, and the AOT build runs then.
- **Sandboxes as local state (agreed with nodules):** a sandbox is ADR 047's `localState` with a new kind,
  `sandbox`: node-local, not synced, its lifecycle tied to the install, with a disk quota. Egress and mounts are
  granted through the permbind/`uses` vocabulary. Import and export across the datum boundary are explicit
  operations under the routine's grants. The datum filesystem shrinks to one live working-copy mount, next to
  R19's granted host directories.
