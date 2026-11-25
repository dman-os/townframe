# End-user programming (claytxt lane, draft)

Owner: claytxt lane; operator reviews.
Updated: 2026-11-17
Scope: End-user programming of the app, including core micros; language question open.

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

## 2. Files for toolchains: dpaths, volumes and containers (revised again 2026-11-17)

Withdrawn: the earlier datum-backed `wasi:filesystem` and the working-copy mount. Materializing datums as files
is **dpaths** (FDR 001, `docs/fdrs/001-dpaths.md` on `feat/dpaths`; pre-`dtxt:` terminology), which the nodules lane owns. Toolchains and agents get files through
Docker-shaped local resources (operator):
- a **filesystem volume** (VFS) as a local-state resource;
- a **sandbox/container** as a named local-state resource, which mounts volumes and dpaths.
Small build routines that run as plain wasm guests (TS type stripping in the engine) need no filesystem at all.

## 3. The sandbox (agents, toolchains, builds)

A container is a named local-state resource: local to one device, not synced, created and destroyed as a unit,
with its own disk and the volumes it mounts. With nodules: ADR 047's `localState` gains `sandbox` (and `volume`)
kinds, with disk quotas and with egress and mounts granted through the permbind/`uses` vocabulary.
- **One contract, platform-blind (operator):** the sandbox resource hides platform differences. We pick a baseline every
  platform can run (likely a native arm64/x86_64 Linux userland from an OCI image, with volumes and an exec/IO API)
  and define the contract against it. Every stronger backend is an optimization under the same contract:
  more isolation, faster boot, checkpointing.
- **Desktop and servers:** microVMs, for the security gain. smolvm (libkrun) boots OCI images in under 200 ms,
  uses virtiofs mounts, has networking off by default with egress allow-lists, an embeddable Rust SDK, and is
  Apache-2.0. It needs KVM, HVF or WHP.
- **Android:** whatever native containerization runs arm64 Linux inside a normal app. The app sandbox (per-app UID,
  SELinux, seccomp) is the boundary. Candidates: proot (ptrace syscall translation, no root, as Termux's proot-distro
  does), or the Android Virtualization Framework where a device exposes it. Open constraint: apps targeting API 29+
  cannot exec binaries from their writable data directory. Survey: `.claude/scratch/android-sandbox-survey.md`.
- **Android findings** (`.claude/scratch/android-sandbox-survey.md`): proot runs unmodified arm64 userlands without
  root, near-native, but is **not a security boundary** (same app UID; containers are directories). AVF is closed
  to ordinary apps. Apps targeting API 29+ cannot exec from writable data, Play requires target API 36, and a
  runtime-downloaded native rootfs is a Play-policy risk. So the contract must report its **isolation level** as a
  capability (VM vs process), and the Android backend depends on distribution: proot for sideload, F-Droid or
  enterprise builds; something under Play's interpreter exception for Play builds (open question for the
  operator: is Play required?).
- **Rejected for Android:** wasm-hosted Linux. WebVM/CheerpX, v86 and container2wasm all emulate a CPU on top of
  wasm, and WASIX is tied to Wasmer (`.claude/scratch/linux-in-wasm-survey.md`).

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
2. Language cartridges and the volume/sandbox resource kinds: they decide the data model.
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
- **Datum filesystem:** withdrawn; dpaths covers materializing datums as files.
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
  operations under the routine's grants.

## Sandbox baseline: wasm everywhere, Nix for packages (2026-11-17, discussion)

- **Web nodes** need a sandbox too, and in a browser only wasm can host one. That makes an emulated Linux in wasm
  the baseline every platform can run (web, Play builds, desktop). Native containers and microVMs are
  optimizations under the same contract.
  - trynix (fzakaria, 2026-09-04): x86_64 Linux under QEMU-in-wasm in a browser, resumed from a pre-booted
    snapshot; Hydra-built closures fetched from cache.nixos.org into an in-memory `/nix/store`, shared into the VM
    over 9p; packages can be added while it runs; about 1.5–7.5 s to a shell; about a 1.5 GiB tab cap; serial
    console only.
- **Play:** a wasm guest, and Linux emulated inside it, likely falls under Play's VM/interpreter exception, unlike
  a downloaded native ELF. That is a policy reading, unverified.
- **ISA:** portability comes from packaging, not one ISA. Cartridges reference Nix store paths or flake outputs
  rather than opaque binaries, and each host substitutes its own system (`x86_64-linux`, `aarch64-linux`) from the
  binary cache. Opaque-binary carts fall back to emulation.
- **Nix rather than a distro base:** a sandbox is a minimal init plus a `/nix/store` volume plus a workspace
  volume, and its contents are a list of store paths. That gives about 310k package versions, side-by-side
  versions without conflicts, packages added live, content addressing that matches datum pins (closures could live
  in big_blobs), a store shared across sandboxes, and cross-arch for free. Nix-on-Droid shows Nix over proot on
  Android. The cost: glibc closures are heavy compared with musl/Alpine.
- **Spikes:**
  1. trynix's approach under our web build.
  2. A Nix closure on proot on a phone.
  3. container2wasm/QEMU under Wasmtime, to see whether one wasm backend serves web, Play and desktop.
