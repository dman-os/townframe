# Sandbox packaging and distribution

Status: Proposal (a note for the operator; nothing here is decided)
Owner: Townframe maintainers
Updated: 2026-11-17
Scope: How software reaches a cartridge's `sandbox` (ADR 047 amendment D, proposed): which packages a cartridge declares, how a node gets them, and whether big_blobs can be the distribution channel. Not about running Linux inside wasm; backends are interchangeable under the sandbox contract.

## The question

A cartridge asks for a sandbox with some software in it (a Python, a compiler, a CLI). The node must provide that software on x86_64 and aarch64, on desktop, server, phone and web, without a per-architecture image the author builds by hand. This note proposes what a cartridge declares, where the bytes live, and what is missing.

## 1. Declare packages, resolve per host

A cartridge declares packages as Nix store paths by `(attr, version)`, for example `(python3, 3.12.7)`. It never declares a store path hash, which is system-specific. Each host resolves the declaration for its own system (x86_64-linux or aarch64-linux) against an index of which attr/version built at which store path, in the style of nixpkgs-multiverse (fzakaria, 2026-08).

This is how an x86-authored cartridge runs on arm with no conversion tooling: the author wrote a package list, not an image. Emulation is then only for the cases resolution cannot cover:
- opaque binaries the cartridge ships itself, which exist for one architecture;
- the roughly 12% of packages Hydra does not build for aarch64.

The index is a datum set (per host system, signed by whoever builds it), so a cartridge's closure on a given system is a lookup, not a build.

## 2. One shared store volume per node

Every sandbox on a node mounts one read-only, content-addressed store volume, as `/nix/store`. Two cartridges that both want `python3` share one copy; a path is present or absent, never partially installed. The volume is populated lazily, per store path, on first use (omnibin-style: "Every package is already installed", fzakaria, 2026-09-24): the mount presents the whole index and a path's bytes are fetched when something opens it. A microVM backend exposes it with virtio-fs; other backends mount it as their platform allows. Because it is read-only and keyed by content, it needs no per-sandbox copy and no cleanup beyond garbage collection of paths nobody pins.

## 3. big_blobs as a Nix substituter

A Nix binary cache is three things (fzakaria, "A Nix store is three functions", 2026-09-11): `nix-cache-info`, a `.narinfo` per store path, and the NAR archive it names. The narinfo signature covers `StorePath`, `NarHash`, `NarSize` and `References`, and not the URL. So where the NAR lives is not part of what is trusted, and:
- NARs can be blobs (big_blobs, content-addressed; ADR 026 holds the drawer inventories that say who has what);
- narinfos can be datums in a drawer, signed by the cache key, syncing with everything else;
- fetching works peer to peer and from relays, because trust is the signature, not the host.

The node serves the narinfo/NAR HTTP surface locally (or a stock `nix` is pointed at it as a substituter), so nothing on the Nix side changes. "One store, two frontends" (fzakaria, 2026-10-17) is related prior art; not reviewed for this note.

## 4. Gaps

- NARs are one compression stream each: no range reads, so lazy per-path fetch (section 2) cannot fetch part of a path, and nothing dedups across closures. A chunked, content-addressed layout (casita, erofs) would fix both; later option, large change.
- glibc closure weight: every closure drags one, and a small cartridge's first run pays for it. A shared store makes it a one-time cost per node, not per cartridge.
- Relocatable stores (a store not at `/nix/store`, needed for process-only backends such as proot on sideload builds) exist only as proposals. Without one, those backends need a bind mount at the fixed prefix.
- Cache signing keys: whose key a node trusts for the index and narinfos is a trust decision this note does not make.

## 5. Spikes, cheapest first

1. Serve a narinfo and its NAR from a drawer (narinfo as a datum, NAR as a blob) and have `nix copy --from` fetch it. Proves section 3 with no sandbox at all.
2. One x86-authored cartridge resolved natively on aarch64 under a microVM with a virtio-fs store. Proves sections 1 and 2. rewindvm (fzakaria, 2026-10-03) and trynix (2026-09-04) are related prior art for this spike; not reviewed for this note.

Do spike 1 first: it is small, and its result (does the signature really survive a changed URL, what does a drawer lookup cost per path) decides whether spike 2 is built on blobs or on a plain HTTP cache.

## Sources

fzakaria: "A Nix store is three functions" (2026-09-11); "Every package is already installed" / omnibin (2026-09-24); nixpkgs-multiverse (2026-08); "One store, two frontends" (2026-10-17); rewindvm (2026-10-03); trynix (2026-09-04). ADR 026 (big_blobs drawer inventories), ADR 047 amendment D.
