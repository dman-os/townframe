# ADR 012: Lenses

- **Status:** Draft
- **Supersedes:** none
- **Depends on:** ADR 010 (bridge & vtree), ADR 011 (checkouts & reconciliation), FDR 001 (dpaths), FDR 003 (VC primitives), FDR 004 (workspace CLI), townframe-2 ADR 007 (plug manifests as drawer docs)
- **Depended on by:** ADR 013 (blob strategy)

## 1. What a lens is

The bridge (010) is format-agnostic; the **lens** is where format knowledge
lives. A lens is the daybook deployment's codec between **docs** (facet
sets on branches, tf2 ADR 007) and **file representations** (entries in a
checkout's trees):

- **ingest**: a file delta (from an fs backend change report) → doc
  operations (facet changes on the doc the path is bound to);
- **render**: a doc's state → the entries that represent it (paths, kinds,
  content);

Lenses are **per-format** (markdown notes, obsidian vault conventions,
image+sidecar pairs, later: zip/.doc atomization, epub) and registered by
**plugs** (plug manifests as drawer docs, tf2 ADR 007) — the set of active
lenses is a deployment/checkout configuration, not a hard-coded list.

The store and bridge never parse a file; every semantic act goes through a
lens. This is what keeps 010/011 honest about agnosticism.

## 2. Locked expectations (the contract)

1. **Idempotence / round-trip fidelity**: `render(ingest(delta))` is stable
   — re-rendering an unchanged doc produces byte-identical entries, and
   ingesting a rendered entry yields no doc ops. The daybook backend's
   heads-based change tracking **leans on this** (ADR 010 §2.1: "report is
   heads-delta → lens-rendered deltas" only works if re-renders are
   no-ops).
2. **Determinism per version**: render is a pure function of
   (doc state, lens version). Same inputs, same entries, same hashes.
3. **Totality or bounce**: a lens either fully renders a doc or **bounces**
   it (validation failure → device-local conflict branch per ADR 011 §5).
   **No partial renders ever reach disk.**
4. **Version stamping**: every rendered entry carries
   `LensAnno { doc_ref, lens_id, lens_ver }` (ADR 010 §3.1); every
   `Origin::Rendered` content reference carries the regenerating triple.
   Stale-projection detection is a field compare, and a lens upgrade
   re-renders only entries whose `lens_ver` is behind.
5. **One doc → N entries** (FDR 001 dpath claims): a doc may claim a single
   file, a directory of uniquely-named files, or `.d`-stacked variants; the
   lens owns the claim's shape, FDR 001 owns the collision rules.
6. **Incremental render**: lenses render *deltas* — only entries whose
   source content changed (the O(changed docs) projection cost that 010's
   commit cycle assumes).

## 3. The lens API (shape)

```rust
#[async_trait]
trait Lens: Send + Sync {
    fn id(&self) -> LensId;
    fn version(&self) -> LensVer;

    /// Which paths does this lens claim? Extension + content sniffing;
    /// first claim wins per the checkout's lens priority (§6).
    fn classify(&self, hint: &FileHint) -> Option<Claim>;

    /// File delta -> doc ops. Parse + validate; a failure is a BOUNCE
    /// (ADR 011 §5), never a partial apply.
    async fn ingest(&self, delta: &Delta) -> Result<Vec<DocOp>, Bounce>;

    /// Doc state -> entry deltas (incremental; see expectation 6).
    async fn render(&self, doc: &DocState, prev: Option<&DocState>)
        -> Res<Vec<EntryDelta>>;

    /// Stdout-shaped diff for the conflict viewer (ADR 011 §5.4) and
    /// `db diff` surfaces. No on-disk rendering, ever.
    fn diff_view(&self, a: &DocState, b: &DocState) -> Res<RenderedDiff>;
}
```

- `DocOp`/`DocState` are **daybook-side types** — the lens trait is generic
  over them (or they live in the deployment layer that instantiates the
  bridge); the bridge crate itself never names them (ADR 010 §5.1 posture:
  the crate is a bridge; lens *implementations* are deployment property).
- `EntryDelta` is vtree-shaped (010 §3.1): path, kind, `ContentRef` —
  rendered content arrives as streams, chunks assigned lazily (010 §2.4).

## 4. Ingest & render cycles (pseudocode)

```rust
// fs delta -> doc ops (the ingest half of a reconcile cycle)
async fn ingest(cx: &mut Cx, deltas: Vec<Delta>) -> Res<Vec<DocOp>> {
    let mut ops = vec![];
    for d in deltas {
        match cx.lens_for(&d)? {                 // classify → claim
            Some(lens) => ops.extend(lens.ingest(&d).await?),   // bounce ? 011 §5
            None => match d {
                // unclaimed paths in a track-only checkout: rep-level only
                Delta::Removed{..} => continue,
                other => cx.track_unclaimed(&other)?,   // no doc, no ingest
            },
        }
    }
    Ok(ops)
}

// doc ops -> entry deltas (the render half)
async fn render(cx: &mut Cx, touched: Vec<(DocRef, DocState)>) -> Res<Vec<EntryDelta>> {
    let mut out = vec![];
    for (doc_ref, state) in touched {
        let lens = cx.lens_of(&doc_ref)?;
        let prev = cx.prev_state(&doc_ref);       // for incrementality
        for mut e in lens.render(&state, prev).await? {
            e.lens = Some(LensAnno{ doc_ref, lens_id: lens.id(), lens_ver: lens.version() });
            out.push(e);
        }
    }
    Ok(out)
}
```

Both halves are consumed by the ordered walks of ADR 011 §6 — bulk ingest
streams batches of `ingest → fold` cycles behind one cursor; render feeds
transfer planning in canonical path order.

## 5. Conflict contract (with ADR 011 §5)

- A lens's **only** conflict surface is the `Bounce` (validation failure).
- `render_conflict` **does not exist**: lenses render last-good states and
  provide `diff_view` for the CLI conflict viewer (stdout, pick-a-version);
  no lens ever writes conflict content into real files.
- A bounced doc's last-good render stays in place until the user picks a
  version from its conflict branch.

## 6. Lens selection & per-checkout knobs

- **Classification precedence**: when several lenses could claim a path,
  the checkout's lens priority list decides (explicit in the `.dtree`
  spec); unknown extensions fall to content sniffing; still-unknown paths
  are *unclaimed* (tracked, not ingested — the adopt-only default).
- **Per-checkout spec knobs** (owed from FDR 004 §3/§4, ADR 011 §2): ignore
  patterns, per-lens parameters, auto-import behavior, lens priority.
  Schema lives in the checkout spec; defaults come from the lens
  registration; **no global (node-level) lens policy**.

## 7. wasi execution model

- Lens codecs run in the **wasi sandbox** (FDR 003 §2's `--stage` mechanics,
  ADR 010 §4.5's WasiBackend): declared inputs (doc bytes, referenced
  files) served lazily by the bridge as stub-capable backend; outputs
  collected as `EntryDelta`s.
- **Determinism expectation**: a lens run is a pure function of its
  declared inputs (same inputs, same outputs, same hashes) — this is what
  makes `Origin::Rendered` re-rendering sound (ADR 010 §2.4) and what makes
  lens versioning meaningful. Non-deterministic codecs (timestamps inside
  render output, etc.) must launder them into declared inputs or bounce.
- No ambient authority: no network, no fs outside the sandbox mounts.

## 8. Gallery & compound formats (deferred, directionally locked)

- **zip/.doc-style atomization**: a lens presents an archive as a
  directory of entries (read: extract lazily via chunk ranges; write:
  re-zip atomically per file). Expensive but stub-compatible.
- **epub & media + metadata sidecars**: blob entries with generated
  sidecars (EXIF-derived markdown, cover images); sidecar regen from
  `Origin::Rendered` means blob GC never orphans metadata.
- These are *lens instances*, not new machinery — the API above must
  suffice. If it doesn't, that's a finding against §3, not an extension.

## 9. Open questions

1. **Lens API exact surface** — `FileHint` contents (mtime? size? first-N
   bytes?), streaming granularity of `render` output, error taxonomy of
   `Bounce` (validation vs transient). _Blocks: implementation._
2. **Round-trip fidelity policy** — ingest normalizes (e.g. markdown
   formatting, frontmatter ordering): which representation is
   authoritative when re-render differs from the file on disk? Lean: doc
   state is authoritative; the fs copy converges on next materialize; a
   user's hand-edits *are* ingest input, so normalization is a lens policy
   knob. _Blocks: FDR 001 §open questions (dpath adoption) alignment._
3. **Customization facets** (deferred from FDR 001): user-tunable render
   templates/layout per doc class — facet-shaped configuration vs lens
   parameters. Lean: facet-shaped (they're shared opinions). _Blocks:
   FDR 001 revision._
4. **Intra-doc entry-set collisions** (deferred from FDR 001): two entries
   of one doc claiming the same name — Rule 1/2/3 mechanics owned by FDR
   001, but the lens-side API for *declaring* the entry set needs one pass.
   _Blocks: §3 `render` signature finalization._
5. **Sandbox capability versioning** — lens_ver pins codec behavior; does
   it also pin the wasi surface (so old lenses keep running)? Lean: yes,
   pin both; re-render-upgrade is opt-in per checkout. _Blocks: 011 §2
   spec knobs._