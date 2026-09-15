# ADR 010 fidelity pass (rev. 2 → proposed rewrite)

Proposed replacement: `.agents/drafts/010-vtree-store.md`. No edit to `docs/`.

| Old ADR content | Disposition |
|---|---|
| Rev. 1 `DirNode` DAG rejection, relational path index, current-state rows | Retained with revision history and SQL sketch. |
| Backend reports vs its rep; ordered merge join; path index; generation | Retained, with generation checked for cursor resume. Replaced N-way merge with pairwise base. |
| Origin vs content vs claim; shapes, symlink target, identity continuity and digest encoding | Restored explicitly. Claim alone no longer licenses removal; receiver proof required. |
| Stat-cached FS scan; small-file digest; large-file dirty identity; producer recipes | Restored as policy with explicit cost caveat; multi-input recipes permitted. |
| `avail` / generic stubs | Intentionally removed by user decision. Missing bytes are not a virtual filesystem entry. |
| Rendered bytes local; no iroh-blobs upload; recipe vs digest identity | Restored. |
| ByReference/ProduceAgain/CopyBytes; streaming; hardlink conditions | Restored with stronger safety rule for mutable checkout inode. |
| Schema, SQLite feature, query files/plan twins, blob encoding, 1M-row concern | Restored for rep rows; pair-base schema intentionally open, stub migration called out. |
| `report`, `accept`, `may_remove`, `locate`, `read`, `materialize`, `link_from`, `remove`, `verify` | Restored as advisory responsibilities; exact signatures await pairwise implementation. |
| Echo suppression, unclaimed target-only paths, nonrecursive removal | Restored and tied to actual-result acknowledgements. |
| Error categories, concrete object-safe boundary error | Restored. |
| VCS exclusions, blob versions/chunks, lens/CLI scope, costs table | Restored, with truthful qualification on verification cost. |
| O(1) cursor crash recovery, rep-before-cycle as merge base | Superseded: cursor requires generation; base must reflect last common applied correspondence. |

**Remaining design risk:** a single `Applied` acknowledgement for FS → Daybook is not the same as a new common pair base; the draft now keeps directional partial progress until both endpoints' outcomes are known. The representation and crash recovery of that partial progress need concrete examples/tests when implementing ADR 011. The report and interface sketches are not a tested implementation; in particular path-only backends, output identity across renames, and whether a single opaque pair-base row can represent splits/merges need design tests before schema freeze.
