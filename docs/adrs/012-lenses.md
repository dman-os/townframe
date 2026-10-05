# ADR 012: Lens recognition, selection, ingestion, and production

- **Status:** Accepted (replaces the superseded revision; superseded decisions are listed inline below).
- **Supersedes:** The original ADR 012's extension-first single-winner classification, ingest-without-render-base API, generic stubs, and render-failure-to-bounce contract.
- **Depends on:** ADRs 010–011, FDRs 001–004, document/branch identity and plug registration designs.
- **Related:** ADR 013 owns blob retention, chunk access, and byte transfer.

## 1. Role and scope

A lens interprets Daybook facets as file representations and file edits as document operations. It is Daybook-side format knowledge, not a generic bridge feature. Lenses can handle raw text/bytes, Markdown notes, Obsidian conventions, image/metadata sidecars, and later compound formats. A document may produce one or many files; a filesystem file is not necessarily a document boundary.

Plugs register lenses and their versions. Active lenses, selected versions, preferences, and parameters are deployment/checkout inputs. Basic text and blob lenses establish a small useful baseline; additional interpretations must compose without every lens knowing its competitors. The design does not require a universal perfect format detector before interop can work.

The pipeline has separate stages:

```text
eligible documents/files
    → requested identification signals
    → interested lenses and proposals
    → overlap detection and selection
    → complete preparation/validation
    → reviewed document operations or output plan
    → backend application and acknowledged correspondence
```

Rendering bytes is not required merely to report that an output exists or changed. Conversely, output discovery or validation may require input inspection for particular formats; those requirements must be declared rather than concealed as cheap metadata checks.

## 2. Selection scope and identification signals

The checkout first determines which documents or candidate filesystem paths are eligible. Dpath selection and by-ID selection are distinct checkout classes under ADR 011. Recognition does not expand authority or silently import unrelated files outside that selection.

For document projection, extract selected dpath facets, target references, facet tags, Body designation, and other declared metadata. A whole-document dpath facet uses the **Body facet** to identify primary content; selected facets may contribute to a compound representation. A selective dpath facet addresses its listed targets. A referenced document/facet is distinct from the holder of the addressing dpath facet.

For file ingestion, collect shared identification signals: path/extension, kind, size, directory shape, sibling relationships, available MIME evidence, magic/header bytes, and any additional declared inspection result. Size is a useful exclusion signal: a lens may reject zero-length or oversized inputs before parsing. Recognition can inspect content; extension alone is neither proof nor a mandatory first winner.

Signal acquisition is demand-driven and incremental. If no interested lens requests magic bytes, do not read them merely because a detector could. Start with cheap available metadata; gather further signals required by plausible candidates, sharing inspection work rather than letting every lens independently reopen the same file. Some formats require fuller parsing. The system should make such work visible and attributable, not claim all recognition is bounded to a header.

Model-based format identification can be added as a signal provider, including small specialized classifiers or later LLM-assisted inspection. This is **identification**, not the rendering contract. Model/version, input evidence, and selected interpretation should be recorded sufficiently to explain the decision; model output does not silently become facet content or an undeclared rendering dependency. Remote recognition, if offered, must have explicitly granted authority to inspect/transmit inputs; sandboxed codecs receive no ambient network authority. Such providers are extensions, not prerequisites or mandatory network calls for basic import.

## 3. Interest declarations and proposals

A lens declares interest in combinations of signals, rather than claiming a filename globally. For example, a workspace lens can require an XML manifest with a particular root, specific sibling files, and a directory layout. A basic XML lens may independently recognize each XML file. The specialized workspace is a stronger interpretation when its required structure exists, not because its plug happened to register first.

An applicable lens produces a proposal declaring:

- lens identity/version and relevant configuration;
- input documents/facets or files, including recognition-only context;
- matched signals and interpretation category/specificity;
- owned editable outputs/input entry set, distinguished from read-only context;
- write destinations for document operations;
- stable output slots, kinds, and proposed path structure;
- declared dependencies needed to reproduce or validate each output.

A context signal does not automatically grant ownership of that file. Two proposals may read the same manifest while owning disjoint outputs. Competing editable ownership and colliding proposed output paths must be detected before selection/application. One editable file cannot silently have several competing writers; a compound lens is responsible for its declared interpretation and destinations.

Proposals describe the complete output structure before visible publication. Formats whose entry sets need parsing perform that discovery during preparation; they do not stream newly discovered paths straight into the live checkout. The exact declaration language, signal registry, and treatment of dynamically discovered entry sets require implementation experiments. They do not change the complete-plan gate.

## 4. Hierarchy, overlap, and overrides

Selection evaluates current signals and configuration. Lenses do not enumerate competing lens IDs or declare a fallback edge to every overlapping plug. The selector provides shared hierarchy categories: explicit user choice, recognized workspace/compound interpretation, recognized individual format, and basic text/blob fallback. Specificity and configured preferences order applicable proposals within that framework.

Select disjoint proposals together. When a specialized workspace proposal owns `project.xml` and `settings.xml`, it can displace individual XML interpretations of those files while leaving an unrelated `notes.md` to another lens. Recognizing a directory does not confer ownership of its entire subtree.

Equally preferred overlaps use a stable configured default, not plug installation order; the selected interpretation and alternatives remain visible and overridable. Exact ordering within a category and the final deterministic tie-breaker remain selector design work. They must not depend on hash-map/scan order or a hidden 'try until something works' execution loop. A numeric scoring language or elaborate global optimizer is not required initially.

A checkout-local dot-prefixed configuration surface exposes preferences and overrides for document IDs, dpaths/paths, or recognized workspaces. Exact filename, match syntax, and scope precedence are technical choices. Local overrides can select raw text rather than a specialized structured interpretation when preserving/editing the literal representation is desired.

Selection is stateless in the useful sense: given the eligible current state, signals, configured lens versions, and overrides, recompute the proposals and winner. It does **not** discard historical bindings, output ownership, or render bases. Changing selection refuses dirty affected entry sets until ingestion or explicit discard. Clean outputs can be reprojected. Selection alone must not split/merge existing document histories or replace a document ID because recognition changed.

Installed lens upgrades do not automatically change the selected version. Explicit upgrade/reselection validates dirty-output safety and prepares reprojection. Provenance in checkout state records which version and configuration produced existing files; no separate textual 'lens log' is required. Old production versions remain pinned where supported or fail explicitly if unavailable, never silently render using a different codec.

## 5. Recognition is not execution fallback

'Not this format' is a recognition result: discard that proposal and consider other applicable interpretations, including text/blob. A selected lens that fails parsing/preparation, lacks input, or crashes is an execution failure. Do not silently select a different interpretation until one succeeds.

Interactive ingestion exposes selected lenses and intended documents/outputs after recognition and preparation; recognition may take time when expensive providers are explicitly enabled. An editor-based review can present that plan, but final CLI grammar/editor integration belongs to FDR 004. Failed operations retain their explanation for status and the next attempt. Users can fix the input, change selection/configuration, or explicitly request fallback/alternative attempts. Fallback is not the default response to recurring execution failures.

Watch uses configured selection without launching an interactive editor. A failure blocks automatic activity for the checkout under ADR 011; GUI surfaces can request retry, changed interpretation, exclusion, or recovery. Source bytes are preserved. Automatic watch creation is limited by selection/ignore rules, not concealed by silently ignoring lens failures.

## 6. Ingestion contract

Ingestion receives changed bytes/path/kind, the bound output slot, the **recorded render heads and production recipe**, relevant facets at those heads, sibling output observations, and declared inputs. It is not `ingest(&Delta)` with no base context. This permits a lens to edit a structured facet using its previous logical state rather than infer everything from current file bytes.

Prepare facet/document operations without publishing them. All selected lenses in the requested batch must prepare successfully before staging/publication begins. Large byte payloads may be staged on disk or in backend storage; 'buffer the batch' does not mean load a photo library into RAM. Revalidate the assumptions used by a reviewed plan before applying it.

Prepared facets must satisfy their schemas. Producing an invalid facet is a lens/preparation error, not a semantic bounce. Individually valid local and upstream states may later have an invalid Automerge merge; ADR 011 validates that candidate in memory and retains checkout branches rather than creating a separate bounce branch. Runtime errors, missing bytes, missing codec versions, and access denial remain distinct failures.

Edits to existing bound files preserve document identity. Replacing a JPEG updates its blob and lens-managed metadata in the declared owning documents. A dpath facet in A may reference a facet in B: content edits target B while path edits can target A's addressing facet. The proposal declares these destinations explicitly. There is no digest-equality document takeover and no automatic creation of a replacement document for changed bytes. New unbound input is a separate import case and may create documents.

Removal of one compound output is an edit offered to the lens, not automatic deletion of its owning document. If it cannot represent the edit, report a preparation failure and preserve the missing-output observation. Removal of a sole ordinary dpath output can remove the addressing dpath facet through checkout policy. Trash and document lifecycle remain explicit product operations.

## 7. Authority and cross-document inputs

Cross-document and cross-facet URLs are supported. A lens can inspect or produce content using resolved references; the generic vtree stores only opaque producer information, not the URL/facet interpretation. Missing references or bytes are availability failures, not empty file content.

Effective write access is the **least permissive access across every input document of the selected lens invocation**, including recognition-only context. This deliberately conservative lens-wide rule avoids claiming we know every internal effect of a codec. Declared write destinations must also be authorized. A later design may introduce finer-grained dependency roles, but v1 does not exempt read-only context from this rule.

Project filesystem permission bits accordingly and refresh them when authority changes. They discourage invalid edits but do not enforce Daybook authority; users can chmod files or replace them through writable parent directories. Stage/publish checks enforce actual authority. Preserve unauthorized local bytes and explain pending/unpublished work. Checkout-local branches retain previously projected bases as specified in ADR 011; independent local ownership is distinct from permission to publish upstream.

## 8. Production, versioning, and round trips

Separate **describe/plan** from **produce bytes**. Output plans identify stable slots, paths/kinds, and opaque recipes. A recipe includes every declared input that affects that output, lens version, execution compatibility, and configuration. It may use several facets/documents and recognition context; the old one-facet-per-path recipe restriction is insufficient.

Incremental production invalidates only affected outputs when dependency information permits. It is not a blanket guarantee of O(changed documents) without inspection: a workspace layout change can affect many outputs, and a codec may require a whole archive parse. Reporting a recipe change does not require producing its digest. A new lens version is meaningful even when resulting bytes are identical.

Determinism is per declared production inputs/version: the same inputs produce the same entry structure and bytes. Timestamps or external data that affect output must be declared inputs, not ambient nondeterminism. A nondeterministic codec is not rescued by calling its failure a bounce.

Round-trip stability remains required: ingesting an unchanged generated representation produces no document operation; repeated production from unchanged inputs is byte-identical. Exact preservation of arbitrary user formatting is **lens-specific**, not a universal round-trip law. Basic raw text preserves actual text bytes under its supported encoding contract; a structured XML lens may normalize formatting. After accepted ingestion, canonical output is the next projection, like a formatter operating upstream. Replace the file only if it still matches the accepted input; a newer edit must not be overwritten. Markdown-oriented lenses should preserve meaningful whitespace, but the bridge cannot provide that property on their behalf.

A virtual WASI receiver can install the producer/version reference without copying bytes. When accessed, it asks the bridge to obtain that version's bytes from the producer. Ranged access is optional: a lens can render once into local cache on first access and then serve ranges. Accurate file size may require lazy production when not already known. No generic stub or zero-length fiction stands in for unavailable content. Rendered caches remain local and are not synced as iroh-blobs artifacts.

## 9. Output plans and naming

One document or one selected interpretation can yield N files, directories (including empty directories), or sidecars. Lenses declare internal output names and stable slots. Checkout naming resolves collisions between independently selected output sets and platform limitations without rewriting dpath identities. `.d` spill allocation belongs to checkout naming, not a lens-specific license to rename incumbents.

Duplicate internal editable slots or contradictory kinds/paths within a proposal must be detected during plan validation. The final path assignment includes literal claims, sibling outputs, reserved checkout surfaces, and existing sticky bindings. Preflight complete plans before writing any visible member.

The filesystem backend may obtain all required bytes in staging storage then apply moves. Per-file atomicity does not mean atomic visibility of a multi-file set. Application failures have explicit progress/recovery under ADR 011; the old 'no partial renders ever reach disk' is replaced by 'no invalid/incomplete plan is deliberately published, and interrupted application is accounted for.'

## 10. Interface responsibilities and execution

The old `classify/ingest/render/diff_view` sketch needs more stages, not a new public symbol for every specialization:

| Stage | Responsibility |
|---|---|
| Interest declaration | Specify required signals, constraints and contextual shape. |
| Proposal | Declare interpretation, inputs, ownership, write destinations, output structure, and dependencies. |
| Preparation | Parse/validate edits or discover a complete output plan; return buffered operations/references, not published writes. |
| Production | Serve output bytes at a specific recipe/version, lazily and streamably where possible. |
| Diff view | Explain logical/rendered differences for status and ordinary branch resolution without inserting conflict markers into files. |

Concrete APIs remain advisory. Document/facet operation types live on the Daybook side; the bridge sees path/kind/opaque identities and byte access. Outputs cross the boundary as descriptions and production references, not eagerly populated byte buffers.

Codecs run in a WASI sandbox with declared document data and referenced files mounted through lazy bridge access. No ambient filesystem access outside mounts or network authority. Outputs are collected/prepared, not immediately copied into the checkout while the codec runs. Lens version and sandbox capability compatibility must be recorded so old production recipes are meaningful; exact packaging and ABI retention are implementation work. Recognition providers with additional capabilities are separately authorized, not an excuse to enlarge every codec's ambient authority.

## 11. Compound-format examples and limitations

- **Obsidian/workspace conventions:** recognition can combine note files and directory metadata without consuming every descendant. A workspace interpretation outranks applicable generic file lenses for its explicitly owned entries; context files need not be owned.
- **Image plus metadata:** a blob and metadata facets can produce an image plus generated sidecar. Changes affect declared owning facets and preserve document IDs. Blob GC must retain real payload dependencies separately from generated cache bytes.
- **Zip/.doc-style atomization:** preparation may inspect archive structure to declare extracted paths; reads can use ranges where the format/backend allows, otherwise cache a full decode. Ingest can rebuild an archive before publication. This is not a promise that every archive supports efficient random access.
- **EPUB/gallery:** cover images, text, and metadata may form multiple outputs with different dependency sets. A sidecar being regenerable does not itself ensure its blob dependency survives GC.

These remain motivating lens instances, not proof that today's API suffices. If a case cannot declare safe ownership, dependencies, or a complete plan, that is an interface/design finding—not permission to hide it behind 'lenses handle it.'

## 12. Remaining technical work

Implement a small selector using basic text/blob plus one specialized compound lens before designing a general constraint language. Test demand-driven signal sharing, stable overlap resolution, context-wide authority, loss of recognition, dirty selection switches, explicit upgrades, normalization, new file imports, multi-output removal, and lazy producer access.

Still open: exact signal/proposal representation; same-category tie-breaker; dynamic entry-set discovery; size/stat behavior for unproduced files; non-UTF-8 filename/encoding policy; customization facets versus local parameters and their precedence; retention of old codecs/inputs; sandbox ABI compatibility; by-ID creation/editing surfaces. Shared customization facets are plausible inputs, but not an unapproved global lens policy. These are bounded follow-up choices; the pipeline, preparation gate, failure model, and ownership/access boundaries above are settled architecture.

## 13. Byte-valued facet keys and URLs

Byte-valued IDs use a shared canonical UTF-8 key codec, not a dpath-only convention. Unicode stays literal without normalization; literal backslashes are doubled, LF/TAB/CR use `\n`/`\t`/`\r`, and other control or invalid UTF-8 bytes use lowercase `\xhh`. Unicode control characters are escaped through their UTF-8 bytes. Keys have no outer quotes, and alternate spellings are rejected. Ordinary facet keys are not implicitly interpreted as escape syntax; decoding is explicit when recovering a byte-valued ID.

URLs percent-encode the escaped facet key itself. Resolution percent-decodes the URL component once and looks up the resulting key literally, without byte-key decoding. Non-UTF-8 native names can use this codec without lossy conversion; a stable native-name byte representation for Windows and filesystem-adapter integration remain separate work. Rust's unspecified `OsStr::as_encoded_bytes` representation is not a persisted interchange format.
