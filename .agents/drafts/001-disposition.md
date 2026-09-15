# FDR 001 disposition — working review, not normative text

Source: `docs/fdrs/001-dpaths.md`. Proposed text: `.agents/drafts/001-dpaths-and-checkouts.md`. Nothing in `docs/` is edited.

| Existing section | Disposition | Reason / destination |
| --- | --- | --- |
| Context; path interop; object-model-agnostic pauperfuse | Retain and clarify | A document is not a file; the checkout consumes paths from dpaths **and** other Daybook projections, including by-id. Keep generic tree machinery ignorant of facets. |
| §1 path grammar, byte-exact comparison, implicit parents | Retain | Clarify leading slash; empty segments only between separators; UTF-8 import of non-UTF-8 FS names is open. Platform name adaptation remains a checkout obligation. |
| §1 no reserved Daybook namespaces | Retain | Checkout surfaces take precedence locally; `/trash` is an ordinary dpath and a default query exclusion, not a reserved Daybook namespace. |
| §1 tags | Retain with narrower claim | Prefix filtering is useful; a selected tag-like path still participates in projection. Not all semantic tags have to be dpaths. |
| §2 key `org.example.daybook.dpath//...`; facet-ref URL; full-doc, selective, cross-doc values | Retain | Include concrete examples and distinguish claiming doc from target doc. Key equality gives one slot per exact path per document, not cross-document import idempotency. Ref heads semantics and URL grammar remain owned by their documents. |
| §2 no lens hints in dpath; separate customization facet | Retain | Dpath identifies targets, not format choice; lens selection lives elsewhere. |
| §3 Rules 1–3 and Appendix A | Revise, not omit | Rule 1 contradicts sticky bindings and example 10; example 4 invents a spill without a child. Preserve all collision *classes* (claimant/claimant, file/child, suffix/literal, reserved surfaces) and examples, but do not assert an unproven total ordering. Naming algorithm is open in ADR; incumbent keeps path. |
| §3 timestamp/object-ID fresh checkout tie-break | Retain as candidate, not locked | State-dependent bindings are agreed; whether `dmeta.createdAt` is trustworthy and compatible with new signed facet model needs review. No cross-device identical-path guarantee. |
| §4 CRDT conflicts and markers | Superseded | No conflict markers in files; local semantic failure can bounce, remote invalid history cannot be retroactively bounced. Detailed acceptance/bounce policy belongs in document-layer ADR. |
| §5 `.dnode`, `.dtree`, `/by-id` | Retain | `/by-id` is a separate path source, not a dpath claim. Keep special-name precedence, no namespace reservation, and nested-checkout re-adoption concern; remove stale ADR 008/009 references. |
| §6 missing content/target/conflict | Retain as user-visible states | Do not promise materialized placeholder files for unavailable bytes; CLI surfaces status. Distinguish missing content from no target and invalid render. |
| §7 Obsidian/DCIM adoption; one-doc-per-file or one-doc/many files | Retain | Track-only adoption creates candidates, import creates documents/dpath claims. No blanket promise of hardlink safety or zero-copy: that is backend/blob policy. |
| §8 dpath removal and GC | Retain separation | Removing an address is not deleting a document; checkout-file deletion never implicitly deletes a document. Byte retention is not guaranteed merely because the document exists. |
| §9 lens hints, JSON view, multi-file output, cross-doc references | Retain with correction | One output's editable owner is one document; cross-doc targeting and missing references need explicit policy before claiming multi-doc editable files. Lens entry sets remain first class; exact selection/ingest implementation is ADR work. |
| Tags and Open Questions | Integrate or list actual unknowns | Remove review archaeology and resolved questions. Preserve single-doc facet granularity, primary display, collision naming, reserved surfaces, multi-lens overlap as pending where genuinely unsettled. |
| Appendix A examples | Rewrite fully | Preserve concrete adversarial cases with outcomes where settled and mark others unresolved rather than giving contradictory answers. |
| Appendix B URLs and nested checkout ownership | Retain concise boundary references | No speculative URL spec in this FDR. |
