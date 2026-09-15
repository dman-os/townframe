# FDR 004 CLI — findings before rewrite

Source `docs/fdrs/004-workspace-cli.md`, checked against proposed FDR 001–003 in `.agents/drafts/` and drawers-branch FDR/ADRs. Nothing in `docs/` edited.

| Existing section | Disposition | Finding |
| --- | --- | --- |
| Intro/use cases | Retain | Photo library, Obsidian vault, agent task tracker are important. Remove review-history prose and hardlink-first / zero-copy guarantees until byte-retention contract exists. |
| §1 ambient ingest | Revise | Read-only `status`/`diff`/`log` must not publish (FDR 003 decision). Explicit mutations and watch ingest locally. “No remote activity unless `db sync`” conflicts with GUI auto-sync and incoming sync service; describe service state instead of pretending remote pulls happen after a CLI `commit`. |
| §2 init/mirror | Split | `db init` creates home drawer + root checkout selecting only it; node display name != directory name/identity. `db mirror <source>` for new-node onboarding retired: sibling-node flow has separate future FDR. Mirror is drawer retention. |
| §3 checkout/adopt/detach | Retain distinctions, correct wording | Track-only adopt cannot create document IDs or dpath facets. `--under` chooses candidate import addresses. `db detach` removes local binding but must not promise to delete `.dtree` without addressing recovery of unsaved/bounced edits and other retention reasons. |
| §3 import policy | Resolve | Track-only adoption is explicit; watch of later new files would silently import and potentially upload compiler output unless ignore/auto-import policy clear. “Defaults never trigger expensive work” contradicts automatic watch import. |
| §4 checkout spec | Rewrite | Checkout is not exclusively a query over dpaths; `/by-id` and future sources exist. `/trash` is a default exclusion convention, not reserved. Home checkout selects home drawer; stage branch selection per logical document. No policy permitting later arrivals to steal clean name. |
| §5 import | Restore one-shot semantics | Operator corrected the clarification: `db import <path>` need not create/use `.dtree` or be idempotent. Repeating a drag-and-drop import may create another document. Large-file deduplication/copy avoidance is a separate byte-handling concern; future duplicate-detection or sparse checkout features must not be smuggled into import identity. Default one-file/one-doc; explicit one-doc/many-files. |
| §6 command auto-commit/messages | Major rewrite | Observational commands do not ingest. No “last change ID across touched docs.” Named historical ranges are a candidate design, not a settled multi-document durable annotation. Local write vs incoming projection ordering and remote sync must be described honestly. |
| §7 sync | Revise | Drawer selection and per-node policy, not a universally mirrored node metadata/my-devices list. Node sibling setup out of scope. Sync may fetch metadata without all bytes; status must distinguish request/grant/admission/presence. |
| §8 status | Retain goals | Un-ingested changes, bounced work, unavailable document/blob, sync/import progress. Checkout file removal is not trash. Status must not publish. |
| §9 GUI | Revise | GUI and CLI share semantics, not necessarily verbs or one daemon for all checkouts; no promise that every GUI behavior maps to a CLI command until specified. |
| Open/resolved sections | Replace | Remove review archaeology, duplicated question numbers, obsolete `db mirror` and mandatory auto-commit. Keep actual unanswered UX/design decisions. |

Clarifications and questions before replacement:
1. **Clarified:** `db watch` is an explicitly started persistent process, not a side effect of `db adopt` and not a per-checkout daemon spawned by a CLI invocation. A node service already running in the background may observe adopted checkouts; per-checkout policy for new files remains to specify.
2. **Corrected:** `db import <path>` is a one-shot, non-idempotent conversion, including drag-and-drop. It does not create a checkout or require `.dtree`; repetition can create another document. Adopted-checkout ingest uses its own durable binding. Avoid duplicating large media bytes when possible, but byte reuse does not imply document-identity reuse.
3. **Message placement open:** `db commit -m` should place the label in change metadata, but the per-document metadata mechanism is not yet decided. Correlation through shared markers/short IDs in individual Automerge change messages is an unvalidated idea, not a product promise.
4. **Root checkout layout:** given that it selects only the home drawer and may include `/by-id`, what does a fresh root actually show before the first import? Is `/by-id` an optional surface with its own scope, or always present?
5. **Remote sync wording:** does `db commit` merely perform local ingest and optionally reconcile *already received* document changes, leaving network traffic to a separately running sync service? Recommend yes.
