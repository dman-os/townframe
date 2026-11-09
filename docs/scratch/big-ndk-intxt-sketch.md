# big_ndk, top down: the intxt nodule as written against the kit we want

Status: Draft (sketch; nothing here compiles yet)
Owner: Townframe maintainers
Updated: 2026-11-10
Scope: The intxt nodule written first, against the `big_ndk` API it wants, so the nodule decides the kit's surface (ledger D38; `intxt-node-requirements.md`). The intxt lane owns the datums; the ones here are stand-ins marked as such. NSIDs use `org.intxt.*` as a placeholder authority.

## What the nodule is for

Agents produce and annotate reviews; the user reviews on the phone through the intxt micro; the agent waits for the user's answer. The nodule holds the rules (what a review is, how annotations anchor, how a revision supersedes another) so that agents, the CLI, MCP and the micro all go through the same verbs instead of writing datums directly. Its methods are what `day mcp` turns into tools and what `day intxt …` turns into subcommands, so the verbs are named for agents.

| Method | Kind | Caller | Does |
|---|---|---|---|
| `org.intxt.createReview` | procedure | agent | Creates a review doc in a drawer from a source (a granted checkout or an uploaded patch) |
| `org.intxt.reviseReview` | procedure | agent | Adds a revision when the agent pushes new commits; earlier annotations stay anchored to their revision |
| `org.intxt.annotate` | procedure | agent, micro | Adds an annotation anchored to a revision, file, side and line range: a note, a concern, or a verification result |
| `org.intxt.respond` | procedure | micro | The user's verdict: approve, or request changes with comments |
| `org.intxt.awaitResponse` | procedure | agent | Long-polls until the user responds or annotates after a cursor; the agent's "wait for review" |
| `org.intxt.getReview` | query | anyone with read | One review: revisions, annotations, verdicts |

## The declarations crate (`intxt_decls`)

Native and wasm-buildable; pure types plus the manifest builder (plug-authoring.md, crate layout). The kit's macros carry the NSID on the type and set the datum macro's runtime path to `datum_core`.

```rust
big_ndk::authority!(AUTHORITY = "org.intxt");

// ---- datums (STAND-INS: the intxt lane owns these) ----
#[big_ndk::datum(authority = AUTHORITY)]
pub struct Review { pub title: String, pub state: ReviewState, pub revisions: Vec<RevisionId> }

#[big_ndk::datum(authority = AUTHORITY)]
pub struct Revision { pub id: RevisionId, pub base: String, pub head: String, pub patch: BlobRef }

#[big_ndk::datum(authority = AUTHORITY)]
pub struct Annotation { pub revision: RevisionId, pub anchor: Anchor, pub author: Author, pub body: AnnotationBody }

// ---- methods: the request type is the contract (D35: `Method`, kinds query/procedure) ----
#[big_ndk::procedure(authority = AUTHORITY, output = ReviewCreated, error = IntxtError)]
pub struct CreateReview {
    /// The drawer the review goes in, presented by the caller (see "Whose authority").
    pub drawer: CapId,
    pub title: String,
    pub source: Source,
}

pub enum Source {
    /// Read from a checkout the user granted to the nodule at enable (R19).
    Checkout { mount: MountName, base: String, head: String },
    /// An agent without a granted checkout uploads `git diff` output as a blob (R8).
    Patch { base: String, head: String, patch: BlobRef },
}

#[big_ndk::procedure(authority = AUTHORITY, output = Annotated, error = IntxtError)]
pub struct Annotate { pub review: CapId, pub revision: RevisionId, pub anchor: Anchor, pub body: AnnotationBody }

pub enum AnnotationBody {
    Note { text: String },
    Concern { text: String, severity: Severity },
    /// A command the agent ran and its result, with the log as a blob. intxt's
    /// "verified results attached to diffs without trusting CI".
    Verification { command: String, exit_code: i32, log: BlobRef },
}

#[big_ndk::procedure(authority = AUTHORITY, output = Responses, error = IntxtError)]
pub struct AwaitResponse { pub review: CapId, pub after: Cursor, pub hold_ms: u32 }

#[big_ndk::query(authority = AUTHORITY, output = ReviewView, error = IntxtError)]
pub struct GetReview { pub review: CapId }

// ---- the cartridge manifest, generated from the declarations ----
pub fn cartridge() -> Cartridge {
    CartridgeBuilder::<Intxt>::new()
        .nodule(|n| n
            .implements::<CreateReview>().implements::<ReviseReview>().implements::<Annotate>()
            .implements::<Respond>().implements::<AwaitResponse>().implements::<GetReview>()
            .requires::<docs::CreateDoc>().requires::<docs::WriteDatums>()
            .requires::<docs::PollDatums>().requires::<blobs::PutBlob>()
            // R19: a named directory the user binds to a real path at enable; read-only.
            .mount("checkout", FsAccess::Read, "The repository whose changes you review"))
        .datum::<Review>().datum::<Revision>().datum::<Annotation>()
        .build()
}
```

The manifest goes into the built component as a custom section (D38: `day` reads it from the wasm and never builds source).

## The nodule crate

```rust
use big_ndk::prelude::*;
use intxt_decls::*;

struct Intxt;

impl Handler<CreateReview> for Intxt {
    async fn call(&self, cx: &Cx, req: CreateReview) -> Result<ReviewCreated, IntxtError> {
        let patch = match req.source {
            Source::Patch { patch, .. } => patch,
            Source::Checkout { mount, base, head } => {
                // A WASI preopen named in the manifest; `cx.mount` fails if the user didn't bind it.
                let repo = cx.mount(&mount)?;
                let diff = git::diff(&repo, &base, &head)?;          // see Q2
                cx.blobs().call(PutBlob::bytes(diff)).await?.blob
            }
        };
        let receipt = cx.docs().call(CreateDoc {
            capability: req.drawer,                                  // the caller's authority, not ours
            command: cx.command(),                                   // idempotency key -> command id
            placement: req.drawer.placement(),
            datums: datums![Review { .. }, Revision { patch, .. }],
        }).await?;
        Ok(ReviewCreated { review: receipt.doc_id })
    }
}

impl Handler<AwaitResponse> for Intxt {
    async fn call(&self, cx: &Cx, req: AwaitResponse) -> Result<Responses, IntxtError> {
        // PollDatums already long-polls (`hold_ms`); the method narrows it to the user's
        // annotations and verdicts. The kit clamps hold_ms to the caller's deadline.
        let page = cx.docs().call(PollDatums {
            capability: req.review,
            predicate: Predicate::sole_doc(req.review.doc()),
            datums: select![Annotation where author = User, Verdict],
            after: req.after.into(), limit: 0,
            hold_ms: cx.hold_within_deadline(req.hold_ms),
        }).await?;
        Ok(Responses::from(page))
    }
}

big_ndk::export_nodule!(Intxt => [CreateReview, ReviseReview, Annotate, Respond, AwaitResponse, GetReview]);

#[cfg(test)]
mod tests {
    // The native backend serves the same handlers in-process against a real node (no wasm),
    // with a temp dir bound to the `checkout` mount. Never a production path (principle 6).
    #[tokio::test]
    async fn agent_creates_and_user_responds() {
        let node = big_ndk::testing::node().with_cartridge(intxt_decls::cartridge(), Intxt)
            .bind_mount("checkout", fixture_repo()).boot().await;
        let review = node.call(CreateReview { .. }).await.unwrap();
        // ...
    }
}
```

## What the sketch asks of `big_ndk`

| Kit surface | Why intxt needs it | Exists today |
|---|---|---|
| `#[big_ndk::procedure/query]` producing `Method` types | Every verb above | `Operation` + `operations!` in `service_core_rs` (rename to `Method`, D35) |
| `Handler<M>` + `export_nodule!` routing `/xrpc/<nsid>` | The handlers | `Handler<Op>` exists natively; the guest export is hand-written (`test_nodule_guest`) |
| `Cx`: typed clients `docs()`, `blobs()` over wasi:http to `dispatch.local` | Every write | `Client<T>` exists; the wasi:http transport doesn't |
| `cx.command()` from the idempotency key | Retried agent calls don't duplicate reviews | Header exists; mapping doesn't |
| Deadline propagation, `cx.hold_within_deadline` | `AwaitResponse` must not outlive its caller | Header exists; guests forward it by hand (ledger: should be automatic, as in gRPC) |
| `cx.mount(name)` over WASI preopens | `Source::Checkout` | wash-runtime `volume_mounts`; nothing in `big_nodule_wasm` (R19) |
| `CartridgeBuilder` with `requires`, `mount`, manifest in a custom section | Install and consent | `big_plugs_core::Plug`, hand-built in tests |
| `big_ndk::testing::node()` | The test module | `big_node` e2e fixtures, not packaged for authors |

## Decisions the sketch forces

1. **Whose authority writes the review? The caller's (proposal).** Every method takes the target as a capability the caller presents (`drawer`, `review`), and the nodule passes it straight into the docs call. The nodule's own grants are used only for its private state. Otherwise the nodule is a confused deputy: an agent holding only the intxt methods could make the nodule write into any drawer the *nodule* can reach. This is the ocap convention (D4) and matches how every `big_cabinet` method already takes `capability` in its body. It needs dispatch to let a nodule present a capability it was handed, which is what `test_nodule_guest`'s `relay` route already does.
2. **git inside the nodule (Q2).** Reading a checkout means a git implementation in wasm32-wasip3 (gitoxide is the candidate; whether it builds for wasip3 is unverified). If it doesn't, `Source::Patch` is the alpha's only source and `mount` waits.
3. **Waiting is a long-poll method.** `AwaitResponse` wraps `PollDatums`' existing `hold_ms`; an MCP tool call simply blocks on it. Subscriptions can come later without changing the verb.
4. **Revisions are datums in one doc**, not big_cabinet branches: a revision supersedes, it doesn't fork. (Branches stay available if intxt later wants "suggested change" forks.)
5. **Harness extension:** a Claude Code plugin needs no code from this lane beyond `day mcp`. It bundles the MCP server entry (`day mcp --capability <intxt-scoped cap>`), a skill telling the agent when to create, revise and await reviews, and optionally a hook that calls `createReview` after `gh pr create`.

## Build order this implies

1. `Method` crate (D35), then `big_ndk` macros generating `Method` types and `export_nodule!`, with `test_nodule_guest`'s echo/relay re-expressed on it as the smoke test.
2. The wasi:http client transport and `Cx` (deadline, idempotency, command id).
3. `big_ndk::testing::node()` and the native backend, so the intxt nodule is developed natively first.
4. The intxt nodule's `Source::Patch` path end to end, then `CartridgeBuilder` and the custom-section manifest.
5. `mount` and R19, gated on the gitoxide check.
