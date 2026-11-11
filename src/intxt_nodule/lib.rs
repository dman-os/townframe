//! intxt's nodule: the handlers of `createIntxt`, `addToIntxt` and `recordObservations`, whose
//! request, output and error types are `intxt_core::methods`. `intxt_nodule_guest` exports
//! [`Intxt`] as the wasm component, and this crate's native tests serve the same value, so both
//! backends run one implementation. The crate has no host-only code and builds for both targets
//! (`big_ndk` picks its backend by target); the cartridge that admits it to a node is
//! `intxt_core::cartridge`.

// Booting a node in the tests (`big_ndk_testing`) makes rustc evaluate deep auto-trait chains
// through its services' futures, which the default limit of 128 cuts off with a warning.
#![cfg_attr(test, recursion_limit = "256")]
//!
//! Every call the handler makes presents the capability its own caller presented as the
//! authority (`as_caller`), not the nodule's: the drawers and documents belong to whoever asked,
//! and the nodule is not a confused deputy. Every `CommandId` is the caller's idempotency key
//! plus a per-step suffix, so a retried call with the same key reuses the same commands and the
//! services that dedupe by command answer the earlier result. Dispatch itself does not dedupe on
//! the key: it only forwards it to the provider.

use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::AddDoc;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::ListDocDrawers;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use big_cabinet_core::reference::DatumReference;
use big_cabinet_core::reference::ReferenceDocId;
use intxt_core::capture::RecordedIds;
use intxt_core::generic;
use intxt_core::work;
use intxt_core::methods::AddToIntxt;
use intxt_core::methods::AddedToIntxt;
use intxt_core::methods::CreateIntxt;
use intxt_core::methods::IntxtCreated;
use intxt_core::methods::IntxtError;
use intxt_core::methods::RecordObservations;
use method_core::ContextError;
use method_core::Method;
use method_core::Unary;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use service_client_core_rs::Client;
use service_client_core_rs::xrpc::Xrpc;
use service_core_rs::HttpMetadata;
use service_core_rs::xrpc::bearer_header_value;
use std::collections::BTreeMap;

/// The operations of other authorities the handler calls, which the cartridge requests.
pub const REQUIRED_OPERATIONS: [&str; 8] = [
    MintDrawerCapability::XRPC.nsid,
    CreateDrawer::XRPC.nsid,
    MintDocCapability::XRPC.nsid,
    WriteDatums::XRPC.nsid,
    ReadDatums::XRPC.nsid,
    ListDocDrawers::XRPC.nsid,
    AddDoc::XRPC.nsid,
    CreateDoc::XRPC.nsid,
];

/// The intxt nodule's handler. A fresh value serves each request (`big_ndk::export_nodule!`
/// cannot be used here because the guest wrapper, not this crate, owns the export), so it holds
/// no state.
#[derive(Debug, Default, Clone)]
pub struct Intxt;

impl big_ndk::Nodule for Intxt {
    async fn route(&self, nsid: &str, cx: &big_ndk::Cx, body: &[u8]) -> Option<big_ndk::Reply> {
        if nsid == CreateIntxt::XRPC.nsid {
            return Some(big_ndk::invoke::<Self, CreateIntxt>(self, cx, body).await);
        }
        if nsid == AddToIntxt::XRPC.nsid {
            return Some(big_ndk::invoke::<Self, AddToIntxt>(self, cx, body).await);
        }
        if nsid == RecordObservations::XRPC.nsid {
            return Some(big_ndk::invoke::<Self, RecordObservations>(self, cx, body).await);
        }
        None
    }
}

mod record;

/// What a handler needs to call as its caller: the caller's capability and the base of the
/// command ids.
pub(crate) struct Caller<'a> {
    cx: &'a big_ndk::Cx,
    capability: &'a str,
    /// The idempotency key, or the request id when the caller sent none (nothing to dedupe
    /// against, so every call is its own).
    command_base: String,
}

impl<'a> Caller<'a> {
    pub(crate) fn new(cx: &'a big_ndk::Cx) -> Result<Self, IntxtError> {
        Ok(Self {
            cx,
            capability: cx
                .caller_capability()
                .ok_or(ContextError::Unauthenticated)?,
            command_base: cx
                .idempotency_key()
                .map_or_else(|| cx.request().request_id.to_string(), str::to_owned),
        })
    }

    /// The command id of `step` writing `content`: the base, the step's fixed suffix and a short
    /// digest of the request's content (canonical JSON). An identical retry is the same command
    /// and dedupes; a write whose content changed since a partial first attempt is a new command
    /// instead of a `CommandReused` refusal.
    pub(crate) fn command(
        &self,
        step: &str,
        content: &impl Serialize,
    ) -> Result<CommandId, IntxtError> {
        let digest = blake3::hash(&serde_json::to_vec(content).expect("a request serializes"));
        CommandId::try_from(format!(
            "{}/{step}/{}",
            self.command_base,
            &digest.to_hex().as_str()[..16]
        ))
        .map_err(|error| {
            IntxtError::Invalid(format!("the idempotency key makes no command id: {error}"))
        })
    }

    /// Calls `input` presenting the caller's capability as the authority of the call.
    pub(crate) async fn call<M: Unary>(&self, input: M) -> Result<M::Output, IntxtError> {
        let metadata = HttpMetadata {
            authorization: Some(bearer_header_value(self.capability)),
            idempotency_key: None,
        };
        Client::new(Xrpc(self.cx.transport()))
            .call_with(self.cx.next_hop(), metadata, input)
            .await
            .map_err(|error| IntxtError::Upstream {
                operation: M::XRPC.nsid.to_owned(),
                reason: error.to_string(),
            })
    }

    pub(crate) async fn doc_capability(&self, scope: DocScope) -> Result<CapId, IntxtError> {
        Ok(self
            .call(MintDocCapability {
                scope,
                rights: Rights::USE,
                subject: None,
            })
            .await?
            .capability_id)
    }

    /// The document's heads and the accepted value of `key` in it at those heads.
    async fn read(
        &self,
        doc: &DocId,
        key: &DatumKey,
    ) -> Result<(Vec<ChangeHash>, Value), IntxtError> {
        let capability = self
            .doc_capability(DocScope {
                read: [doc.clone()].into(),
                ..DocScope::default()
            })
            .await?;
        let mut read = self
            .call(ReadDatums {
                capability,
                doc_id: doc.clone(),
                at: At::Current,
                datums: Some([key.clone()].into()),
                optimistic: false,
            })
            .await?;
        match read.datums.remove(key) {
            Some(DatumRead::Accepted { value, .. }) => Ok((read.heads, value)),
            other => Err(IntxtError::Invalid(format!(
                "{key} in {doc} is not readable: {other:?}"
            ))),
        }
    }
}

/// `value` at `slot` of record `tag`; the key names the type, so the value carries no tag of its
/// own.
fn datum(value: Value, tag: &str, slot: &str) -> (DatumKey, Value) {
    (
        DatumKey::new(tag, slot.as_bytes()).expect("a slot taken from a valid key or `main`"),
        value,
    )
}

pub(crate) fn to_value(value: &impl Serialize) -> Value {
    serde_json::to_value(value).expect("a record serializes")
}

impl big_ndk::Handler<CreateIntxt> for Intxt {
    /// Four calls: a capability to create drawers, the drawer, a capability to write its
    /// descriptor document (the drawer's id is the descriptor's `DocId`), and the three
    /// descriptor datums in one write. A failure between the calls leaves a plain drawer, which a
    /// reader does not show as an intxt.
    async fn call(&self, cx: &big_ndk::Cx, input: CreateIntxt) -> Result<IntxtCreated, IntxtError> {
        let caller = Caller::new(cx)?;
        let drawers = caller
            .call(MintDrawerCapability {
                scope: DrawerScope {
                    drawers: Default::default(),
                    create: true,
                },
                rights: Rights::USE,
                subject: None,
            })
            .await?
            .capability_id;
        let drawer_id = caller
            .call(CreateDrawer {
                capability: drawers,
                command: caller.command("create-drawer", &CreateLayout::SingleGroup)?,
                layout: CreateLayout::SingleGroup,
            })
            .await?
            .drawer_id;
        let descriptor = caller
            .doc_capability(DocScope {
                write: [drawer_id.clone()].into(),
                ..DocScope::default()
            })
            .await?;
        let set: BTreeMap<DatumKey, Value> = [
            datum(
                to_value(&generic::Intxt::default()),
                generic::Intxt::ID,
                "main",
            ),
            datum(
                to_value(&generic::Title {
                    title: input.title,
                    description: input.description,
                    extra: Default::default(),
                }),
                generic::Title::ID,
                "main",
            ),
            datum(
                to_value(&generic::Body {
                    order: Vec::new(),
                    extra: Default::default(),
                }),
                generic::Body::ID,
                "main",
            ),
        ]
        .into();
        caller
            .call(WriteDatums {
                capability: descriptor,
                command: Some(caller.command("write-descriptor", &set)?),
                doc_id: drawer_id.clone(),
                mode: WriteMode::AtCurrent,
                set,
                remove: Default::default(),
            })
            .await?;
        Ok(IntxtCreated { drawer_id })
    }
}

impl big_ndk::Handler<AddToIntxt> for Intxt {
    /// Writes the entry for the part and appends it to the body in one write, based on the heads
    /// the body was read at, so two writers adding different parts both land in the body's list;
    /// then enters the part's document in the intxt's drawer at Read. The entry's slot is the
    /// part's own identity (ADR 040: the worktree's UUID, the repository datum's slot, the
    /// harness's session id), so two writers adding the same part write one entry.
    ///
    /// The drawer the part's document is lent from is the first, ordered by drawer id, that the
    /// caller reaches it through at Read or above, other than the intxt's own: the drawers
    /// protocol does not say which of them are the node's local ones, so none is preferred.
    async fn call(&self, cx: &big_ndk::Cx, input: AddToIntxt) -> Result<AddedToIntxt, IntxtError> {
        let caller = Caller::new(cx)?;
        let AddToIntxt { intxt, part } = input;
        let ReferenceDocId::Concrete(part_doc) = part.document() else {
            return Err(IntxtError::Invalid(
                "a part is named by its document id, not `self`".to_owned(),
            ));
        };
        let (_, part_value) = caller.read(part_doc, part.key()).await?;
        let tag = part.key().tag().to_string();
        let malformed = |error: serde_json::Error| {
            IntxtError::Invalid(format!("{part} is not a valid `{tag}`: {error}"))
        };
        let (entry_key, entry) = if tag == work::Worktree::ID {
            let worktree: work::Worktree = serde_json::from_value(part_value).map_err(malformed)?;
            datum(
                to_value(&work::WorktreeEntry {
                    worktree: part.clone(),
                    extra: Default::default(),
                }),
                work::WorktreeEntry::ID,
                &worktree.id,
            )
        } else if tag == work::GitRepo::ID {
            datum(
                to_value(&work::RepoEntry {
                    repo: part.clone(),
                    extra: Default::default(),
                }),
                work::RepoEntry::ID,
                // A fork and its upstream share a root commit, so the entry is keyed by the
                // repository's document, not by the `gitRepo` slot (ADR 040 §8).
                &part_doc.to_string(),
            )
        } else if tag == work::AgentSession::ID {
            let session: work::AgentSession =
                serde_json::from_value(part_value).map_err(malformed)?;
            datum(
                to_value(&work::SessionEntry {
                    session: part.clone(),
                    extra: Default::default(),
                }),
                work::SessionEntry::ID,
                &session.session_id,
            )
        } else {
            return Err(IntxtError::Invalid(format!(
                "`{tag}` is not a part an intxt includes: name a worktree, gitRepo or agentSession datum"
            )));
        };

        let body_key = DatumKey::new(generic::Body::ID, b"main").expect("`main` is a valid slot");
        let (body_heads, body_value) = caller.read(&intxt, &body_key).await?;
        let mut body: generic::Body = serde_json::from_value(body_value)
            .map_err(|error| IntxtError::Invalid(format!("{intxt} has no valid body: {error}")))?;
        let entry_ref = DatumReference::new(ReferenceDocId::SelfDoc, entry_key.clone());
        if !body.order.contains(&entry_ref) {
            body.order.push(entry_ref.clone());
        }
        let descriptor = caller
            .doc_capability(DocScope {
                write: [intxt.clone()].into(),
                ..DocScope::default()
            })
            .await?;
        let set: BTreeMap<DatumKey, Value> = [
            (entry_key, entry),
            datum(to_value(&body), generic::Body::ID, "main"),
        ]
        .into();
        caller
            .call(WriteDatums {
                capability: descriptor,
                command: Some(caller.command("write-entry", &(&set, &body_heads))?),
                doc_id: intxt.clone(),
                mode: WriteMode::AtHeads { heads: body_heads },
                set,
                remove: Default::default(),
            })
            .await?;

        let from = caller
            .call(ListDocDrawers {
                doc_id: part_doc.clone(),
            })
            .await?
            .drawers
            .into_iter()
            .find(|drawer| drawer.access >= DrawerAccess::Read && drawer.drawer_id != intxt)
            .ok_or_else(|| IntxtError::NotReachable(part_doc.clone()))?
            .drawer_id;
        let drawers = caller
            .call(MintDrawerCapability {
                scope: DrawerScope {
                    drawers: [
                        (intxt.clone(), DrawerAccess::Write),
                        (from, DrawerAccess::Read),
                    ]
                    .into(),
                    create: false,
                },
                rights: Rights::USE,
                subject: None,
            })
            .await?
            .capability_id;
        caller
            .call(AddDoc {
                capability: drawers,
                reach: drawers,
                command: caller.command("add-doc", &(&intxt, part_doc))?,
                drawer_id: intxt.clone(),
                doc_id: part_doc.clone(),
                access: DrawerAccess::Read,
            })
            .await?;
        Ok(AddedToIntxt {
            entry: DatumReference::new(ReferenceDocId::Concrete(intxt), entry_ref.key().clone()),
        })
    }
}

#[cfg(test)]
mod e2e;
