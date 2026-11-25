//! intxt's nodule: the handlers of `createIntxt` and `addToIntxt`, whose
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
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
use intxt_core::client::Caller;
use intxt_core::client::to_value;
use intxt_core::generic;
use intxt_core::methods::AddToIntxt;
use intxt_core::methods::AddedToIntxt;
use intxt_core::methods::CreateIntxt;
use intxt_core::methods::IntxtCreated;
use intxt_core::methods::IntxtError;
use method_core::ContextError;
use method_core::Method;
use serde_json::Value;
use std::collections::BTreeMap;

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
        None
    }
}

/// The caller of a request: the nodule's cabinet calls present the capability its own caller
/// presented, over the transport the request was delivered with, so the nodule adds no
/// authority. The command ids start from the caller's idempotency key (or the request id when it
/// sent none).
fn caller(cx: &big_ndk::Cx) -> Result<Caller<big_ndk::HttpHandle>, IntxtError> {
    Ok(Caller::new(
        cx.transport(),
        cx.request().clone(),
        Some(
            cx.caller_capability()
                .ok_or(ContextError::Unauthenticated)?,
        ),
        cx.idempotency_key()
            .map_or_else(|| cx.request().request_id.to_string(), str::to_owned),
    ))
}

/// `value` at `slot` of record `tag`; the key names the type, so the value carries no tag of its
/// own.
fn datum(value: Value, tag: &str, slot: &str) -> (DatumKey, Value) {
    (
        DatumKey::new(tag, slot.as_bytes()).expect("a slot taken from a valid key or `main`"),
        value,
    )
}

impl big_ndk::Handler<CreateIntxt> for Intxt {
    /// Four calls: a capability to create drawers, the drawer, a capability to write its
    /// descriptor document (the drawer's id is the descriptor's `DocId`), and the three
    /// descriptor datums in one write. A failure between the calls leaves a plain drawer, which a
    /// reader does not show as an intxt.
    async fn call(&self, cx: &big_ndk::Cx, input: CreateIntxt) -> Result<IntxtCreated, IntxtError> {
        let caller = caller(cx)?;
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
    /// `Caller::add_part`: writes the entry for the part and appends it to the body in one write,
    /// then enters the part's document in the intxt's drawer at Read.
    async fn call(&self, cx: &big_ndk::Cx, input: AddToIntxt) -> Result<AddedToIntxt, IntxtError> {
        let AddToIntxt { intxt, part } = input;
        Ok(caller(cx)?.add_part(&intxt, &part).await?.into())
    }
}

#[cfg(test)]
mod e2e;
