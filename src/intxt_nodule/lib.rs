//! intxt's nodule: the handlers of `createIntxt`, `addToIntxt` and `summarizeIntxts`, whose
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
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::DatumRead;
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
use intxt_core::methods::IntxtSummary;
use intxt_core::methods::IntxtsSummarized;
use intxt_core::methods::MAX_SUMMARIZED;
use intxt_core::methods::SummarizeIntxts;
use intxt_core::methods::SummaryStatus;
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
    async fn route(
        &self,
        nsid: &str,
        cx: &big_ndk::Cx,
        body: big_ndk::XrpcBody,
    ) -> Option<big_ndk::Reply> {
        if nsid == CreateIntxt::XRPC.nsid {
            return Some(big_ndk::invoke::<Self, CreateIntxt>(self, cx, body).await);
        }
        if nsid == AddToIntxt::XRPC.nsid {
            return Some(big_ndk::invoke::<Self, AddToIntxt>(self, cx, body).await);
        }
        if nsid == SummarizeIntxts::XRPC.nsid {
            return Some(big_ndk::invoke::<Self, SummarizeIntxts>(self, cx, body).await);
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
    /// Five calls: a capability to create drawers, the drawer, a capability to create a document
    /// in it and the index document (the title and the empty body, written with it), then the
    /// drawer document's two datums, the marker and the pointer to the index, in one write. The
    /// drawer's own document is a relay-readable metadata root (ADR 019), so it holds nothing
    /// about the work. The marker is last: a failure between the calls leaves a plain drawer,
    /// which a reader does not show as an intxt.
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
        let index: BTreeMap<DatumKey, Value> = [
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
        let command = caller.command("create-index", &index)?;
        let index = caller.create_doc(&drawer_id, command, index).await?;
        let set: BTreeMap<DatumKey, Value> = [
            datum(
                to_value(&generic::Intxt::default()),
                generic::Intxt::ID,
                "main",
            ),
            datum(
                to_value(&generic::Index {
                    doc: index.to_string(),
                    extra: Default::default(),
                }),
                generic::Index::ID,
                "main",
            ),
        ]
        .into();
        let descriptor = caller.write_capability(&drawer_id, set.keys()).await?;
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
    /// `Caller::add_part`: writes the entry for the part into the intxt's index document and appends
    /// it to the body in one write, then enters the part's document in the intxt's drawer at Read.
    async fn call(&self, cx: &big_ndk::Cx, input: AddToIntxt) -> Result<AddedToIntxt, IntxtError> {
        let AddToIntxt { intxt, part } = input;
        Ok(caller(cx)?.add_part(&intxt, &part).await?.into())
    }
}

impl big_ndk::Handler<SummarizeIntxts> for Intxt {
    /// One read of each drawer's own document (the marker and the index pointer) and one of its
    /// index document, sequentially and in order. A drawer's failure is its own `unreadable` result, with the reason, and never the
    /// call's.
    async fn call(
        &self,
        cx: &big_ndk::Cx,
        input: SummarizeIntxts,
    ) -> Result<IntxtsSummarized, IntxtError> {
        if input.drawers.len() > MAX_SUMMARIZED {
            return Err(IntxtError::Invalid(format!(
                "{} drawers named, at most {MAX_SUMMARIZED}",
                input.drawers.len()
            )));
        }
        let caller = caller(cx)?;
        let mut results = Vec::with_capacity(input.drawers.len());
        for drawer in input.drawers {
            results.push(summarize(&caller, drawer).await);
        }
        Ok(IntxtsSummarized { results })
    }
}

/// The summary of `drawer`: found when its drawer document carries the intxt marker, not found
/// when it does not, unreadable when a read fails or the drawer document or index document of an
/// intxt is not what `createIntxt` writes.
async fn summarize(
    caller: &Caller<big_ndk::HttpHandle>,
    drawer: big_cabinet_core::drawer::protocol::DrawerId,
) -> IntxtSummary {
    let summary = |status: SummaryStatus| IntxtSummary {
        drawer: drawer.clone(),
        status,
        title: None,
        description: None,
        entries: None,
        updated: None,
        link: None,
        reason: None,
    };
    let unreadable = |reason: String| IntxtSummary {
        reason: Some(reason),
        ..summary(SummaryStatus::Unreadable)
    };
    let key = |tag: &str| DatumKey::new(tag, b"main").expect("`main` is a valid slot");
    let drawer_keys = [key(generic::Intxt::ID), key(generic::Index::ID)];
    let mut read = match caller
        .read_datums(&drawer, Some(drawer_keys.iter().cloned().collect()))
        .await
    {
        Ok((_, read)) => read,
        Err(error) => return unreadable(error.to_string()),
    };
    // The marker decides what the drawer is; the rest is then required.
    match read.remove(&drawer_keys[0]) {
        Some(DatumRead::Accepted { .. }) => {}
        Some(DatumRead::Absent { .. }) | None => return summary(SummaryStatus::NotFound),
        Some(other) => return unreadable(format!("the intxt marker is not readable: {other:?}")),
    }
    let index = match read.remove(&drawer_keys[1]) {
        Some(DatumRead::Accepted { value, .. }) => {
            match serde_json::from_value::<generic::Index>(value)
                .map_err(|error| error.to_string())
                .and_then(|index| {
                    index
                        .doc
                        .parse::<DocId>()
                        .map_err(|error| error.to_string())
                }) {
                Ok(index) => index,
                Err(error) => return unreadable(format!("the index pointer is invalid: {error}")),
            }
        }
        other => return unreadable(format!("the index pointer is not readable: {other:?}")),
    };
    let index_keys = [key(generic::Title::ID), key(generic::Body::ID)];
    let mut read = match caller
        .read_datums(&index, Some(index_keys.iter().cloned().collect()))
        .await
    {
        Ok((_, read)) => read,
        Err(error) => return unreadable(error.to_string()),
    };
    let mut accepted = |key: &DatumKey| match read.remove(key) {
        Some(DatumRead::Accepted { value, version, .. }) => Ok((value, version.clock)),
        other => Err(format!("{key} is not readable: {other:?}")),
    };
    let described = (|| {
        let (title, title_clock) = accepted(&index_keys[0])?;
        let (body, body_clock) = accepted(&index_keys[1])?;
        let title: generic::Title = serde_json::from_value(title).map_err(|e| e.to_string())?;
        let body: generic::Body = serde_json::from_value(body).map_err(|e| e.to_string())?;
        Ok::<_, String>((title, body, title_clock.max(body_clock)))
    })();
    match described {
        Ok((title, body, clock)) => IntxtSummary {
            title: Some(title.title),
            description: title.description,
            entries: Some(u32::try_from(body.order.len()).unwrap_or(u32::MAX)),
            updated: Some(clock),
            link: Some(generic::Intxt::reference(&drawer, []).to_string()),
            ..summary(SummaryStatus::Found)
        },
        Err(reason) => unreadable(reason),
    }
}

#[cfg(test)]
mod e2e;
