//! Typed, request/response access to the node's document datums.
//!
//! This crate deliberately does not cache or mirror document state. A caller supplies the
//! document capability for every operation; polling returns the node's change metadata and the
//! caller decides when to poll again or read a changed value.

use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumTag;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::ChangeHash;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::ListDatums;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::doc::QueriedDatumRef;
use big_cabinet_core::doc::QueryDatumRefs;
use big_cabinet_core::doc::DatumRefSelector;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::ResolveKeyRef;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_cabinet_core::doc::WriteReceipt;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use serde::Serialize;
use serde::de::DeserializeOwned;
use service_client_core_rs::Client;
use service_client_core_rs::UnaryTransport;
use service_core_rs::ClientError;
use service_core_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use thiserror::Error;

/// A Rust type that can be stored under one datum tag.
///
/// The tag is part of the datum's identity, not part of its JSON value. Slots are supplied to the
/// operations separately and may contain arbitrary bytes.
pub trait TypedDatum: Serialize + DeserializeOwned {
    const TAG: &'static str;
}

/// Errors from document calls and from validating JSON returned by the node.
#[derive(Debug, Error)]
pub enum Error {
    #[error("document request failed: {0}")]
    Request(#[from] ClientError<DocsError>),
    #[error("invalid datum key: {0}")]
    InvalidKey(String),
    #[error("failed to encode datum {key}: {message}")]
    Encode { key: DatumKey, message: String },
    #[error("failed to decode datum {key}: {message}")]
    Decode { key: DatumKey, message: String },
    #[error("network returned datum tag {actual} for {key}; expected {expected}")]
    WrongTag {
        key: DatumKey,
        expected: String,
        actual: String,
    },
    #[error("datum {key} was not accepted: {status}")]
    Unaccepted { key: DatumKey, status: String },
}

/// One non-cached page of changes for a typed datum tag.
#[derive(Debug, Clone, PartialEq)]
pub struct PollPage {
    /// `DatumEvent::Datum` entries are guaranteed to have the requested tag. `Leave` entries
    /// describe a document leaving the predicate and therefore have no datum key.
    pub events: Vec<DatumEvent>,
    /// Pass this value as `after` to the next call.
    pub next_cursor: u64,
    pub caught_up: bool,
}

/// Read one current value. An absent datum is represented by `Ok(None)`.
///
/// `PollDatums` carries versions and membership changes rather than values; use this operation
/// after a present poll event when the changed value is needed.
pub async fn read<T, D>(
    client: &Client<T>,
    context: RequestContext,
    capability: CapId,
    doc_id: DocId,
    slot: impl Into<Vec<u8>>,
) -> Result<Option<D>, Error>
where
    T: UnaryTransport<ReadDatums>,
    D: TypedDatum,
{
    let key = datum_key::<D>(slot)?;
    let response: DatumsRead = client
        .call(
            context,
            ReadDatums {
                capability,
                doc_id,
                at: At::Current,
                datums: Some(BTreeSet::from([key.clone()])),
                optimistic: false,
            },
        )
        .await?;

    for returned_key in response.datums.keys() {
        if returned_key.tag().as_str() != D::TAG {
            return Err(Error::WrongTag {
                key: returned_key.clone(),
                expected: D::TAG.to_owned(),
                actual: returned_key.tag().as_str().to_owned(),
            });
        }
    }

    let Some(datum) = response.datums.get(&key) else {
        return Ok(None);
    };
    match datum {
        DatumRead::Accepted { value, .. } => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|error| Error::Decode {
                key,
                message: error.to_string(),
            }),
        DatumRead::Absent { .. } => Ok(None),
        DatumRead::Pending { reason, .. } => Err(Error::Unaccepted {
            key,
            status: format!("pending: {reason}"),
        }),
        DatumRead::Rejected { reason } => Err(Error::Unaccepted {
            key,
            status: format!("rejected: {reason}"),
        }),
        DatumRead::Undecodable { reason, .. } => Err(Error::Unaccepted {
            key,
            status: format!("undecodable: {reason}"),
        }),
    }
}

/// List the keys of a document whose tag is `D::TAG`.
///
/// The node's `ListDatums` method returns versions, not values. This helper intentionally returns
/// only keys so callers do not mistake a listing for a value read.
pub async fn list<T, D>(
    client: &Client<T>,
    context: RequestContext,
    capability: CapId,
    doc_id: DocId,
) -> Result<Vec<DatumKey>, Error>
where
    T: UnaryTransport<ListDatums>,
    D: TypedDatum,
{
    let response = client
        .call(
            context,
            ListDatums {
                capability,
                doc_id,
                at: At::Current,
                with_authors: false,
            },
        )
        .await?;

    Ok(response
        .datums
        .into_keys()
        .filter_map(|key| (key.tag().as_str() == D::TAG).then_some(key))
        .collect())
}

/// Set and/or remove typed datums in one `WriteDatums` request.
///
/// The write uses `AtCurrent`, matching the plain shared layer. Callers that need a conditional
/// write can use `service_client_core_rs::Client` with `WriteDatums` directly.
pub async fn write<T, D, S, R>(
    client: &Client<T>,
    context: RequestContext,
    capability: CapId,
    doc_id: DocId,
    set: impl IntoIterator<Item = (S, D)>,
    remove: impl IntoIterator<Item = R>,
) -> Result<WriteReceipt, Error>
where
    T: UnaryTransport<WriteDatums>,
    D: TypedDatum,
    S: Into<Vec<u8>>,
    R: Into<Vec<u8>>,
{
    let mut encoded_set = BTreeMap::new();
    for (slot, value) in set {
        let key = datum_key::<D>(slot)?;
        let encoded = serde_json::to_value(value).map_err(|error| Error::Encode {
            key: key.clone(),
            message: error.to_string(),
        })?;
        encoded_set.insert(key, encoded);
    }

    let mut encoded_remove = BTreeSet::new();
    for slot in remove {
        encoded_remove.insert(datum_key::<D>(slot)?);
    }

    Ok(client
        .call(
            context,
            WriteDatums {
                capability,
                command: None,
                doc_id,
                mode: WriteMode::AtCurrent,
                set: encoded_set,
                remove: encoded_remove,
            },
        )
        .await?)
}

/// Perform one long-poll request for `D::TAG` and return its change metadata and next cursor.
///
/// The predicate is supplied by the caller because it carries the authorization anchor. The
/// helper adds the requested tag to the datum selection and performs no retry or polling loop.
pub async fn poll<T, D>(
    client: &Client<T>,
    context: RequestContext,
    capability: CapId,
    predicate: Predicate,
    after: u64,
    limit: u32,
    hold_ms: u32,
) -> Result<PollPage, Error>
where
    T: UnaryTransport<PollDatums>,
    D: TypedDatum,
{
    let tag = datum_tag::<D>()?;
    let response = client
        .call(
            context,
            PollDatums {
                capability,
                predicate,
                datums: DatumSelection::Of {
                    tags: BTreeSet::from([tag]),
                    keys: BTreeSet::new(),
                },
                after,
                limit,
                hold_ms,
            },
        )
        .await?;

    let events = response
        .events
        .into_iter()
        .map(|event| match &event {
            DatumEvent::Datum { datum_key, .. } if datum_key.tag().as_str() != D::TAG => {
                Err(Error::WrongTag {
                    key: datum_key.clone(),
                    expected: D::TAG.to_owned(),
                    actual: datum_key.tag().as_str().to_owned(),
                })
            }
            _ => Ok(event),
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(PollPage {
        events,
        next_cursor: response.through,
        caught_up: response.caught_up,
    })
}

/// A content-addressed datum [`resolve`] found: the document the copy was read from, at which
/// heads, and its value.
#[derive(Debug, Clone, PartialEq)]
pub struct Resolved<D> {
    pub doc_id: DocId,
    pub heads: Vec<ChangeHash>,
    pub value: D,
}

/// Resolve a `#keyRef` to `D` under `slot` (ADR 035 §2.9): read a copy from any document the
/// capability reaches whose value hashes to the key. `D::TAG` must be a content-addressed kind.
///
/// The node answers `KeyRefUnresolved` both when no copy exists and when the caller can read none,
/// so the two are not told apart here either: both are `Ok(None)`, like an absent datum in
/// [`read`].
pub async fn resolve<T, D>(
    client: &Client<T>,
    context: RequestContext,
    capability: CapId,
    slot: impl Into<Vec<u8>>,
) -> Result<Option<Resolved<D>>, Error>
where
    T: UnaryTransport<ResolveKeyRef>,
    D: TypedDatum,
{
    let key = datum_key::<D>(slot)?;
    let response = match client
        .call(
            context,
            ResolveKeyRef {
                capability,
                key: key.clone(),
            },
        )
        .await
    {
        Ok(response) => response,
        Err(ClientError::Method(DocsError::KeyRefUnresolved { .. })) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let value = serde_json::from_value(response.value).map_err(|error| Error::Decode {
        key,
        message: error.to_string(),
    })?;
    Ok(Some(Resolved {
        doc_id: response.doc_id,
        heads: response.heads,
        value,
    }))
}

/// Every `#keyRef` naming `D` under `slot`, whichever document holds the referrer: "what
/// references this commit" is one call. `drawer_id` narrows the answer to one drawer's documents.
///
/// The answer names referrers, not their values: read the ones you need with [`read`].
pub async fn referrers<T, D>(
    client: &Client<T>,
    context: RequestContext,
    capability: CapId,
    drawer_id: Option<DocId>,
    slot: impl Into<Vec<u8>>,
) -> Result<Vec<QueriedDatumRef>, Error>
where
    T: UnaryTransport<QueryDatumRefs>,
    D: TypedDatum,
{
    let key = datum_key::<D>(slot)?;
    Ok(client
        .call(
            context,
            QueryDatumRefs {
                capability,
                drawer_id,
                selector: DatumRefSelector::Key { key },
            },
        )
        .await?
        .refs)
}

fn datum_tag<D: TypedDatum>() -> Result<DatumTag, Error> {
    DatumTag::new(D::TAG).map_err(|error| Error::InvalidKey(error.to_string()))
}

fn datum_key<D: TypedDatum>(slot: impl Into<Vec<u8>>) -> Result<DatumKey, Error> {
    DatumKey::new(D::TAG, slot).map_err(|error| Error::InvalidKey(error.to_string()))
}

#[cfg(test)]
mod tests;
