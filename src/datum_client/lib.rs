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
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::ListDatums;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::doc::ReadDatums;
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

fn datum_tag<D: TypedDatum>() -> Result<DatumTag, Error> {
    DatumTag::new(D::TAG).map_err(|error| Error::InvalidKey(error.to_string()))
}

fn datum_key<D: TypedDatum>(slot: impl Into<Vec<u8>>) -> Result<DatumKey, Error> {
    DatumKey::new(D::TAG, slot).map_err(|error| Error::InvalidKey(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::doc::DatumPage;
    use big_cabinet_core::doc::DatumVersion;
    use big_cabinet_core::doc::ObservedDatum;
    use serde::Deserialize;
    use service_core_rs::Envelope;
    use service_core_rs::ResponseEnvelope;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;

    const NOTE_TAG: &str = "org.example.note";

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Note {
        text: String,
    }

    impl TypedDatum for Note {
        const TAG: &'static str = NOTE_TAG;
    }

    #[derive(Default)]
    struct FakeTransport {
        reads: Mutex<VecDeque<DatumsRead>>,
        listings: Mutex<VecDeque<big_cabinet_core::doc::DatumListing>>,
        writes: Mutex<VecDeque<WriteReceipt>>,
        polls: Mutex<VecDeque<DatumPage>>,
        requests: Mutex<Vec<RecordedRequest>>,
    }

    struct FakeTransportHandle(Arc<FakeTransport>);

    #[derive(Debug)]
    enum RecordedRequest {
        Read(ReadDatums),
        List(ListDatums),
        Write(WriteDatums),
        Poll(PollDatums),
    }

    impl service_client_core_rs::UnaryTransport<ReadDatums> for FakeTransportHandle {
        async fn call(
            &self,
            request: Envelope<ReadDatums>,
        ) -> Result<ResponseEnvelope<DatumsRead>, ClientError<DocsError>> {
            self.0
                .requests
                .lock()
                .unwrap()
                .push(RecordedRequest::Read(request.params));
            Ok(self
                .0
                .reads
                .lock()
                .unwrap()
                .pop_front()
                .expect("fake read response")
                .into())
        }
    }

    impl service_client_core_rs::UnaryTransport<ListDatums> for FakeTransportHandle {
        async fn call(
            &self,
            request: Envelope<ListDatums>,
        ) -> Result<ResponseEnvelope<big_cabinet_core::doc::DatumListing>, ClientError<DocsError>>
        {
            self.0
                .requests
                .lock()
                .unwrap()
                .push(RecordedRequest::List(request.params));
            Ok(self
                .0
                .listings
                .lock()
                .unwrap()
                .pop_front()
                .expect("fake list response")
                .into())
        }
    }

    impl service_client_core_rs::UnaryTransport<WriteDatums> for FakeTransportHandle {
        async fn call(
            &self,
            request: Envelope<WriteDatums>,
        ) -> Result<ResponseEnvelope<WriteReceipt>, ClientError<DocsError>> {
            self.0
                .requests
                .lock()
                .unwrap()
                .push(RecordedRequest::Write(request.params));
            Ok(self
                .0
                .writes
                .lock()
                .unwrap()
                .pop_front()
                .expect("fake write response")
                .into())
        }
    }

    impl service_client_core_rs::UnaryTransport<PollDatums> for FakeTransportHandle {
        async fn call(
            &self,
            request: Envelope<PollDatums>,
        ) -> Result<ResponseEnvelope<DatumPage>, ClientError<DocsError>> {
            self.0
                .requests
                .lock()
                .unwrap()
                .push(RecordedRequest::Poll(request.params));
            Ok(self
                .0
                .polls
                .lock()
                .unwrap()
                .pop_front()
                .expect("fake poll response")
                .into())
        }
    }

    fn doc() -> DocId {
        DocId::new([7; 32])
    }

    fn key(tag: &str, slot: &str) -> DatumKey {
        DatumKey::from_text(tag, slot).unwrap()
    }

    fn accepted(value: serde_json::Value) -> DatumRead {
        DatumRead::Accepted {
            value,
            version: DatumVersion::default(),
            authors: Vec::new(),
        }
    }

    fn read_response(key: DatumKey, datum: DatumRead) -> DatumsRead {
        DatumsRead {
            heads: Vec::new(),
            datums: BTreeMap::from([(key, datum)]),
        }
    }

    fn capability() -> CapId {
        CapId::from_bytes([3; 16])
    }

    #[tokio::test]
    async fn read_decodes_json_and_records_the_explicit_capability() {
        let transport = Arc::new(FakeTransport::default());
        let expected = Note {
            text: "hello".to_owned(),
        };
        transport.reads.lock().unwrap().push_back(read_response(
            key(NOTE_TAG, "main"),
            accepted(serde_json::to_value(&expected).unwrap()),
        ));
        let client = Client::new(FakeTransportHandle(Arc::clone(&transport)));

        assert_eq!(
            read::<_, Note>(
                &client,
                RequestContext::root(),
                capability(),
                doc(),
                String::from("main"),
            )
            .await
            .unwrap(),
            Some(expected)
        );
        let requests = transport.requests.lock().unwrap();
        let RecordedRequest::Read(request) = &requests[0] else {
            panic!("expected read request");
        };
        assert_eq!(request.capability, capability());
        assert_eq!(
            request.datums,
            Some(BTreeSet::from([key(NOTE_TAG, "main")])),
        );
    }

    #[tokio::test]
    async fn missing_datum_is_none_and_remove_is_encoded_in_one_write() {
        let transport = Arc::new(FakeTransport::default());
        transport.reads.lock().unwrap().push_back(read_response(
            key(NOTE_TAG, "main"),
            DatumRead::Absent {
                version: DatumVersion::default(),
            },
        ));
        transport.writes.lock().unwrap().push_back(WriteReceipt {
            doc_id: doc(),
            heads: Vec::new(),
        });
        let client = Client::new(FakeTransportHandle(Arc::clone(&transport)));
        assert_eq!(
            read::<_, Note>(
                &client,
                RequestContext::root(),
                capability(),
                doc(),
                String::from("main"),
            )
            .await
            .unwrap(),
            None
        );
        write(
            &client,
            RequestContext::root(),
            capability(),
            doc(),
            [(
                String::from("new"),
                Note {
                    text: "value".to_owned(),
                },
            )],
            [String::from("old")],
        )
        .await
        .unwrap();

        let requests = transport.requests.lock().unwrap();
        let RecordedRequest::Write(request) = &requests[1] else {
            panic!("expected write request");
        };
        assert_eq!(request.mode, WriteMode::AtCurrent);
        assert_eq!(request.remove, BTreeSet::from([key(NOTE_TAG, "old")]));
        assert_eq!(
            request.set[&key(NOTE_TAG, "new")],
            serde_json::json!({"text": "value"})
        );
    }

    #[tokio::test]
    async fn list_filters_the_listing_to_the_typed_tag() {
        let transport = Arc::new(FakeTransport::default());
        transport
            .listings
            .lock()
            .unwrap()
            .push_back(big_cabinet_core::doc::DatumListing {
                heads: Vec::new(),
                datums: BTreeMap::from([
                    (key(NOTE_TAG, "one"), DatumVersion::default()),
                    (key("org.example.other", "two"), DatumVersion::default()),
                ]),
                authors: BTreeMap::new(),
            });
        let client = Client::new(FakeTransportHandle(Arc::clone(&transport)));

        assert_eq!(
            list::<_, Note>(&client, RequestContext::root(), capability(), doc())
                .await
                .unwrap(),
            vec![key(NOTE_TAG, "one")]
        );

        let requests = transport.requests.lock().unwrap();
        let RecordedRequest::List(request) = &requests[0] else {
            panic!("expected list request");
        };
        assert_eq!(request.capability, capability());
    }

    #[tokio::test]
    async fn poll_selects_the_typed_tag_and_returns_the_server_cursor() {
        let transport = Arc::new(FakeTransport::default());
        transport.polls.lock().unwrap().push_back(DatumPage {
            events: vec![
                DatumEvent::Datum {
                    revision: 12,
                    doc_id: doc(),
                    datum_key: key(NOTE_TAG, "main"),
                    datum: ObservedDatum::Present {
                        doc_heads: Vec::new(),
                        version: DatumVersion::default(),
                    },
                },
                DatumEvent::Leave {
                    revision: 13,
                    doc_id: doc(),
                },
            ],
            through: 13,
            caught_up: true,
        });
        let client = Client::new(FakeTransportHandle(Arc::clone(&transport)));

        let page = poll::<_, Note>(
            &client,
            RequestContext::root(),
            capability(),
            Predicate::IsDoc { doc_id: doc() },
            9,
            20,
            500,
        )
        .await
        .unwrap();
        assert_eq!(page.next_cursor, 13);
        assert!(page.caught_up);
        assert_eq!(page.events.len(), 2);

        let requests = transport.requests.lock().unwrap();
        let RecordedRequest::Poll(request) = &requests[0] else {
            panic!("expected poll request");
        };
        assert_eq!(request.capability, capability());
        assert_eq!(request.after, 9);
        assert_eq!(request.limit, 20);
        assert_eq!(request.hold_ms, 500);
        assert_eq!(
            request.datums,
            DatumSelection::Of {
                tags: BTreeSet::from([DatumTag::new(NOTE_TAG).unwrap()]),
                keys: BTreeSet::new(),
            }
        );
    }

    #[tokio::test]
    async fn a_wrong_tag_from_the_network_is_an_error() {
        let transport = Arc::new(FakeTransport::default());
        transport.reads.lock().unwrap().push_back(read_response(
            key("org.example.other", "main"),
            accepted(serde_json::json!({"text": "wrong row"})),
        ));
        let client = Client::new(FakeTransportHandle(Arc::clone(&transport)));

        let error = read::<_, Note>(
            &client,
            RequestContext::root(),
            capability(),
            doc(),
            String::from("main"),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::WrongTag { .. }));
    }
}
