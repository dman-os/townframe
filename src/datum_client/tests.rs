// The unit tests are kept in a sibling module so production edits do not conflict with test setup.
use super::*;
use big_cabinet_core::doc::DatumPage;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::QueriedDatumRefs;
use big_cabinet_core::doc::ResolvedKeyRef;
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
    resolves: Mutex<VecDeque<Result<ResolvedKeyRef, DocsError>>>,
    ref_queries: Mutex<VecDeque<QueriedDatumRefs>>,
    requests: Mutex<Vec<RecordedRequest>>,
}

struct FakeTransportHandle(Arc<FakeTransport>);

#[derive(Debug)]
enum RecordedRequest {
    Read(ReadDatums),
    List(ListDatums),
    Write(WriteDatums),
    Poll(PollDatums),
    Resolve(ResolveKeyRef),
    QueryRefs(QueryDatumRefs),
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
    ) -> Result<ResponseEnvelope<big_cabinet_core::doc::DatumListing>, ClientError<DocsError>> {
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

impl service_client_core_rs::UnaryTransport<ResolveKeyRef> for FakeTransportHandle {
    async fn call(
        &self,
        request: Envelope<ResolveKeyRef>,
    ) -> Result<ResponseEnvelope<ResolvedKeyRef>, ClientError<DocsError>> {
        self.0
            .requests
            .lock()
            .unwrap()
            .push(RecordedRequest::Resolve(request.params));
        self.0
            .resolves
            .lock()
            .unwrap()
            .pop_front()
            .expect("fake resolve response")
            .map(Into::into)
            .map_err(ClientError::Method)
    }
}

impl service_client_core_rs::UnaryTransport<QueryDatumRefs> for FakeTransportHandle {
    async fn call(
        &self,
        request: Envelope<QueryDatumRefs>,
    ) -> Result<ResponseEnvelope<QueriedDatumRefs>, ClientError<DocsError>> {
        self.0
            .requests
            .lock()
            .unwrap()
            .push(RecordedRequest::QueryRefs(request.params));
        Ok(self
            .0
            .ref_queries
            .lock()
            .unwrap()
            .pop_front()
            .expect("fake ref query response")
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

#[tokio::test]
async fn resolve_decodes_the_copy_and_unresolved_is_none() {
    let transport = Arc::new(FakeTransport::default());
    let expected = Note {
        text: "content-addressed".to_owned(),
    };
    {
        let mut resolves = transport.resolves.lock().unwrap();
        resolves.push_back(Ok(ResolvedKeyRef {
            doc_id: doc(),
            heads: Vec::new(),
            value: serde_json::to_value(&expected).unwrap(),
        }));
        resolves.push_back(Err(DocsError::KeyRefUnresolved {
            datum_key: key(NOTE_TAG, "gone"),
        }));
    }
    let client = Client::new(FakeTransportHandle(Arc::clone(&transport)));

    assert_eq!(
        resolve::<_, Note>(
            &client,
            RequestContext::root(),
            capability(),
            String::from("4c1f"),
        )
        .await
        .unwrap(),
        Some(Resolved {
            doc_id: doc(),
            heads: Vec::new(),
            value: expected,
        })
    );
    assert_eq!(
        resolve::<_, Note>(
            &client,
            RequestContext::root(),
            capability(),
            String::from("gone"),
        )
        .await
        .unwrap(),
        None
    );

    let requests = transport.requests.lock().unwrap();
    let RecordedRequest::Resolve(request) = &requests[0] else {
        panic!("expected resolve request");
    };
    assert_eq!(request.capability, capability());
    assert_eq!(request.key, key(NOTE_TAG, "4c1f"));
}

#[tokio::test]
async fn referrers_query_by_key_and_return_the_node_answer() {
    let transport = Arc::new(FakeTransport::default());
    let referrer = QueriedDatumRef {
        origin_doc_id: doc(),
        origin_datum_key: key("org.example.entry", "range"),
        path: "/commits/0".to_owned(),
        target_doc_id: None,
        target_datum_key: key(NOTE_TAG, "4c1f"),
        ref_heads: Vec::new(),
    };
    transport
        .ref_queries
        .lock()
        .unwrap()
        .push_back(QueriedDatumRefs {
            refs: vec![referrer.clone()],
        });
    let client = Client::new(FakeTransportHandle(Arc::clone(&transport)));

    assert_eq!(
        referrers::<_, Note>(
            &client,
            RequestContext::root(),
            capability(),
            Some(doc()),
            String::from("4c1f"),
        )
        .await
        .unwrap(),
        vec![referrer]
    );

    let requests = transport.requests.lock().unwrap();
    let RecordedRequest::QueryRefs(request) = &requests[0] else {
        panic!("expected ref query request");
    };
    assert_eq!(request.capability, capability());
    assert_eq!(request.drawer_id, Some(doc()));
    assert_eq!(
        request.selector,
        DatumRefSelector::Key {
            key: key(NOTE_TAG, "4c1f"),
        }
    );
}
