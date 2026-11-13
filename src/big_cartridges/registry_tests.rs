use super::*;
use big_cabinet_core::datum::Nsid;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DocsMessage;
use big_cartridges_core::Digest;
use big_cartridges_core::WasmComponent;
use big_cartridges_core::test_support::cartridge_id;
use big_cartridges_core::test_support::sign_as_example;
use big_cartridges_core::test_support::sign_with_seed;
use futures::future::BoxFuture;
use serde_json::json;
use service_client_native_rs::InlineRoute;
use service_client_native_rs::Transport;
use std::sync::Arc;
use std::sync::Mutex;

fn accepted(value: Json) -> DatumRead {
    DatumRead::Accepted {
        value,
        version: DatumVersion::default(),
        authors: Vec::new(),
    }
}

#[derive(Clone)]
struct PinnedDocs {
    values: Arc<Mutex<BTreeMap<DatumKey, DatumRead>>>,
    requests: Arc<Mutex<Vec<Vec<ChangeHash>>>>,
    heads: Arc<Mutex<Vec<ChangeHash>>>,
}

impl InlineRoute<DocsProtocol> for PinnedDocs {
    fn route_inline(self: Arc<Self>, message: DocsMessage) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let DocsMessage::ReadDatums(message) = message else {
                panic!("resolution only performs typed reads")
            };
            let params = message.inner.params;
            let At::Heads(requested) = params.at else {
                panic!("resolution must read pinned heads")
            };
            self.requests.lock().unwrap().push(requested);
            let key = params
                .datums
                .expect("resolution requests one datum")
                .into_iter()
                .next()
                .expect("resolution requests one datum");
            let datum =
                self.values
                    .lock()
                    .unwrap()
                    .get(&key)
                    .cloned()
                    .unwrap_or(DatumRead::Absent {
                        version: DatumVersion::default(),
                    });
            let heads = self.heads.lock().unwrap().clone();
            message
                .tx
                .send(Ok(DatumsRead {
                    heads,
                    datums: BTreeMap::from([(key, datum)]),
                }
                .into()))
                .await
                .unwrap();
        })
    }
}

fn pinned_docs(
    values: BTreeMap<DatumKey, DatumRead>,
    heads: Vec<ChangeHash>,
) -> (Client<DocsProtocol>, CapId, Arc<PinnedDocs>) {
    let capability = CapId::from_bytes([7; 16]);
    let stub = Arc::new(PinnedDocs {
        values: Arc::new(Mutex::new(values)),
        requests: Arc::new(Mutex::new(Vec::new())),
        heads: Arc::new(Mutex::new(heads)),
    });
    let client = Client::new(Transport::direct(Arc::clone(&stub)));
    (client, capability, stub)
}

fn projection_state(docs: Client<DocsProtocol>, capability: CapId) -> State {
    State {
        docs,
        capability,
        sql: sqlite_rs::test_support::db(&[]).unwrap().clone(),
        first_seen: BTreeMap::new(),
        views: BTreeMap::new(),
        resolution_cache: BTreeMap::new(),
        claims: BTreeMap::new(),
        epoch: Uuid7::new(),
        seq: 0,
        seq_tx: watch::channel(0).0,
        changes: BTreeMap::new(),
    }
}

fn envelope_with_lexicon(doc: &DocId, heads: &[ChangeHash]) -> (Cartridge, Nsid) {
    let nsid = Nsid::literal("org.example.photos.album");
    let mut envelope = Cartridge::new(cartridge_id("photos"), "1.0.0".parse().unwrap());
    envelope.lexicons.insert(
        nsid.clone(),
        pin_for(doc, lexicon_key(&envelope.version, &nsid), heads),
    );
    (sign_as_example(envelope), nsid)
}
fn cartridge(version: &str, title: &str) -> Cartridge {
    let mut cartridge = Cartridge::new(cartridge_id("photos"), version.parse().unwrap());
    cartridge.title = Some(title.into());
    sign_as_example(cartridge)
}

/// A registry document for `photos` holding `versions` as `(slot, datum)`.
fn document(versions: Vec<(&str, DatumRead)>) -> DatumsRead {
    let mut datums = BTreeMap::from([(
        identity_key(),
        accepted(json!({ "id": cartridge_id("photos").to_string() })),
    )]);
    for (slot, datum) in versions {
        datums.insert(DatumKey::from_text(Cartridge::ID, slot).unwrap(), datum);
    }
    DatumsRead {
        heads: Vec::new(),
        datums,
    }
}

fn pins(cartridges: &[&Cartridge]) -> FirstSeenMap {
    cartridges
        .iter()
        .map(|cartridge| {
            (
                (cartridge.id.clone(), cartridge.version.clone()),
                FirstSeen {
                    revision: cartridge.revision(),
                    envelope: (*cartridge).clone(),
                },
            )
        })
        .collect()
}

fn datum(cartridge: &Cartridge) -> DatumRead {
    accepted(serde_json::to_value(cartridge).unwrap())
}

#[test]
fn an_unseen_version_is_first_seen_and_a_document_without_identity_is_no_cartridge() {
    let v1 = cartridge("1.0.0", "Photos");
    let (view, fresh) = fold_doc(&document(vec![("1.0.0", datum(&v1))]), &pins(&[])).unwrap();
    assert_eq!(fresh, std::slice::from_ref(&v1));
    assert_eq!(view.versions[&v1.version].conflict, None);
    let anonymous = DatumsRead {
        heads: Vec::new(),
        datums: BTreeMap::new(),
    };
    assert!(fold_doc(&anonymous, &pins(&[])).is_none());
}

#[test]
fn a_changed_datum_is_conflicted_and_the_first_seen_envelope_is_served() {
    let seen = cartridge("1.0.0", "Photos");
    let rewritten = cartridge("1.0.0", "Rewritten");
    let (view, fresh) = fold_doc(
        &document(vec![("1.0.0", datum(&rewritten))]),
        &pins(&[&seen]),
    )
    .unwrap();
    assert!(fresh.is_empty());
    let state = &view.versions[&seen.version];
    assert_eq!(state.envelope, seen);
    assert!(state.conflict.as_deref().unwrap().contains("first seen"));
    // The same revision is the version, unconflicted.
    let (view, _) = fold_doc(&document(vec![("1.0.0", datum(&seen))]), &pins(&[&seen])).unwrap();
    assert_eq!(view.versions[&seen.version].conflict, None);
}

#[test]
fn unreadable_slots_are_reported_and_an_unreadable_pinned_version_is_conflicted() {
    let seen = cartridge("1.0.0", "Photos");
    let mislabelled = cartridge("2.0.0", "Photos");
    let view = fold_doc(
        &document(vec![
            ("1.0", datum(&seen)),
            ("1.1.0", datum(&mislabelled)),
            ("1.2.0", accepted(json!({ "nonsense": true }))),
            (
                "1.0.0",
                DatumRead::Rejected {
                    reason: "bad".into(),
                },
            ),
        ]),
        &pins(&[&seen]),
    )
    .unwrap()
    .0;
    assert_eq!(
        view.unreadable
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["1.0", "1.1.0", "1.2.0"]
    );
    assert_eq!(
        view.versions[&seen.version].conflict.as_deref(),
        Some("rejected: bad")
    );
}

#[test]
fn an_unseen_version_with_a_bad_signature_is_listed_unverified_and_never_pinned() {
    let genuine = cartridge("1.0.0", "Photos");
    let forged = sign_with_seed(cartridge("1.1.0", "Photos"), &[9; 32]);
    let mut unsigned = cartridge("1.2.0", "Photos");
    unsigned.signature = None;
    let mut web = cartridge("1.3.0", "Photos");
    web.id = big_cartridges_core::CartridgeId::new("did:web:example.com", "photos").unwrap();
    let (view, fresh) = fold_doc(
        &document(vec![
            ("1.0.0", datum(&genuine)),
            ("1.1.0", datum(&forged)),
            ("1.2.0", datum(&unsigned)),
        ]),
        &pins(&[]),
    )
    .unwrap();
    // Only the verified version is pinned as first-seen, so a bad copy cannot reserve a label.
    assert_eq!(fresh, std::slice::from_ref(&genuine));
    assert_eq!(view.versions[&genuine.version].unverified, None);
    for (version, reason) in [
        (&forged.version, "does not verify"),
        (&unsigned.version, "no signature"),
    ] {
        let reason_found = view.versions[version].unverified.as_deref().unwrap();
        assert!(reason_found.contains(reason), "{reason_found}");
    }
    // A version already pinned stays served from the pin even if the drawer's copy lost its
    // signature: the revision is the same content, and the pin was verified when it was taken.
    let (view, _) = fold_doc(
        &document(vec![(
            "1.0.0",
            datum(&{
                let mut stripped = genuine.clone();
                stripped.signature = None;
                stripped
            }),
        )]),
        &pins(&[&genuine]),
    )
    .unwrap();
    let state = &view.versions[&genuine.version];
    assert_eq!(
        (state.conflict.as_deref(), state.unverified.as_deref()),
        (None, None)
    );
    assert_eq!(state.envelope, genuine);
}
#[tokio::test]
async fn resolved_projection_serves_lexicon_and_component_contents() {
    let doc = DocId::new([41; 32]);
    let payload_heads = [ChangeHash([1; 32])];
    let blob_heads = [ChangeHash([2; 32])];
    let nsid = Nsid::literal("org.example.photos.album");
    let digest = Digest([8; 32]);
    let wasm = WasmComponent {
        bytes: pin_for(
            &doc,
            artifact_key(&"1.0.0".parse().unwrap(), "handler"),
            &blob_heads,
        ),
        digest,
        size: 3,
        media_type: "application/wasm".into(),
        extra: Default::default(),
    };
    let mut envelope = Cartridge::new(cartridge_id("photos"), "1.0.0".parse().unwrap());
    envelope.lexicons.insert(
        nsid.clone(),
        pin_for(&doc, lexicon_key(&envelope.version, &nsid), &payload_heads),
    );
    envelope.components.insert(
        "handler".into(),
        pin_for(
            &doc,
            component_key(&envelope.version, "handler"),
            &payload_heads,
        ),
    );
    let envelope = sign_as_example(envelope);
    let blob = Blob {
        mime: "application/wasm".into(),
        length_octets: 3,
        digest: digest.to_string(),
        inline: None,
        urls: None,
        extra: Default::default(),
    };
    let lexicon_key_value = lexicon_key(&envelope.version, &nsid);
    let component_key_value = component_key(&envelope.version, "handler");
    let blob_key = artifact_key(&envelope.version, "handler");
    let (docs, capability, stub) = pinned_docs(
        BTreeMap::from([
            (
                lexicon_key_value,
                accepted(json!({"id": nsid.to_string(), "title": "old"})),
            ),
            (
                component_key_value,
                accepted(serde_json::to_value(Component::Wasm(wasm.clone())).unwrap()),
            ),
            (blob_key, accepted(serde_json::to_value(blob).unwrap())),
        ]),
        payload_heads.to_vec(),
    );
    let resolved = resolve_envelope(&docs, capability, &doc, &envelope)
        .await
        .unwrap();
    assert_eq!(resolved.lexicons[&nsid].get("title"), Some(&json!("old")));
    assert_eq!(resolved.components["handler"], Component::Wasm(wasm));
    assert_eq!(
        *stub.requests.lock().unwrap(),
        vec![
            payload_heads.to_vec(),
            payload_heads.to_vec(),
            blob_heads.to_vec()
        ]
    );
}

#[tokio::test]
async fn historical_heads_keep_the_value_written_before_an_overwrite() {
    let doc = DocId::new([42; 32]);
    let pinned_heads = [ChangeHash([3; 32])];
    let (mut envelope, nsid) = envelope_with_lexicon(&doc, &pinned_heads);
    envelope = sign_as_example(envelope);
    let key = lexicon_key(&envelope.version, &nsid);
    let (docs, capability, stub) = pinned_docs(
        BTreeMap::from([(
            key,
            accepted(json!({"id": nsid.to_string(), "value": "old"})),
        )]),
        vec![ChangeHash([9; 32])],
    );
    let resolved = resolve_envelope(&docs, capability, &doc, &envelope)
        .await
        .unwrap();
    assert_eq!(resolved.lexicons[&nsid].get("value"), Some(&json!("old")));
    assert_eq!(*stub.requests.lock().unwrap(), vec![pinned_heads.to_vec()]);
}

#[tokio::test]
async fn a_pin_naming_another_document_is_refused() {
    let doc = DocId::new([43; 32]);
    let wrong_doc = DocId::new([44; 32]);
    let nsid = Nsid::literal("org.example.photos.album");
    let mut envelope = Cartridge::new(cartridge_id("photos"), "1.0.0".parse().unwrap());
    envelope.lexicons.insert(
        nsid.clone(),
        pin_for(
            &wrong_doc,
            lexicon_key(&envelope.version, &nsid),
            &[ChangeHash([1; 32])],
        ),
    );
    let envelope = sign_as_example(envelope);
    let (docs, capability, _) = pinned_docs(BTreeMap::new(), Vec::new());
    assert!(matches!(
        resolve_envelope(&docs, capability, &doc, &envelope).await,
        Err(CartridgesError::PinnedDocumentMismatch { .. })
    ));
}

#[tokio::test]
async fn a_tampered_pin_fails_resolution_before_reading_payloads() {
    let doc = DocId::new([45; 32]);
    let (mut envelope, nsid) = envelope_with_lexicon(&doc, &[ChangeHash([1; 32])]);
    envelope = sign_as_example(envelope);
    envelope.lexicons.insert(
        nsid.clone(),
        pin_for(
            &doc,
            lexicon_key(&envelope.version, &nsid),
            &[ChangeHash([2; 32])],
        ),
    );
    let (docs, capability, stub) = pinned_docs(BTreeMap::new(), Vec::new());
    assert!(matches!(
        resolve_envelope(&docs, capability, &doc, &envelope).await,
        Err(CartridgesError::Signature(_))
    ));
    assert!(stub.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unavailable_pinned_heads_are_unreadable_until_they_arrive() {
    let doc = DocId::new([46; 32]);
    let pinned_heads = [ChangeHash([4; 32])];
    let (envelope, nsid) = envelope_with_lexicon(&doc, &pinned_heads);
    let key = lexicon_key(&envelope.version, &nsid);
    let (docs, capability, stub) = pinned_docs(
        BTreeMap::from([(
            key.clone(),
            DatumRead::Pending {
                version: DatumVersion::default(),
                reason: "heads have not synced".into(),
                value: None,
            },
        )]),
        vec![ChangeHash([5; 32])],
    );
    let read = DatumsRead {
        heads: vec![ChangeHash([5; 32])],
        datums: BTreeMap::from([
            (
                identity_key(),
                accepted(json!({"id": envelope.id.to_string()})),
            ),
            (version_key(&envelope.version), datum(&envelope)),
        ]),
    };
    let mut state = projection_state(docs, capability);
    let (view, fresh) = state
        .fold_doc_resolved(&doc, &read)
        .await
        .expect("the identity is readable");
    assert!(view.versions.is_empty());
    assert!(view.unreadable.contains_key("1.0.0"));
    assert!(fresh.is_empty());

    stub.values.lock().unwrap().insert(
        key,
        accepted(json!({"id": nsid.to_string(), "value": "ready"})),
    );
    *stub.heads.lock().unwrap() = vec![ChangeHash([6; 32])];
    let read = DatumsRead {
        heads: vec![ChangeHash([6; 32])],
        datums: read.datums,
    };
    let (view, fresh) = state
        .fold_doc_resolved(&doc, &read)
        .await
        .expect("the identity is readable");
    assert!(view.versions.contains_key(&envelope.version));
    assert_eq!(fresh, vec![envelope]);
    assert_eq!(
        view.versions[&"1.0.0".parse().unwrap()]
            .resolved
            .as_ref()
            .unwrap()
            .lexicons[&nsid]
            .get("value"),
        Some(&json!("ready"))
    );
}
