//! The executor seam and the nodule config document's artifact references, against a fake
//! executor (no Wasm): what `ensure` and `retire` are asked, when, and what dispatch and the
//! nodule record report as a result.

use super::Node;
use super::dispatch::Echo;
use super::dispatch::not_ready;
use super::dispatch::serving_plug;
use super::dispatch::serving_plug_of;
use super::id;
use crate::dispatch::Provider;
use crate::dispatch::ProviderKind;
use crate::dispatch::ProviderState;
use crate::executor::HandlerSpec;
use crate::executor::Hosted;
use crate::executor::NoduleExecutor;
use crate::executor::Reensure;
use async_trait::async_trait;
use big_blobs_core::Blob;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::DatumRead;
use big_cabinet_core::doc::DatumsRead;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::doc::WriteMode;
use big_nodule_core::config::artifact_key;
use big_nodule_core::dispatch::DispatchError;
use big_nodule_core::dispatch::NotReadyReason;
use big_nodule_core::inventory::ArtifactState;
use big_nodule_core::inventory::Readiness;
use big_nodule_core::protocol::UninstallNodule;
use big_plugs_core::Digest;
use big_plugs_core::PlugId;
use serde_json::json;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;

const OWN: &str = "org.example.photos.list";
const PHOTOS: &str = "@example/photos";

/// Hosts nothing: it answers `ensure` with the readiness a test sets, serves a shared [`Echo`]
/// once ready, and logs each workload it starts or stops. Like any executor it treats `ensure`
/// for a revision it already hosts as a lookup, since the reconciler calls it on every
/// reconcile (a config mutation reconciles once itself and once more on the watch event).
pub(super) struct FakeExecutor {
    state: Mutex<FakeState>,
    provider: Arc<dyn Provider>,
    /// How many times `ensure` has answered ready, for a test to wait on without polling.
    ready_answers: watch::Sender<usize>,
}

#[derive(Default)]
struct FakeState {
    ready: bool,
    hosted: BTreeMap<PlugId, Digest>,
    /// Every `ensure` call, which the reconciler makes once per reconcile of a hosted nodule.
    ensures: usize,
    log: Vec<String>,
    reensure: BTreeMap<PlugId, Reensure>,
}

impl FakeExecutor {
    fn new() -> Arc<Self> {
        Self::hosting(Arc::<Echo>::default(), false)
    }

    /// An executor whose hosted handlers are `provider`, answering `ensure` ready from the
    /// start when `ready` (otherwise once [`Self::arrive`] says the bytes did).
    pub(super) fn hosting(provider: Arc<dyn Provider>, ready: bool) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(FakeState {
                ready,
                ..FakeState::default()
            }),
            provider,
            ready_answers: watch::channel(0).0,
        })
    }

    fn log(&self) -> Vec<String> {
        self.state.lock().unwrap().log.clone()
    }

    fn provider(&self) -> Arc<dyn Provider> {
        Arc::clone(&self.provider)
    }

    /// The bytes arrived: flip to ready and ask the state actor to ensure `plug_id` again, then
    /// wait until it has and the answer was ready.
    async fn arrive(&self, plug_id: &PlugId) {
        let reensure = {
            let mut state = self.state.lock().unwrap();
            state.ready = true;
            state.reensure[plug_id].clone()
        };
        let mut answers = self.ready_answers.subscribe();
        let seen = *answers.borrow_and_update();
        reensure.request();
        answers.wait_for(|answers| *answers > seen).await.unwrap();
    }
}

#[async_trait]
impl NoduleExecutor for FakeExecutor {
    fn kind(&self) -> &'static str {
        "fake"
    }

    async fn ensure(&self, spec: &HandlerSpec) -> Hosted {
        let mut state = self.state.lock().unwrap();
        state.ensures += 1;
        if state.hosted.get(&spec.plug_id) != Some(&spec.handler.digest) {
            state
                .hosted
                .insert(spec.plug_id.clone(), spec.handler.digest);
            state
                .log
                .push(format!("host {} {}", spec.plug_id, spec.handler.digest));
        }
        state
            .reensure
            .insert(spec.plug_id.clone(), spec.reensure.clone());
        if state.ready {
            drop(state);
            self.ready_answers.send_modify(|answers| *answers += 1);
            Hosted::Ready(self.provider())
        } else {
            Hosted::NotReady(Readiness::ArtifactsUnavailable)
        }
    }

    async fn retire(&self, plug_id: &PlugId) {
        let mut state = self.state.lock().unwrap();
        assert!(
            state.hosted.remove(plug_id).is_some(),
            "retired a plug that is not hosted"
        );
        state.log.push(format!("retire {plug_id}"));
    }
}

fn host_of(plug_id: &str, bytes: &str) -> String {
    format!("host {plug_id} {}", Digest::of(bytes.as_bytes()))
}

#[tokio::test]
async fn a_hosted_handler_is_registered_not_ready_until_the_executor_signals_and_retired_with_the_nodule()
 {
    let executor = FakeExecutor::new();
    let node = super::node_with(Some(Arc::clone(&executor) as Arc<dyn NoduleExecutor>)).await;
    let operator = node.operator();
    let photos = id(PHOTOS);
    node.admit(operator, serving_plug(PHOTOS, OWN))
        .await
        .unwrap();
    // A disabled nodule is not hosted.
    assert!(executor.log().is_empty());

    node.set_enabled(PHOTOS, true).await;
    assert_eq!(executor.log(), [host_of(PHOTOS, "photos")]);
    assert_eq!(
        node.invoke(operator, OWN, json!({})).await,
        Err(not_ready(NotReadyReason::Provider(
            Readiness::ArtifactsUnavailable
        )))
    );
    let record = node.get(operator, PHOTOS).await.unwrap();
    assert_eq!(record.readiness, Readiness::ArtifactsUnavailable);
    assert_eq!(
        record.artifacts,
        BTreeMap::from([("handler".to_owned(), ArtifactState::Unavailable)])
    );

    // The bytes arrive after `ensure` returned: the executor's signal alone makes the
    // reconciler register the provider, and the caller is passed through to it.
    executor.arrive(&photos).await;
    let record = node.get(operator, PHOTOS).await.unwrap();
    assert_eq!(record.readiness, Readiness::Ready);
    assert!(record.readiness.is_ready());
    assert_eq!(
        record.artifacts,
        BTreeMap::from([("handler".to_owned(), ArtifactState::Available)])
    );
    assert_eq!(
        node.dispatch.router.resolve(OWN).unwrap().state,
        ProviderState::Ready(ProviderKind::Executor {
            kind: "fake",
            provider: executor.provider(),
        })
    );
    let output = node.invoke(operator, OWN, json!({ "n": 1 })).await.unwrap();
    assert_eq!(output["echo"], json!({ "n": 1 }));
    assert_eq!(
        output["caller"]["actor"],
        json!(node.authority.service.operator_uuid())
    );

    // Disabling retires the workload and unbinds the operation; enabling hosts it again.
    node.set_enabled(PHOTOS, false).await;
    assert_eq!(executor.log().last().unwrap(), &format!("retire {PHOTOS}"));
    assert_eq!(
        node.invoke(operator, OWN, json!({})).await,
        Err(DispatchError::UnknownOperation)
    );
    assert_eq!(
        node.get(operator, PHOTOS).await.unwrap().readiness,
        Readiness::Disabled
    );
    // A request from the workload that was just retired names nothing to redo.
    let before = executor.log();
    executor.state.lock().unwrap().reensure[&photos].request();
    node.get(operator, PHOTOS).await.unwrap();
    assert_eq!(executor.log(), before);

    node.set_enabled(PHOTOS, true).await;
    assert_eq!(executor.log().last().unwrap(), &host_of(PHOTOS, "photos"));
    assert!(node.invoke(operator, OWN, json!({})).await.is_ok());

    node.call(UninstallNodule {
        authorization: operator,
        plug_id: photos,
    })
    .await
    .unwrap();
    assert_eq!(executor.log().last().unwrap(), &format!("retire {PHOTOS}"));
    assert_eq!(
        node.invoke(operator, OWN, json!({})).await,
        Err(DispatchError::UnknownOperation)
    );
    node.shutdown().await;
}

#[tokio::test]
async fn an_upgrade_retires_the_old_revision_before_ensuring_the_new_one() {
    let executor = FakeExecutor::new();
    let node = super::node_with(Some(Arc::clone(&executor) as Arc<dyn NoduleExecutor>)).await;
    let operator = node.operator();
    node.admit(operator, serving_plug_of(PHOTOS, "1.0.0", OWN, "one"))
        .await
        .unwrap();
    node.set_enabled(PHOTOS, true).await;
    executor.arrive(&id(PHOTOS)).await;

    let upgraded = node
        .admit(operator, serving_plug_of(PHOTOS, "1.1.0", OWN, "two"))
        .await
        .unwrap();
    assert_eq!(upgraded.readiness, Readiness::Ready);
    assert_eq!(
        executor.log(),
        [
            host_of(PHOTOS, "one"),
            format!("retire {PHOTOS}"),
            host_of(PHOTOS, "two"),
        ]
    );
    assert!(node.invoke(operator, OWN, json!({})).await.is_ok());
    node.shutdown().await;
}

#[tokio::test]
async fn a_config_change_reconciles_once_and_a_change_from_elsewhere_reconciles_again() {
    let executor = FakeExecutor::new();
    let node = super::node_with(Some(Arc::clone(&executor) as Arc<dyn NoduleExecutor>)).await;
    let operator = node.operator();
    node.admit(operator, serving_plug(PHOTOS, OWN))
        .await
        .unwrap();
    let ensures = || executor.state.lock().unwrap().ensures;
    // Enough turns for a watch event that is already queued to be handled: the services are all
    // in-process, so a ready event needs no I/O.
    let settle = || async {
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
    };

    // The mutation reconciles itself; its own watch event finds the heads already applied.
    let before = ensures();
    node.set_enabled(PHOTOS, true).await;
    settle().await;
    assert_eq!(ensures() - before, 1);

    // A write the actor did not make (here, a stray artifact reference in the config document,
    // as a synced edit would bring) moves the heads and reconciles once.
    let before = ensures();
    let config_doc = DocId::from(node.drawers.local().config_id.clone());
    let write = node
        .docs
        .client
        .call(
            RequestContext::root(),
            MintDocCapability {
                authorization: Authorization::identity(node.drawers_actor.capability),
                scope: DocScope {
                    write: BTreeSet::from([config_doc.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    node.docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: write.capability_id,
                command: None,
                doc_id: config_doc,
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([(
                    DatumKey::from_text(Blob::ID, "nodule.artifact/garbage").unwrap(),
                    json!({
                        "$type": Blob::ID,
                        "mime": "image/png",
                        "lengthOctets": 3,
                        "digest": Digest::of(b"png").to_string(),
                    }),
                )]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    // The store's view of the document reaches the actor through the watch, so wait for it.
    while ensures() == before {
        tokio::task::yield_now().await;
    }
    settle().await;
    assert_eq!(ensures() - before, 1);
    node.get(operator, PHOTOS).await.unwrap();
    assert_eq!(
        ensures() - before,
        1,
        "reading at applied heads reconciles nothing"
    );
    node.shutdown().await;
}

async fn config_blobs(node: &Node) -> BTreeMap<DatumKey, DatumRead> {
    let config_doc = DocId::from(node.drawers.local().config_id.clone());
    let capability = node
        .docs
        .client
        .call(
            RequestContext::root(),
            MintDocCapability {
                authorization: Authorization::identity(node.drawers_actor.capability),
                scope: DocScope {
                    read: BTreeSet::from([config_doc.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    let DatumsRead { mut datums, .. } = node
        .docs
        .client
        .call(
            RequestContext::root(),
            ReadDatums {
                capability: capability.capability_id,
                doc_id: config_doc,
                at: At::Current,
                datums: None,
                optimistic: false,
            },
        )
        .await
        .unwrap();
    datums.retain(|key, datum| {
        key.tag().as_str() == Blob::ID && !matches!(datum, DatumRead::Absent { .. })
    });
    datums
}

fn blob_of(datum: &DatumRead) -> Blob {
    let DatumRead::Accepted { value, .. } = datum else {
        panic!("expected an accepted Blob, got {datum:?}");
    };
    serde_json::from_value(value.clone()).unwrap()
}

#[tokio::test]
async fn handler_artifacts_are_referenced_from_the_config_document_through_the_nodule_lifecycle() {
    let node = super::node().await;
    let operator = node.operator();
    let slot = artifact_key(&id(PHOTOS), "handler");

    // Another service's `Blob` and a malformed one under our prefix share the document.
    let config_doc = DocId::from(node.drawers.local().config_id.clone());
    let write = node
        .docs
        .client
        .call(
            RequestContext::root(),
            MintDocCapability {
                authorization: Authorization::identity(node.drawers_actor.capability),
                scope: DocScope {
                    write: BTreeSet::from([config_doc.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
            },
        )
        .await
        .unwrap();
    let foreign = DatumKey::from_text(Blob::ID, "avatar").unwrap();
    let malformed = DatumKey::from_text(Blob::ID, "nodule.artifact/garbage").unwrap();
    let neighbour = json!({
        "$type": Blob::ID,
        "mime": "image/png",
        "lengthOctets": 3,
        "digest": Digest::of(b"png").to_string(),
    });
    node.docs
        .client
        .call(
            RequestContext::root(),
            WriteDatums {
                capability: write.capability_id,
                command: None,
                doc_id: config_doc,
                mode: WriteMode::AtCurrent,
                set: BTreeMap::from([
                    (foreign.clone(), neighbour.clone()),
                    (malformed.clone(), neighbour),
                ]),
                remove: BTreeSet::new(),
            },
        )
        .await
        .unwrap();
    let neighbours = config_blobs(&node).await;
    assert_eq!(neighbours.len(), 2);

    // Admitting writes the reference with the install, to the digest and size the envelope pins.
    node.admit(operator, serving_plug_of(PHOTOS, "1.0.0", OWN, "one"))
        .await
        .unwrap();
    let blobs = config_blobs(&node).await;
    assert_eq!(
        blobs.keys().collect::<BTreeSet<_>>(),
        BTreeSet::from([&foreign, &malformed, &slot])
    );
    let blob = blob_of(&blobs[&slot]);
    assert_eq!(blob.digest, Digest::of(b"one").to_string());
    assert_eq!(blob.length_octets, 3);
    assert_eq!(blob.mime, "application/wasm");
    assert_eq!(blobs[&foreign], neighbours[&foreign]);
    assert_eq!(blobs[&malformed], neighbours[&malformed]);

    // Upgrading to another handler swaps the reference; disabling and enabling leave it.
    node.admit(operator, serving_plug_of(PHOTOS, "1.1.0", OWN, "three"))
        .await
        .unwrap();
    node.set_enabled(PHOTOS, true).await;
    node.set_enabled(PHOTOS, false).await;
    let blobs = config_blobs(&node).await;
    let blob = blob_of(&blobs[&slot]);
    assert_eq!(blob.digest, Digest::of(b"three").to_string());
    assert_eq!(blob.length_octets, 5);
    assert_eq!(blobs.len(), 3);

    // Uninstalling removes the reference and nothing else.
    node.call(UninstallNodule {
        authorization: operator,
        plug_id: id(PHOTOS),
    })
    .await
    .unwrap();
    let blobs = config_blobs(&node).await;
    assert_eq!(
        blobs.keys().collect::<BTreeSet<_>>(),
        BTreeSet::from([&foreign, &malformed])
    );
    assert_eq!(blobs[&foreign], neighbours[&foreign]);
    node.shutdown().await;
}
