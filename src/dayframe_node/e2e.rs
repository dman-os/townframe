//! The composition end to end: it boots a real node from a directory, a Wasmtime micro (the
//! `dispatch` fixture, prepared as `DAYFRAME_DISPATCH_COMPONENT`) calls a granted method through
//! the egress the composition installed for it, a second node pairs with the first by a
//! bootstrap ticket, and shutdown is clean.

use crate::MicroInstall;
use crate::NodeComposition;
use crate::NodeConfig;
use crate::read_tickets;
use async_trait::async_trait;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::doc::At;
use big_cabinet_core::doc::CommandId;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::MintDocCapability;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_dispatch::Caller;
use big_dispatch::Operation;
use big_dispatch::Provider;
use big_dispatch::ProviderError;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch_core::Body;
use big_dispatch_core::InvocationContext;
use bytes::Bytes;
use clay_guest_session::InitialContext;
use clay_guest_session::capabilities::DeclaredPublication;
use clay_guest_session::capabilities::PublicationKind;
use clay_guest_session::capabilities::presentation_component;
use clay_guest_session::egress::DispatchEgress;
use clay_host::ComponentRef;
use clay_host::Host;
use clay_host::MicroId;
use clay_rt_wasmtime::WasmPackage;
use clay_rt_wasmtime::WasmRuntime;
use clay_surface::Mutation;
use clay_surface::PresentationError;
use clay_surface::SurfaceSink;
use http_body_util::BodyExt;
use http_body_util::Full;
use method_core::Method;
use serde_json::Value;
use serde_json::json;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;

const ECHO: &str = "org.example.clay.test.dispatch.echo";
const REVEAL: &str = "org.example.clay.test.dispatch.reveal";
const FIXTURE: &str = "did:example:clay-local/dispatch-fixture";

/// Answers every operation with its name and the text it was given.
struct TestMethods;

#[async_trait]
impl Provider for TestMethods {
    async fn call(
        &self,
        _caller: &Caller,
        operation: &str,
        _request: &RequestContext,
        _context: &InvocationContext,
        input: Value,
        _body: Option<Body>,
    ) -> Result<Body, ProviderError> {
        Ok(Body::Json(json!({
            "operation": operation,
            "echo": input["text"],
        })))
    }
}

/// Collects the text the guest renders.
struct TextSink(Mutex<Vec<String>>);

impl SurfaceSink<ComponentRef> for TextSink {
    fn is_live(&self) -> bool {
        true
    }

    fn apply(&self, edits: &[Mutation<ComponentRef>]) -> Result<u64, PresentationError> {
        let mut texts = self.0.lock().unwrap();
        texts.extend(edits.iter().filter_map(|edit| match edit {
            Mutation::CreateText(text) => Some(text.clone()),
            _ => None,
        }));
        Ok(0)
    }

    fn withdraw(&self) {}
}

/// Runs the `dispatch` fixture under Wasmtime with the egress the composition holds for
/// [`FIXTURE`], and returns what it reported, by attempt.
async fn run_fixture(composition: &NodeComposition) -> BTreeMap<String, String> {
    let host = Host::new();
    let instance = host.start("dispatch-fixture");
    let package = WasmPackage::from_env("dispatch")
        .await
        .expect("the prepared dispatch component is valid");
    let context = InitialContext {
        publications: [(
            "view".to_owned(),
            DeclaredPublication {
                kind: PublicationKind::Presentation,
                contract: presentation_component(),
            },
        )]
        .into(),
        micro: Some(MicroId::new("dispatch-fixture")),
        egress: composition.egress(FIXTURE),
        ..InitialContext::default()
    };
    let (runtime, driver) = WasmRuntime::initialize(&host, instance.clone(), &package, context)
        .await
        .unwrap_or_else(|error| panic!("the fixture activates: {error}"));
    let sink = Arc::new(TextSink(Mutex::new(Vec::new())));
    let session = runtime
        .mount("view", Arc::clone(&sink) as _)
        .unwrap_or_else(|error| {
            panic!(
                "the fixture mounts: {error:?}, fault {:?}",
                host.fault(&instance)
            )
        });
    let reports = sink.0.lock().unwrap().clone();
    let [report] = reports.as_slice() else {
        panic!("the fixture renders one report, got {reports:?}");
    };
    let attempts = report
        .lines()
        .map(|line| {
            let (name, outcome) = line.split_once('=').expect("name=outcome");
            (name.to_owned(), outcome.to_owned())
        })
        .collect();
    drop(session);
    drop(driver);
    host.close();
    attempts
}

fn no_schemas() -> Arc<big_cabinet::SchemaRepo> {
    Arc::new(big_cabinet::SchemaRepo::open(std::iter::empty()).unwrap())
}

/// A vocabulary of one datum kind, [`NOTE`].
fn note_schemas() -> Arc<big_cabinet::SchemaRepo> {
    Arc::new(
        big_cabinet::SchemaRepo::open([big_cabinet::SchemaRegistrationGroup::new(
            None,
            [big_cabinet::SchemaRegistration::new(
                NOTE,
                json!({
                    "lexicon": 1,
                    "id": NOTE,
                    "defs": {"main": {"type": "record", "key": "tid", "record": {
                        "type": "object",
                        "required": ["value"],
                        "properties": {"value": {"type": "string"}}
                    }}}
                }),
            )],
            Vec::new(),
        )])
        .unwrap(),
    )
}

fn install(micro: &str, methods: &[&str]) -> MicroInstall {
    MicroInstall {
        drawers: vec![],
        micro: micro.to_owned(),
        methods: methods
            .iter()
            .map(|nsid| (*nsid).to_owned())
            .collect::<BTreeSet<_>>(),
    }
}

/// A node directory whose peers service binds loopback only, so no test reaches a relay.
fn loopback_root(root: &std::path::Path) {
    day_node::NodeSettings::create(root).unwrap();
    day_node::NodeSettings::update(root, |settings| {
        settings.peers = Some(day_node::PeerSettings {
            loopback_only: true,
            ..Default::default()
        });
        Ok(())
    })
    .unwrap();
}

/// A micro installed with `ECHO` granted calls it through its egress on a booted node, is denied
/// what it was not granted, and the composition then stops cleanly. The node survives a reboot
/// on the same directory with the same identity.
#[tokio::test]
async fn a_wasmtime_micro_calls_a_granted_method_through_the_composition() {
    let dir = tempfile::tempdir().unwrap();
    let config = || NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: no_schemas(),
        installs: vec![install(FIXTURE, &[ECHO])],
    };
    let composition = NodeComposition::boot(config()).await.unwrap();
    let node_id = composition.node().node_id.clone();
    composition
        .node()
        .dispatch
        .router
        .register(
            "clay-test",
            [ECHO, REVEAL].map(|nsid| {
                (
                    nsid.to_owned(),
                    Operation::unchecked(Some(
                        big_cabinet_core::datum::operation::OperationKind::Procedure,
                    )),
                )
            }),
            ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(TestMethods))),
        )
        .expect("the test methods are unclaimed");
    assert!(
        composition
            .egress("did:example:clay-local/not-installed")
            .is_none(),
        "a micro outside the install table has no egress"
    );

    let attempts = run_fixture(&composition).await;

    let granted = attempts["granted"]
        .strip_prefix("ok(")
        .and_then(|rest| rest.strip_suffix(')'))
        .unwrap_or_else(|| panic!("granted should answer: {}", attempts["granted"]));
    assert_eq!(
        serde_json::from_str::<Value>(granted).unwrap(),
        json!({ "operation": ECHO, "echo": "ping" })
    );
    let ungranted = &attempts["ungranted"];
    assert!(
        ungranted.starts_with("err(") && ungranted.contains("403") && ungranted.contains("Denied"),
        "a method outside the install table's grant is denied by dispatch: {ungranted}"
    );
    composition.stop().await.unwrap();

    let again = NodeComposition::boot(config()).await.unwrap();
    assert_eq!(
        again.node().node_id,
        node_id,
        "the identity is the directory's"
    );
    again.stop().await.unwrap();
}

/// A micro whose install grants nothing can call nothing.
#[tokio::test]
async fn a_micro_installed_with_no_methods_calls_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let composition = NodeComposition::boot(NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: no_schemas(),
        installs: vec![install(FIXTURE, &[])],
    })
    .await
    .unwrap();

    let attempts = run_fixture(&composition).await;

    for name in ["granted", "ungranted", "forged"] {
        let outcome = &attempts[name];
        assert!(
            outcome.starts_with("err(") && outcome.contains("401"),
            "{name} has no credential to present: {outcome}"
        );
    }
    composition.stop().await.unwrap();
}

/// The operator pairs a node with another by putting its ticket in a bootstrap file: the node
/// dials it at boot, and both then report the connection.
#[tokio::test(flavor = "multi_thread")]
async fn a_node_dials_the_ticket_in_its_bootstrap_file() {
    let (host_dir, phone_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    loopback_root(host_dir.path());
    loopback_root(phone_dir.path());
    let host = NodeComposition::boot(NodeConfig {
        root: host_dir.path().to_owned(),
        bootstrap: vec![],
        schemas: no_schemas(),
        installs: vec![],
    })
    .await
    .unwrap();
    let tickets = phone_dir.path().join("bootstrap.txt");
    assert!(
        read_tickets(&tickets).unwrap().is_empty(),
        "an unpaired node has no tickets"
    );
    std::fs::write(
        &tickets,
        format!("# the laptop\n\n  {}  \n", host.address()),
    )
    .unwrap();
    let phone = NodeComposition::boot(NodeConfig {
        root: phone_dir.path().to_owned(),
        bootstrap: read_tickets(&tickets).unwrap(),
        schemas: no_schemas(),
        installs: vec![],
    })
    .await
    .unwrap();

    for (this, other) in [(&phone, &host), (&host, &phone)] {
        let peers = this.node().peers.as_ref().unwrap();
        let other = other.node().repo.local_peer_id().to_bytes32().unwrap();
        let mut events = peers.subscribe();
        loop {
            let connected = peers.connected().await.unwrap();
            if connected
                .iter()
                .any(|peer| peer.to_bytes32().unwrap() == other)
            {
                break;
            }
            match events.recv().await {
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("the peers service stopped")
                }
            }
        }
    }

    // The ticket outlived the boot: the node dials it again with no file.
    let settings = day_node::NodeSettings::load(phone_dir.path()).unwrap();
    assert_eq!(settings.peers.unwrap().bootstrap, [host.address()]);
    phone.stop().await.unwrap();
    host.stop().await.unwrap();
}

const NOTE: &str = "org.example.note";

/// Creates a note in `drawer` as the node's drawers actor does, and returns its id.
async fn create_note(node: &big_node::BigNode, drawer: &DrawerId, text: &str) -> DocId {
    let placement = format!("drawer {drawer}");
    let capability = node
        .docs
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDocCapability {
                scope: DocScope {
                    create: BTreeSet::from([placement.clone()]),
                    ..DocScope::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
    node.docs
        .client
        .call(
            RequestContext::root(),
            CreateDoc {
                capability,
                command: CommandId::try_from(format!("create-{text}")).unwrap(),
                placement,
                datums: BTreeMap::from([(
                    format!("{NOTE}/main").parse::<DatumKey>().unwrap(),
                    json!({"$type": NOTE, "value": text}),
                )]),
            },
        )
        .await
        .unwrap()
        .doc_id
}

/// What a guest's `readDatums` is on the wire: a JSON `POST` to `dispatch.local`, answered by the
/// micro's egress. Returns the HTTP status and the JSON body.
async fn read_datums(
    egress: &Arc<dyn DispatchEgress>,
    capability: CapId,
    doc_id: &DocId,
) -> (u16, Value) {
    let body = serde_json::to_vec(&ReadDatums {
        capability,
        doc_id: doc_id.clone(),
        at: At::Current,
        datums: None,
        optimistic: false,
    })
    .unwrap();
    let request = http::Request::post(format!(
        "http://dispatch.local/xrpc/{}",
        ReadDatums::XRPC.nsid
    ))
    .header(http::header::CONTENT_TYPE, "application/json")
    .body(
        Full::new(Bytes::from(body))
            .map_err(|never| match never {})
            .boxed_unsync(),
    )
    .unwrap();
    let response = egress.send(request).await;
    let status = response.status().as_u16();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

/// The interim document capability is bound to the micro's actor and scoped to the drawers its
/// install names: the micro reads a document in a granted drawer through its egress with it, and
/// is denied a document in a drawer it was not granted, even though the node holds both.
#[tokio::test]
async fn a_micro_reads_documents_only_from_its_granted_drawers() {
    let dir = tempfile::tempdir().unwrap();
    let mut composition = NodeComposition::boot(NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: note_schemas(),
        installs: vec![],
    })
    .await
    .unwrap();
    let local = composition.node().drawers.local();
    let (granted_drawer, other_drawer) = (local.present_id.clone(), local.originated_id.clone());
    let granted_doc = create_note(composition.node(), &granted_drawer, "visible").await;
    let other_doc = create_note(composition.node(), &other_drawer, "hidden").await;
    assert!(
        composition.doc_capability(FIXTURE).is_none(),
        "nothing is minted for a micro that is not installed"
    );

    composition
        .install_micro(MicroInstall {
            drawers: vec![(granted_drawer, DrawerAccess::Read)],
            ..install(FIXTURE, &[ReadDatums::XRPC.nsid])
        })
        .await
        .unwrap();
    let capability = composition
        .doc_capability(FIXTURE)
        .expect("a micro with drawers is minted a document capability");
    let egress = composition.egress(FIXTURE).unwrap();

    let (status, read) = read_datums(&egress, capability, &granted_doc).await;
    assert_eq!(status, 200, "{read}");
    let text = serde_json::to_string(&read).unwrap();
    assert!(
        text.contains("visible"),
        "the granted drawer's note is read: {text}"
    );
    let (status, denied) = read_datums(&egress, capability, &other_doc).await;
    assert_eq!(status, 403, "{denied}");
    assert!(
        !denied.to_string().contains("hidden"),
        "a refused read leaks nothing of the document: {denied}"
    );

    // A micro with no drawers has no capability to present.
    composition
        .install_micro(install(
            "did:example:clay-local/other",
            &[ReadDatums::XRPC.nsid],
        ))
        .await
        .unwrap();
    assert!(
        composition
            .doc_capability("did:example:clay-local/other")
            .is_none()
    );
    composition.stop().await.unwrap();
}
