//! The composition end to end: it boots a real node from a directory, a Wasmtime micro (the
//! `dispatch` fixture, prepared as `DAYTXT_DISPATCH_COMPONENT`) calls a granted method through
//! the egress the composition installed for it, a second node pairs with the first by a
//! bootstrap ticket, and shutdown is clean.

use crate::InstallConfig;
use crate::MicroInstall;
use crate::NodeComposition;
use crate::NodeConfig;
use crate::read_tickets;
use crate::test_support::NOTE;
use crate::test_support::create_drawer;
use crate::test_support::create_note;
use crate::test_support::no_schemas;
use crate::test_support::note_schemas;
use crate::test_support::read_datums;
use async_trait::async_trait;
use big_cabinet_core::doc::ReadDatums;
use big_cabinet_core::drawer::DrawerAccess;
use big_cabinet_core::drawer::EntryAccess;
use big_cartridges_core::CLAY_MICRO_MANIFEST_SCHEMA;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_cartridges_core::MicroContribution;
use big_cartridges_core::Necessity;
use big_cartridges_core::Permbind;
use big_cartridges_core::PermbindDatum;
use big_cartridges_core::PermbindDrawers;
use big_cartridges_core::PermbindMethod;
use big_cartridges_core::RoutineUse;
use big_cartridges_core::config::Component;
use big_cartridges_core::config::GrantBody;
use big_cartridges_core::config::GrantKey;
use big_cartridges_core::protocol::GetCartridge;
use big_cartridges_core::protocol::InstallCartridge;
use big_cartridges_core::protocol::PublishCartridgeVersion;
use big_cartridges_core::protocol::SetCartridgeEnabled;
use big_cartridges_core::protocol::SetCartridgeGrant;
use big_cartridges_core::registry::version_key;
use big_cartridges_core::registry::version_pin;
use big_cartridges_core::test_support::FixtureCartridge;
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

mod egress;

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

fn install(micro: &str, methods: &[&str]) -> MicroInstall {
    MicroInstall {
        drawers: vec![],
        config: None,
        micro: micro.to_owned(),
        methods: methods
            .iter()
            .map(|nsid| (*nsid).to_owned())
            .collect::<BTreeSet<_>>(),
    }
}

/// A node directory whose peers service binds loopback only, so no test reaches a relay.
async fn loopback_root(root: &std::path::Path) {
    let dir = day_node::NodeDir::new(root.to_owned());
    dir.init().await.unwrap();
    dir.update_settings(|settings| {
        settings.peers = Some(day_node::PeerSettings {
            loopback_only: true,
            ..Default::default()
        });
        Ok(())
    })
    .await
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
        serve: false,
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

/// A serving host holds the node's socket where `day` finds it, so `day` and the host share one
/// node; a second host on the same directory is refused with who holds it; the socket goes with
/// the holder.
#[tokio::test]
async fn a_serving_composition_holds_the_node_where_day_finds_it() {
    let dir = tempfile::tempdir().unwrap();
    let config = || NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: no_schemas(),
        installs: vec![],
        serve: true,
    };
    let composition = NodeComposition::boot(config()).await.unwrap();
    let node_dir = day_node::NodeDir::new(dir.path().to_owned());
    assert_eq!(
        node_dir.probe_socket().await.unwrap(),
        day_node::SocketState::Live
    );

    let refused = match NodeComposition::boot(config()).await {
        Ok(_) => panic!("a second holder boots the same storage"),
        Err(error) => format!("{error:#}"),
    };
    assert!(
        refused.contains(&format!("held by process {}", std::process::id())),
        "{refused}"
    );

    composition.stop().await.unwrap();
    assert_eq!(
        node_dir.probe_socket().await.unwrap(),
        day_node::SocketState::Absent
    );
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
        serve: false,
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
    loopback_root(host_dir.path()).await;
    loopback_root(phone_dir.path()).await;
    let host = NodeComposition::boot(NodeConfig {
        root: host_dir.path().to_owned(),
        bootstrap: vec![],
        schemas: no_schemas(),
        installs: vec![],
        serve: false,
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
        serve: false,
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
        serve: false,
    })
    .await
    .unwrap();
    let granted_drawer = create_drawer(composition.node(), "granted").await;
    let other_drawer = create_drawer(composition.node(), "other").await;
    let granted_doc = create_note(composition.node(), &granted_drawer, "visible").await;
    let other_doc = create_note(composition.node(), &other_drawer, "hidden").await;
    assert!(
        composition.doc_capability(FIXTURE).is_none(),
        "nothing is minted for a micro that is not installed"
    );

    composition
        .install_micro(MicroInstall {
            drawers: vec![(granted_drawer.clone(), DrawerAccess::Read)],
            // The shape the review micro's install takes: its methods, its drawers and a config
            // datum naming them, to which the composition adds the capability.
            config: Some(InstallConfig {
                blob_drawers: None,
                tag: "org.example.test.config".to_owned(),
                key: "main".to_owned(),
                value: serde_json::Map::from_iter([(
                    "drawers".to_owned(),
                    json!([granted_drawer.to_string()]),
                )]),
            }),
            ..install(
                FIXTURE,
                &[
                    ReadDatums::XRPC.nsid,
                    "org.example.test.summarize",
                    "org.example.test.poll",
                ],
            )
        })
        .await
        .unwrap();
    let capability = composition
        .doc_capability(FIXTURE)
        .expect("a micro with drawers is minted a document capability");
    let egress = composition.egress(FIXTURE).unwrap();
    let (tag, key, config) = composition.config_datum(FIXTURE).unwrap();
    assert_eq!(
        (tag.as_str(), key.as_str()),
        ("org.example.test.config", "main")
    );
    assert_eq!(
        *config,
        json!({
            "drawers": [granted_drawer.to_string()],
            "docCap": serde_json::to_value(capability).unwrap(),
        }),
        "the config datum carries the install's members and the minted capability"
    );

    let (status, read) = read_datums(&egress, capability, &granted_doc).await;
    assert_eq!(status, 200, "{read}");
    let text = serde_json::to_string(&read).unwrap();
    assert!(
        text.contains("visible"),
        "the granted drawer's note is read: {text}"
    );
    let (status, denied) = read_datums(&egress, capability, &other_doc).await;
    assert_eq!(
        (status, denied["error"].as_str()),
        (400, Some("Denied")),
        "a drawer outside the install is denied by the docs service: {denied}"
    );
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

/// Drives a node's cartridges service as its operator, as the CLI and the phone's install do.
struct Operator(intxt_core::client::Caller<crate::OperatorTransport>);

impl Operator {
    fn of(composition: &NodeComposition) -> Self {
        Self(intxt_core::client::Caller::new(
            composition.operator_transport(),
            RequestContext::root(),
            None,
            "cartridge-micro-test".to_owned(),
        ))
    }

    /// Publishes `fixture` and installs it by the pin of the version just published, with its
    /// micro enabled.
    async fn install_micro(&self, fixture: FixtureCartridge) -> CartridgeId {
        let (id, version) = (
            fixture.resolved.cartridge.id.clone(),
            fixture.resolved.cartridge.version.clone(),
        );
        let component = fixture
            .blobs
            .values()
            .next()
            .expect("the fixture carries its micro's component");
        intxt_core::install::put_blob(&self.0, component)
            .await
            .unwrap();
        let staged = self.0.call(fixture.stage_request()).await.unwrap();
        self.0
            .call(PublishCartridgeVersion {
                envelope: fixture.publishable(staged),
            })
            .await
            .unwrap();
        let doc_id = self
            .0
            .call(GetCartridge { id: id.clone() })
            .await
            .unwrap()
            .doc_id;
        let (heads, _) = self.0.read(&doc_id, &version_key(&version)).await.unwrap();
        self.0
            .call(InstallCartridge {
                cartridge: version_pin(doc_id, &version, heads),
            })
            .await
            .unwrap();
        self.set_micro_enabled(&id, true).await;
        id
    }

    async fn set_micro_enabled(&self, id: &CartridgeId, enabled: bool) {
        self.0
            .call(SetCartridgeEnabled {
                cartridge_id: id.clone(),
                component: Component::Micro,
                enabled,
            })
            .await
            .unwrap();
    }

    async fn grant(&self, id: &CartridgeId, grant: GrantKey, value: Option<GrantBody>) {
        self.0
            .call(SetCartridgeGrant {
                cartridge_id: id.clone(),
                grant,
                value,
            })
            .await
            .unwrap();
    }
}

/// A micro-only cartridge: the contribution names `component`, whose bytes are `wasm`, and
/// `permbinds` are the manifest's, of which the micro `uses` some.
fn micro_cartridge(
    name: &str,
    permbinds: impl IntoIterator<Item = (&'static str, Permbind)>,
    uses: impl IntoIterator<Item = (&'static str, RoutineUse)>,
) -> FixtureCartridge {
    let mut fixture = FixtureCartridge::new(Cartridge::new(
        CartridgeId::new(big_cartridges_core::example_namespace!(), name).unwrap(),
        "3.0.0".parse().unwrap(),
    ));
    fixture.resolved.cartridge.authority = Some("org.example.clay.test".to_owned());
    fixture.with_wasm("component", MICRO_WASM);
    fixture.resolved.cartridge.permbinds = permbinds
        .into_iter()
        .map(|(key, permbind)| (key.to_owned(), permbind))
        .collect();
    fixture.resolved.cartridge.micro = Some(MicroContribution {
        component: "component".to_owned(),
        schema: CLAY_MICRO_MANIFEST_SCHEMA.to_owned(),
        description: [("entrypoints".to_owned(), json!({"view": "presentation"}))]
            .into_iter()
            .collect(),
        uses: uses
            .into_iter()
            .map(|(key, usage)| (key.to_owned(), usage))
            .collect(),
        extra: Default::default(),
    });
    fixture
}

/// The bytes of the fixture micro's component: the loader reads them, nothing runs them here.
const MICRO_WASM: &[u8] = b"\0asm\x01\0\0\0cartridge-micro-fixture";

fn method_permbind(nsid: &str) -> Permbind {
    Permbind::Method(PermbindMethod {
        nsid: nsid.parse().unwrap(),
        description: format!("calls {nsid}"),
        necessity: Necessity::Optional,
        min_revision: None,
        extra: Default::default(),
    })
}

fn uses_read() -> RoutineUse {
    RoutineUse {
        read: true,
        ..Default::default()
    }
}

/// The wire form of a guest's call to `nsid` with `input`, answered by the micro's egress.
async fn call_method(egress: &Arc<dyn DispatchEgress>, nsid: &str, input: &Value) -> (u16, Value) {
    let request = http::Request::post(format!("http://dispatch.local/xrpc/{nsid}"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(
            Full::new(Bytes::from(serde_json::to_vec(input).unwrap()))
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .unwrap();
    let response = egress.send(request).await;
    let status = response.status().as_u16();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

/// A micro a cartridge contributes is loaded from the catalog by the `component` its contribution
/// names, and its egress is minted from its `uses` intersected with the cartridge's grants: a
/// method it uses but was not granted is denied, one it was granted but does not use is denied
/// (the grant is the cartridge's, and each component's `uses` narrows it), a grant that is
/// given or revoked reaches the running micro's egress through its grants follower, and a
/// disabled micro is dropped on the next sync.
#[tokio::test]
async fn a_cartridge_micro_is_minted_from_its_uses_and_the_cartridges_grants() {
    let dir = tempfile::tempdir().unwrap();
    let mut composition = NodeComposition::boot(NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: no_schemas(),
        installs: vec![],
        serve: false,
    })
    .await
    .unwrap();
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
    let operator = Operator::of(&composition);
    assert!(
        composition
            .sync_cartridge_micros()
            .await
            .unwrap()
            .is_empty(),
        "no cartridge is installed"
    );

    let id = operator
        .install_micro(micro_cartridge(
            "cart-micro",
            [
                ("echo", method_permbind(ECHO)),
                ("reveal", method_permbind(REVEAL)),
            ],
            [("echo", RoutineUse::default())],
        ))
        .await;
    let micro = id.to_string();
    let loaded = composition.sync_cartridge_micros().await.unwrap();
    let [loaded] = loaded.as_slice() else {
        panic!("one cartridge micro is installed: {loaded:?}");
    };
    let loaded = loaded.as_ref().unwrap();
    assert_eq!(loaded.micro, micro, "the micro's id is the cartridge's");
    assert_eq!(
        loaded.component, MICRO_WASM,
        "the component is the one the contribution's `component` key pins"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&loaded.manifest_json).unwrap(),
        json!({
            "entrypoints": {"view": "presentation"},
            "id": "cart-micro",
            "package_version": 3,
            "authority": "org.example.clay.test",
        }),
        "the manifest is the contribution's description with the identity the envelope owns"
    );

    let egress = composition.egress(&micro).unwrap();
    let (status, denied) = call_method(&egress, ECHO, &json!({"text": "ping"})).await;
    assert_eq!(
        (status, denied["error"].as_str()),
        (403, Some("Denied")),
        "nothing is granted yet: {denied}"
    );

    let mut changes = composition.handles_changed(&micro).unwrap();
    for method in [ECHO, REVEAL] {
        operator
            .grant(
                &id,
                GrantKey::Method(method.to_owned()),
                Some(GrantBody::present()),
            )
            .await;
    }
    // The micro's grants follower mints its handles again, and the egress the micro holds
    // presents the new capability from its next call.
    changes.changed().await.unwrap();
    let (status, answer) = call_method(&egress, ECHO, &json!({"text": "ping"})).await;
    assert_eq!(
        (status, &answer),
        (200, &json!({"operation": ECHO, "echo": "ping"}))
    );
    let (status, denied) = call_method(&egress, REVEAL, &json!({"text": "ping"})).await;
    assert_eq!(
        (status, denied["error"].as_str()),
        (403, Some("Denied")),
        "REVEAL is granted to the cartridge but not in the micro's `uses`: {denied}"
    );

    operator
        .grant(&id, GrantKey::Method(ECHO.to_owned()), None)
        .await;
    changes.changed().await.unwrap();
    let (status, _) = call_method(&egress, ECHO, &json!({"text": "ping"})).await;
    assert_eq!(
        status, 403,
        "a revoked grant is gone once the follower mints again"
    );

    operator.set_micro_enabled(&id, false).await;
    assert!(
        composition
            .sync_cartridge_micros()
            .await
            .unwrap()
            .is_empty(),
        "a disabled micro is not loaded"
    );
    assert!(composition.egress(&micro).is_none(), "and it holds nothing");
    composition.stop().await.unwrap();
}

/// A micro's document capability is lowered from its `#drawers` and `#datum` uses and the drawers
/// the cartridge was granted: it reads the notes of a granted drawer, and only those.
#[tokio::test]
async fn a_cartridge_micros_document_capability_follows_the_granted_drawers() {
    let dir = tempfile::tempdir().unwrap();
    let mut composition = NodeComposition::boot(NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: note_schemas(),
        installs: vec![],
        serve: false,
    })
    .await
    .unwrap();
    let granted_drawer = create_drawer(composition.node(), "granted").await;
    let other_drawer = create_drawer(composition.node(), "other").await;
    let granted_doc = create_note(composition.node(), &granted_drawer, "visible").await;
    let other_doc = create_note(composition.node(), &other_drawer, "hidden").await;
    let operator = Operator::of(&composition);
    let id = operator
        .install_micro(micro_cartridge(
            "cart-reader",
            [
                (
                    "drawers",
                    Permbind::Drawers(PermbindDrawers {
                        description: "Notebooks".to_owned(),
                        resolution: Default::default(),
                        read: Some(Necessity::Required),
                        write: None,
                        create: None,
                        delete: None,
                        extra: Default::default(),
                    }),
                ),
                (
                    "notes",
                    Permbind::Datum(PermbindDatum {
                        nsid: NOTE.parse().unwrap(),
                        description: "Notes".to_owned(),
                        read: Some(Necessity::Required),
                        write: None,
                        create: None,
                        delete: None,
                        extra: Default::default(),
                    }),
                ),
                ("readDatums", method_permbind(ReadDatums::XRPC.nsid)),
            ],
            [
                ("drawers", uses_read()),
                ("notes", uses_read()),
                ("readDatums", RoutineUse::default()),
            ],
        ))
        .await;
    let micro = id.to_string();
    for loaded in composition.sync_cartridge_micros().await.unwrap() {
        loaded.unwrap();
    }
    assert!(
        composition.doc_capability(&micro).is_none(),
        "no drawer is granted, so the micro reaches no document"
    );

    let mut changes = composition.handles_changed(&micro).unwrap();
    operator
        .grant(
            &id,
            GrantKey::Drawer(granted_drawer.clone()),
            Some(GrantBody::access(EntryAccess::Read)),
        )
        .await;
    operator
        .grant(
            &id,
            GrantKey::Method(ReadDatums::XRPC.nsid.to_owned()),
            Some(GrantBody::present()),
        )
        .await;
    changes.changed().await.unwrap();
    let capability = composition
        .doc_capability(&micro)
        .expect("a granted drawer the micro uses mints it a document capability");
    let egress = composition.egress(&micro).unwrap();
    let (status, read) = read_datums(&egress, capability, &granted_doc).await;
    assert_eq!(status, 200, "{read}");
    assert!(
        serde_json::to_string(&read).unwrap().contains("visible"),
        "the granted drawer's note is read: {read}"
    );
    let (status, denied) = read_datums(&egress, capability, &other_doc).await;
    assert_eq!(
        (status, denied["error"].as_str()),
        (400, Some("Denied")),
        "a drawer the cartridge was not granted is denied: {denied}"
    );
    composition.stop().await.unwrap();
}
