//! The composition end to end: it boots a real node from a directory, a Wasmtime micro (the
//! `dispatch` fixture, prepared as `DAYFRAME_DISPATCH_COMPONENT`) calls a granted method through
//! the egress the composition installed for it, a second node pairs with the first by a
//! bootstrap ticket, and shutdown is clean.

use crate::InstallConfig;
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
use big_cabinet_core::drawer::EntryAccess;
use big_cabinet_core::drawer::protocol::CreateDrawer;
use big_cabinet_core::drawer::protocol::CreateLayout;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawerScope;
use big_cabinet_core::drawer::protocol::MintDrawerCapability;
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
        config: None,
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

/// Creates a drawer of its own as the node's drawers actor does. (The node's two required local
/// drawers hold every document, so a document in one is in the other.)
async fn create_drawer(node: &big_node::BigNode, name: &str) -> DrawerId {
    node.drawer_access
        .grants()
        .grant(
            &RequestContext::root(),
            node.drawers_actor.actor.actor_id,
            big_cabinet_core::drawer::protocol::CREATE_DRAWER_OBJECT.into(),
            Rights::USE,
            None,
        )
        .await
        .unwrap();
    let capability = node
        .drawers
        .client
        .call_with(
            RequestContext::root(),
            Authorization::identity(node.drawers_actor.capability).metadata(),
            MintDrawerCapability {
                scope: DrawerScope {
                    create: true,
                    ..DrawerScope::default()
                },
                rights: Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
    node.drawers
        .client
        .call(
            RequestContext::root(),
            CreateDrawer {
                capability,
                command: CommandId::try_from(format!("create-drawer-{name}")).unwrap(),
                layout: CreateLayout::SingleGroup,
            },
        )
        .await
        .unwrap()
        .drawer_id
}

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

const REVIEW: &str = "did:example:clay-local/dayframe.review";

/// The review micro's install from the operator's `review.json`: its drawers are read from the
/// file, its config datum is the file's members plus the minted `docCap`, the capability reads the
/// listed drawers and no other, and the datum is where the micro's manifest asks for it. With no
/// file the micro installs unconfigured: no drawers, no datum, no capability.
#[tokio::test]
async fn a_review_install_file_configures_the_micro_and_no_file_leaves_it_unconfigured() {
    let dir = tempfile::tempdir().unwrap();
    let mut composition = NodeComposition::boot(NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: note_schemas(),
        installs: vec![],
    })
    .await
    .unwrap();
    let github = create_drawer(composition.node(), "github").await;
    let intxt = create_drawer(composition.node(), "intxt").await;
    let other = create_drawer(composition.node(), "other").await;
    let github_doc = create_note(composition.node(), &github, "a pull").await;
    let other_doc = create_note(composition.node(), &other, "hidden").await;

    let file = json!({
        "githubDrawer": github.to_string(),
        "intxtDrawers": [intxt.to_string()],
        // A file that carries `docCap` must not decide it: only the composition mints one.
        "docCap": "forged",
    });
    let file = serde_json::to_vec(&file).unwrap();
    let installs = MicroInstall::interim([REVIEW.to_owned()], |name| {
        assert_eq!(name, "review.json");
        Ok(Some(file.clone()))
    })
    .unwrap();
    assert_eq!(installs.len(), 1);
    assert!(
        installs[0]
            .methods
            .contains("org.example.intxt.summarizeIntxts"),
        "the review micro's grants come with its install: {:?}",
        installs[0].methods
    );
    for install in installs {
        composition.install_micro(install).await.unwrap();
    }
    let capability = composition
        .doc_capability(REVIEW)
        .expect("a configured review micro is minted a document capability");
    let (tag, key, config) = composition.config_datum(REVIEW).unwrap();
    // The datum goes where the micro's manifest reads its configuration.
    let manifest = micro_review_manifest::manifest();
    assert!(
        manifest.configuration_requests.iter().any(|request| {
            request.facets.iter().any(|facet| {
                facet.tag.as_str() == tag && facet.key_id.as_deref().is_none_or(|id| id == key)
            })
        }),
        "the review manifest requests {tag}/{key}"
    );
    assert_eq!(
        *config,
        json!({
            "githubDrawer": github.to_string(),
            "intxtDrawers": [intxt.to_string()],
            "docCap": serde_json::to_value(capability).unwrap(),
        })
    );
    let egress = composition.egress(REVIEW).unwrap();
    let (status, read) = read_datums(&egress, capability, &github_doc).await;
    assert_eq!(status, 200, "{read}");
    let (status, denied) = read_datums(&egress, capability, &other_doc).await;
    assert_eq!((status, denied["error"].as_str()), (400, Some("Denied")));

    // No file: the micro is installed with its grants and nothing configured.
    let installs = MicroInstall::interim([REVIEW.to_owned()], |_| Ok(None)).unwrap();
    for install in installs {
        composition.install_micro(install).await.unwrap();
    }
    assert!(composition.doc_capability(REVIEW).is_none());
    assert!(composition.config_datum(REVIEW).is_none());

    // A file naming something that is not a drawer is refused, not half read.
    let refused = MicroInstall::interim([REVIEW.to_owned()], |_| {
        Ok(Some(
            br#"{"githubDrawer": "nope", "intxtDrawers": []}"#.to_vec(),
        ))
    })
    .unwrap_err();
    assert!(
        format!("{refused:#}").contains("githubDrawer"),
        "the refusal names the member: {refused:#}"
    );
    composition.stop().await.unwrap();
}

/// Where intxt's handler component is built: `INTXT_GUEST_WASM`, else the `wasm_utils_rs`
/// workspace's release output (the same lookup `day_cli`'s e2e uses).
fn intxt_guest() -> Vec<u8> {
    let path = std::env::var_os("INTXT_GUEST_WASM")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../wasm_utils_rs/target/wasm32-wasip3/release/intxt_nodule_guest.wasm")
        });
    std::fs::read(&path).unwrap_or_else(|error| {
        panic!(
            "{}: {error}; build it with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest --target wasm32-wasip3 --release` or set INTXT_GUEST_WASM",
            path.display()
        )
    })
}

/// What the phone does at boot: install intxt from its bundled handler over the composition's own
/// operator transport. The review micro's actor then reaches `summarizeIntxts` through its
/// egress (it could not before: the node did not serve the method), and the install is
/// idempotent, so the next boot's call writes nothing.
#[tokio::test]
async fn installing_intxt_at_boot_makes_summarize_intxts_reachable_for_the_review_micro() {
    let wasm = intxt_guest();
    let dir = tempfile::tempdir().unwrap();
    let mut composition = NodeComposition::boot(NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: no_schemas(),
        installs: vec![],
    })
    .await
    .unwrap();
    let drawer = create_drawer(composition.node(), "intxt").await;
    let installs = MicroInstall::interim([REVIEW.to_owned()], |_| Ok(None)).unwrap();
    for install in installs {
        composition.install_micro(install).await.unwrap();
    }
    let egress = composition.egress(REVIEW).unwrap();
    let summarize = |drawers: Vec<String>| {
        let egress = Arc::clone(&egress);
        async move {
            let body = serde_json::to_vec(&json!({ "drawers": drawers })).unwrap();
            let request =
                http::Request::post("http://dispatch.local/xrpc/org.example.intxt.summarizeIntxts")
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
            (status, serde_json::from_slice::<Value>(&body).unwrap())
        }
    };
    let (status, before) = summarize(vec![]).await;
    assert_ne!(status, 200, "intxt is not installed yet: {before}");

    let installed = composition.install_intxt(&wasm).await.unwrap();
    assert!(!installed.already_installed);
    let (status, empty) = summarize(vec![]).await;
    assert_eq!((status, &empty), (200, &json!({ "results": [] })));
    let (status, one) = summarize(vec![drawer.to_string()]).await;
    assert_eq!(status, 200, "{one}");
    assert_eq!(
        one["results"][0]["drawer"],
        json!(drawer.to_string()),
        "a drawer that holds no intxt is answered, not failed: {one}"
    );

    assert!(
        composition
            .install_intxt(&wasm)
            .await
            .unwrap()
            .already_installed,
        "a second boot's install is a no-op"
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
/// revoked takes effect at the next sync, and a disabled micro is dropped.
#[tokio::test]
async fn a_cartridge_micro_is_minted_from_its_uses_and_the_cartridges_grants() {
    let dir = tempfile::tempdir().unwrap();
    let mut composition = NodeComposition::boot(NodeConfig {
        root: dir.path().to_owned(),
        bootstrap: vec![],
        schemas: no_schemas(),
        installs: vec![],
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

    for method in [ECHO, REVEAL] {
        operator
            .grant(
                &id,
                GrantKey::Method(method.to_owned()),
                Some(GrantBody::present()),
            )
            .await;
    }
    // The handle was minted for the grants of the last sync: a grant is seen at the next one.
    let (status, _) = call_method(&egress, ECHO, &json!({"text": "ping"})).await;
    assert_eq!(status, 403, "the held capability does not widen by itself");
    // The egress the micro holds is the same one: it presents the re-minted capability.
    composition.sync_cartridge_micros().await.unwrap();
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
    composition.sync_cartridge_micros().await.unwrap();
    let (status, _) = call_method(&egress, ECHO, &json!({"text": "ping"})).await;
    assert_eq!(status, 403, "a revoked grant is gone after the next sync");

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
    composition.sync_cartridge_micros().await.unwrap();
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
