//! Listener attachments (ADR 032 §3, §5): the node's router served over a Unix socket and TCP.
//! The docs call goes through the typed `Client<DocsProtocol>::xrpc`, the nodule call through
//! plain HTTP, since a nodule's operations have no Rust request type.

use super::dispatch::narrowed;
use super::dispatch::operations;
use super::nodule_wasm::Spec;
use super::nodule_wasm::admit;
use super::nodule_wasm::caller;
use super::nodule_wasm::guest_bytes;
use super::nodule_wasm::plug;
use super::nodule_wasm::put;
use super::nodule_wasm::set_enabled;
use super::*;
use big_cabinet_core::doc::DocsProtocol;
use big_dispatch_core::BearerToken;
use service_utils_rs::Client;
use service_utils_rs::Envelope;
use service_utils_rs::HttpMetadata;
use service_utils_rs::Operation;
use service_utils_rs::XrpcClient;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;

const MAX_RESPONSE: usize = 1 << 20;

/// A node with the echo nodule enabled, listening as `listeners` says.
async fn boot_listening(secrets: &SecretStore, listeners: Listeners) -> (BigNode, StopToken, Spec) {
    let bytes = guest_bytes();
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let mut config = config(secrets, &runtime, sql, StorageConfig::Memory);
    config.listeners = listeners;
    let (node, stop) = BigNode::boot(config).await.unwrap();
    let spec = caller();
    put(&node, &bytes).await;
    admit(&node, plug(&spec, &bytes, &[])).await;
    set_enabled(&node, &spec, true).await;
    (node, stop, spec)
}

fn unix(path: &Path) -> Listeners {
    Listeners {
        unix_socket: Some(path.to_owned()),
        ..Default::default()
    }
}

fn owner_only_directory(path: &Path) -> PathBuf {
    std::fs::create_dir(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path.to_owned()
}

fn tcp() -> Listeners {
    Listeners {
        tcp: Some("127.0.0.1:0".parse().unwrap()),
        ..Default::default()
    }
}

fn bearer(authorization: Authorization) -> HttpMetadata {
    HttpMetadata {
        authorization: Some(BearerToken(authorization).header_value()),
        idempotency_key: None,
    }
}

/// One plain XRPC procedure call: the status and the JSON body.
async fn post(
    client: &XrpcClient,
    nsid: &str,
    authorization: Option<Authorization>,
    input: Value,
) -> (u16, Value) {
    let mut request = client
        .http_client()
        .post(client.base_url().join(&format!("xrpc/{nsid}")).unwrap())
        .json(&input);
    if let Some(authorization) = authorization {
        request = request.header("authorization", BearerToken(authorization).header_value());
    }
    let response = request.send().await.unwrap();
    (response.status().as_u16(), response.json().await.unwrap())
}

fn tcp_client(node: &BigNode) -> XrpcClient {
    let address = node.listeners.tcp.expect("the node listens on TCP");
    XrpcClient::new(format!("http://{address}/").parse().unwrap(), MAX_RESPONSE).unwrap()
}

async fn create_note(
    node: &BigNode,
    docs: &Client<DocsProtocol>,
    metadata: HttpMetadata,
) -> Result<WriteReceipt, service_utils_rs::ClientError<big_cabinet_core::doc::DocsError>> {
    let placement = format!("drawer {}", node.drawers.local().present_id);
    let capability = capability(
        node,
        DocScope {
            create: BTreeSet::from([placement.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    docs.call_with_metadata(Envelope::with_metadata(
        RequestContext::root(),
        metadata,
        CreateDoc {
            capability,
            command: CommandId::try_from("note-over-http".to_owned()).unwrap(),
            placement,
            datums: BTreeMap::from([(key(), json!({"$type": NOTE, "value": "over http"}))]),
        },
    ))
    .await
    .map(|response| response.output)
}

#[tokio::test]
async fn the_operator_reaches_built_in_and_nodule_operations_over_the_socket_without_a_token() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    // The node creates the missing parent of the socket, owner-only.
    let socket = directory.path().join("run/node.sock");
    let (node, stop, spec) = boot_listening(&secrets, unix(&socket)).await;
    assert_eq!(
        node.listeners.unix_socket.as_deref(),
        Some(socket.as_path())
    );
    assert_eq!(
        std::fs::metadata(socket.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    let client = XrpcClient::unix(&socket, MAX_RESPONSE).unwrap();

    let docs = Client::<DocsProtocol>::xrpc(client.clone());
    let receipt = create_note(&node, &docs, HttpMetadata::default())
        .await
        .unwrap();
    let stored = read(&node, &receipt.doc_id).await;
    assert!(matches!(
        &stored.datums[&key()],
        DatumRead::Accepted { value, .. } if value["value"] == "over http"
    ));

    let (status, echoed) = post(&client, &spec.echo(), None, json!({ "n": 7 })).await;
    assert_eq!(status, 200, "{echoed}");
    assert_eq!(echoed["input"], json!({ "n": 7 }), "{echoed}");

    // An operation nothing claims is unknown to the one router, not a transport failure.
    let (status, body) = post(&client, "org.example.nothing.here", None, json!({})).await;
    assert_eq!(
        (status, body["error"].as_str()),
        (404, Some("UnknownMethod"))
    );
    shutdown(node, stop).await;
    assert!(!socket.exists());
}

#[tokio::test]
async fn a_token_narrows_a_socket_connection_to_its_scope() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let (node, stop, spec) = boot_listening(&secrets, unix(&socket)).await;
    let client = XrpcClient::unix(&socket, MAX_RESPONSE).unwrap();
    let token = narrowed(&node, operations(&[&spec.echo()])).await;

    let (status, echoed) = post(&client, &spec.echo(), Some(token), json!({ "n": 1 })).await;
    assert_eq!(status, 200, "{echoed}");
    // The same uid, so the operator without a token could create the document. The token
    // narrows it: docs operations are outside its scope and denied before the service runs.
    let docs = Client::<DocsProtocol>::xrpc(client.clone());
    let denied = create_note(&node, &docs, bearer(token)).await;
    let Err(denied) = denied else {
        panic!("a token without docs scope must not create a document");
    };
    assert!(format!("{denied:?}").contains("403"), "{denied:?}");
    let (status, body) = post(&client, CreateDoc::XRPC.nsid, Some(token), json!({})).await;
    assert_eq!((status, body["error"].as_str()), (403, Some("Forbidden")));
    let (status, _) = post(&client, CreateDoc::XRPC.nsid, None, json!({})).await;
    assert_ne!(status, 403, "without a token the operator is not narrowed");
    shutdown(node, stop).await;
}

#[tokio::test]
async fn tcp_admits_only_the_capability_each_request_presents() {
    let secrets = secrets().await;
    let (node, stop, spec) = boot_listening(&secrets, tcp()).await;
    assert_ne!(node.listeners.tcp.unwrap().port(), 0);
    let client = tcp_client(&node);
    let operator = Authorization::identity(node.authority.root);

    let (status, body) = post(&client, &spec.echo(), None, json!({})).await;
    assert_eq!(
        (status, body["error"].as_str()),
        (401, Some("AuthenticationRequired"))
    );
    let malformed = client
        .http_client()
        .post(
            client
                .base_url()
                .join(&format!("xrpc/{}", spec.echo()))
                .unwrap(),
        )
        .header("authorization", "Bearer not-a-token")
        .json(&json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status().as_u16(), 401);

    let (status, echoed) = post(&client, &spec.echo(), Some(operator), json!({ "n": 2 })).await;
    assert_eq!(status, 200, "{echoed}");
    let narrow = narrowed(&node, operations(&[&spec.echo()])).await;
    assert_eq!(
        post(&client, &spec.echo(), Some(narrow), json!({})).await.0,
        200
    );
    let (status, body) = post(&client, CreateDoc::XRPC.nsid, Some(narrow), json!({})).await;
    assert_eq!((status, body["error"].as_str()), (403, Some("Forbidden")));
    // A capability the node never minted is no capability at all.
    let forged = Authorization::identity(CapId::new_random());
    assert_eq!(
        post(&client, &spec.echo(), Some(forged), json!({})).await.0,
        403
    );
    shutdown(node, stop).await;
}

#[tokio::test]
async fn stopping_closes_both_listeners_and_removes_the_socket() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let (node, stop, _) = boot_listening(
        &secrets,
        Listeners {
            unix_socket: Some(socket.clone()),
            tcp: Some("127.0.0.1:0".parse().unwrap()),
        },
    )
    .await;
    let tcp = tcp_client(&node);
    let address = node.listeners.tcp.unwrap();
    assert!(socket.exists());
    shutdown(node, stop).await;
    assert!(!socket.exists());
    assert!(tokio::net::TcpStream::connect(address).await.is_err());
    drop(tcp);
}

#[tokio::test]
async fn a_stale_socket_is_replaced_but_a_live_socket_or_a_foreign_file_is_not() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let run = owner_only_directory(&directory.path().join("run"));
    let socket = run.join("node.sock");
    // A socket file left by a process that died: nothing listens on it.
    drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
    assert!(socket.exists());
    let (node, stop, _) = boot_listening(&secrets, unix(&socket)).await;

    // A second node on the same path must not take the live socket over.
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let mut second = config(&secrets, &runtime, sql, StorageConfig::Memory);
    second.listeners = unix(&socket);
    let Err(error) = BigNode::boot(second).await else {
        panic!("a live socket must not be replaced");
    };
    assert!(
        format!("{error:#}").contains("another process is listening"),
        "{error:#}"
    );
    // The failed boot did not remove the live node's socket.
    assert!(socket.exists());
    shutdown(node, stop).await;

    let file: PathBuf = run.join("regular");
    std::fs::write(&file, b"not a socket").unwrap();
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let mut config = config(&secrets, &runtime, sql, StorageConfig::Memory);
    config.listeners = unix(&file);
    let Err(error) = BigNode::boot(config).await else {
        panic!("a regular file must not be replaced");
    };
    assert!(
        format!("{error:#}").contains("is not a socket"),
        "{error:#}"
    );
    assert_eq!(std::fs::read(&file).unwrap(), b"not a socket");
}

#[tokio::test]
async fn a_socket_directory_other_users_can_enter_is_refused() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let parent = directory.path().join("shared");
    std::fs::create_dir(&parent).unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o755)).unwrap();
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let mut config = config(&secrets, &runtime, sql, StorageConfig::Memory);
    config.listeners = unix(&parent.join("node.sock"));
    let Err(error) = BigNode::boot(config).await else {
        panic!("an open directory must be refused");
    };
    assert!(
        format!("{error:#}").contains("accessible to other users"),
        "{error:#}"
    );
    assert!(!parent.join("node.sock").exists());
}
