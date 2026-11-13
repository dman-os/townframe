//! Listener attachments (ADR 033 §3, §5): the node's router served over a Unix socket and TCP.
//! The docs call goes through the typed `Client<DocsProtocol>::xrpc`, the nodule call through
//! plain HTTP, since a nodule's operations have no Rust request type.

use super::cartridges::operator;
use super::dispatch::narrowed;
use super::dispatch::operations;
use super::nodule_wasm::Spec;
use super::nodule_wasm::admit;
use super::nodule_wasm::caller;
use super::nodule_wasm::cartridge;
use super::nodule_wasm::guest_bytes;
use super::nodule_wasm::put;
use super::nodule_wasm::relay;
use super::nodule_wasm::set_enabled;
use super::nodule_wasm::set_operations;
use super::*;
use big_blobs_core::BlobId;
use big_blobs_core::MAX_PUT_OCTETS;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::GetDocAvailability;
use big_cartridges_core::Digest;
use big_dispatch::Operation;
use big_dispatch::Provider;
use big_dispatch::ProviderKind;
use big_dispatch::ProviderState;
use big_dispatch_core::Body;
use big_dispatch_core::DispatchError;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_dispatch_core::Origin;
use method_core::Method;
use service_client_native_rs::Client;
use service_client_native_rs::DescriptorClient;
use service_client_native_rs::Transport;
use service_client_native_rs::XrpcClient;
use service_utils_rs::Envelope;
use service_utils_rs::HttpMetadata;
use service_utils_rs::descriptor;
use std::io::Seek;
use std::io::SeekFrom;
use std::os::fd::OwnedFd;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use tokio::io::Interest;
use tokio::net::UnixStream;

pub(super) const MAX_RESPONSE: usize = 1 << 20;

#[cfg(unix)]
async fn send_descriptor_part(
    stream: &UnixStream,
    bytes: &[u8],
    descriptors: &[OwnedFd],
) -> std::io::Result<()> {
    stream
        .async_io(Interest::WRITABLE, || {
            descriptor::send_part(stream, bytes, descriptors)
        })
        .await
        .map(|_| ())
}

#[cfg(unix)]
async fn assert_descriptor_connection_refused(label: &str, mut stream: UnixStream) {
    let mut response = [0; 1];
    let read = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        stream.read(&mut response),
    )
    .await
    .expect("descriptor connection did not close")
    .unwrap_or_else(|error| {
        if error.kind() == std::io::ErrorKind::ConnectionReset {
            return 0;
        }
        panic!("reading refused descriptor connection failed: {error}");
    });
    assert_eq!(
        read, 0,
        "{label}: refused descriptor connection returned a response"
    );
}

/// A node with the echo nodule enabled, listening as `listeners` says.
async fn boot_listening(
    secrets: &Arc<SecretStore>,
    listeners: Listeners,
) -> (BigNode, StopToken, Spec) {
    boot_listening_requesting(secrets, listeners, &[]).await
}

/// [`boot_listening`] with the nodule requesting (optionally) each of `calls`, which its grant can
/// then cover.
async fn boot_listening_requesting(
    secrets: &Arc<SecretStore>,
    listeners: Listeners,
    calls: &[String],
) -> (BigNode, StopToken, Spec) {
    let bytes = guest_bytes();
    let runtime = Arc::new(SqliteRuntime::new(Default::default()));
    let sql = runtime.ephemeral(&[]).unwrap();
    let mut config = config(
        Arc::clone(secrets),
        sql,
        big_repo::Storage::memory(&runtime).unwrap(),
    );
    config.listeners = listeners;
    let (node, stop) = BigNode::boot(config).await.unwrap();
    let spec = caller();
    put(&node, &bytes).await;
    admit(&node, cartridge(&spec, &bytes, calls)).await;
    set_enabled(&node, &spec, true).await;
    (node, stop, spec)
}

pub(super) fn unix(path: &Path) -> Listeners {
    Listeners {
        unix_socket: Some(path.to_owned()),
        ..Default::default()
    }
}

pub(super) fn owner_only_directory(path: &Path) -> PathBuf {
    std::fs::create_dir(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path.to_owned()
}

pub(super) fn tcp() -> Listeners {
    Listeners {
        tcp: Some("127.0.0.1:0".parse().unwrap()),
        ..Default::default()
    }
}

/// One plain XRPC procedure call: the status and the JSON body.
pub(super) async fn post(
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
        request = request.header("authorization", authorization.header_value());
    }
    let response = request.send().await.unwrap();
    (response.status().as_u16(), response.json().await.unwrap())
}

/// One plain XRPC query call: the status and the JSON body. `query` is the raw query string.
async fn get(
    client: &XrpcClient,
    nsid: &str,
    authorization: Option<Authorization>,
    query: &str,
) -> (u16, Value) {
    let mut url = client.base_url().join(&format!("xrpc/{nsid}")).unwrap();
    url.set_query(Some(query));
    let mut request = client.http_client().get(url);
    if let Some(authorization) = authorization {
        request = request.header("authorization", authorization.header_value());
    }
    let response = request.send().await.unwrap();
    (response.status().as_u16(), response.json().await.unwrap())
}

pub(super) fn tcp_client(node: &BigNode) -> XrpcClient {
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

    let docs = Client::new(Transport::<DocsProtocol>::xrpc(client.clone()));
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
    let docs = Client::new(Transport::<DocsProtocol>::xrpc(client.clone()));
    let denied = create_note(&node, &docs, token.metadata()).await;
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

#[cfg(unix)]
#[tokio::test]
async fn import_file_batches_descriptors_over_the_native_socket() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let (node, stop, _) = boot_listening(&secrets, unix(&socket)).await;
    let first_path = directory.path().join("first.txt");
    let second_path = directory.path().join("second.txt");
    let first_bytes = b"first descriptor";
    let second_bytes = b"second descriptor";
    std::fs::write(&first_path, first_bytes).unwrap();
    std::fs::write(&second_path, second_bytes).unwrap();

    let mut first_file = std::fs::File::open(&first_path).unwrap();
    first_file.seek(SeekFrom::Start(5)).unwrap();
    let first_fd: OwnedFd = first_file.try_clone().unwrap().into();
    let (_pipe_read, pipe_write) = rustix::pipe::pipe().unwrap();
    let second_fd: OwnedFd = std::fs::File::open(&second_path).unwrap().into();
    let capability = super::blobs::put_cap(&node).await;
    let parameters = serde_json::json!({ "capability": capability });
    let results: Vec<serde_json::Value> = DescriptorClient::unix(
        node.listeners
            .fd_socket
            .as_ref()
            .expect("descriptor listener is attached"),
    )
    .call(
        "org.example.daybook.blobs.importFile",
        &parameters,
        vec![first_fd, pipe_write, second_fd],
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0]["kind"], "imported");
    assert_eq!(results[1]["kind"], "refused");

    assert!(
        results[1]["error"]["reason"]
            .as_str()
            .unwrap()
            .contains("descriptor is not a regular file")
    );
    assert_eq!(results[2]["kind"], "imported");
    assert_eq!(first_file.stream_position().unwrap(), 5);

    for (result, bytes) in [
        (&results[0], first_bytes.as_slice()),
        (&results[2], second_bytes.as_slice()),
    ] {
        let info: big_blobs_core::BlobInfo =
            serde_json::from_value(result["info"].clone()).unwrap();
        assert_eq!(info.blob_id, BlobId::new(Digest::of(bytes).0));
        let body = node
            .blobs
            .read(
                RequestContext::root(),
                super::blobs::read_cap(&node, &info.blob_id).await,
                info.blob_id,
                None,
            )
            .await
            .unwrap();
        assert_eq!(body.collect(MAX_PUT_OCTETS).await.unwrap().as_ref(), bytes);
    }

    shutdown(node, stop).await;
}

#[cfg(unix)]
#[tokio::test]
async fn import_file_refuses_an_oversized_item_but_imports_the_rest() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let (node, stop, _) = boot_listening(&secrets, unix(&socket)).await;
    let oversized_path = directory.path().join("oversized");
    let normal_path = directory.path().join("normal");
    std::fs::File::create(&oversized_path)
        .unwrap()
        .set_len(MAX_PUT_OCTETS + 1)
        .unwrap();
    std::fs::write(&normal_path, b"after oversized").unwrap();
    let capability = super::blobs::put_cap(&node).await;
    let parameters = serde_json::json!({ "capability": capability });
    let results: Vec<serde_json::Value> = DescriptorClient::unix(
        node.listeners
            .fd_socket
            .as_ref()
            .expect("descriptor listener is attached"),
    )
    .call_files(
        "org.example.daybook.blobs.importFile",
        &parameters,
        &[oversized_path, normal_path],
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 2);

    assert_eq!(results[0]["kind"], "refused");
    assert_eq!(results[0]["error"]["code"], "tooLarge");
    assert_eq!(results[0]["error"]["limitOctets"], MAX_PUT_OCTETS);
    assert_eq!(results[1]["kind"], "imported");
    shutdown(node, stop).await;
}

#[cfg(unix)]
#[tokio::test]
async fn descriptor_socket_rejects_late_descriptors_and_second_frames() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let (node, stop, _) = boot_listening(&secrets, unix(&socket)).await;
    let fd_socket = node.listeners.fd_socket.as_ref().unwrap().clone();
    let capability = super::blobs::put_cap(&node).await;
    let parameters = serde_json::json!({ "capability": capability });
    let frame =
        descriptor::encode_request("org.example.daybook.blobs.importFile", &parameters, 1).unwrap();

    let late = UnixStream::connect(&fd_socket).await.unwrap();
    send_descriptor_part(&late, &frame[..4], &[]).await.unwrap();
    let late_fd: OwnedFd = std::fs::File::open("/etc/hosts").unwrap().into();
    send_descriptor_part(&late, &frame[4..], &[late_fd])
        .await
        .unwrap();
    assert_descriptor_connection_refused("late descriptor", late).await;

    let second = UnixStream::connect(&fd_socket).await.unwrap();
    let first_fd: OwnedFd = std::fs::File::open("/etc/hosts").unwrap().into();
    send_descriptor_part(
        &second,
        &[frame.as_slice(), frame.as_slice()].concat(),
        &[first_fd],
    )
    .await
    .unwrap();
    assert_descriptor_connection_refused("second frame", second).await;

    shutdown(node, stop).await;
}

#[cfg(unix)]
#[tokio::test]
async fn descriptor_import_is_refused_from_a_remote_origin() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let (node, stop, _) = boot_listening(&secrets, unix(&socket)).await;
    let capability = super::blobs::put_cap(&node).await;
    let descriptor: OwnedFd = std::fs::File::open("/etc/hosts").unwrap().into();
    let result = node
        .dispatch
        .client
        .call_with(
            RequestContext::root(),
            operator(&node).metadata(),
            Invoke {
                operation: "org.example.daybook.blobs.importFile".into(),
                parameters: serde_json::json!({ "capability": capability }),
                body: Body::Descriptors(descriptor::DescriptorBody::from_fds(vec![descriptor])),
                context: InvocationContext {
                    origin: Origin::Remote,
                    ..Default::default()
                },
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(service_utils_rs::ClientError::Method(
            DispatchError::LocalOnly
        ))
    ));
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

/// A query is served over GET: its query string is typed by the method's Lexicon `parameters`
/// (undeclared names such as a cache-buster are ignored), checked, and the answer is the same
/// JSON a POST of a procedure gives. The wrong verb for a method's kind is atproto's
/// `InvalidRequest`.
#[tokio::test]
async fn a_query_is_served_over_get_and_the_wrong_verb_is_invalid() {
    let secrets = secrets().await;
    let (node, stop, _) = boot_listening(&secrets, tcp()).await;
    let client = tcp_client(&node);
    let operator = Authorization::identity(node.authority.root);
    let note = create(&node, "get-query", json!({"$type": NOTE, "value": "x"})).await;
    let capability = capability(
        &node,
        DocScope {
            read: BTreeSet::from([note.doc_id.clone()]),
            ..DocScope::default()
        },
    )
    .await;
    let availability = GetDocAvailability::XRPC.nsid;
    let query = format!(
        "capability={}&docId={}&cacheBuster=7f3",
        serde_json::to_value(capability).unwrap().as_str().unwrap(),
        serde_json::to_value(&note.doc_id)
            .unwrap()
            .as_str()
            .unwrap(),
    );

    let (status, body) = get(&client, availability, Some(operator), &query).await;
    assert_eq!(status, 200, "{body}");
    let (status, node_info) = get(&client, crate::GET_NODE_INFO, Some(operator), "").await;
    assert_eq!(status, 200, "{node_info}");
    assert!(node_info["nodeId"].is_string(), "{node_info}");

    // A missing required parameter is the caller's: checked against the Lexicon, not the
    // service.
    let (status, body) = get(&client, availability, Some(operator), "docId=nope").await;
    assert_eq!(
        (status, body["error"].as_str()),
        (400, Some("InvalidRequest"))
    );
    // Without a token the listener refuses before dispatch, as it does a POST.
    let (status, _) = get(&client, availability, None, &query).await;
    assert_eq!(status, 401);
    // atproto: `Incorrect HTTP method (POST) expected GET`, and the converse.
    let (status, body) = post(&client, availability, Some(operator), json!({})).await;
    assert_eq!(
        (status, body["error"].as_str()),
        (400, Some("InvalidRequest"))
    );
    assert!(
        body["message"].as_str().unwrap().contains("expected GET"),
        "{body}"
    );
    let (status, body) = get(&client, CreateDoc::XRPC.nsid, Some(operator), "").await;
    assert_eq!(
        (status, body["error"].as_str()),
        (400, Some("InvalidRequest"))
    );
    assert!(
        body["message"].as_str().unwrap().contains("expected POST"),
        "{body}"
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
            fd_socket: None,
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
    let mut second = config(
        Arc::clone(&secrets),
        sql,
        big_repo::Storage::memory(&runtime).unwrap(),
    );
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
    let mut config = config(
        Arc::clone(&secrets),
        sql,
        big_repo::Storage::memory(&runtime).unwrap(),
    );
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
    let mut config = config(
        Arc::clone(&secrets),
        sql,
        big_repo::Storage::memory(&runtime).unwrap(),
    );
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

#[tokio::test]
async fn draining_closes_the_listener_first_and_keeps_the_clients_it_had_accepted() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let (node, stop, spec) = boot_listening(&secrets, unix(&socket)).await;
    let clients = node
        .unix_clients
        .as_ref()
        .expect("the socket listener runs");

    // A client with a request answered holds an accepted, counted connection.
    let client = XrpcClient::unix(&socket, MAX_RESPONSE).unwrap();
    let (status, _) = post(&client, &spec.echo(), None, json!({ "n": 1 })).await;
    assert_eq!(status, 200);

    clients.close_listener().await.unwrap();
    // Closed means refused for a newcomer, while the accepted client is still served.
    let refused = tokio::net::UnixStream::connect(&socket).await.unwrap_err();
    assert!(
        matches!(
            refused.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
        ),
        "{refused:?}"
    );
    let (status, _) = post(&client, &spec.echo(), None, json!({ "n": 2 })).await;
    assert_eq!(status, 200);

    // Idle once the last client has gone.
    drop(client);
    clients.wait_until_idle().await.unwrap();

    shutdown(node, stop).await;
}

/// The descriptor socket follows the HTTP socket's lifecycle: draining closes its listener and
/// removes its file, but the serving task stays until it is stopped, so stopping afterwards is the
/// clean stop it asks for (a task that ended on drain made `stop` panic).
#[tokio::test]
async fn a_drained_descriptor_socket_still_stops_cleanly() {
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let (node, stop, _spec) = boot_listening(&secrets, unix(&socket)).await;
    let fd_socket = node.listeners.fd_socket.as_ref().unwrap().clone();
    let fd_clients = node
        .fd_clients
        .as_ref()
        .expect("the descriptor listener runs");

    fd_clients.drain().await.unwrap();
    let refused = UnixStream::connect(&fd_socket).await.unwrap_err();
    assert!(
        matches!(
            refused.kind(),
            std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
        ),
        "{refused:?}"
    );
    // The next daemon replaces the socket file; stopping this node must not need the old task
    // to have ended on its own.
    shutdown(node, stop).await;
}

/// Answers `{"ok": true}` to whatever it is asked.
struct AlwaysOk;

#[async_trait::async_trait]
impl Provider for AlwaysOk {
    async fn call(
        &self,
        _: &big_dispatch::Caller,
        _: &str,
        _: &RequestContext,
        _: &InvocationContext,
        _: Value,
        _: Option<Body>,
    ) -> Result<Body, big_dispatch::ProviderError> {
        Result::Ok(Body::Json(json!({ "ok": true })))
    }
}

/// A local-only method (ADR 042 §3) is served over the Unix socket, whose peer the kernel says
/// is the node's own uid, and refused to the very same operator capability over TCP and to a
/// nodule's egress, even when the nodule is serving a call that was itself local.
#[tokio::test]
async fn a_local_only_method_is_served_over_the_socket_and_refused_over_tcp_and_to_a_guest() {
    const NSID: &str = "org.example.local.only";
    let secrets = secrets().await;
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("run/node.sock");
    let (node, stop, spec) = boot_listening_requesting(
        &secrets,
        Listeners {
            tcp: tcp().tcp,
            ..unix(&socket)
        },
        &[NSID.to_owned()],
    )
    .await;
    node.dispatch
        .router
        .register(
            "local",
            [(
                NSID.to_owned(),
                Operation {
                    local_only: true,
                    ..Operation::unchecked(Some(OperationKind::Procedure))
                },
            )],
            ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(AlwaysOk))),
        )
        .unwrap();

    let unix_client = XrpcClient::unix(&socket, MAX_RESPONSE).unwrap();
    let (status, body) = post(&unix_client, NSID, None, json!({})).await;
    assert_eq!((status, &body), (200, &json!({ "ok": true })));

    let operator = Authorization::identity(node.authority.root);
    let (status, body) = post(&tcp_client(&node), NSID, Some(operator), json!({})).await;
    assert_eq!((status, body["error"].as_str()), (403, Some("LocalOnly")));

    set_operations(
        &node,
        &spec,
        DispatchScope {
            operations: [NSID.to_owned()].into(),
            ..Default::default()
        },
    )
    .await;
    let relayed = relay(
        &node,
        &spec,
        NSID,
        None,
        InvocationContext {
            origin: Origin::Embedded,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(relayed["status"], 403, "{relayed}");
    assert_eq!(relayed["body"]["error"], "LocalOnly", "{relayed}");
    shutdown(node, stop).await;
}
