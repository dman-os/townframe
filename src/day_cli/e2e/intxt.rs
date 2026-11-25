//! The intxt layer of the library e2e tests: the real handler component installed on a node the
//! way `day x intxt install --handler` does, and the commands that run against it.

use super::library::connect;
use super::library::new_node;
use crate::client::NodeClient;
use crate::interlude::*;
use big_cabinet_core::datum::DatumMember as _;
use intxt_core::work;

mod patch;

/// Where the handler component is built (see `intxt install --handler`): CI builds it before
/// these tests, and `INTXT_GUEST_WASM` names it when it is elsewhere.
pub(super) fn guest_path() -> PathBuf {
    use std::path::Path;
    std::env::var_os("INTXT_GUEST_WASM")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../wasm_utils_rs/target/wasm32-wasip3/release/intxt_nodule_guest.wasm")
        })
}

/// Runs `body` against an initialized node with the real handler component installed the way
/// `day x intxt install --handler` does and loaded by the node's wasm executor, then stops the
/// node on every exit path. A failed assertion in `body` (or in the install) still stops the
/// node before the panic resumes, so its owner tasks shut down in order and the first failure
/// is the only one reported.
///
/// Needs the component, built with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml
/// -p intxt_nodule_guest --target wasm32-wasip3 --release`.
pub(super) async fn with_intxt_node(body: impl AsyncFnOnce(&tempfile::TempDir, &NodeClient)) {
    use big_dispatch_core::Readiness;
    use utils_rs::prelude::FutureExt;

    let (tmp, dir) = new_node().await;

    let wasm_path = guest_path();
    let wasm = std::fs::read(&wasm_path).unwrap_or_else(|error| {
        panic!(
            "{}: {error}; build it with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest --target wasm32-wasip3 --release` or set INTXT_GUEST_WASM",
            wasm_path.display()
        )
    });
    let client = connect(&dir).await.unwrap();
    let outcome = std::panic::AssertUnwindSafe(async {
        let installed = intxt_core::install::install(&operator(&client), &wasm, &[])
            .await
            .unwrap();
        assert!(!installed.already_installed);
        assert_eq!(installed.record.readiness, Readiness::Ready);
        assert!(installed.record.methods.unwrap().granted);
        body(&tmp, &client).await;
    })
    .catch_unwind()
    .await;
    client.stop().await.unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// A caller with the operator's own authority over the node `client` reaches, as `day x intxt
/// install` makes.
fn operator(client: &NodeClient) -> intxt_core::client::Caller<&NodeClient> {
    intxt_core::client::Caller::new(
        client,
        service_utils_rs::RequestContext::root(),
        None,
        "intxt-install".to_owned(),
    )
}

/// The index document of `drawer`: where its title, entries and body order are.
pub(super) async fn index_of(
    client: &NodeClient,
    drawer: &big_cabinet_core::drawer::protocol::DrawerId,
) -> big_cabinet_core::DocId {
    operator(client).index(drawer).await.unwrap()
}

/// A document capability the operator mints for `scope`.
pub(super) async fn doc_capability(
    client: &NodeClient,
    scope: big_cabinet_core::doc::DocScope,
) -> Res<big_cabinet_core::authority::CapId> {
    Ok(crate::cmds::intxt::call(
        client,
        big_cabinet_core::doc::MintDocCapability {
            scope,
            rights: big_cabinet_core::authority::Rights::USE,
            subject: None,
        },
    )
    .await?
    .capability_id)
}

/// The document's heads and the accepted value of `key` in it at those heads.
pub(super) async fn read_at(
    client: &NodeClient,
    doc: &big_cabinet_core::DocId,
    key: &big_cabinet_core::DatumKey,
) -> Res<(Vec<big_cabinet_core::doc::ChangeHash>, serde_json::Value)> {
    use big_cabinet_core::doc::At;
    use big_cabinet_core::doc::DatumRead;
    use big_cabinet_core::doc::DocScope;
    use big_cabinet_core::doc::ReadDatums;

    let capability = doc_capability(
        client,
        DocScope {
            read: DocScope::read_all([doc.clone()]),
            ..DocScope::default()
        },
    )
    .await?;
    let mut read = crate::cmds::intxt::call(
        client,
        ReadDatums {
            capability,
            doc_id: doc.clone(),
            at: At::Current,
            datums: Some([key.clone()].into()),
            optimistic: false,
        },
    )
    .await?;
    match read.datums.remove(key) {
        Some(DatumRead::Accepted { value, .. }) => Ok((read.heads, value)),
        other => eyre::bail!("{key} in {doc} is not readable: {other:?}"),
    }
}

/// A command id no earlier write used: the node deduplicates writes by command id.
pub(super) fn command(kind: &str) -> big_cabinet_core::doc::CommandId {
    let nonce: [u8; 16] = rand::random();
    big_cabinet_core::doc::CommandId::try_from(format!(
        "{kind}-{}",
        data_encoding::HEXLOWER.encode(&nonce)
    ))
    .expect("a kind and a hex nonce make a command id")
}

/// `day x <nsid> <input>`: the operation through the router, whose answer must be a success.
pub(super) async fn day_x(
    client: &NodeClient,
    nsid: &str,
    input: serde_json::Value,
) -> serde_json::Value {
    let reply = client
        .call_untyped(nsid, method_core::HttpMethod::Post, &input)
        .await
        .unwrap();
    assert!(reply.is_success(), "{nsid} answered {reply:?}");
    reply.body
}

/// `org.example.intxt.createIntxt`, through the real installed handler, creates a drawer whose
/// own document carries the intxt marker and the pointer to its index document, which holds the
/// title and an empty body, readable as written.
#[tokio::test(flavor = "multi_thread")]
async fn create_intxt_creates_a_drawer_marked_as_an_intxt() {
    use big_cabinet_core::drawer::protocol::DrawerId;

    with_intxt_node(async |_tmp, client| {
        let created = day_x(
            client,
            "org.example.intxt.createIntxt",
            json!({"title": "Datum references move to dtxt:"}),
        )
        .await;
        let drawer: DrawerId = created["drawerId"].as_str().unwrap().parse().unwrap();
        let index = index_of(client, &drawer).await;
        let accepted = |doc: &big_cabinet_core::DocId, key: &str| {
            let (client, doc) = (&client, doc.clone());
            let key = key.parse().unwrap();
            async move { read_at(client, &doc, &key).await.unwrap().1 }
        };
        assert_eq!(
            accepted(&drawer, "org.example.intxt/main").await,
            json!({"empty": true})
        );
        assert_eq!(
            accepted(&drawer, "org.example.intxt.index/main").await,
            json!({"doc": index.to_string()})
        );
        assert_eq!(
            accepted(&index, "org.example.intxt.title/main").await,
            json!({"title": "Datum references move to dtxt:"})
        );
        assert_eq!(
            accepted(&index, "org.example.intxt.body/main").await,
            json!({"order": []})
        );
        assert_drawer_document_holds_only_the_pointers(client, &drawer).await;
    })
    .await;
}

/// The drawer's own document is a public metadata root a relay can read (ADR 019, "Meta docs"),
/// so it holds the cabinet's descriptor and roster (`org.example.daybook.drawer…`) and, of intxt's,
/// only the marker and the pointer to the index document: no title, no entry, no comment, no
/// commit.
async fn assert_drawer_document_holds_only_the_pointers(
    client: &NodeClient,
    drawer: &big_cabinet_core::drawer::protocol::DrawerId,
) {
    let keys: std::collections::BTreeSet<String> = operator(client)
        .datum_keys(drawer)
        .await
        .unwrap()
        .into_iter()
        .map(|key| key.to_string())
        .filter(|key| !key.starts_with("org.example.daybook.drawer"))
        .collect();
    assert_eq!(
        keys,
        [
            "org.example.intxt/main".to_owned(),
            "org.example.intxt.index/main".to_owned()
        ]
        .into(),
        "the drawer's own document holds more than the descriptor, the marker and the pointer"
    );
}

/// Installing again, with the node's handler already installed and enabled, is a no-op: it answers
/// the record it already holds and publishes no second version.
#[tokio::test(flavor = "multi_thread")]
async fn installing_intxt_twice_changes_nothing() {
    use big_cartridges_core::protocol::GetCartridge;

    with_intxt_node(async |_tmp, client| {
        let wasm = std::fs::read(guest_path()).unwrap();
        let before = crate::cmds::intxt::call(
            client,
            GetCartridge {
                id: intxt_core::cartridge::cartridge(&wasm).id.clone(),
            },
        )
        .await
        .unwrap();
        let again = intxt_core::install::install(&operator(client), &wasm, &[])
            .await
            .unwrap();
        assert!(again.already_installed);
        assert_eq!(again.record.readiness, big_dispatch_core::Readiness::Ready);
        let after = crate::cmds::intxt::call(
            client,
            GetCartridge {
                id: before.id.clone(),
            },
        )
        .await
        .unwrap();
        assert_eq!(after, before);
        assert_eq!(after.versions.len(), 1);
    })
    .await;
}

/// An older revision of intxt's cartridge, as an earlier build of `intxt_core` published it:
/// the current manifest without `createDoc` (its permbind and `createIntxt`'s use of it), the
/// 7-method set a node installed before that request existed. Installed the way `install` does,
/// granting the methods it requests, and enabled.
async fn install_revision_without_create_doc(
    client: &NodeClient,
    wasm: &[u8],
) -> big_nodule_core::protocol::NoduleRecord {
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

    let caller = operator(client);
    let mut fixture = intxt_core::cartridge::cartridge(wasm);
    assert!(fixture.permbinds.remove("createDoc").is_some());
    let routines = &mut fixture.nodule.as_mut().unwrap().routines;
    for routine in routines.values_mut() {
        routine.uses.remove("createDoc");
    }
    let (cartridge_id, version) = (fixture.id.clone(), fixture.version.clone());
    intxt_core::install::put_blob(&caller, wasm).await.unwrap();
    let staged = caller.call(fixture.stage_request()).await.unwrap();
    caller
        .call(PublishCartridgeVersion {
            envelope: fixture.publishable(staged),
        })
        .await
        .unwrap();
    let doc_id = caller
        .call(GetCartridge {
            id: cartridge_id.clone(),
        })
        .await
        .unwrap()
        .doc_id;
    let (heads, _) = caller.read(&doc_id, &version_key(&version)).await.unwrap();
    caller
        .call(InstallCartridge {
            cartridge: version_pin(doc_id, &version, heads),
        })
        .await
        .unwrap();
    for method in intxt_core::cartridge::requested_methods(&fixture) {
        caller
            .call(SetCartridgeGrant {
                cartridge_id: cartridge_id.clone(),
                grant: GrantKey::Method(method.to_owned()),
                value: Some(GrantBody::present()),
            })
            .await
            .unwrap();
    }
    caller
        .call(SetCartridgeEnabled {
            cartridge_id: cartridge_id.clone(),
            component: Component::Nodule,
            enabled: true,
        })
        .await
        .unwrap();
    caller
        .call(big_nodule_core::protocol::GetNodule { cartridge_id })
        .await
        .unwrap()
}

/// Installing a build whose manifest requests a method the node's installed revision did not
/// (`createDoc`) grants it, so the nodule is ready: both when the new build has another handler
/// (another version) and when only the manifest changed under the same handler bytes (the same
/// version, which `install` takes for current).
#[tokio::test(flavor = "multi_thread")]
async fn installing_a_revision_that_requests_a_new_method_grants_it() {
    use big_dispatch_core::Readiness;

    let wasm = std::fs::read(guest_path()).unwrap();
    // Another handler build: a custom section changes the bytes and so the version.
    let mut older = wasm.clone();
    older.extend_from_slice(&[0, 2, 1, b'x']);
    for (case, old_wasm) in [("another handler", &older), ("the same handler", &wasm)] {
        let (_tmp, dir) = new_node().await;
        let client = connect(&dir).await.unwrap();
        let before = install_revision_without_create_doc(&client, old_wasm).await;
        assert_eq!(before.readiness, Readiness::Ready, "{case}");
        const CREATE_DOC: &str = "org.example.daybook.docs.createDoc";
        assert!(
            !before.methods.unwrap().desired.operations.contains(CREATE_DOC),
            "{case}"
        );

        let installed = intxt_core::install::install(&operator(&client), &wasm, &[])
            .await
            .unwrap();
        let methods = installed.record.methods.clone().unwrap();
        assert_eq!(
            installed.record.readiness,
            Readiness::Ready,
            "{case}: {:?}",
            installed.record
        );
        assert!(
            methods.desired.operations.contains(CREATE_DOC),
            "{case}: {methods:?}"
        );
        client.stop().await.unwrap();
    }
}

/// Runs `git` in `dir` with nothing taken from the machine. Hooks do not run: capture installs a
/// `post-rewrite` hook that execs `day`, which must never find a real one on the `PATH` when a
/// test amends or rebases.
fn git(dir: &std::path::Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "core.hooksPath")
        .env("GIT_CONFIG_VALUE_0", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Ada")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_COMMITTER_NAME", "Grace")
        .env("GIT_COMMITTER_EMAIL", "grace@example.com")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A repository at `<base>/repo` with `main` holding `files` in one commit, and `feat/x`
/// switched to.
fn init_repo(base: &std::path::Path, files: &[(&str, &str)]) -> PathBuf {
    let repo = base.join("repo");
    std::fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    for (path, content) in files {
        std::fs::write(repo.join(path), content).unwrap();
    }
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-q", "-m", "first"]);
    git(&repo, &["switch", "-q", "-c", "feat/x"]);
    repo
}

/// The commit `spec` names in `repo`.
fn rev(repo: &std::path::Path, spec: &str) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["rev-parse", spec])
        .output()
        .unwrap();
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

/// A scripted Claude Code session through `capture`: the hooks' JSON in, the real handler's
/// documents out. A tool call writes a text file and a binary one; the binary file's bytes go
/// to the blob store and its file document carries the blob datum, which `file.blob`
/// references (ledger D45).
#[tokio::test(flavor = "multi_thread")]
async fn capture_records_a_session_and_stores_a_binary_file_as_a_blob() {
    use crate::cmds::intxt::capture;
    use big_cabinet_core::DocId;
    use big_cabinet_core::datum::DatumMember;
    use big_cabinet_core::drawer::DrawerAccess;
    use big_cabinet_core::drawer::protocol::DrawerId;
    use big_cabinet_core::drawer::protocol::ListDocs;
    use big_cabinet_core::drawer::protocol::MintDrawerCapability;
    use intxt_core::work;
    use std::io::Write;

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let repo = base.join("repo");
        std::fs::create_dir(&repo).unwrap();
        git(&repo, &["init", "-q", "-b", "main"]);
        std::fs::write(repo.join("a.txt"), "one\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "first"]);
        let transcript = base.join("claude/s1.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        let mut file = std::fs::File::create(&transcript).unwrap();
        for record in [
            json!({"type": "user", "uuid": "u1", "parentUuid": null, "timestamp": "2026-11-10T10:00:00Z",
                   "cwd": repo, "sessionId": "s1", "message": {"role": "user", "content": "make files"}}),
            json!({"type": "assistant", "uuid": "a1", "parentUuid": "u1", "timestamp": "2026-11-10T10:00:01Z",
                   "cwd": repo, "sessionId": "s1",
                   "message": {"id": "m-t1", "content": [{"type": "tool_use", "id": "t1", "name": "Bash",
                               "input": {"command": "make files"}}]}}),
        ] {
            writeln!(file, "{record}").unwrap();
        }
        let hook = |event: &str, tool: bool| {
            let mut input = json!({
                "session_id": "s1",
                "transcript_path": transcript,
                "cwd": repo,
                "hook_event_name": event,
            });
            if tool {
                input["tool_name"] = json!("Bash");
                input["tool_use_id"] = json!("t1");
                input["tool_input"] = json!({"command": "make files"});
            }
            input.to_string().into_bytes()
        };

        let started = capture::run(client, &node_root, &hook("SessionStart", false))
            .await
            .unwrap();
        assert!(started.recorded.is_some());
        capture::run(client, &node_root, &hook("PreToolUse", true))
            .await
            .unwrap();
        std::fs::write(repo.join("a.txt"), "one\ntwo\n").unwrap();
        let binary = [0u8, 159, 146, 150, 1, 2];
        std::fs::write(repo.join("pic.png"), binary).unwrap();
        let edited = capture::run(client, &node_root, &hook("PostToolUse", true))
            .await
            .unwrap();
        assert_eq!(edited.uncaused_tool_calls, 0);
        let recorded = edited.recorded.unwrap();
        let root = repo.display().to_string();
        let docs = &recorded.worktrees[&root].docs;

        // The worktree's first op is the edit this call made, caused by the call.
        let worktree: work::Worktree = serde_json::from_value(
            read_at(
                client,
                &docs.worktree.as_ref().unwrap().parse::<DocId>().unwrap(),
                &format!("{}/main", work::Worktree::ID).parse().unwrap(),
            )
            .await
            .unwrap()
            .1,
        )
        .unwrap();
        let log: DocId = worktree.op_logs[0].doc.parse().unwrap();
        let op = read_at(
            client,
            &log,
            &format!("{}/0", work::WorktreeOp::ID).parse().unwrap(),
        )
        .await
        .unwrap()
        .1;
        assert_eq!(op["kind"], "edit");
        assert!(
            op["cause"].as_str().unwrap().contains("promptTurn/u1"),
            "{op}"
        );
        // Ops carry git blob ids, and git resolves them to the content the call wrote: no
        // file documents, no uploaded bytes.
        let cat = |blob: &str| {
            std::process::Command::new("git")
                .arg("-C")
                .arg(&repo)
                .args(["cat-file", "blob", blob])
                .output()
                .unwrap()
                .stdout
        };
        assert_eq!(cat(op["files"]["a.txt"].as_str().unwrap()), b"one\ntwo\n");
        assert_eq!(cat(op["files"]["pic.png"].as_str().unwrap()), binary);

        // A hook with nothing new records nothing; the machine and the capture drawer are the
        // node's, remembered for the next hook.
        let stopped = capture::run(client, &node_root, &hook("Stop", false))
            .await
            .unwrap();
        assert!(stopped.recorded.is_none(), "{stopped:?}");
        let stored: serde_json::Value =
            serde_json::from_slice(&std::fs::read(node_root.join("intxt/capture.json")).unwrap())
                .unwrap();
        assert!(
            stored["machine"]
                .as_str()
                .unwrap()
                .starts_with("dtxt:actor:")
        );
        let drawer: DrawerId = stored["drawer"].as_str().unwrap().parse().unwrap();
        let listing = crate::cmds::intxt::call(
            client,
            MintDrawerCapability {
                scope: big_cabinet_core::drawer::protocol::DrawerScope {
                    drawers: [(drawer.clone(), DrawerAccess::Read)].into(),
                    create: false,
                },
                rights: big_cabinet_core::authority::Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
        let listed = crate::cmds::intxt::call(
            client,
            ListDocs {
                capability: listing,
                drawer_id: drawer,
                offset: 0,
                limit: None,
            },
        )
        .await
        .unwrap();
        for doc in [
            docs.worktree.as_ref().unwrap(),
            recorded.session.as_ref().unwrap(),
        ] {
            let doc: DocId = doc.parse().unwrap();
            assert!(
                listed.docs.iter().any(|listed| listed.doc_id == doc),
                "{doc} is not in the capture drawer: {listed:?}"
            );
        }
    })
    .await;
}

/// Claude Code can fire a tool's post hook before the transcript shows the call: the op is
/// recorded without a cause, and the next hook, once the transcript has the call, gives the op
/// the cause in the log.
#[tokio::test(flavor = "multi_thread")]
async fn capture_backfills_the_cause_of_an_op_recorded_before_the_transcript_showed_its_call() {
    use crate::cmds::intxt::capture;
    use big_cabinet_core::DocId;
    use big_cabinet_core::datum::DatumMember;
    use intxt_core::work;
    use std::io::Write;

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let repo = init_repo(&base, &[("a.txt", "one\n")]);
        let transcript = base.join("claude/s1.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        let mut file = std::fs::File::create(&transcript).unwrap();
        writeln!(
            file,
            "{}",
            json!({"type": "user", "uuid": "u1", "parentUuid": null, "timestamp": "2026-11-10T10:00:00Z",
                   "cwd": repo, "sessionId": "s1", "message": {"role": "user", "content": "edit"}})
        )
        .unwrap();
        let hook = |event: &str, tool: bool| {
            let mut input = json!({
                "session_id": "s1",
                "transcript_path": transcript,
                "cwd": repo,
                "hook_event_name": event,
            });
            if tool {
                input["tool_name"] = json!("Bash");
                input["tool_use_id"] = json!("t1");
                input["tool_input"] = json!({"command": "edit"});
            }
            input.to_string().into_bytes()
        };
        capture::run(client, &node_root, &hook("SessionStart", false))
            .await
            .unwrap();
        capture::run(client, &node_root, &hook("PreToolUse", true))
            .await
            .unwrap();
        std::fs::write(repo.join("a.txt"), "one\ntwo\n").unwrap();
        // The transcript has no `t1` yet.
        let post = capture::run(client, &node_root, &hook("PostToolUse", true))
            .await
            .unwrap();
        assert_eq!(post.uncaused_tool_calls, 0);
        let recorded = post.recorded.unwrap();
        let docs = &recorded.worktrees[&repo.display().to_string()].docs;
        let worktree: work::Worktree = serde_json::from_value(
            read_at(
                client,
                &docs.worktree.as_ref().unwrap().parse::<DocId>().unwrap(),
                &format!("{}/main", work::Worktree::ID).parse().unwrap(),
            )
            .await
            .unwrap()
            .1,
        )
        .unwrap();
        let log: DocId = worktree.op_logs[0].doc.parse().unwrap();
        let op_key = format!("{}/0", work::WorktreeOp::ID).parse().unwrap();
        let op = read_at(client, &log, &op_key).await.unwrap().1;
        assert_eq!(op["kind"], "edit");
        assert!(op.get("cause").is_none(), "{op}");

        writeln!(
            file,
            "{}",
            json!({"type": "assistant", "uuid": "a1", "parentUuid": "u1", "timestamp": "2026-11-10T10:00:01Z",
                   "cwd": repo, "sessionId": "s1",
                   "message": {"id": "m-t1", "content": [{"type": "tool_use", "id": "t1", "name": "Bash",
                               "input": {"command": "edit"}}]}})
        )
        .unwrap();
        let stopped = capture::run(client, &node_root, &hook("Stop", false))
            .await
            .unwrap();
        assert_eq!(stopped.uncaused_tool_calls, 0);
        let backfilled = read_at(client, &log, &op_key).await.unwrap().1;
        assert!(
            backfilled["cause"]
                .as_str()
                .unwrap()
                .contains("promptTurn/u1"),
            "{backfilled}"
        );
        assert_eq!(backfilled["files"], op["files"]);
    })
    .await;
}

/// Records, through the capture hooks, a Claude Code session `s1` in `repo` whose two tool calls
/// each write a file and commit it, so the content of both commits was produced by a recorded call.
async fn record_session_with_two_commits(
    client: &NodeClient,
    node_root: &std::path::Path,
    base: &std::path::Path,
    repo: &std::path::Path,
) {
    use crate::cmds::intxt::capture;
    use std::io::Write;

    let transcript = base.join("claude/s1.jsonl");
    std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
    let mut file = std::fs::File::create(&transcript).unwrap();
    for record in [
        json!({"type": "user", "uuid": "u1", "parentUuid": null, "timestamp": "2026-11-10T10:00:00Z",
               "cwd": repo, "sessionId": "s1", "message": {"role": "user", "content": "make two commits"}}),
        json!({"type": "assistant", "uuid": "a1", "parentUuid": "u1", "timestamp": "2026-11-10T10:00:01Z",
               "cwd": repo, "sessionId": "s1",
               "message": {"id": "m-t1", "content": [{"type": "tool_use", "id": "t1", "name": "Bash",
                           "input": {"command": "first commit"}}]}}),
        json!({"type": "assistant", "uuid": "a2", "parentUuid": "a1", "timestamp": "2026-11-10T10:00:02Z",
               "cwd": repo, "sessionId": "s1",
               "message": {"id": "m-t2", "content": [{"type": "tool_use", "id": "t2", "name": "Bash",
                           "input": {"command": "second commit"}}]}}),
    ] {
        writeln!(file, "{record}").unwrap();
    }
    let hook = |event: &str, tool: Option<&str>| {
        let mut input = json!({
            "session_id": "s1",
            "transcript_path": transcript,
            "cwd": repo,
            "hook_event_name": event,
        });
        if let Some(tool) = tool {
            input["tool_name"] = json!("Bash");
            input["tool_use_id"] = json!(tool);
            input["tool_input"] = json!({"command": "commit"});
        }
        input.to_string().into_bytes()
    };
    capture::run(client, node_root, &hook("SessionStart", None))
        .await
        .unwrap();
    // Each tool call writes a file and commits it, so the content of both commits was
    // produced by a recorded call.
    for (tool, path, content) in [("t1", "b.txt", "two\n"), ("t2", "c.txt", "three\n")] {
        capture::run(client, node_root, &hook("PreToolUse", Some(tool)))
            .await
            .unwrap();
        std::fs::write(repo.join(path), content).unwrap();
        git(repo, &["add", "-A"]);
        git(repo, &["commit", "-q", "-m", &format!("add {path}")]);
        let post = capture::run(client, node_root, &hook("PostToolUse", Some(tool)))
            .await
            .unwrap();
        assert_eq!(post.uncaused_tool_calls, 0);
    }
    capture::run(client, node_root, &hook("Stop", None))
        .await
        .unwrap();
}

/// The intxt's commits document holds a `gitCommit/<sha>` for each commit of the range
/// `merge_base..tip`, found by the membership query anchored on the intxt's drawer, and holds
/// nothing else but its marker and the entry's blobs.
async fn assert_commit_membership(
    client: &NodeClient,
    drawer: &big_cabinet_core::drawer::protocol::DrawerId,
    repo: &std::path::Path,
    tip: &str,
    merge_base: &str,
) {
    // The intxt's commits document holds a `gitCommit/<sha>` for each of the range's
    // two commits (and nothing for the base, which is not in the range), found by the
    // membership query anchored on the intxt's drawer.
    let caller = intxt_core::client::Caller::new(
        client,
        service_utils_rs::RequestContext::root(),
        None,
        "test-which".to_owned(),
    );
    let (first, second) = (rev(repo, "feat/x~1"), tip.to_owned());
    let holders = caller.commit_holders(drawer, tip).await.unwrap();
    let [commits_doc] = holders
        .iter()
        .cloned()
        .collect::<Vec<_>>()
        .try_into()
        .unwrap();
    assert_eq!(
        caller.commit_holders(drawer, &first).await.unwrap(),
        holders
    );
    assert!(
        caller
            .commit_holders(drawer, merge_base)
            .await
            .unwrap()
            .is_empty()
    );
    // Besides the commits, the document holds its marker, naming the writer, and the entry's two
    // blob datums (its pack and its patch), each with the cipherBlob and JWK that a blob in a
    // content document is stored as, at the blob's own slot (ADR 026 §3).
    let (blobs, held): (Vec<_>, Vec<_>) = caller
        .datum_keys(&commits_doc)
        .await
        .unwrap()
        .into_iter()
        .partition(|key| key.tag().as_str() == big_blobs_core::Blob::ID);
    assert_eq!(blobs.len(), 2, "{blobs:?}");
    let marker = format!("{}/main", work::Commits::ID);
    let mut held: Vec<_> = held.into_iter().map(|key| key.to_string()).collect();
    assert_eq!(held.iter().filter(|key| **key == marker).count(), 1);
    held.retain(|key| *key != marker);
    held.sort();
    let writer = caller.actor().await.unwrap().to_string();
    assert_eq!(
        caller
            .read(&commits_doc, &marker.parse().unwrap())
            .await
            .unwrap()
            .1,
        json!({"writer": writer})
    );
    assert_eq!(
        caller.tagged_docs(drawer, work::Commits::ID).await.unwrap(),
        [(commits_doc.clone(), writer)].into()
    );
    assert_eq!(
        held,
        {
            let mut expected: Vec<_> = blobs
                .iter()
                .flat_map(|blob| {
                    let blob = blob.to_string();
                    let (_, slot) = blob.split_once('/').expect("a datum key is tag/slot");
                    [
                        format!("org.example.daybook.cipherBlob/{slot}"),
                        format!("org.example.daybook.jwk/{slot}"),
                    ]
                })
                .chain(
                    [first.clone(), second.clone()]
                        .map(|sha| format!("org.example.intxt.work.gitCommit/{sha}")),
                )
                .collect();
            expected.sort();
            expected
        },
        "the commits document holds the range's commits and the entry's blobs, nothing else"
    );
    let (_, datum) = caller
        .read(
            &commits_doc,
            &format!("org.example.intxt.work.gitCommit/{second}")
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(datum["message"], "add c.txt");
    assert!(datum.get("raw").is_none(), "{datum}");
}

/// `new --with` lends the related intxt's commits documents: the new drawer holds the same
/// documents, so the membership query anchored on either drawer finds the commits once; a name
/// that is no intxt creates nothing.
async fn assert_new_with_shares_commits_document(
    client: &NodeClient,
    packaging: &crate::cmds::intxt::package::Packaging<'_>,
    drawer: &big_cabinet_core::drawer::protocol::DrawerId,
    short: &str,
    tip: &str,
) {
    let mut out = Vec::new();
    let follow_up = packaging
        .create(&mut out, "Follow-up", None, Some(short))
        .await
        .unwrap();
    let out = String::from_utf8(out).unwrap();
    assert!(out.contains("lends the 1 commits document(s) of"), "{out}");
    let lookup = intxt_core::client::Caller::new(
        client,
        service_utils_rs::RequestContext::root(),
        None,
        "test-with".to_owned(),
    );
    assert_eq!(
        lookup
            .tagged_docs(&follow_up, work::Commits::ID)
            .await
            .unwrap(),
        lookup.tagged_docs(drawer, work::Commits::ID).await.unwrap()
    );
    assert_eq!(
        lookup.commit_holders(&follow_up, tip).await.unwrap(),
        lookup.commit_holders(drawer, tip).await.unwrap(),
    );
    // A name that is no intxt creates nothing.
    let list = async || {
        let mut out = Vec::new();
        packaging.list(&mut out).await.unwrap();
        String::from_utf8(out).unwrap()
    };
    let before = list().await;
    assert!(
        packaging
            .create(&mut Vec::new(), "Orphan", None, Some("nope"))
            .await
            .is_err()
    );
    assert_eq!(list().await, before);
}

/// An intxt whose data cannot be read, such as a drawer with no index document, is
/// listed as unreadable instead of failing `list` for every other intxt, is only counted by
/// `status`, and can still be dropped, which is how it is cleaned up.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_intxt_is_listed_and_dropped_without_failing_the_others() {
    use crate::cmds::intxt::package::Packaging;
    use intxt_core::generic;

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let repo = init_repo(&base, &[("a.txt", "one\n")]);
        let packaging = Packaging::new(client, &repo, &node_root);
        packaging
            .create(&mut Vec::new(), "Readable", None, None)
            .await
            .unwrap();
        let stale = packaging
            .create(&mut Vec::new(), "Stale", None, None)
            .await
            .unwrap();
        // A marked drawer with no index pointer, as a `createIntxt` that did not finish leaves.
        operator(client)
            .write_datums(
                &stale,
                Default::default(),
                [intxt_core::client::key(generic::Index::ID, "main").unwrap()].into(),
            )
            .await
            .unwrap();
        let run = async |command: &str| {
            let mut out = Vec::new();
            match command {
                "list" => packaging.list(&mut out).await.unwrap(),
                "status" => packaging.status(&mut out).await.unwrap(),
                _ => packaging.drop(&mut out, &stale.to_string()).await.unwrap(),
            }
            String::from_utf8(out).unwrap()
        };

        let listed = run("list").await;
        assert!(listed.contains("Readable"), "{listed}");
        assert!(
            listed.contains("(unreadable: it has no index document)"),
            "{listed}"
        );
        assert!(listed.contains(&format!("day x intxt drop {stale}")), "{listed}");
        let status = run("status").await;
        assert!(
            status.contains("1 intxt(s) on this node cannot be read"),
            "{status}"
        );
        let dropped = run("drop").await;
        assert!(dropped.contains("(unreadable)"), "{dropped}");
        let listed = run("list").await;
        assert!(listed.contains("Readable"), "{listed}");
        assert!(!listed.contains("unreadable"), "{listed}");
    })
    .await;
}

/// Packaging a branch of two commits whose content a recorded session's tool calls produced:
/// `add --branch` writes one commit entry carrying the pack, tracks the ref on this clone
/// and brings the session; `show` lists all three; adding again changes nothing; `status`
/// finds the intxt from the worktree.
#[tokio::test(flavor = "multi_thread")]
async fn intxt_add_branch_packages_commits_ref_and_session_and_readding_is_a_no_op() {
    use crate::cmds::intxt::package::Packaging;
    use intxt_core::edit::Named;

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let repo = init_repo(&base, &[("a.txt", "one\n")]);

        record_session_with_two_commits(client, &node_root, &base, &repo).await;

        let packaging = Packaging::new(client, &repo, &node_root);
        let mut created = Vec::new();
        let drawer = packaging
            .create(&mut created, "Two files", Some("b and c"), None)
            .await
            .unwrap();
        let created = String::from_utf8(created).unwrap();
        let short = short_ids::ShortIds::new([drawer.to_string()])
            .render(&drawer.to_string())
            .unique;
        assert!(
            created.starts_with(&format!("intxt {short} \"Two files\"")),
            "{created}"
        );
        assert!(
            created.contains(&format!("undo: day x intxt drop {short}")),
            "{created}"
        );
        assert!(created.contains("this worktree is on it now"), "{created}");
        let add = async |name: &str| {
            let mut out = Vec::new();
            packaging
                .add(
                    &mut out,
                    Some(name),
                    Named::Branch("feat/x".to_owned()),
                    false,
                )
                .await
                .unwrap();
            String::from_utf8(out).unwrap()
        };
        let show = async |name: &str| {
            let mut out = Vec::new();
            packaging.show(&mut out, Some(name)).await.unwrap();
            String::from_utf8(out).unwrap()
        };

        let list = async || {
            let mut out = Vec::new();
            packaging.list(&mut out).await.unwrap();
            String::from_utf8(out).unwrap()
        };

        let listed_before_add = list().await;
        // The intxt's row: its title, then no commits, refs or sessions yet.
        assert!(
            listed_before_add.lines().any(|line| line.contains("Two files")
                && line.split_whitespace().rev().take(3).all(|count| count == "0")),
            "{listed_before_add}"
        );
        let (tip, merge_base) = (rev(&repo, "feat/x"), rev(&repo, "main"));
        let range = format!("{}..{}", &merge_base[..7], &tip[..7]);

        let added = add(&short).await;
        assert!(added.starts_with("added to "), "{added}");
        assert!(
            added.contains(&format!(
                "commits    feat/x: {range}, 2 commit(s) (thin pack"
            )),
            "{added}"
        );
        assert!(
            added.contains("ref        refs/heads/feat/x (tracked on this clone)"),
            "{added}"
        );
        assert!(added.contains("session    claude-code"), "{added}");
        assert!(added.contains("undo: day x intxt rm"), "{added}");

        let shown = show(&short).await;
        assert!(shown.contains("\"Two files\""), "{shown}");
        assert!(shown.contains("b and c"), "{shown}");
        // The link names the drawer's descriptor document and hints at this node.
        let link = packaging.reference(&drawer).unwrap();
        assert!(
            link.to_string()
                .starts_with(&format!("dtxt:doc:{drawer}?via=")),
            "{link}"
        );
        assert_eq!(link.via().len(), 1, "{link}");
        assert!(shown.contains(&format!("link: {link}\n")), "{shown}");
        // `new` put the worktree on the intxt, so naming none is naming it, until `use`
        // takes the worktree off it.
        let mut on = Vec::new();
        packaging.show(&mut on, None).await.unwrap();
        assert_eq!(String::from_utf8(on).unwrap(), shown);
        packaging
            .use_intxt(&mut Vec::new(), None, true)
            .await
            .unwrap();
        let refused = packaging.show(&mut Vec::new(), None).await.unwrap_err();
        assert!(refused.to_string().contains("day x intxt use"), "{refused}");
        let mut used = Vec::new();
        packaging
            .use_intxt(&mut used, Some(&short), false)
            .await
            .unwrap();
        let used = String::from_utf8(used).unwrap();
        assert!(
            used.starts_with(&format!("this worktree is on {short}")),
            "{used}"
        );
        assert!(shown.contains("commits (1):"), "{shown}");
        assert!(shown.contains(&range), "{shown}");
        assert!(shown.contains("tracked refs (1):"), "{shown}");
        assert!(shown.contains("refs/heads/feat/x"), "{shown}");
        assert!(shown.contains("sessions (1):"), "{shown}");
        assert!(shown.contains("s1  claude-code"), "{shown}");

        // The entry's pack is a blob datum in the writer's commits document, not the index.
        let (_, entry) = read_at(
            client,
            &index_of(client, &drawer).await,
            &format!("org.example.intxt.work.gitCommitEntry/{tip}")
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            entry["tip"],
            format!("org.example.intxt.work.gitCommit/{tip}")
        );
        assert_eq!(
            entry["base"],
            format!("org.example.intxt.work.gitCommit/{merge_base}")
        );
        let pack =
            big_cabinet_core::DatumReference::parse(entry["pack"].as_str().unwrap()).unwrap();
        let big_cabinet_core::ReferenceDocId::Concrete(pack_doc) = pack.document() else {
            panic!("{pack} names a document, not the entry's own");
        };
        let caller = intxt_core::client::Caller::new(
            client,
            service_utils_rs::RequestContext::root(),
            None,
            "test-blobs".to_owned(),
        );
        assert_eq!(
            caller.commit_holders(&drawer, &tip).await.unwrap(),
            [pack_doc.clone()].into(),
            "the pack is in the commits document"
        );
        let in_index: Vec<_> = caller
            .datum_keys(&index_of(client, &drawer).await)
            .await
            .unwrap()
            .into_iter()
            .filter(|key| key.tag().as_str() == big_blobs_core::Blob::ID)
            .collect();
        assert!(
            in_index.is_empty(),
            "the index document holds no blob datums: {in_index:?}"
        );
        // After `new` and `add`, the drawer's own document is still only the pointers.
        assert_drawer_document_holds_only_the_pointers(client, &drawer).await;
        let (_, blob) = read_at(client, pack_doc, pack.key()).await.unwrap();
        assert_eq!(blob["mime"], "application/x-git-pack");
        assert!(blob["lengthOctets"].as_u64().unwrap() > 0, "{blob}");

        assert_commit_membership(client, &drawer, &repo, &tip, &merge_base).await;

        let which = async |name: &str| {
            let mut out = Vec::new();
            packaging.which(&mut out, name).await.unwrap();
            String::from_utf8(out).unwrap()
        };
        let held_line = format!("{short}  \"Two files\"  link: {link}\n");
        assert_eq!(which(&tip).await, held_line);
        assert_eq!(which("feat/x~1").await, held_line);
        // A unique prefix names the commit as well.
        assert_eq!(which(&tip[..10]).await, held_line);
        assert_eq!(
            which("main").await,
            "",
            "a commit on main only is in no intxt"
        );

        let again = add(&short).await;
        assert!(again.starts_with("nothing to add to "), "{again}");
        assert!(again.contains("(already in)"), "{again}");
        assert!(again.contains("(already tracked)"), "{again}");
        assert_eq!(show(&short).await, shown, "adding again changed the intxt");

        let mut out = Vec::new();
        packaging.drop(&mut out, &short).await.unwrap();
        let dropped = String::from_utf8(out).unwrap();
        assert!(dropped.starts_with("dropped "), "{dropped}");
        assert!(
            dropped.contains(&format!("undo: day x intxt show {drawer}")),
            "{dropped}"
        );
        let listed_after_drop = list().await;
        assert!(
            !listed_after_drop.contains("Two files"),
            "{listed_after_drop}"
        );

        let restored = show(&drawer.to_string()).await;
        assert!(restored.contains("\"Two files\""), "{restored}");
        let listed_after_restore = list().await;
        assert!(
            listed_after_restore.contains("Two files"),
            "{listed_after_restore}"
        );

        let mut out = Vec::new();
        packaging.status(&mut out).await.unwrap();
        let status = String::from_utf8(out).unwrap();
        assert!(status.contains("(feat/x)"), "{status}");
        assert!(status.contains("s1  (current)"), "{status}");
        assert!(status.contains("in intxts:"), "{status}");
        assert!(
            status.contains(&format!("\"Two files\": commits to {}", &tip[..7])),
            "{status}"
        );
        assert!(
            status.contains("tracks feat/x") || status.contains("tracks refs/heads/feat/x"),
            "{status}"
        );
        assert!(status.contains("session s1"), "{status}");

        // `rm` takes the ref entry out by its short id and leaves the rest.
        let ref_line = shown
            .lines()
            .find(|line| line.contains("refs/heads/feat/x"))
            .unwrap();
        let ref_id = ref_line.split_whitespace().next().unwrap();
        let mut out = Vec::new();
        packaging
            .rm(&mut out, &short, &[ref_id.to_owned()])
            .await
            .unwrap();
        let removed = String::from_utf8(out).unwrap();
        assert!(removed.contains("gitRefEntry/"), "{removed}");
        let after = show(&short).await;
        assert!(!after.contains("tracked refs"), "{after}");
        assert!(
            after.contains("commits (1):") && after.contains("sessions (1):"),
            "{after}"
        );

        assert_new_with_shares_commits_document(client, &packaging, &drawer, &short, &tip).await;
    })
    .await;
}

/// FDR 008 W6: a branch made with no capture at all. `add --branch` imports its commits, its
/// ref and the identity documents first (no sessions: none were recorded, and `show` says
/// so); a hook afterwards records only the live move, with no second baseline and no
/// commit twice.
#[tokio::test(flavor = "multi_thread")]
async fn intxt_add_branch_imports_history_that_was_never_recorded() {
    use crate::cmds::intxt::capture;
    use crate::cmds::intxt::package::Packaging;
    use big_cabinet_core::DocId;
    use big_cabinet_core::datum::DatumMember;
    use intxt_capture::host;
    use intxt_capture::identity;
    use intxt_core::client::Caller;
    use intxt_core::edit::Named;
    use intxt_core::work;
    use service_utils_rs::RequestContext;
    use std::io::Write;

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let repo = init_repo(&base, &[("a.txt", "one\n")]);
        for path in ["b.txt", "c.txt"] {
            std::fs::write(repo.join(path), format!("{path}\n")).unwrap();
            git(&repo, &["add", "-A"]);
            git(&repo, &["commit", "-q", "-m", &format!("add {path}")]);
        }
        // Nothing has run in this clone: no state, no documents.
        let worktree = identity::resolve(&repo).unwrap().unwrap();
        assert_eq!(host::recorded_worktree(&worktree).unwrap().docs.repo, None);

        let packaging = Packaging::new(client, &repo, &node_root);
        let mut created = Vec::new();
        let drawer = packaging
            .create(&mut created, "Unrecorded", None, None)
            .await
            .unwrap();
        let short = short_ids::ShortIds::new([drawer.to_string()])
            .render(&drawer.to_string())
            .unique;
        let mut out = Vec::new();
        packaging
            .add(&mut out, Some(&short), Named::Branch("feat/x".to_owned()), false)
            .await
            .unwrap();
        let added = String::from_utf8(out).unwrap();
        assert!(added.starts_with("added to "), "{added}");
        assert!(added.contains("2 commit(s)"), "{added}");
        assert!(added.contains("ref        refs/heads/feat/x (tracked on this clone)"), "{added}");
        assert!(added.contains("sessions   none recorded"), "{added}");
        let mut out = Vec::new();
        packaging.show(&mut out, Some(&short)).await.unwrap();
        let shown = String::from_utf8(out).unwrap();
        assert!(shown.contains("commits (1):"), "{shown}");
        assert!(shown.contains("tracked refs (1):"), "{shown}");
        assert!(!shown.contains("sessions ("), "{shown}");
        assert!(shown.contains("sessions: none recorded"), "{shown}");

        // The import recorded the identity documents and exactly the range's commits. The
        // worktree was clean, so it recorded no op.
        let recorded = host::recorded_worktree(&worktree).unwrap();
        assert_eq!(recorded.ops, 0);
        let caller = Caller::new(client, RequestContext::root(), None, "test".to_owned());
        let repo_doc: DocId = recorded.docs.repo.as_deref().unwrap().parse().unwrap();
        let commits = async || {
            let (_, datums) = caller.read_datums(&repo_doc, None).await.unwrap();
            datums
                .into_keys()
                .filter(|key| key.tag().as_str() == work::GitCommit::ID)
                .count()
        };
        assert_eq!(commits().await, 2);

        // A live move is recorded by capture as usual: one commit op, one more commit, and no
        // second baseline of the ref.
        let transcript = base.join("claude/s1.jsonl");
        std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        writeln!(
            std::fs::File::create(&transcript).unwrap(),
            "{}",
            json!({"type": "user", "uuid": "u1", "parentUuid": null, "timestamp": "2026-11-10T10:00:00Z",
                   "cwd": repo, "sessionId": "s1", "message": {"role": "user", "content": "go on"}})
        )
        .unwrap();
        let hook = |event: &str| {
            json!({
                "session_id": "s1",
                "transcript_path": transcript,
                "cwd": repo,
                "hook_event_name": event,
            })
            .to_string()
            .into_bytes()
        };
        capture::run(client, &node_root, &hook("SessionStart"))
            .await
            .unwrap();
        assert_eq!(host::recorded_worktree(&worktree).unwrap().ops, 0);
        assert_eq!(commits().await, 2);

        std::fs::write(repo.join("d.txt"), "d\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "add d.txt"]);
        capture::run(client, &node_root, &hook("Stop")).await.unwrap();
        assert_eq!(host::recorded_worktree(&worktree).unwrap().ops, 1);
        assert_eq!(commits().await, 3);

        capture::run(client, &node_root, &hook("Stop")).await.unwrap();
        assert_eq!(host::recorded_worktree(&worktree).unwrap().ops, 1);
        assert_eq!(commits().await, 3);

        // Adding the branch again, now that it moved, imports nothing: capture has the move.
        let mut out = Vec::new();
        packaging
            .add(&mut out, Some(&short), Named::Branch("feat/x".to_owned()), false)
            .await
            .unwrap();
        assert_eq!(host::recorded_worktree(&worktree).unwrap().ops, 1);
        assert_eq!(commits().await, 3);

    })
    .await;
}

/// A rewrite git's `post-rewrite` hook reported (recorded by `capture git-rewrite` in the capture
/// drawer) is copied by `add` into the intxt's commits document as an exact link, even where the
/// amended commit's patch id changed and matching could not have found it, and an amend repeated
/// before the next `add` (held -> amended -> amended again) still links the held commit to the
/// last.
#[tokio::test(flavor = "multi_thread")]
async fn intxt_add_copies_the_rewrites_the_hook_reported() {
    use crate::cmds::intxt::capture;
    use crate::cmds::intxt::package::Packaging;
    use big_cabinet_core::datum::DatumMember;
    use intxt_core::client::Caller;
    use intxt_core::edit::Named;
    use intxt_core::work;
    use service_utils_rs::RequestContext;

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let repo = init_repo(&base, &[("a.txt", "one\n")]);
        std::fs::write(repo.join("b.txt"), "b\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "add b.txt"]);
        let held = rev(&repo, "feat/x");

        let packaging = Packaging::new(client, &repo, &node_root);
        let drawer = packaging
            .create(&mut Vec::new(), "Rewritten", None, None)
            .await
            .unwrap();
        let short = short_ids::ShortIds::new([drawer.to_string()])
            .render(&drawer.to_string())
            .unique;
        let add = async || {
            let mut out = Vec::new();
            packaging
                .add(
                    &mut out,
                    Some(&short),
                    Named::Branch("feat/x".to_owned()),
                    false,
                )
                .await
                .unwrap();
            String::from_utf8(out).unwrap()
        };
        assert!(add().await.contains("1 commit(s)"));

        // Amends that change the patch (so no patch id would match), each reported by the hook
        // as git reports it: `<old> <new>`.
        let amend = async |content: &str| {
            let old = rev(&repo, "feat/x");
            std::fs::write(repo.join("b.txt"), content).unwrap();
            git(&repo, &["commit", "-q", "-a", "--amend", "-m", "add b.txt"]);
            let new = rev(&repo, "feat/x");
            let recorded =
                capture::git_rewrite(client, &node_root, &repo, &format!("{old} {new}\n"))
                    .await
                    .unwrap();
            assert_eq!(recorded, 1);
            new
        };
        let first = amend("bee\n").await;
        let added = add().await;
        assert!(
            added.contains(&format!(
                "rewrite    {} replaces {} (post-rewrite hook)",
                &first[..7],
                &held[..7]
            )),
            "{added}"
        );

        let caller = Caller::new(client, RequestContext::root(), None, "test-hook".to_owned());
        // The intxt's commits document is the one the writer's marker names.
        let [commits_doc] = caller
            .tagged_docs(&drawer, work::Commits::ID)
            .await
            .unwrap()
            .into_keys()
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        let slot = |tag: &str, sha: &str| format!("{tag}/{sha}").parse().unwrap();
        let (_, rewrite) = caller
            .read(&commits_doc, &slot(work::Rewrite::ID, &held))
            .await
            .unwrap();
        assert_eq!(rewrite, json!({"successor": first, "how": "hook"}));
        let (_, from) = caller
            .read(&commits_doc, &slot(work::RewrittenFrom::ID, &first))
            .await
            .unwrap();
        assert_eq!(from, json!({"predecessor": held, "how": "hook"}));

        // Two amends before the next `add`: the intxt holds the first commit of the chain, and
        // its link goes to the last.
        amend("bee bee\n").await;
        let last = amend("bee bee bee\n").await;
        let added = add().await;
        assert!(
            added.contains(&format!(
                "rewrite    {} replaces {} (post-rewrite hook)",
                &last[..7],
                &first[..7]
            )),
            "{added}"
        );
        let (_, from) = caller
            .read(&commits_doc, &slot(work::RewrittenFrom::ID, &last))
            .await
            .unwrap();
        assert_eq!(from, json!({"predecessor": first, "how": "hook"}));

        // A hook pair whose chain reaches no held commit does not stop patch-id matching: this
        // reworded amend has the held commit's patch, and the hook (made to name a commit the
        // intxt does not hold) says something else.
        let before = rev(&repo, "feat/x");
        git(&repo, &["commit", "-q", "--amend", "-m", "reworded"]);
        let reworded = rev(&repo, "feat/x");
        let unrelated = rev(&repo, "main");
        capture::git_rewrite(
            client,
            &node_root,
            &repo,
            &format!("{unrelated} {reworded}\n"),
        )
        .await
        .unwrap();
        let added = add().await;
        assert!(
            added.contains(&format!(
                "rewrite    {} replaces {} (same patch id)",
                &reworded[..7],
                &before[..7]
            )),
            "{added}"
        );
    })
    .await;
}

/// Review through `comment`, `reply` and `inbox`, between two participants of one actor (the
/// operator, and an agent detected from `AI_AGENT` and its session variable, both the node's
/// operator as far as the cabinet attests, ledger D69): a comment on a commit's lines, the other side's inbox
/// and reply, a rebase that inserts lines above the target (the comment follows to its new
/// lines), an edit of the targeted text (outdated), and resolving, after which the thread
/// leaves the inbox.
#[tokio::test(flavor = "multi_thread")]
async fn intxt_review_comments_follow_a_rebase_and_resolve() {
    use crate::cmds::intxt::package::Packaging;
    use crate::cmds::intxt::participant::Detected;
    use crate::cmds::intxt::review::Side;
    use intxt_core::annotation::Motivation;
    use intxt_core::edit::Named;

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let numbered =
            |prefix: &str| -> String { (1..=10).map(|n| format!("{prefix}{n}\n")).collect() };
        let repo = init_repo(&base, &[("b.rs", &numbered("b"))]);
        // The first commit rewrites line 8; the second adds a file.
        std::fs::write(
            repo.join("b.rs"),
            numbered("b").replace("b8\n", "b8 edited\n"),
        )
        .unwrap();
        git(&repo, &["commit", "-q", "-am", "edit b8"]);
        std::fs::write(repo.join("c.rs"), "c\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "add c"]);
        let (first, second) = (rev(&repo, "feat/x~1"), rev(&repo, "feat/x"));

        let operator = Packaging::new(client, &repo, &node_root);
        // The environment of a Claude Code session, handed in as the CLI edge would read it.
        let agent = Packaging::new(client, &repo, &node_root).participating_as(
            Detected::from_env(&node_root, |name| match name {
                "AI_AGENT" => Some("claude-code_2-1-322_agent".to_owned()),
                "CLAUDE_CODE_SESSION_ID" => Some("session-1".to_owned()),
                _ => None,
            })
            .unwrap(),
        );

        let mut created = Vec::new();
        let drawer = operator
            .create(&mut created, "Review me", None, None)
            .await
            .unwrap();
        let intxt = short_ids::ShortIds::new([drawer.to_string()])
            .render(&drawer.to_string())
            .unique;
        let add = async || {
            operator
                .add(
                    &mut Vec::new(),
                    Some(&intxt),
                    Named::Branch("feat/x".to_owned()),
                    false,
                )
                .await
                .unwrap();
        };
        add().await;

        let comment = async |who: &Packaging<'_>, target: &str, text: &str, side: Side| {
            let mut out = Vec::new();
            let result = who
                .comment(&mut out, &intxt, target, text, side, Motivation::Commenting)
                .await;
            (result, String::from_utf8(out).unwrap())
        };
        let inbox = async |who: &Packaging<'_>| {
            let mut out = Vec::new();
            who.inbox(&mut out, Some(&intxt)).await.unwrap();
            String::from_utf8(out).unwrap()
        };
        let reply =
            async |who: &Packaging<'_>, to: &str, text: Option<&str>, resolved: Option<bool>| {
                let mut out = Vec::new();
                who.reply(&mut out, Some(&intxt), to, text, resolved)
                    .await
                    .unwrap();
                String::from_utf8(out).unwrap()
            };

        // 1. A comment on a range of the first commit.
        let target = format!("{}:b.rs:7-9", &first[..7]);
        let (commented, out) = comment(&operator, &target, "Why edit b8?", Side::New).await;
        commented.unwrap();
        let root = out
            .strip_prefix("comment ")
            .and_then(|rest| rest.split(' ').next())
            .unwrap_or_else(|| panic!("{out}"))
            .to_owned();
        assert!(
            out.contains(&format!("on {} b.rs:7-9", &first[..7])),
            "{out}"
        );
        assert!(
            out.contains(&format!("undo: day x intxt rm {intxt} {root}")),
            "{out}"
        );

        // A commit the intxt does not hold, and the old side of a commit whose parent it
        // does not hold, are refused, saying what it holds.
        let (refused, _) = comment(
            &operator,
            &format!("{}:b.rs:7", &rev(&repo, "main")[..7]),
            "no",
            Side::New,
        )
        .await;
        let refused = format!("{:#}", refused.unwrap_err());
        assert!(
            refused.contains("is not a commit the intxt") && refused.contains("holds"),
            "{refused}"
        );
        let (refused, _) = comment(&operator, &target, "no", Side::Old).await;
        assert!(format!("{:#}", refused.unwrap_err()).contains("parent"));
        // The old side of the second commit is the first, which is held.
        let (old_side, out) = comment(
            &operator,
            &format!("{}:b.rs:8", &second[..7]),
            "was b8 edited?",
            Side::Old,
        )
        .await;
        old_side.unwrap();
        assert!(out.contains(&format!("on {} b.rs:8", &first[..7])), "{out}");
        // `rm` is the undo of a comment.
        let removed_short = out
            .split("rm ")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().nth(1))
            .unwrap()
            .to_owned();
        let mut rm_out = Vec::new();
        operator
            .rm(&mut rm_out, &intxt, &[removed_short])
            .await
            .unwrap();

        // 3. The other side sees the thread, placed on the branch's current tip.
        let seen_by_agent = inbox(&agent).await;
        assert!(seen_by_agent.contains("1 thread(s)"), "{seen_by_agent}");
        assert!(
            seen_by_agent.contains(&format!("b.rs:7-9 @ {}", &second[..7])),
            "{seen_by_agent}"
        );
        assert!(seen_by_agent.contains("waiting on you"), "{seen_by_agent}");
        assert!(
            seen_by_agent.contains("someone (claimed): Why edit b8?"),
            "{seen_by_agent}"
        );
        // The author's own inbox has nothing: it wrote last.
        let own = inbox(&operator).await;
        assert!(own.contains("nothing waiting"), "{own}");

        // 2. The reply. The thread is then waiting on the author, and the replier, who has
        // seen everything, has nothing left.
        // Named as the inbox names it now: ids grow as comments are added, and a prefix printed
        // earlier can be ambiguous by now.
        let thread_id = |inbox: &str| -> String {
            inbox
                .lines()
                .find_map(|line| line.strip_prefix("thread "))
                .and_then(|rest| rest.split_whitespace().next())
                .unwrap_or_else(|| panic!("no thread in {inbox}"))
                .to_owned()
        };
        let root = thread_id(&seen_by_agent);
        let replied = reply(&agent, &root, Some("Fixes the off-by-one."), None).await;
        assert!(replied.starts_with("reply "), "{replied}");
        assert!(inbox(&agent).await.contains("nothing waiting"));
        let waiting = inbox(&operator).await;
        assert!(
            waiting.contains("claude-code (claimed): Fixes the off-by-one."),
            "{waiting}"
        );
        assert!(waiting.contains("you: Why edit b8?"), "{waiting}");
        // Seen, but still waiting on the author until they answer.
        assert!(inbox(&operator).await.contains("waiting on you"));

        // 4. A rebase onto a main that inserted three lines at the top of `b.rs`: the
        // comment's lines are three lower on the rebased commit.
        git(&repo, &["switch", "-q", "main"]);
        std::fs::write(repo.join("b.rs"), format!("n1\nn2\nn3\n{}", numbered("b"))).unwrap();
        git(&repo, &["commit", "-q", "-am", "insert above"]);
        git(&repo, &["switch", "-q", "feat/x"]);
        git(&repo, &["rebase", "-q", "main"]);
        let rebased = rev(&repo, "feat/x");
        assert_ne!(rebased, second);
        add().await;
        let shifted = inbox(&operator).await;
        assert!(
            shifted.contains(&format!("b.rs:10-12 @ {}", &rebased[..7])),
            "{shifted}"
        );
        // The old commits are still held: the comment is still on the first.
        assert!(!shifted.contains("outdated"), "{shifted}");
        // The rebased commit is the old one rewritten (same patch id): its lookup finds the
        // intxt holding it and the one holding the commit it replaced, here the same document.
        let lookup = intxt_core::client::Caller::new(
            client,
            service_utils_rs::RequestContext::root(),
            None,
            "test-rewrites".to_owned(),
        );
        let holders = lookup.commit_holders(&drawer, &second).await.unwrap();
        assert_eq!(holders.len(), 1);
        assert_eq!(
            lookup.commit_holders(&drawer, &rebased).await.unwrap(),
            holders
        );

        // 5. An edit of the commented text.
        std::fs::write(
            repo.join("b.rs"),
            std::fs::read_to_string(repo.join("b.rs"))
                .unwrap()
                .replace("b8 edited", "b8 rewritten"),
        )
        .unwrap();
        git(&repo, &["commit", "-q", "-am", "rewrite b8"]);
        let edited = rev(&repo, "feat/x");
        add().await;
        let outdated = inbox(&operator).await;
        assert!(
            outdated.contains(&format!("b.rs (outdated at {}", &edited[..7])),
            "{outdated}"
        );
        assert!(
            outdated.contains(&format!("was {}:7-9", &first[..7])),
            "{outdated}"
        );

        // 6. Resolved, the thread is gone, from both sides' inboxes.
        let root = thread_id(&outdated);
        let resolved = reply(&operator, &root, None, Some(true)).await;
        assert!(resolved.contains("resolved"), "{resolved}");
        // The thread is named by the shortest prefix that is unique now, which may be longer than
        // the one `comment` printed before later ids existed, so the undo line is checked against
        // the name this very answer gives the thread.
        let thread = resolved
            .strip_prefix("thread ")
            .and_then(|rest| rest.split(' ').next())
            .unwrap_or_else(|| panic!("{resolved}"));
        assert!(thread.starts_with(&root), "{thread} is not {root}");
        assert!(
            resolved.contains(&format!(
                "undo: day x intxt reply {thread} --intxt {intxt} --reopen"
            )),
            "{resolved}"
        );
        assert!(inbox(&operator).await.contains("nothing waiting"));
        assert!(inbox(&agent).await.contains("nothing waiting"));
        // Reopened, it is waiting on the author again: the agent wrote last.
        reply(&operator, thread, None, Some(false)).await;
        let reopened = inbox(&operator).await;
        assert!(reopened.contains("1 thread(s)"), "{reopened}");
    })
    .await;
}

/// `org.example.intxt.addToIntxt` includes captured parts: each gets an entry keyed by the
/// part's identity, the body lists the entries in the order they were added, adding a part
/// again changes nothing, and the parts' documents are entered in the intxt's drawer so its
/// members can read them.
#[tokio::test(flavor = "multi_thread")]
async fn add_to_intxt_includes_parts_once_and_shares_their_documents() {
    use crate::cmds::intxt;
    use big_cabinet_core::DocId;
    use big_cabinet_core::doc::CreateDoc;
    use big_cabinet_core::doc::DocScope;
    use big_cabinet_core::drawer::DrawerAccess;
    use big_cabinet_core::drawer::protocol::DrawerId;
    use big_cabinet_core::drawer::protocol::ListDocs;
    use big_cabinet_core::drawer::protocol::MintDrawerCapability;
    use big_cabinet_core::reference::DatumReference;

    with_intxt_node(async |_tmp, client| {
        let create = |title: &'static str| {
            let client = &client;
            async move {
                let created = day_x(
                    client,
                    "org.example.intxt.createIntxt",
                    json!({ "title": title }),
                )
                .await;
                created["drawerId"]
                    .as_str()
                    .unwrap()
                    .parse::<DrawerId>()
                    .unwrap()
            }
        };
        let captures = create("captures").await;
        let review = create("review").await;
        let review_index = index_of(client, &review).await;
        let capture = |tag: &str, value: serde_json::Value| {
            let client = &client;
            let captures = &captures;
            let tag = tag.to_owned();
            async move {
                let placement = format!("drawer {captures}");
                let capability = doc_capability(
                    client,
                    DocScope {
                        create: [placement.clone()].into(),
                        ..DocScope::default()
                    },
                )
                .await
                .unwrap();
                let doc: DocId = intxt::call(
                    client,
                    CreateDoc {
                        capability,
                        command: command("capture"),
                        placement,
                        datums: [(format!("{tag}/main").parse().unwrap(), value)].into(),
                    },
                )
                .await
                .unwrap()
                .doc_id;
                format!("dtxt:datum:{doc}/{tag}/main")
                    .parse::<DatumReference>()
                    .unwrap()
            }
        };
        let worktree = capture(
            "org.example.intxt.work.worktree",
            json!({
                "clone": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.gitClone/0193c",
                "id": "0193a",
                "path": "/work/townframe",
                "files": {},
                "opLogs": []
            }),
        )
        .await;
        let session = capture(
            "org.example.intxt.work.agentSession",
            json!({"harness": "claude-code", "sessionId": "eb26ecaa", "cwd": "/work/townframe"}),
        )
        .await;

        let add = |part: &DatumReference| {
            let client = &client;
            let review = &review;
            let part = part.to_string();
            async move {
                let added = day_x(
                    client,
                    "org.example.intxt.addToIntxt",
                    json!({"intxt": review, "part": part}),
                )
                .await;
                added["entry"].as_str().unwrap().to_owned()
            }
        };
        let added = [
            add(&worktree).await,
            add(&session).await,
            add(&worktree).await,
        ];
        assert_eq!(
            added,
            [
                format!("dtxt:datum:{review_index}/org.example.intxt.work.worktreeEntry/0193a"),
                format!("dtxt:datum:{review_index}/org.example.intxt.work.sessionEntry/eb26ecaa"),
                format!("dtxt:datum:{review_index}/org.example.intxt.work.worktreeEntry/0193a"),
            ]
        );
        let (_, entry) = read_at(
            client,
            &review_index,
            &"org.example.intxt.work.worktreeEntry/0193a"
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(entry, json!({"worktree": worktree.to_string()}));
        let (_, body) = read_at(
            client,
            &review_index,
            &"org.example.intxt.body/main".parse().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            body,
            json!({"order": [
                "dtxt:datum:self/org.example.intxt.work.worktreeEntry/0193a",
                "dtxt:datum:self/org.example.intxt.work.sessionEntry/eb26ecaa",
            ]})
        );

        let listing = intxt::call(
            client,
            MintDrawerCapability {
                scope: big_cabinet_core::drawer::protocol::DrawerScope {
                    drawers: [(review.clone(), DrawerAccess::Read)].into(),
                    create: false,
                },
                rights: big_cabinet_core::authority::Rights::USE,
                subject: None,
            },
        )
        .await
        .unwrap()
        .capability_id;
        let listed = intxt::call(
            client,
            ListDocs {
                capability: listing,
                drawer_id: review.clone(),
                offset: 0,
                limit: None,
            },
        )
        .await
        .unwrap();
        for part in [&worktree, &session] {
            let big_cabinet_core::reference::ReferenceDocId::Concrete(doc) = part.document() else {
                unreachable!()
            };
            assert!(
                listed.docs.iter().any(|listed| &listed.doc_id == doc),
                "{doc} is not in the intxt's drawer: {listed:?}"
            );
        }
    })
    .await;
}

/// `day x intxt github pull` with the fetch injected: the pull request lands in its own
/// document in the node's github drawer (remembered in `<node dir>/intxt/github.json`),
/// the printout names the intxts the description links, and pulling again overwrites the datum
/// only when `updated_at` moved.
#[tokio::test(flavor = "multi_thread")]
async fn intxt_github_pull_imports_the_pull_request_once_per_update() {
    use crate::cmds::intxt::package::Packaging;
    use big_cabinet_core::DocId;
    use big_cabinet_core::authority::Rights;
    use big_cabinet_core::datum::DatumMember;
    use big_cabinet_core::drawer::DrawerAccess;
    use big_cabinet_core::drawer::listing::KnownDrawers;
    use big_cabinet_core::drawer::protocol::DrawerScope;
    use big_cabinet_core::drawer::protocol::ListKnownDrawers;
    use big_cabinet_core::drawer::protocol::MintDrawerCapability;
    use intxt_core::client::Caller;
    use intxt_core::generic;
    use intxt_core::work;
    use method_core::Method;
    use serde_json::Value;
    use service_utils_rs::RequestContext;

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let packaging = Packaging::new(client, tmp.path(), &node_root);
        let mut out = Vec::new();
        let drawer = packaging
            .create(&mut out, "Port the sync", None, None)
            .await
            .unwrap();
        // `pr-block` prints the block the description carries: this intxt's link (hinting at this
        // node) and title.
        let mut printed = Vec::new();
        packaging
            .pr_block(&mut printed, &[drawer.to_string()])
            .await
            .unwrap();
        let printed = String::from_utf8(printed).unwrap();
        let reference = packaging.reference(&drawer).unwrap();
        let link = reference.to_string();
        assert!(link.contains("?via="), "{link}");
        let block = generic::PrLinks::read(&printed).unwrap();
        assert_eq!(block.intxts.len(), 1, "{printed}");
        assert_eq!(block.intxts[0].reference.via(), reference.via(), "{printed}");
        assert_eq!(block.intxts[0].title.as_deref(), Some("Port the sync"));
        let description = block.write_into("Part of the port.\n").unwrap();
        let answer = |title: &str, updated_at: &str| {
            json!({
                "title": title, "state": "open", "merged_at": null,
                "body": description,
                "head": {"ref": "feat/x", "sha": "4c1f0a"},
                "base": {"ref": "main", "sha": "9a0e11"},
                "updated_at": updated_at,
                "html_url": "https://github.com/o/r/pull/12"
            })
        };
        let pull = async |title: &str, updated_at: &str, target: &str| {
            let mut out = Vec::new();
            let canned = answer(title, updated_at);
            packaging
                .github_pull(&mut out, target, async |pull| {
                    assert_eq!(
                        (pull.owner.as_str(), pull.repo.as_str(), pull.number),
                        ("o", "r", 12)
                    );
                    Ok(canned)
                })
                .await
                .unwrap();
            String::from_utf8(out).unwrap()
        };

        // A malformed block fails the pull rather than naming nothing.
        let broken = packaging
            .github_pull(&mut Vec::new(), "o/r#12", async |_| {
                Ok(json!({
                    "title": "t", "state": "open", "merged_at": null,
                    "body": "<details>\n\n```json\n{\"$type\": \"org.example.intxt.prLinks\", \"intxts\": [{}]}\n```\n\n</details>",
                    "head": {"ref": "feat/x", "sha": "4c1f0a"},
                    "base": {"ref": "main", "sha": "9a0e11"},
                    "updated_at": "2026-11-17T09:00:00Z",
                    "html_url": "https://github.com/o/r/pull/12"
                }))
            })
            .await;
        let broken = format!("{:#}", broken.unwrap_err());
        assert!(broken.contains("intxt block") && broken.contains("ref"), "{broken}");

        let mut broken_show = Vec::new();
        let broken_show_error = packaging
            .github_show(&mut broken_show, "o/r#12")
            .await
            .unwrap_err();
        let broken_show_error = format!("{broken_show_error:#}");
        assert!(broken_show_error.starts_with("intxt block:"), "{broken_show_error}");
        assert!(String::from_utf8(broken_show).unwrap().contains("o/r#12  Open  \"t\""));

        let first = pull("First title", "2026-11-17T10:00:00Z", "o/r#12").await;
        assert!(
            first.contains("o/r#12 \"First title\"") && first.contains("imported"),
            "{first}"
        );
        assert!(first.contains("\"Port the sync\""), "{first}");
        assert!(first.contains(&format!("link: {link}")), "{first}");

        let mut listed = Vec::new();
        packaging.github_ls(&mut listed).await.unwrap();
        assert_eq!(
            String::from_utf8(listed).unwrap(),
            "o/r#12  Open  \"First title\"\n"
        );
        let mut shown = Vec::new();
        packaging.github_show(&mut shown, "https://github.com/o/r/pull/12").await.unwrap();
        let shown = String::from_utf8(shown).unwrap();
        assert!(shown.contains("o/r#12  Open  \"First title\""), "{shown}");
        assert!(shown.contains("url: https://github.com/o/r/pull/12"), "{shown}");
        assert!(shown.contains("Part of the port."), "{shown}");
        assert!(
            shown.contains(&format!("dtxt:doc:{drawer}  \"Port the sync\"  1 entries")),
            "{shown}"
        );

        // The drawer and the pull request's own document are remembered, and hold the datum.
        let state: Value =
            serde_json::from_slice(&std::fs::read(node_root.join("intxt/github.json")).unwrap())
                .unwrap();
        let doc: DocId = state["pulls"]["o/r#12"].as_str().unwrap().parse().unwrap();
        let github_drawer: DocId = state["drawer"].as_str().unwrap().parse().unwrap();
        let info = crate::cmds::status::node_info(client).await.unwrap();
        let minted = client
            .call_untyped(
                MintDrawerCapability::XRPC.nsid,
                MintDrawerCapability::XRPC.method,
                &serde_json::to_value(MintDrawerCapability {
                    scope: DrawerScope {
                        drawers: [(info.known_drawers_id, DrawerAccess::Admin)].into(),
                        create: false,
                    },
                    rights: Rights::USE,
                    subject: None,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(minted.is_success(), "{}", minted.body);
        let capability: big_cabinet_core::authority::CapId =
            serde_json::from_value(minted.body["capabilityId"].clone()).unwrap();
        let listed = client
            .call_untyped(
                ListKnownDrawers::XRPC.nsid,
                ListKnownDrawers::XRPC.method,
                &serde_json::to_value(ListKnownDrawers { capability }).unwrap(),
            )
            .await
            .unwrap();
        assert!(listed.is_success(), "{}", listed.body);
        let known: KnownDrawers = serde_json::from_value(listed.body).unwrap();
        assert!(
            known
                .drawers
                .iter()
                .any(|entry| entry.drawer_id == github_drawer && entry.mirror),
            "{known:?}"
        );

        let caller = Caller::new(
            client,
            RequestContext::root(),
            None,
            "test-github".to_owned(),
        );
        let slot: big_cabinet_core::DatumKey =
            format!("{}/o/r/12", work::GithubPull::ID).parse().unwrap();
        let held = async || caller.read(&doc, &slot).await.unwrap().1;
        let datum = held().await;
        assert_eq!(datum["title"], "First title");
        assert_eq!(datum["state"], "open");
        assert_eq!(datum["headSha"], "4c1f0a");

        // The intxt the description names includes it: an entry that references the pull
        // request's document, which the intxt's drawer now lists.
        assert!(first.contains("pull request added to it"), "{first}");
        let entry_key: big_cabinet_core::DatumKey = format!("{}/o/r/12", work::GithubPullEntry::ID)
            .parse()
            .unwrap();
        let index = caller.index(&drawer).await.unwrap();
        let entry = caller.read(&index, &entry_key).await.unwrap().1;
        assert_eq!(
            entry["pull"],
            format!("dtxt:datum:{doc}/{}/o/r/12", work::GithubPull::ID)
        );
        let drawers = caller
            .call(big_cabinet_core::drawer::protocol::ListDocDrawers {
                doc_id: doc.clone(),
            })
            .await
            .unwrap()
            .drawers;
        assert!(
            drawers.iter().any(|listed| listed.drawer_id == drawer),
            "{drawers:?}"
        );

        // The same `updated_at`: nothing is written, whatever else the answer says. A URL names
        // the same pull request.
        let again = pull(
            "Edited title",
            "2026-11-17T10:00:00Z",
            "https://github.com/o/r/pull/12",
        )
        .await;
        assert!(again.contains("unchanged"), "{again}");
        assert_eq!(held().await["title"], "First title");
        // A newer `updated_at` overwrites it, in the same document.
        let newer = pull("Edited title", "2026-11-17T12:00:00Z", "o/r#12").await;
        assert!(newer.contains("imported"), "{newer}");
        assert!(newer.contains("pull request already in it"), "{newer}");
        assert_eq!(held().await["title"], "Edited title");
        let state: Value =
            serde_json::from_slice(&std::fs::read(node_root.join("intxt/github.json")).unwrap())
                .unwrap();
        assert_eq!(state["pulls"]["o/r#12"], doc.to_string());

        // A run that lost `github.json` creates the document under the same command and gets
        // the first one back.
        std::fs::remove_file(node_root.join("intxt/github.json")).unwrap();
        let recovered = pull("Edited again", "2026-11-17T13:00:00Z", "o/r#12").await;
        assert!(recovered.contains("imported"), "{recovered}");
        let state: Value =
            serde_json::from_slice(&std::fs::read(node_root.join("intxt/github.json")).unwrap())
                .unwrap();
        assert_eq!(state["pulls"]["o/r#12"], doc.to_string());
        assert_eq!(held().await["title"], "Edited again");
    })
    .await;
}

/// A repository with a `feat/x` branch of two commits and an intxt named "Edit me" that holds
/// it, as `(repo, the intxt's short id)`, on the node `client` reaches.
async fn edit_fixture(
    tmp: &tempfile::TempDir,
    client: &NodeClient,
) -> (
    PathBuf,
    big_cabinet_core::drawer::protocol::DrawerId,
    String,
) {
    use crate::cmds::intxt::package::Packaging;
    use intxt_core::edit::Named;

    let node_root = tmp.path().join("node");
    let base = tmp.path().canonicalize().unwrap();
    let repo = init_repo(&base, &[("a.txt", "one\n")]);
    for path in ["b.txt", "c.txt"] {
        std::fs::write(repo.join(path), format!("{path}\n")).unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", &format!("add {path}")]);
    }
    let packaging = Packaging::new(client, &repo, &node_root);
    let drawer = packaging
        .create(&mut Vec::new(), "Edit me", Some("Before."), None)
        .await
        .unwrap();
    let short = short_ids::ShortIds::new([drawer.to_string()])
        .render(&drawer.to_string())
        .unique;
    packaging
        .add(
            &mut Vec::new(),
            Some(&short),
            Named::Branch("feat/x".to_owned()),
            false,
        )
        .await
        .unwrap();
    (repo, drawer, short)
}

/// `day x intxt edit` (ledger D64): the document printed, changed by an agent's partial and full
/// documents and by a scripted `$EDITOR`, and what `show` says after each.
#[tokio::test(flavor = "multi_thread")]
async fn intxt_edit_prints_and_applies_partial_and_full_documents() {
    use crate::cmds::intxt::package::Packaging;
    use intxt_core::edit::Applied;
    use intxt_core::edit::Failed;
    use serde_json::Value;
    use serde_json::json;

    fn code(code: ExitCode) -> String {
        format!("{code:?}")
    }

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let (repo, drawer, short) = edit_fixture(tmp, client).await;
        let packaging = Packaging::new(client, &repo, &node_root);

        let show = async || {
            let mut out = Vec::new();
            packaging.show(&mut out, Some(&short)).await.unwrap();
            String::from_utf8(out).unwrap()
        };
        let print = async || {
            let mut out = Vec::new();
            packaging.edit_print(&mut out, Some(&short)).await.unwrap();
            String::from_utf8(out).unwrap()
        };
        // Applies `document` from a file; answers (exit code, stdout, stderr).
        let apply = async |document: &str, json: bool, dry_run: bool| {
            let file = tmp.path().join("document.json");
            std::fs::write(&file, document).unwrap();
            let (mut out, mut err) = (Vec::new(), Vec::new());
            let exit = packaging
                .edit_apply(
                    &mut out,
                    &mut err,
                    Some(&short),
                    file.to_str().unwrap(),
                    json,
                    dry_run,
                )
                .await
                .unwrap();
            (
                code(exit),
                String::from_utf8(out).unwrap(),
                String::from_utf8(err).unwrap(),
            )
        };

        // The printed document: a fixed layout, one entry per line, full ids.
        let printed = print().await;
        println!("{printed}");
        let document: Value = serde_json::from_str(&printed).unwrap();
        assert_eq!(document["$type"], "org.example.intxt.edit");
        assert_eq!(document["intxt"], format!("dtxt:doc:{drawer}"));
        assert!(
            document["$schema"]
                .as_str()
                .unwrap()
                .ends_with("intxt-edit.schema.json")
        );
        assert!(
            printed.contains("\n  \"title\": \"Edit me\",\n"),
            "{printed}"
        );
        assert_eq!(printed, print().await, "printing twice is the same text");
        let entries = document["entries"].as_array().unwrap();
        assert_eq!(entries.len(), 2);
        let line_of = |kind: &str| {
            printed
                .lines()
                .filter(|line| line.contains(&format!("\"{kind}\":")))
                .count()
        };
        assert_eq!((line_of("commits"), line_of("ref")), (1, 1), "{printed}");
        let id_of = |kind: &str| -> String {
            entries
                .iter()
                .find(|entry| entry.get(kind).is_some())
                .unwrap()["id"]
                .as_str()
                .unwrap()
                .to_owned()
        };
        let ref_id = id_of("ref");
        assert_eq!((ref_id.len(), id_of("commits").len()), (32, 32));
        let base_heads = document["base"].clone();

        // An unchanged document changes nothing, and nothing is written for it.
        let (exit, out, _) = apply(&printed, false, false).await;
        assert_eq!(
            (exit, out.as_str()),
            (code(ExitCode::SUCCESS), "nothing to change\n")
        );
        assert_eq!(print().await, printed);

        // A partial document, with a short prefix of the ref's id, as a dry run first.
        let partial = json!({
            "base": base_heads,
            "title": "Renamed",
            "description": null,
            "entryChanges": [
                { "remove": &ref_id[..3] },
                { "add": { "commits": "feat/x~1" } },
            ],
        })
        .to_string();
        let (exit, out, _) = apply(&partial, false, true).await;
        assert_eq!(exit, code(ExitCode::SUCCESS));
        assert!(out.starts_with("dry run, nothing written"), "{out}");
        assert!(
            out.contains("title") && out.contains("remove") && out.contains("--commits feat/x~1"),
            "{out}"
        );
        assert_eq!(print().await, printed, "a dry run writes nothing");
        let (_, json_out, _) = apply(&partial, true, true).await;
        let dry: Applied = serde_json::from_str(&json_out).unwrap();
        assert!(
            dry.dry_run && dry.changes.len() == 4 && dry.undo.is_empty(),
            "{json_out}"
        );

        let (exit, out, err) = apply(&partial, false, false).await;
        assert_eq!(
            (exit.as_str(), err.as_str()),
            (code(ExitCode::SUCCESS).as_str(), ""),
            "{out}"
        );
        assert!(out.starts_with("changed:"), "{out}");
        let shown = show().await;
        assert!(shown.contains("\"Renamed\""), "{shown}");
        assert!(!shown.contains("Before."), "{shown}");
        assert!(!shown.contains("tracked refs"), "{shown}");
        assert!(shown.contains("commits (2):"), "{shown}");
        let after = print().await;
        assert_ne!(after, printed);

        // The same document again is stale: refused with exit 3 and the current document.
        let (exit, out, err) = apply(&partial, false, false).await;
        assert_eq!(exit, code(ExitCode::from(3)));
        assert_eq!(out, after);
        assert!(err.starts_with("/base:"), "{err}");
        let (exit, out, _) = apply(&partial, true, false).await;
        assert_eq!(exit, code(ExitCode::from(3)));
        let stale: Failed = serde_json::from_str(&out).unwrap();
        assert_eq!(stale.errors[0].path, "/base");
        assert_eq!(stale.current.unwrap().to_text(), after);

        // A retry of a document that has applied, whose base is stale, has nothing left to do:
        // exit 0, not a conflict.
        let retried = json!({
            "base": base_heads,
            "title": "Renamed",
            "description": null,
            "entryChanges": [{ "remove": ref_id }],
        })
        .to_string();
        let (exit, out, _) = apply(&retried, false, false).await;
        assert_eq!(
            (exit, out.as_str()),
            (code(ExitCode::SUCCESS), "nothing to change\n")
        );
        assert_eq!(print().await, after);

        // A document that is wrong: each error has a path, and exit is 1.
        let now: Value = serde_json::from_str(&after).unwrap();
        let wrong = json!({
            "base": now["base"],
            "entryChanges": [{ "remove": "nope" }, { "add": { "branch": "no/such" } }],
        })
        .to_string();
        let (exit, out, _) = apply(&wrong, true, false).await;
        assert_eq!(exit, code(ExitCode::FAILURE));
        let failed: Failed = serde_json::from_str(&out).unwrap();
        assert!(failed.current.is_none());
        assert_eq!(failed.errors[0].path, "/entryChanges/0/remove", "{out}");
        let (exit, _, err) = apply(&wrong, false, false).await;
        assert_eq!(exit, code(ExitCode::FAILURE));
        assert!(err.contains("/entryChanges/0/remove"), "{err}");
        assert!(show().await.contains("\"Renamed\""));

        // The undo of the title comes first and is a document at the intxt's new base.
        let (_, json_out, _) = apply(
            &json!({ "base": now["base"], "title": "Third", "description": "Again." }).to_string(),
            true,
            false,
        )
        .await;
        let applied: Applied = serde_json::from_str(&json_out).unwrap();
        assert_eq!(applied.changes.len(), 2, "{json_out}");
        let undo = &applied.undo[0];
        let body = undo
            .strip_prefix("echo '")
            .and_then(|rest| rest.split_once("' | day x intxt edit"))
            .map(|(body, _)| body.replace("'\\''", "'"))
            .unwrap_or_else(|| panic!("{undo}"));
        let (exit, _, _) = apply(&body, false, false).await;
        assert_eq!(exit, code(ExitCode::SUCCESS));
        let shown = show().await;
        assert!(
            shown.contains("\"Renamed\"") && !shown.contains("Again."),
            "{shown}"
        );

        // Removing by the full id, and what undoes it.
        let current: Value = serde_json::from_str(&print().await).unwrap();
        let commits_id = current["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry.get("commits").is_some())
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let (_, json_out, _) = apply(
            &json!({ "base": current["base"], "entryChanges": [{ "remove": commits_id }] })
                .to_string(),
            true,
            false,
        )
        .await;
        let removed: Applied = serde_json::from_str(&json_out).unwrap();
        assert_eq!(removed.changes[0].op, "remove");
        assert!(removed.undo[0].contains("day x intxt add"), "{removed:?}");
    })
    .await;
}

/// `day x intxt edit` with `$EDITOR` a script that rewrites the file, as `GIT_SEQUENCE_EDITOR`
/// does: errors reopen the editor, and an unchanged file aborts.
#[tokio::test(flavor = "multi_thread")]
async fn intxt_edit_runs_the_editor_and_reopens_it_with_errors() {
    use crate::cmds::intxt::package::Packaging;
    use std::os::unix::fs::PermissionsExt;

    fn code(code: ExitCode) -> String {
        format!("{code:?}")
    }

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let (repo, _, short) = edit_fixture(tmp, client).await;
        let packaging = Packaging::new(client, &repo, &node_root);
        let show = async || {
            let mut out = Vec::new();
            packaging.show(&mut out, Some(&short)).await.unwrap();
            String::from_utf8(out).unwrap()
        };

        // The first pass saves an empty title, which is refused and reopens with `errors`;
        // the second sees them and fixes the title.
        let script = tmp.path().join("editor.sh");
        std::fs::write(
            &script,
            r##"#!/bin/sh
if grep -q '"errors"' "$1"; then t=Fixed; else t=; fi
sed -i "s/^  \(\"title\": \)\"[^\"]*\"/  \1\"$t\"/" "$1"
"##,
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut out = Vec::new();
        let exit = packaging
            .edit_interactive(&mut out, Some(&short), script.to_str().unwrap(), false)
            .await
            .unwrap();
        let out = String::from_utf8(out).unwrap();
        assert_eq!(code(exit), code(ExitCode::SUCCESS), "{out}");
        assert!(out.starts_with("changed:"), "{out}");
        assert!(show().await.contains("\"Fixed\""));

        // An editor that changes nothing aborts, as does one that leaves the errors alone.
        let mut out = Vec::new();
        let exit = packaging
            .edit_interactive(&mut out, Some(&short), "true", false)
            .await
            .unwrap();
        assert_eq!(code(exit), code(ExitCode::SUCCESS));
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "edit aborted, nothing changed\n"
        );
        let blank = tmp.path().join("blank.sh");
        std::fs::write(
            &blank,
            r##"#!/bin/sh
sed -i 's/^  \("title": \)"[^"]*"/  \1""/' "$1"
"##,
        )
        .unwrap();
        std::fs::set_permissions(&blank, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut out = Vec::new();
        let exit = packaging
            .edit_interactive(
                &mut out,
                Some(&short),
                &format!("sh {}", blank.display()),
                false,
            )
            .await
            .unwrap();
        assert_eq!(code(exit), code(ExitCode::FAILURE));
        assert!(show().await.contains("\"Fixed\""));
    })
    .await;
}
