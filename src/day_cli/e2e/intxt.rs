//! The intxt layer of the library e2e tests: the real handler component installed on a node the
//! way `day x intxt install --handler` does, and the commands that run against it.

use super::library::connect;
use super::library::new_node;
use crate::client::NodeClient;
use crate::interlude::*;

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
    use crate::cmds::intxt;
    use big_dispatch_core::Readiness;
    use utils_rs::prelude::FutureExt;

    let (tmp, dir) = new_node();

    let wasm_path = guest_path();
    let wasm = std::fs::read(&wasm_path).unwrap_or_else(|error| {
        panic!(
            "{}: {error}; build it with `cargo build --manifest-path src/wasm_utils_rs/Cargo.toml -p intxt_nodule_guest --target wasm32-wasip3 --release` or set INTXT_GUEST_WASM",
            wasm_path.display()
        )
    });
    let client = connect(&dir).await.unwrap();
    let outcome = std::panic::AssertUnwindSafe(async {
        let record = intxt::install(&client, &wasm).await.unwrap();
        assert_eq!(record.readiness, Readiness::Ready);
        assert!(record.methods.unwrap().granted);
        body(&tmp, &client).await;
    })
    .catch_unwind()
    .await;
    client.stop().await.unwrap();
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
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
            read: [doc.clone()].into(),
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
        .call(nsid, method_core::HttpMethod::Post, &input)
        .await
        .unwrap();
    assert!(reply.is_success(), "{nsid} answered {reply:?}");
    reply.body
}

/// `org.example.intxt.createIntxt`, through the real installed handler, creates a drawer whose
/// descriptor document carries the intxt marker, the title and an empty body, readable as
/// written.
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
        let accepted = |key: &str| {
            let client = &client;
            let drawer = &drawer;
            let key = key.parse().unwrap();
            async move { read_at(client, drawer, &key).await.unwrap().1 }
        };
        assert_eq!(
            accepted("org.example.intxt/main").await,
            json!({"empty": true})
        );
        assert_eq!(
            accepted("org.example.intxt.title/main").await,
            json!({"title": "Datum references move to dtxt:"})
        );
        assert_eq!(
            accepted("org.example.intxt.body/main").await,
            json!({"order": []})
        );
    })
    .await;
}

/// Runs `git` in `dir` with nothing taken from the machine.
fn git(dir: &std::path::Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
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
/// nothing else.
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
    let held: Vec<_> = caller
        .datum_keys(&commits_doc)
        .await
        .unwrap()
        .into_iter()
        .map(|key| key.to_string())
        .collect();
    assert_eq!(
        held,
        {
            let mut expected = [first.clone(), second.clone()]
                .map(|sha| format!("org.example.intxt.work.gitCommit/{sha}"));
            expected.sort();
            expected.to_vec()
        },
        "the commits document holds the range's commits and nothing else"
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

/// Packaging a branch of two commits whose content a recorded session's tool calls produced:
/// `add --branch` writes one commit entry carrying the pack, tracks the ref on this clone
/// and brings the session; `show` lists all three; adding again changes nothing; `status`
/// finds the intxt from the worktree.
#[tokio::test(flavor = "multi_thread")]
async fn intxt_add_branch_packages_commits_ref_and_session_and_readding_is_a_no_op() {
    use crate::cmds::intxt::package::Named;
    use crate::cmds::intxt::package::Packaging;

    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let repo = init_repo(&base, &[("a.txt", "one\n")]);

        record_session_with_two_commits(client, &node_root, &base, &repo).await;

        let packaging = Packaging::new(client, &repo, &node_root);
        let mut created = Vec::new();
        let drawer = packaging
            .create(&mut created, "Two files", Some("b and c"))
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
        assert!(
            listed_before_add.contains("\"Two files\""),
            "{listed_before_add}"
        );
        assert!(
            listed_before_add.contains("commits: 0, refs: 0, sessions: 0"),
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
        assert!(
            shown.contains(&format!(
                "link: dtxt:datum:{drawer}/org.example.intxt/main\n"
            )),
            "{shown}"
        );
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

        // The entry's pack is a blob datum in the intxt's own document.
        let (_, entry) = read_at(
            client,
            &drawer,
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
        let pack = entry["pack"].as_str().unwrap();
        let blob_key = pack
            .strip_prefix("dtxt:datum:self/")
            .unwrap_or_else(|| panic!("{pack} names the intxt's own document"));
        let (_, blob) = read_at(client, &drawer, &blob_key.parse().unwrap())
            .await
            .unwrap();
        assert_eq!(blob["mime"], "application/x-git-pack");
        assert!(blob["lengthOctets"].as_u64().unwrap() > 0, "{blob}");

        assert_commit_membership(client, &drawer, &repo, &tip, &merge_base).await;

        let which = async |name: &str| {
            let mut out = Vec::new();
            packaging.which(&mut out, name).await.unwrap();
            String::from_utf8(out).unwrap()
        };
        let held_line =
            format!("{short}  \"Two files\"  link: dtxt:datum:{drawer}/org.example.intxt/main\n");
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
            listed_after_restore.contains("\"Two files\""),
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
    })
    .await;
}

/// FDR 008 W6: a branch made with no capture at all. `add --branch` records its ref and the
/// identity documents first (no sessions: none were recorded, and `show` says so) and the
/// intxt holds the commits, read from git; the repository document never holds a `gitCommit`.
/// A hook afterwards records only the live move, with no second baseline.
#[tokio::test(flavor = "multi_thread")]
async fn intxt_add_branch_imports_history_that_was_never_recorded() {
    use crate::cmds::intxt::capture;
    use crate::cmds::intxt::package::Named;
    use crate::cmds::intxt::package::Packaging;
    use big_cabinet_core::DocId;
    use big_cabinet_core::datum::DatumMember;
    use intxt_capture::host;
    use intxt_capture::identity;
    use intxt_core::client::Caller;
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
            .create(&mut created, "Unrecorded", None)
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

        // The observation `add` made recorded the identity documents and the branch's ref, and
        // no commit: the repository document keeps identity only, the intxt holds the commits.
        // The worktree was clean, so it recorded no op.
        let recorded = host::recorded_worktree(&worktree).unwrap();
        assert_eq!(recorded.ops, 0);
        let caller = Caller::new(client, RequestContext::root(), None, "test".to_owned());
        let repo_doc: DocId = recorded.docs.repo.as_deref().unwrap().parse().unwrap();
        let repo_commits = async || {
            let (_, datums) = caller.read_datums(&repo_doc, None).await.unwrap();
            datums
                .into_keys()
                .filter(|key| key.tag().as_str() == work::GitCommit::ID)
                .count()
        };
        assert_eq!(repo_commits().await, 0);

        // A live move is recorded by capture as usual: one commit op, no commit datum, and no
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
        assert_eq!(repo_commits().await, 0);

        std::fs::write(repo.join("d.txt"), "d\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "add d.txt"]);
        capture::run(client, &node_root, &hook("Stop")).await.unwrap();
        assert_eq!(host::recorded_worktree(&worktree).unwrap().ops, 1);
        assert_eq!(repo_commits().await, 0);

        capture::run(client, &node_root, &hook("Stop")).await.unwrap();
        assert_eq!(host::recorded_worktree(&worktree).unwrap().ops, 1);
        assert_eq!(repo_commits().await, 0);

        // Adding the branch again, now that it moved, imports nothing: capture has the move.
        let mut out = Vec::new();
        packaging
            .add(&mut out, Some(&short), Named::Branch("feat/x".to_owned()), false)
            .await
            .unwrap();
        assert_eq!(host::recorded_worktree(&worktree).unwrap().ops, 1);
        assert_eq!(repo_commits().await, 0);

    })
    .await;
}

/// Review through `comment`, `reply` and `inbox`, between two actors of one node (the
/// operator and an `agent` it opens): a comment on a commit's lines, the other side's inbox
/// and reply, a rebase that inserts lines above the target (the comment follows to its new
/// lines), an edit of the targeted text (outdated), and resolving, after which the thread
/// leaves the inbox.
#[tokio::test(flavor = "multi_thread")]
async fn intxt_review_comments_follow_a_rebase_and_resolve() {
    use crate::cmds::intxt::package::Named;
    use crate::cmds::intxt::package::Packaging;
    use crate::cmds::intxt::review::Side;
    use big_cabinet_core::authority::OpenActor;
    use big_cabinet_core::authority::Rights;
    use intxt_core::annotation::Motivation;

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
        let agent_actor = crate::cmds::intxt::call(
            client,
            OpenActor {
                parent: client.booted_node().authority.root,
                path: "agent".into(),
                rights: Rights::USE,
            },
        )
        .await
        .unwrap()
        .actor
        .actor_id;
        let agent = Packaging::new(client, &repo, &node_root).acting_as(agent_actor);

        let mut created = Vec::new();
        let drawer = operator
            .create(&mut created, "Review me", None)
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
                who.reply(&mut out, to, text, resolved).await.unwrap();
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
            seen_by_agent.contains("them: Why edit b8?"),
            "{seen_by_agent}"
        );
        // The author's own inbox has nothing: it wrote last.
        let own = inbox(&operator).await;
        assert!(own.contains("nothing waiting"), "{own}");

        // 2. The reply. The thread is then waiting on the author, and the replier, who has
        // seen everything, has nothing left.
        let replied = reply(&agent, &root, Some("Fixes the off-by-one."), None).await;
        assert!(replied.starts_with("reply "), "{replied}");
        assert!(inbox(&agent).await.contains("nothing waiting"));
        let waiting = inbox(&operator).await;
        assert!(waiting.contains("them: Fixes the off-by-one."), "{waiting}");
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
        let resolved = reply(&operator, &root, None, Some(true)).await;
        assert!(resolved.contains("resolved"), "{resolved}");
        assert!(
            resolved.contains(&format!("undo: day x intxt reply {root} --reopen")),
            "{resolved}"
        );
        assert!(inbox(&operator).await.contains("nothing waiting"));
        assert!(inbox(&agent).await.contains("nothing waiting"));
        // Reopened, it is waiting on the author again: the agent wrote last.
        reply(&operator, &root, None, Some(false)).await;
        let reopened = inbox(&operator).await;
        assert!(reopened.contains("1 thread(s)"), "{reopened}");
    })
    .await;
}

/// `capture` is for the daemon: with none serving, its entry point refuses, naming the command
/// that starts one, without booting the node.
#[tokio::test(flavor = "multi_thread")]
async fn capture_refuses_without_a_daemon() {
    let (_tmp, dir) = new_node();
    let error = crate::cmds::intxt::require_daemon(&dir).await.unwrap_err();
    assert!(format!("{error:#}").contains("day node start"), "{error:#}");
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
                format!("dtxt:datum:{review}/org.example.intxt.work.worktreeEntry/0193a"),
                format!("dtxt:datum:{review}/org.example.intxt.work.sessionEntry/eb26ecaa"),
                format!("dtxt:datum:{review}/org.example.intxt.work.worktreeEntry/0193a"),
            ]
        );
        let (_, entry) = read_at(
            client,
            &review,
            &"org.example.intxt.work.worktreeEntry/0193a"
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(entry, json!({"worktree": worktree.to_string()}));
        let (_, body) = read_at(
            client,
            &review,
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
