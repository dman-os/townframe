use crate::*;
use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::CompiledSchema;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::Stored;
use big_cabinet_core::datum::hydrate_value;
use big_cabinet_core::datum::reconcile_json_atomic;
use big_cabinet_core::dmeta::Uuid7;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::json;

fn admitted_schema(id: &str) -> CompiledSchema {
    let bundle = Bundle::new(&UNIVERSE, Vec::new());
    bundle.validate().unwrap();
    let lexicons = bundle.lexicons().unwrap();
    let lexicon = lexicons.iter().find(|lexicon| lexicon.id == id).unwrap();
    // The reference defs a datum field refers to are carried by every registration.
    let defs = big_cabinet_core::reference::defs_lexicon();
    let carried: Vec<&Value> = lexicons
        .iter()
        .filter(|other| other.id != id)
        .map(|other| &other.file)
        .chain([&defs])
        .collect();
    bundle
        .validate_lexicon(id, &lexicon.file, &carried)
        .unwrap_or_else(|error| panic!("{id}: {error}"))
}

/// The full chain for one value: typed decode and re-encode byte-identically, schema check,
/// reconcile into Automerge, save, load, hydrate, and decode again.
fn codec_round_trip<T>(value: Value)
where
    T: DatumMember + Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let original = serde_json::to_vec(&value).unwrap();
    let typed: T = serde_json::from_value(value.clone()).unwrap();
    let encoded = serde_json::to_value(&typed).unwrap();
    assert_eq!(serde_json::to_vec(&encoded).unwrap(), original, "{}", T::ID);
    let schema = admitted_schema(T::ID);
    Stored::check(&schema, value.clone()).unwrap_or_else(|error| panic!("{}: {error}", T::ID));
    let mut doc = automerge::Automerge::new();
    let transaction = doc.transaction();
    let transaction = reconcile_json_atomic(
        transaction,
        &schema,
        &automerge::ROOT,
        "datum".into(),
        value,
    )
    .unwrap();
    transaction.commit();
    let restored = automerge::Automerge::load(&doc.save()).unwrap();
    let hydrated = hydrate_value(&restored, &automerge::ROOT, "datum".into(), &schema).unwrap();
    assert_eq!(
        serde_json::to_vec(&hydrated).unwrap(),
        original,
        "{}",
        T::ID
    );
    assert_eq!(serde_json::from_value::<T>(hydrated).unwrap(), typed);
}

#[test]
fn every_datum_declares_publishes_and_admits() {
    let universe = declarations();
    assert_eq!(
        universe
            .datums
            .iter()
            .map(|datum| datum.id.as_str())
            .collect::<Vec<_>>(),
        [
            "org.example.intxt",
            "org.example.intxt.title",
            "org.example.intxt.path",
            "org.example.intxt.note",
            "org.example.intxt.body",
            "org.example.intxt.comment",
            "org.example.intxt.threadState",
            "org.example.intxt.structure",
            "org.example.intxt.seen",
            "org.example.intxt.annotationsEntry",
            "org.example.intxt.work.gitRepo",
            "org.example.intxt.work.gitClone",
            "org.example.intxt.work.worktree",
            "org.example.intxt.work.worktreeOp",
            "org.example.intxt.work.file",
            "org.example.intxt.work.gitCommit",
            "org.example.intxt.work.rewrite",
            "org.example.intxt.work.rewrittenFrom",
            "org.example.intxt.work.githubPull",
            "org.example.intxt.work.gitRef",
            "org.example.intxt.work.jjOperation",
            "org.example.intxt.work.jjChange",
            "org.example.intxt.work.agentSession",
            "org.example.intxt.work.promptTurn",
            "org.example.intxt.work.plan",
            "org.example.intxt.work.repoEntry",
            "org.example.intxt.work.commitsDoc",
            "org.example.intxt.work.worktreeEntry",
            "org.example.intxt.work.sessionEntry",
            "org.example.intxt.work.githubPullEntry",
            "org.example.intxt.work.gitCommitEntry",
            "org.example.intxt.work.jjChangeEntry",
            "org.example.intxt.work.gitRefEntry",
        ]
    );
    for datum in &universe.datums {
        admitted_schema(&datum.id);
    }
}

/// The ADRs' examples are what an agent writes by hand (ADR 036 §1 rule 3), so each must pass the
/// whole chain as written.
#[test]
fn the_adr_examples_are_valid_datums() {
    codec_round_trip::<generic::Intxt>(json!({"empty": true}));
    codec_round_trip::<generic::Title>(json!({
        "title": "Datum references move to dtxt:",
        "description": "Rewrites `reference.rs` for ADR 035."
    }));
    codec_round_trip::<generic::PathName>(json!({"path": "src/big_cabinet_core/reference.rs"}));
    codec_round_trip::<generic::Note>(json!({"mime": "text/x-rust", "content": "fn main() {}\n"}));
    codec_round_trip::<generic::Body>(json!({"order": [
        "dtxt:datum:self/org.example.intxt.work.sessionEntry/eb26ecaa",
        "dtxt:datum:self/org.example.intxt.work.worktreeEntry/0193a",
        "dtxt:datum:self/org.example.intxt.annotationsEntry/main"
    ]}));
    // A comment on code: the op and a path, whose content is git's blob at that op (ADR 041 §2).
    codec_round_trip::<annotation::Comment>(json!({
        "targets": [{
            "source": {
                "ref": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.worktreeOp/41"
            },
            "path": "src/big_cabinet_core/reference.rs",
            "lines": {"start": 120, "end": 124},
            "quote": {"exact": "fn parse("}
        }],
        "body": {"mime": "text/markdown", "content": "Why a second parser?"},
        "motivation": "commenting"
    }));
    codec_round_trip::<annotation::Comment>(json!({
        "targets": [{
            "source": {
                "ref": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.note/main#content/%5B12@a3f-40@a3f%5D",
                "heads": ["z11111111111111111111111111111111"]
            },
            "lines": {"start": 120, "end": 124},
            "quote": {"exact": "fn parse(", "prefix": "pub ", "suffix": "value: &str)"}
        }],
        "body": {"mime": "text/markdown", "content": "Why not reuse the fragment here?"},
        "motivation": "commenting"
    }));
    codec_round_trip::<annotation::Comment>(json!({
        "targets": [{"source": {"ref": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.comment/0193"}}],
        "body": {"mime": "text/markdown", "content": "Because the slot can contain `/`."},
        "motivation": "replying",
        "replyTo": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.comment/0193"
    }));
    codec_round_trip::<annotation::ThreadState>(json!({"resolved": false}));
    codec_round_trip::<annotation::Structure>(json!({
        "kind": "task",
        "title": "Move references to dtxt:",
        "status": "completed",
        "targets": [{
            "source": {"ref": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.promptTurn/t20"},
            "to": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.promptTurn/t45"
        }],
        "order": "15"
    }));
    codec_round_trip::<annotation::Seen>(json!({
        "datums": {"org.example.intxt.comment/0193": ["z11111111111111111111111111111111"]},
        "reviewed": {"refs/heads/feat/intxt": "77b2"}
    }));
    codec_round_trip::<work::GitRepo>(json!({
        "roots": ["a1b2"],
        "remotes": ["git@github.com:dman-os/townframe.git"]
    }));
    codec_round_trip::<work::GitClone>(json!({
        "repo": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.gitRepo/0193b",
        "machine": "dtxt:actor:z11111111111111111111111111111111",
        "path": "/run/media/p3N/townframe-p3N-3/.git",
        "vcs": ["git", "jj"]
    }));
    codec_round_trip::<work::Worktree>(json!({
        "clone": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.gitClone/0193c",
        "id": "0193a",
        "path": "/run/media/p3N/townframe-p3N-3",
        "opLogs": [{"doc": "zOps0", "from": 0}, {"doc": "zOps1", "from": 256}]
    }));
    codec_round_trip::<work::WorktreeOp>(json!({
        "at": "2026-11-10T21:04:11Z",
        "kind": "edit",
        "cause": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.promptTurn/t41#toolCalls/%7B%22id%22:%22toolu_01%22%7D",
        "files": {"src/big_cabinet_core/reference.rs": "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"},
        "deleted": ["src/old.rs"]
    }));
    codec_round_trip::<work::WorktreeOp>(json!({
        "at": "2026-11-10T21:05:00Z",
        "kind": "amend",
        "files": {},
        "git": {"head": "4c1f", "ref": "refs/heads/feat/intxt", "from": "77b2", "to": "4c1f",
                "reflog": "commit (amend): refactor(reference): dtxt scheme"},
        "jj": {"operation": "e1f0", "change": "kxqoltvm"}
    }));
    codec_round_trip::<work::File>(
        json!({"worktree": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.worktree/main"}),
    );
    codec_round_trip::<work::GitCommit>(json!({
        "parents": ["9a0e"],
        "changeId": "kxqoltvm",
        "author": {"name": "dman-os", "email": "dman-os@example.org", "at": "2026-11-10T21:04:11Z"},
        "committer": {"name": "dman-os", "email": "dman-os@example.org", "at": "2026-11-10T21:04:11Z"},
        "message": "refactor(reference): dtxt scheme",
        "files": [
            {"path": "src/big_cabinet_core/reference.rs", "status": "modified"},
            {"path": "src/url.rs", "status": "renamed", "oldPath": "src/uri.rs"}
        ],
        "patchId": "3f9a1c0de4b2"
    }));
    codec_round_trip::<work::GithubPull>(json!({
        "title": "Splits the reference parser", "body": "Intxt: dtxt:doc:z1",
        "state": "merged", "head": "feat/refs", "base": "main",
        "headSha": "4c1f0a", "baseSha": "9a0e11",
        "updatedAt": "2026-11-17T10:00:00Z", "url": "https://github.com/o/r/pull/12"
    }));
    codec_round_trip::<work::GithubPull>(json!({
        "title": "t", "body": "", "state": "draft", "head": "h", "base": "b",
        "headSha": "1", "baseSha": "2", "updatedAt": "2026-11-17T10:00:00Z", "url": "u",
        "reviewDecision": "approved"
    }));
    codec_round_trip::<work::Rewrite>(json!({"successor": "4c1f0a", "how": "patchId"}));
    codec_round_trip::<work::Rewrite>(json!({"successor": "4c1f0a", "how": "hook"}));
    codec_round_trip::<work::RewrittenFrom>(json!({"predecessor": "9a0e11", "how": "patchId"}));
    codec_round_trip::<work::GitRef>(json!({"target": "4c1f", "base": "9a0e"}));
    codec_round_trip::<work::JjOperation>(json!({
        "parents": ["d00d"],
        "description": "snapshot working copy",
        "at": "2026-11-10T21:04:11Z",
        "heads": ["4c1f"],
        "bookmarks": {"feat/intxt": "4c1f"}
    }));
    codec_round_trip::<work::JjChange>(json!({"commits": ["77b2", "4c1f"]}));
    codec_round_trip::<work::AgentSession>(json!({
        "harness": "claude-code",
        "sessionId": "eb26ecaa",
        "title": "intxt design",
        "cwd": "/run/media/p3N/townframe-p3N-3",
        "parent": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.agentSession/main"
    }));
    codec_round_trip::<work::PromptTurn>(json!({
        "seq": 41,
        "at": "2026-11-10T21:04:00Z",
        "parentTurn": "t40",
        "stopReason": "end_turn",
        "messages": [
            {"id": "m1", "seq": 0, "at": "2026-11-10T21:04:00Z", "kind": "userMessage", "text": "Rename the scheme."},
            {"id": "m2", "seq": 2, "at": "2026-11-10T21:04:30Z", "kind": "agentMessage", "text": "Done."}
        ],
        "toolCalls": [{
            "id": "toolu_01", "seq": 1, "title": "Edit reference.rs", "kind": "edit", "status": "completed",
            "cwd": "/run/media/p3N/townframe-p3N-3/src",
            "locations": [{"path": "/run/media/p3N/townframe-p3N-3/src/reference.rs", "line": 120}],
            "diffs": [{"path": "src/reference.rs", "patch": "@@ -120 +120 @@\n-a\n+b\n"}],
            "output": "updated"
        }]
    }));
    codec_round_trip::<work::Plan>(json!({"entries": [
        {"content": "Rewrite reference.rs", "priority": "high", "status": "completed"}
    ]}));
    codec_round_trip::<work::RepoEntry>(
        json!({"repo": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.gitRepo/0193b"}),
    );
    codec_round_trip::<work::WorktreeEntry>(
        json!({"worktree": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.worktree/main"}),
    );
    codec_round_trip::<work::SessionEntry>(
        json!({"session": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.agentSession/main"}),
    );
    codec_round_trip::<work::GitCommitEntry>(json!({
        "tip": "org.example.intxt.work.gitCommit/4c1f0a",
        "base": "org.example.intxt.work.gitCommit/9a0e11",
        "pack": "dtxt:datum:self/org.example.daybook.blob/af1349b9f5f9a1a6a0404dea36dcc949"
    }));
    // A range that reaches the root has no base.
    codec_round_trip::<work::GitCommitEntry>(json!({
        "tip": "org.example.intxt.work.gitCommit/4c1f0a",
        "pack": "dtxt:datum:self/org.example.daybook.blob/af1349b9f5f9a1a6a0404dea36dcc949"
    }));
    // The patch is optional: an entry written before it existed decodes without one.
    codec_round_trip::<work::GitCommitEntry>(json!({
        "tip": "org.example.intxt.work.gitCommit/4c1f0a",
        "base": "org.example.intxt.work.gitCommit/9a0e11",
        "pack": "dtxt:datum:self/org.example.daybook.blob/af1349b9f5f9a1a6a0404dea36dcc949",
        "patch": "dtxt:datum:self/org.example.daybook.blob/7b0c22d1e0b94f34a2d6e3a1c9f58e10"
    }));
    let old: work::GitCommitEntry = serde_json::from_value(json!({
        "tip": "org.example.intxt.work.gitCommit/4c1f0a",
        "pack": "dtxt:datum:self/org.example.daybook.blob/af1349b9f5f9a1a6a0404dea36dcc949"
    }))
    .unwrap();
    assert_eq!(old.patch, None);
    codec_round_trip::<work::JjChangeEntry>(json!({
        "pack": "dtxt:datum:self/org.example.daybook.blob/af1349b9f5f9a1a6a0404dea36dcc949",
        "patch": "dtxt:datum:self/org.example.daybook.blob/7b0c22d1e0b94f34a2d6e3a1c9f58e10"
    }));
    codec_round_trip::<work::JjChangeEntry>(
        json!({"pack": "dtxt:datum:self/org.example.daybook.blob/af1349b9f5f9a1a6a0404dea36dcc949"}),
    );
    codec_round_trip::<work::CommitsDoc>(json!({
        "doc": "z11111111111111111111111111111111",
        "note": "carried"
    }));
    codec_round_trip::<work::GithubPullEntry>(json!({
        "pull": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.githubPull/o/r/12",
        "note": "carried"
    }));
    codec_round_trip::<work::GitRefEntry>(json!({
        "ref": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.gitRef/0193c/refs/heads/feat/refs",
        "base": "org.example.intxt.work.gitCommit/9a0e11"
    }));
    codec_round_trip::<annotation::AnnotationsEntry>(json!({"empty": true}));
}

fn node(byte: u8) -> big_cabinet_core::dmeta::MultibaseKey {
    big_cabinet_core::dmeta::MultibaseKey::try_from([byte; 32].as_slice()).unwrap()
}

/// Two links as an author's tooling would write them: the first with a title, refs and a hint
/// naming two nodes, the second a bare reference.
fn sample_links() -> generic::PrLinks {
    let first: big_cabinet_core::DocId = "z11111111111111111111111111111111".parse().unwrap();
    let second: big_cabinet_core::DocId = "z11111111111111111111111111111112".parse().unwrap();
    let mut with_title =
        generic::PrLink::new(generic::Intxt::reference(&first, [node(7), node(3)]));
    with_title.title = Some("intxt: commits live with the intxt".into());
    with_title.refs = vec!["refs/heads/stack/intxt-16".into()];
    generic::PrLinks {
        intxts: vec![
            with_title,
            generic::PrLink::new(generic::Intxt::reference(&second, [])),
        ],
        ..Default::default()
    }
}

/// A reference names the intxt's descriptor document, and the `via` hint it carries survives the
/// block while equality ignores it.
#[test]
fn the_pr_block_keeps_the_reference_with_its_via_hint() {
    let links = sample_links();
    let reference = &links.intxts[0].reference;
    assert_eq!(
        reference.to_string(),
        format!(
            "dtxt:doc:z11111111111111111111111111111111?via={}|{}",
            node(3),
            node(7)
        )
    );
    let read = generic::PrLinks::read(&links.block()).unwrap();
    assert_eq!(read, links);
    assert_eq!(read.intxts[0].reference.via(), [node(3), node(7)]);
    assert_eq!(read.intxts[1].reference.via(), []);
    assert_eq!(read.intxts[0].title, links.intxts[0].title);
    assert_eq!(read.intxts[0].refs, links.intxts[0].refs);
}

/// The golden vectors of the PR description block (`docs/scratch/intxt-review-micro.md`, "The PR
/// description block"): every case's description reads as exactly the listed links (the block's
/// own JSON, `$type` left out, `via` included), or is an error. A reader in another language is
/// correct when it agrees on every case. A case that reads also survives writing back: the links
/// replace their own block and read the same.
#[test]
fn the_pr_block_reader_agrees_with_the_golden_vectors() {
    #[derive(serde::Deserialize)]
    struct Case {
        name: String,
        description: String,
        links: Option<Value>,
        #[serde(default)]
        error: bool,
    }
    let cases: Vec<Case> = serde_json::from_str(include_str!("testdata/pr_block.json")).unwrap();
    assert!(cases.len() >= 20);
    assert!(cases.iter().any(|case| case.error));
    for case in cases {
        let read = generic::PrLinks::read(&case.description);
        if case.error {
            assert!(read.is_err(), "{}: {read:?}", case.name);
            continue;
        }
        let read = read.unwrap_or_else(|error| panic!("{}: {error}", case.name));
        assert_eq!(
            serde_json::to_value(&read).unwrap(),
            case.links.unwrap(),
            "{}",
            case.name
        );
        // Writing the links back leaves a description that reads the same and writes the same.
        let written = read.write_into(&case.description).unwrap();
        assert_eq!(
            serde_json::to_value(generic::PrLinks::read(&written).unwrap()).unwrap(),
            serde_json::to_value(&read).unwrap(),
            "{}",
            case.name
        );
        assert_eq!(read.write_into(&written).unwrap(), written, "{}", case.name);
    }
}

#[test]
fn the_pr_block_writer_inserts_replaces_and_is_idempotent() {
    let links = sample_links();
    let block = links.block();
    assert!(block.starts_with("<details>\n<summary>intxt</summary>\n\n```json\n{"));
    assert!(block.ends_with("}\n```\n\n</details>"));
    assert!(block.contains("\"$type\": \"org.example.intxt.prLinks\""));

    // Into an empty description: the block alone.
    assert_eq!(links.write_into("").unwrap(), block);

    // After existing text: that text stays as it was, set off by a blank line.
    let text = "Splits the parser.\n\n- one\n- two\n";
    let inserted = links.write_into(text).unwrap();
    assert_eq!(inserted, format!("{text}\n{block}"));
    assert_eq!(
        links.write_into("No newline at the end").unwrap(),
        format!("No newline at the end\n\n{block}")
    );

    // Over an existing block: only the block changes, text before and after is untouched.
    let before = format!("Intro.\n\n{block}\n\nFooter.\n");
    let mut newer = links.clone();
    newer.intxts.pop();
    newer.intxts[0].title = Some("renamed".into());
    let replaced = newer.write_into(&before).unwrap();
    assert_eq!(
        replaced,
        format!("Intro.\n\n{}\n\nFooter.\n", newer.block())
    );
    assert_eq!(generic::PrLinks::read(&replaced).unwrap(), newer);

    // Idempotent: writing the same links again returns the same text, with LF and CRLF alike.
    assert_eq!(links.write_into(&inserted).unwrap(), inserted);
    assert_eq!(newer.write_into(&replaced).unwrap(), replaced);
    let crlf = text.replace('\n', "\r\n");
    let once = links.write_into(&crlf).unwrap();
    assert!(!once.replace("\r\n", "").contains('\n'), "{once:?}");
    assert_eq!(links.write_into(&once).unwrap(), once);
    assert_eq!(generic::PrLinks::read(&once).unwrap(), links);

    // A malformed block is not replaced silently.
    let broken =
        "<details>\n\n```json\n{\"$type\": \"org.example.intxt.prLinks\"\n```\n\n</details>";
    assert!(links.write_into(broken).is_err());

    // Carried fields round trip: the block's extras come back on write.
    let carried = format!(
        "<details>\n<summary>intxt</summary>\n\n```json\n{}\n```\n\n</details>",
        json!({
            "$type": "org.example.intxt.prLinks",
            "tickets": ["T-1"],
            "intxts": [{"ref": "dtxt:doc:z11111111111111111111111111111111", "note": 1}]
        })
    );
    let read = generic::PrLinks::read(&carried).unwrap();
    assert_eq!(read.extra["tickets"], json!(["T-1"]));
    assert_eq!(read.intxts[0].extra["note"], json!(1));
    let rewritten = read.write_into(&carried).unwrap();
    assert_eq!(generic::PrLinks::read(&rewritten).unwrap(), read);
    assert!(rewritten.contains("\"tickets\"") && rewritten.contains("\"note\""));
}

/// Carrying is not operating: a peer's future fields and vocabulary values survive the whole
/// chain, and the typed seam reports the vocabulary value as unknown.
#[test]
fn future_fields_and_vocabulary_values_are_carried() {
    codec_round_trip::<annotation::Comment>(json!({
        "targets": [{"source": {"ref": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.note/main"}, "selector": {"kind": "svg"}}],
        "body": {"mime": "text/markdown", "content": "Looks right.", "language": "en"},
        "motivation": "approving",
        "visibility": "team"
    }));
    let comment: annotation::Comment = serde_json::from_value(json!({
        "targets": [], "body": {"mime": "text/plain", "content": ""}, "motivation": "approving"
    }))
    .unwrap();
    assert_eq!(
        comment.motivation,
        annotation::Motivation::Unknown("approving".into())
    );
    codec_round_trip::<work::WorktreeOp>(json!({
        "at": "2026-11-10T21:04:11Z",
        "kind": "stash",
        "files": {},
        "watcher": {"source": "inotify"}
    }));
    codec_round_trip::<work::PromptTurn>(json!({
        "seq": 0,
        "at": "2026-11-10T21:04:00Z",
        "stopReason": "context_overflow",
        "messages": [{"id": "m", "seq": 0, "at": "2026-11-10T21:04:00Z", "kind": "hookOutput", "text": ""}],
        "toolCalls": [{"id": "t", "seq": 1, "title": "x", "kind": "browse", "status": "queued"}],
        "usage": {"inputTokens": 12}
    }));
    codec_round_trip::<work::GitClone>(json!({
        "repo": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.gitRepo/r", "machine": "dtxt:actor:z11111111111111111111111111111111/recorders/git", "path": "p", "vcs": ["pijul"]
    }));
    let hooked: work::Rewrite =
        serde_json::from_value(json!({"successor": "4c1f0a", "how": "hook"})).unwrap();
    assert_eq!(hooked.how, work::RewriteHow::Hook);
    let rewrite: work::Rewrite =
        serde_json::from_value(json!({"successor": "4c1f0a", "how": "jjChange"})).unwrap();
    assert_eq!(rewrite.how, work::RewriteHow::Unknown("jjChange".into()));
    let pull: work::GithubPull = serde_json::from_value(json!({
        "title": "t", "body": "", "state": "draft", "head": "h", "base": "b",
        "headSha": "1", "baseSha": "2", "updatedAt": "2026-11-17T10:00:00Z", "url": "u"
    }))
    .unwrap();
    assert_eq!(pull.state, work::PullState::Unknown("draft".into()));
}

/// A note's content is collaborative text, so a recorder's line splice merges with a concurrent
/// one instead of replacing the whole string.
#[test]
fn note_content_is_stored_as_text() {
    let schema = admitted_schema(generic::Note::ID);
    let mut doc = automerge::Automerge::new();
    let transaction = doc.transaction();
    let transaction = reconcile_json_atomic(
        transaction,
        &schema,
        &automerge::ROOT,
        "datum".into(),
        json!({"mime": "text/plain", "content": "one\ntwo\n"}),
    )
    .unwrap();
    transaction.commit();
    use automerge::ReadDoc;
    let (_, note) = doc.get(automerge::ROOT, "datum").unwrap().unwrap();
    let (content, _) = doc.get(&note, "content").unwrap().unwrap();
    assert!(
        matches!(content, automerge::Value::Object(automerge::ObjType::Text)),
        "content is {content:?}"
    );
}

fn posted(
    slot: &str,
    reply_to: Option<&str>,
    authors: &[Uuid7],
    heads: &[&str],
) -> annotation::Posted {
    let target = |slot: &str| {
        format!("dtxt:datum:z11111111111111111111111111111111/org.example.intxt.comment/{slot}")
    };
    annotation::Posted {
        key: format!("org.example.intxt.comment/{slot}").parse().unwrap(),
        comment: serde_json::from_value(json!({
            "targets": [],
            "body": {"mime": "text/markdown", "content": slot},
            "motivation": if reply_to.is_some() { "replying" } else { "commenting" },
            "replyTo": reply_to.map(target),
        }))
        .unwrap(),
        heads: heads.iter().map(|head| (*head).to_owned()).collect(),
        authors: authors.to_vec(),
    }
}

#[test]
fn threads_group_replies_under_their_first_comment_in_time_order() {
    let (ada, grace) = (Uuid7::new(), Uuid7::new());
    // Comment slots are time-ordered; given in a different order, replies of replies included,
    // and one reply whose parent was removed.
    let threads = annotation::Thread::group(vec![
        posted("0193-d", Some("0193-c"), &[ada], &["h"]),
        posted("0193-c", Some("0193-a"), &[grace], &["h"]),
        posted("0193-b", None, &[grace], &["h"]),
        posted("0193-a", None, &[ada], &["h"]),
        posted("0193-e", Some("0193-gone"), &[grace], &["h"]),
    ]);
    let slots: Vec<Vec<String>> = threads
        .iter()
        .map(|thread| {
            thread
                .comments
                .iter()
                .map(|post| post.comment.body.content.clone())
                .collect()
        })
        .collect();
    assert_eq!(
        slots,
        [
            vec!["0193-a", "0193-c", "0193-d"],
            vec!["0193-b"],
            vec!["0193-e"]
        ]
    );
    assert_eq!(
        threads[0].root.to_string(),
        "org.example.intxt.comment/0193-a"
    );
    // The orphan's thread is named by the comment it answered.
    assert_eq!(
        threads[2].root.to_string(),
        "org.example.intxt.comment/0193-gone"
    );
}

#[test]
fn a_reply_loop_ends_instead_of_hanging() {
    let ada = Uuid7::new();
    let threads = annotation::Thread::group(vec![
        posted("0193-a", Some("0193-b"), &[ada], &["h"]),
        posted("0193-b", Some("0193-a"), &[ada], &["h"]),
    ]);
    assert_eq!(
        threads
            .iter()
            .map(|thread| thread.comments.len())
            .sum::<usize>(),
        2
    );
}

#[test]
fn a_thread_waits_on_whoever_did_not_write_last_and_is_unseen_until_recorded() {
    let (ada, grace) = (Uuid7::new(), Uuid7::new());
    let thread = annotation::Thread::group(vec![
        posted("0193-a", None, &[ada], &["h1"]),
        posted("0193-b", Some("0193-a"), &[grace], &["h2"]),
    ])
    .remove(0);
    assert!(thread.waiting_on(&ada));
    assert!(!thread.waiting_on(&grace));

    let mut seen = std::collections::BTreeMap::new();
    // Ada wrote the first comment, so only the reply can be new to her; Grace is the reverse.
    assert!(thread.unseen_by(&ada, &seen));
    assert!(thread.unseen_by(&grace, &seen));
    seen.insert(
        "org.example.intxt.comment/0193-b".to_owned(),
        vec!["h2".to_owned()],
    );
    assert!(!thread.unseen_by(&ada, &seen));
    seen.insert(
        "org.example.intxt.comment/0193-a".to_owned(),
        vec!["h1".to_owned()],
    );
    assert!(!thread.unseen_by(&grace, &seen));
    // An edit makes a new version, which is unseen again.
    seen.insert(
        "org.example.intxt.comment/0193-b".to_owned(),
        vec!["old".to_owned()],
    );
    assert!(thread.unseen_by(&ada, &seen));
    // A version no verified author wrote is someone else's.
    let anonymous = annotation::Thread::group(vec![posted("0193-c", None, &[], &["h"])]).remove(0);
    assert!(anonymous.waiting_on(&ada));
}
