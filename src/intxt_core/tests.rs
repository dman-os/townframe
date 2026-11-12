use crate::*;
use big_cabinet_core::datum::Bundle;
use big_cabinet_core::datum::CompiledSchema;
use big_cabinet_core::datum::DatumMember;
use big_cabinet_core::datum::Stored;
use big_cabinet_core::datum::hydrate_value;
use big_cabinet_core::datum::reconcile_json_atomic;
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
            "org.example.intxt.work.gitRef",
            "org.example.intxt.work.jjOperation",
            "org.example.intxt.work.jjChange",
            "org.example.intxt.work.agentSession",
            "org.example.intxt.work.promptTurn",
            "org.example.intxt.work.plan",
            "org.example.intxt.work.repoEntry",
            "org.example.intxt.work.worktreeEntry",
            "org.example.intxt.work.sessionEntry",
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
        ]
    }));
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
    codec_round_trip::<work::JjChangeEntry>(
        json!({"pack": "dtxt:datum:self/org.example.daybook.blob/af1349b9f5f9a1a6a0404dea36dcc949"}),
    );
    codec_round_trip::<work::GitRefEntry>(json!({
        "ref": "dtxt:datum:z11111111111111111111111111111111/org.example.intxt.work.gitRef/0193c/refs/heads/feat/refs",
        "base": "org.example.intxt.work.gitCommit/9a0e11"
    }));
    codec_round_trip::<annotation::AnnotationsEntry>(json!({"empty": true}));
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
