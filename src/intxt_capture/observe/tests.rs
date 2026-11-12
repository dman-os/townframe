//! Real git repositories in temporary directories, driven by the git CLI.

use super::*;
use crate::git::GitMove;
use crate::identity;
use crate::testing::Repo;
use crate::testing::git_in;
use intxt_core::capture::ChangeKind;
use intxt_core::work::FileStatus;
use intxt_core::work::OpKind;

trait Observe {
    /// Observe, carrying `state` forward.
    fn observe(&self, state: &mut WorktreeState) -> Observation;
}

impl Observe for Repo {
    fn observe(&self, state: &mut WorktreeState) -> Observation {
        let identity = identity::resolve(&self.root).unwrap().unwrap();
        let (observation, next) = observe(state, identity, None, &[]).unwrap();
        *state = next;
        observation
    }
}

/// What `git cat-file` says the blob `id` holds in the clone at `root`.
fn git_cat_file(root: &std::path::Path, id: &str) -> Vec<u8> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["cat-file", "blob", id])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{id}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn kinds(moves: &[GitMove]) -> Vec<OpKind> {
    moves.iter().map(|mv| mv.kind.clone()).collect()
}

#[test]
fn identities_are_minted_once_and_a_linked_worktree_has_its_own() {
    let repo = Repo::new();
    repo.write("a.txt", "a\n");
    let first = repo.commit_all("first");
    repo.git(&["remote", "add", "origin", "git@example.com:org/b.git"]);
    repo.git(&["remote", "add", "mirror", "git@example.com:org/a.git"]);

    let main = identity::resolve(&repo.root).unwrap().unwrap();
    assert_eq!(main.root, repo.root);
    assert_eq!(main.git_dir, main.common_dir);
    assert_eq!(main.git_dir, repo.root.join(".git"));
    assert_ne!(main.clone_id, main.worktree_id);
    assert_eq!(main.roots, [first]);
    assert_eq!(
        main.remotes,
        ["git@example.com:org/a.git", "git@example.com:org/b.git"]
    );
    assert!(repo.root.join(".git/intxt-id").is_file());
    assert!(repo.root.join(".git/intxt-worktree-id").is_file());

    // A path need not exist, and a stable id does not change on the next look.
    let deep = identity::resolve(&repo.root.join("no/such/file.rs"))
        .unwrap()
        .unwrap();
    assert_eq!(deep, main);

    let linked_root = repo.root.parent().unwrap().join("linked");
    repo.git(&[
        "worktree",
        "add",
        "-q",
        "-b",
        "other",
        linked_root.to_str().unwrap(),
    ]);
    let linked = identity::resolve(&linked_root.join("a.txt"))
        .unwrap()
        .unwrap();
    assert_eq!(linked.root, linked_root);
    assert_eq!(linked.common_dir, main.common_dir);
    assert_ne!(linked.git_dir, linked.common_dir);
    assert_eq!(linked.clone_id, main.clone_id);
    assert_ne!(linked.worktree_id, main.worktree_id);
    assert!(linked.git_dir.join("intxt-id").is_file());
    assert_eq!(identity::resolve(&linked_root).unwrap().unwrap(), linked);
    assert_eq!(
        identity::resolve(&repo.root).unwrap().unwrap().worktree_id,
        main.worktree_id
    );
}

#[test]
fn the_deepest_worktree_wins_and_no_worktree_is_none() {
    let repo = Repo::new();
    repo.write("a.txt", "a\n");
    repo.commit_all("first");
    let inner = repo.root.join("vendor/inner");
    std::fs::create_dir_all(&inner).unwrap();
    git_in(&inner, &["init", "-q", "-b", "main"]);

    let found = identity::resolve(&inner.join("x.rs")).unwrap().unwrap();
    assert_eq!(found.root, inner);
    assert_ne!(
        found.clone_id,
        identity::resolve(&repo.root).unwrap().unwrap().clone_id
    );
    assert!(
        found.roots.is_empty(),
        "an unborn branch has no root commit"
    );

    let elsewhere = tempfile::tempdir().unwrap();
    assert_eq!(identity::resolve(elsewhere.path()).unwrap(), None);
}

#[test]
fn snapshots_diff_into_changes_carrying_blob_ids() {
    let repo = Repo::new();
    for (path, content) in [
        ("a.txt", "one\n"),
        ("old.txt", "rename me\n"),
        ("gone.txt", "bye\n"),
    ] {
        repo.write(path, content);
    }
    repo.commit_all("first");
    let mut state = WorktreeState::default();
    let baseline = repo.observe(&mut state);
    assert!(baseline.changes.is_empty() && baseline.git.moves.is_empty());

    repo.write("a.txt", "one\nedited\n");
    repo.write("new.txt", "new\n");
    std::fs::remove_file(repo.root.join("gone.txt")).unwrap();
    std::fs::rename(repo.root.join("old.txt"), repo.root.join("renamed.txt")).unwrap();
    // Binary and large files are blobs like any other: no classification.
    let binary = [0u8, 159, 146, 150];
    repo.write("bin.dat", binary);
    let large = "a".repeat(300 * 1024);
    repo.write("big.txt", &large);

    let observed = repo.observe(&mut state);
    let change = |path: &str| {
        observed
            .changes
            .iter()
            .find(|change| change.path == path)
            .unwrap_or_else(|| panic!("no change for {path}: {:?}", observed.changes))
    };
    // The blob id resolves to the content the worktree had, in the clone's object store.
    let content = |path: &str| {
        let blob = change(path)
            .blob
            .as_deref()
            .unwrap_or_else(|| panic!("{path}"));
        git_cat_file(&repo.root, blob)
    };

    assert_eq!(observed.changes.len(), 7, "{:?}", observed.changes);
    assert_eq!(change("a.txt").kind, ChangeKind::Modified);
    assert_eq!(content("a.txt"), b"one\nedited\n");
    assert_eq!(change("new.txt").kind, ChangeKind::Added);
    assert_eq!(content("new.txt"), b"new\n");
    assert_eq!(change("gone.txt").kind, ChangeKind::Deleted);
    assert_eq!(change("gone.txt").blob, None);
    // A rename is a deletion and an addition: readers pair them with `git diff -M`.
    assert_eq!(change("old.txt").kind, ChangeKind::Deleted);
    assert_eq!(change("renamed.txt").kind, ChangeKind::Added);
    assert_eq!(content("renamed.txt"), b"rename me\n");
    assert_eq!(content("bin.dat"), binary);
    assert_eq!(content("big.txt"), large.as_bytes());

    // Nothing changed since, and committing what was observed is not a change either.
    assert!(repo.observe(&mut state).changes.is_empty());
    repo.write("a.txt", "one\nedited\n");
    let sha = repo.commit_all("everything");
    let after_commit = repo.observe(&mut state);
    assert!(after_commit.changes.is_empty());
    assert_eq!(kinds(&after_commit.git.moves), [OpKind::Commit]);
    let commit = &after_commit.git.commits[&sha];
    let status = |path: &str| {
        commit
            .files
            .iter()
            .find(|file| file.path == path)
            .unwrap_or_else(|| panic!("{path}"))
    };
    assert_eq!(status("renamed.txt").status, FileStatus::Renamed);
    assert_eq!(status("renamed.txt").old_path.as_deref(), Some("old.txt"));
    assert_eq!(status("gone.txt").status, FileStatus::Deleted);
    assert_eq!(status("new.txt").status, FileStatus::Added);
    assert_eq!(status("a.txt").status, FileStatus::Modified);
}

#[test]
fn the_first_observation_records_what_already_differs_from_head_staged_or_not() {
    let repo = Repo::new();
    repo.write("a.txt", "one\n");
    repo.write("b.txt", "bee\n");
    repo.write("c.txt", "sea\n");
    repo.commit_all("first");
    // Staged and unchanged since, staged and edited again, and untouched: only the last has its
    // content in `HEAD`'s tree alone.
    repo.write("a.txt", "staged\n");
    repo.write("b.txt", "staged\n");
    repo.git(&["add", "a.txt", "b.txt"]);
    repo.write("b.txt", "staged then edited\n");

    let mut state = WorktreeState::default();
    let observed = repo.observe(&mut state);
    let paths: Vec<_> = observed
        .changes
        .iter()
        .map(|change| change.path.as_str())
        .collect();
    assert_eq!(paths, ["a.txt", "b.txt"]);
    let blobs: Vec<_> = observed
        .changes
        .iter()
        .map(|change| git_cat_file(&repo.root, change.blob.as_deref().unwrap()))
        .collect();
    assert_eq!(
        blobs,
        [b"staged\n".to_vec(), b"staged then edited\n".to_vec()]
    );
}

#[test]
fn a_staged_edit_is_the_same_change_as_an_unstaged_one() {
    let repo = Repo::new();
    repo.write("a.txt", "one\n");
    repo.commit_all("first");
    let mut state = WorktreeState::default();
    repo.observe(&mut state);
    repo.write("a.txt", "two\n");
    repo.write("b.txt", "b\n");
    let observed = repo.observe(&mut state);
    assert_eq!(observed.changes.len(), 2);
    repo.git(&["add", "-A"]);
    assert!(repo.observe(&mut state).changes.is_empty());
}

#[test]
fn head_moves_are_classified_from_the_reflog() {
    let repo = Repo::new();
    repo.write("a.txt", "one\n");
    let first = repo.commit_all("first");
    let mut state = WorktreeState::default();
    let baseline = repo.observe(&mut state);
    assert_eq!(baseline.git.head.target.as_deref(), Some(first.as_str()));
    assert_eq!(
        baseline.git.head.reference.as_deref(),
        Some("refs/heads/main")
    );
    assert!(baseline.git.refs.contains_key("refs/heads/main"));
    assert!(baseline.git.commits.contains_key(&first));
    assert!(repo.observe(&mut state).git.moves.is_empty());

    // A commit.
    repo.write("a.txt", "two\n");
    let second = repo.commit_all("second");
    let observed = repo.observe(&mut state);
    assert_eq!(kinds(&observed.git.moves), [OpKind::Commit]);
    let mv = &observed.git.moves[0];
    assert_eq!(mv.observation.from.as_deref(), Some(first.as_str()));
    assert_eq!(mv.observation.to.as_deref(), Some(second.as_str()));
    assert_eq!(mv.observation.head.as_deref(), Some(second.as_str()));
    assert_eq!(mv.observation.reference.as_deref(), Some("refs/heads/main"));
    assert_eq!(mv.observation.reflog.as_deref(), Some("commit: second"));
    let datum = &observed.git.commits[&second];
    assert_eq!(datum.parents, std::slice::from_ref(&first));
    assert_eq!(datum.author.name, "Ada");
    assert_eq!(datum.committer.email, "grace@example.com");
    assert_eq!(datum.message, "second");
    assert_eq!(datum.files.len(), 1);
    assert_eq!(observed.git.refs["refs/heads/main"].target, second);

    // An amend replaces the commit.
    repo.git(&["commit", "-q", "--amend", "-m", "second, amended"]);
    let amended = repo.git(&["rev-parse", "HEAD"]);
    let observed = repo.observe(&mut state);
    assert_eq!(kinds(&observed.git.moves), [OpKind::Amend]);
    assert_eq!(
        observed.git.moves[0].observation.from.as_deref(),
        Some(second.as_str())
    );
    assert_eq!(observed.git.commits[&amended].message, "second, amended");

    // A checkout of another branch, a commit on it, and back, in one observation.
    repo.git(&["checkout", "-q", "-b", "feat"]);
    repo.write("f.txt", "f\n");
    let on_feat = repo.commit_all("on feat");
    repo.git(&["checkout", "-q", "main"]);
    let observed = repo.observe(&mut state);
    assert_eq!(
        kinds(&observed.git.moves),
        [OpKind::Checkout, OpKind::Commit, OpKind::Checkout]
    );
    let refs: Vec<Option<&str>> = observed
        .git
        .moves
        .iter()
        .map(|mv| mv.observation.reference.as_deref())
        .collect();
    assert_eq!(
        refs,
        [
            Some("refs/heads/feat"),
            Some("refs/heads/feat"),
            Some("refs/heads/main")
        ]
    );
    assert!(observed.git.commits.contains_key(&on_feat));
    assert_eq!(
        observed.git.head.reference.as_deref(),
        Some("refs/heads/main")
    );
    assert_eq!(observed.git.refs["refs/heads/feat"].target, on_feat);
    // The checkout changed the files, and the snapshot diff sees it.
    assert!(
        observed.changes.is_empty(),
        "main's files are as they were: {:?}",
        observed.changes
    );

    // A reset.
    repo.write("a.txt", "three\n");
    repo.commit_all("third");
    repo.git(&["reset", "-q", "--hard", "HEAD~1"]);
    let observed = repo.observe(&mut state);
    assert_eq!(kinds(&observed.git.moves), [OpKind::Commit, OpKind::Reset]);

    // A detached HEAD has no ref.
    repo.git(&["checkout", "-q", "--detach"]);
    let observed = repo.observe(&mut state);
    assert_eq!(kinds(&observed.git.moves), [OpKind::Checkout]);
    assert_eq!(observed.git.moves[0].observation.reference, None);
    assert_eq!(observed.git.head.reference, None);
    repo.git(&["checkout", "-q", "main"]);
    repo.observe(&mut state);

    // A ref moved while HEAD was elsewhere is a ref move of its own.
    repo.git(&["branch", "-q", "-f", "feat", "main"]);
    let observed = repo.observe(&mut state);
    assert_eq!(kinds(&observed.git.moves), [OpKind::RefMove]);
    assert_eq!(
        observed.git.moves[0].observation.reference.as_deref(),
        Some("refs/heads/feat")
    );
    assert_eq!(
        observed.git.moves[0].observation.from.as_deref(),
        Some(on_feat.as_str())
    );
}

#[test]
fn a_run_of_rebase_steps_is_one_move() {
    let repo = Repo::new();
    repo.write("a.txt", "one\n");
    repo.commit_all("first");
    repo.git(&["checkout", "-q", "-b", "topic"]);
    repo.write("t.txt", "t\n");
    let before = repo.commit_all("topic work");
    repo.git(&["checkout", "-q", "main"]);
    repo.write("m.txt", "m\n");
    repo.commit_all("main work");
    repo.git(&["checkout", "-q", "topic"]);
    let mut state = WorktreeState::default();
    repo.observe(&mut state);

    repo.git(&["rebase", "-q", "main"]);
    let rebased = repo.git(&["rev-parse", "HEAD"]);
    let observed = repo.observe(&mut state);
    assert_eq!(kinds(&observed.git.moves), [OpKind::Rebase]);
    let mv = &observed.git.moves[0];
    assert_eq!(mv.observation.from.as_deref(), Some(before.as_str()));
    assert_eq!(mv.observation.to.as_deref(), Some(rebased.as_str()));
    assert_eq!(
        mv.observation.reference.as_deref(),
        Some("refs/heads/topic")
    );
    assert!(observed.git.commits.contains_key(&rebased));
    assert_eq!(
        observed.git.head.reference.as_deref(),
        Some("refs/heads/topic")
    );
}

#[test]
fn an_expired_reflog_is_reported_not_replayed() {
    let repo = Repo::new();
    repo.write("a.txt", "one\n");
    let first = repo.commit_all("first");
    let mut state = WorktreeState::default();
    repo.observe(&mut state);
    repo.write("a.txt", "two\n");
    let second = repo.commit_all("second");
    repo.git(&["reflog", "expire", "--expire=now", "--all"]);
    let observed = repo.observe(&mut state);
    assert!(observed.git.reflog_gap);
    // One synthetic move over the lost entries: from where HEAD was last seen to where it is.
    assert_eq!(kinds(&observed.git.moves), [OpKind::RefMove]);
    let mv = &observed.git.moves[0];
    assert_eq!(mv.observation.from.as_deref(), Some(first.as_str()));
    assert_eq!(mv.observation.to.as_deref(), Some(second.as_str()));
    assert_eq!(mv.observation.reflog, None);
    assert!(observed.git.commits.contains_key(&second));
    // The next look resumes from an empty reflog without a gap.
    repo.write("a.txt", "three\n");
    repo.commit_all("third");
    let observed = repo.observe(&mut state);
    assert!(!observed.git.reflog_gap);
    assert_eq!(kinds(&observed.git.moves), [OpKind::Commit]);
}

#[test]
fn state_survives_serialization_and_a_second_worktree_has_its_own() {
    let repo = Repo::new();
    repo.write("a.txt", "one\n");
    repo.commit_all("first");
    let mut state = WorktreeState::default();
    repo.observe(&mut state);
    let mut state: WorktreeState =
        serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
    repo.write("a.txt", "two\n");
    repo.commit_all("second");
    let observed = repo.observe(&mut state);
    assert_eq!(kinds(&observed.git.moves), [OpKind::Commit]);

    let linked_root = repo.root.parent().unwrap().join("linked");
    repo.git(&[
        "worktree",
        "add",
        "-q",
        "-b",
        "other",
        linked_root.to_str().unwrap(),
    ]);
    let mut linked_state = WorktreeState::default();
    let (linked, next) = observe(
        &linked_state,
        identity::resolve(&linked_root).unwrap().unwrap(),
        None,
        &[],
    )
    .unwrap();
    linked_state = next;
    assert_ne!(linked.identity.worktree_id, observed.identity.worktree_id);
    assert_eq!(linked.identity.clone_id, observed.identity.clone_id);
    assert_eq!(
        linked.git.head.reference.as_deref(),
        Some("refs/heads/other")
    );
    // Its HEAD reflog is its own: main's commits are not its moves.
    std::fs::write(linked_root.join("l.txt"), "l\n").unwrap();
    git_in(&linked_root, &["add", "-A"]);
    git_in(&linked_root, &["commit", "-q", "-m", "linked"]);
    let (linked, _) = observe(
        &linked_state,
        identity::resolve(&linked_root).unwrap().unwrap(),
        None,
        &[],
    )
    .unwrap();
    assert_eq!(kinds(&linked.git.moves), [OpKind::Commit]);
    assert_eq!(repo.observe(&mut state).git.moves, []);
}

#[test]
fn first_sight_reports_what_already_differs_from_the_index() {
    let repo = Repo::new();
    repo.write("a.txt", "one\n");
    repo.write("b.txt", "b\n");
    repo.commit_all("first");
    repo.write("a.txt", "edited\n");
    repo.write("untracked.txt", "u\n");
    std::fs::remove_file(repo.root.join("b.txt")).unwrap();
    let mut state = WorktreeState::default();
    let first = repo.observe(&mut state);
    let found: Vec<(&str, &ChangeKind)> = first
        .changes
        .iter()
        .map(|change| (change.path.as_str(), &change.kind))
        .collect();
    assert_eq!(
        found,
        [
            ("a.txt", &ChangeKind::Modified),
            ("b.txt", &ChangeKind::Deleted),
            ("untracked.txt", &ChangeKind::Added),
        ]
    );
    assert!(first.git.moves.is_empty());
    assert!(repo.observe(&mut state).changes.is_empty());
}

#[test]
fn merges_cherry_picks_reverts_and_fast_forwards_are_told_apart() {
    let repo = Repo::new();
    repo.write("a.txt", "one\n");
    repo.commit_all("first");
    repo.git(&["checkout", "-q", "-b", "side"]);
    repo.write("s.txt", "s\n");
    repo.commit_all("side work");
    repo.git(&["checkout", "-q", "main"]);
    let mut state = WorktreeState::default();
    repo.observe(&mut state);

    repo.git(&["merge", "-q", "side"]);
    assert_eq!(
        kinds(&repo.observe(&mut state).git.moves),
        [OpKind::RefMove],
        "fast-forward"
    );

    repo.git(&["checkout", "-q", "-b", "other", "HEAD~1"]);
    repo.write("o.txt", "o\n");
    let other_work = repo.commit_all("other work");
    repo.git(&["merge", "-q", "--no-ff", "-m", "merge side", "side"]);
    let kinds_seen = kinds(&repo.observe(&mut state).git.moves);
    assert_eq!(
        kinds_seen,
        [OpKind::Checkout, OpKind::Commit, OpKind::Merge]
    );

    repo.git(&["checkout", "-q", "main"]);
    repo.write("c.txt", "c\n");
    repo.commit_all("main work");
    repo.git(&["cherry-pick", &other_work]);
    repo.git(&["revert", "--no-edit", "HEAD"]);
    let kinds_seen = kinds(&repo.observe(&mut state).git.moves);
    assert_eq!(
        kinds_seen,
        [
            OpKind::Checkout,
            OpKind::Commit,
            OpKind::CherryPick,
            OpKind::Revert
        ]
    );
}

/// Imported commits (history the reflog never showed, FDR 008 W6) are reported as commit datums
/// and nothing else: no move, and the state carried forward is the one an observation without
/// the import carries, so the next hook neither repeats nor misses anything.
#[test]
fn imported_commits_are_reported_as_commits_and_leave_the_state_alone() {
    let repo = Repo::new();
    repo.write("a.txt", "one\n");
    let first = repo.commit_all("first");
    repo.write("b.txt", "two\n");
    let second = repo.commit_all("second");
    let state = WorktreeState::default();
    let identity = || identity::resolve(&repo.root).unwrap().unwrap();

    let (plain, plain_next) = observe(&state, identity(), None, &[]).unwrap();
    assert_eq!(
        plain.git.commits.keys().collect::<Vec<_>>(),
        [&second],
        "a baseline reports only the commit HEAD is at"
    );

    let (imported, imported_next) =
        observe(&state, identity(), None, &[first.clone(), second.clone()]).unwrap();
    let mut wanted = [first, second];
    wanted.sort();
    assert_eq!(
        imported.git.commits.keys().cloned().collect::<Vec<_>>(),
        wanted
    );
    assert!(imported.git.moves.is_empty());
    assert_eq!(imported_next, plain_next);
}
