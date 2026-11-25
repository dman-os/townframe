use super::*;
use crate::testing::Repo;
use std::process::Command;

const OLD: &str = "1111111111111111111111111111111111111111";
const NEW: &str = "2222222222222222222222222222222222222222";

fn repo() -> Repo {
    let repo = Repo::new();
    repo.write("a.txt", "one\n");
    repo.commit_all("first");
    repo
}

fn hook_path(repo: &Repo) -> PathBuf {
    repo.root.join(".git/hooks/post-rewrite")
}

#[test]
fn post_rewrite_input_is_parsed_line_by_line() {
    let pairs = parse_post_rewrite(&format!("{OLD} {NEW}\n{NEW} {OLD} extra info\n\n")).unwrap();
    assert_eq!(
        pairs
            .iter()
            .map(|pair| (pair.old.as_str(), pair.new.as_str()))
            .collect::<Vec<_>>(),
        [(OLD, NEW), (NEW, OLD)]
    );
    assert!(parse_post_rewrite("").unwrap().is_empty());
    // A commit rebased onto itself is not a rewrite.
    assert!(
        parse_post_rewrite(&format!("{OLD} {OLD}\n"))
            .unwrap()
            .is_empty()
    );
    // sha-256 repositories have 64 digit ids.
    let long = "a".repeat(64);
    assert_eq!(
        parse_post_rewrite(&format!("{long} {}\n", "b".repeat(64)))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn malformed_post_rewrite_input_is_an_error() {
    for bad in [
        OLD.to_owned(),
        format!("{OLD} not-a-sha"),
        format!("{OLD} {}", &NEW[..39]),
        format!("{OLD}\t{}z", &NEW[..39]),
    ] {
        let error = parse_post_rewrite(&bad).unwrap_err().to_string();
        assert!(error.contains("post-rewrite input"), "{bad:?}: {error}");
    }
}

#[test]
fn a_fresh_clone_gets_an_executable_hook_carrying_the_marker() {
    let repo = repo();
    let outcome = install_post_rewrite(&repo.root).unwrap();
    assert_eq!(outcome, Install::Installed(hook_path(&repo)));
    assert_eq!(outcome.state(), HookState::Installed);
    let text = std::fs::read_to_string(hook_path(&repo)).unwrap();
    assert!(text.starts_with("#!/bin/sh\n"), "{text}");
    assert!(text.lines().any(|line| line == MARKER), "{text}");
    assert!(
        text.contains("exec day x intxt capture git-rewrite \"$1\""),
        "{text}"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(hook_path(&repo))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o111, 0o111, "{mode:o}");
    }
}

#[test]
fn installing_ours_again_changes_nothing() {
    let repo = repo();
    install_post_rewrite(&repo.root).unwrap();
    let before = std::fs::metadata(hook_path(&repo))
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(
        install_post_rewrite(&repo.root).unwrap(),
        Install::AlreadyInstalled(hook_path(&repo))
    );
    assert_eq!(
        std::fs::metadata(hook_path(&repo))
            .unwrap()
            .modified()
            .unwrap(),
        before,
        "an up to date hook is not rewritten"
    );
}

#[cfg(unix)]
#[test]
fn ours_that_lost_its_executable_bit_or_its_text_is_repaired() {
    use std::os::unix::fs::PermissionsExt;
    let repo = repo();
    install_post_rewrite(&repo.root).unwrap();
    let path = hook_path(&repo);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        install_post_rewrite(&repo.root).unwrap(),
        Install::AlreadyInstalled(path.clone())
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o111,
        0o111
    );

    std::fs::write(
        &path,
        format!("#!/bin/sh\n{MARKER}\nexec day old-command\n"),
    )
    .unwrap();
    assert_eq!(
        install_post_rewrite(&repo.root).unwrap(),
        Install::Installed(path.clone())
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), script());
}

#[test]
fn a_foreign_post_rewrite_is_left_alone() {
    let repo = repo();
    let path = hook_path(&repo);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "#!/bin/sh\necho mine\n").unwrap();
    let outcome = install_post_rewrite(&repo.root).unwrap();
    assert_eq!(outcome, Install::Foreign(path.clone()));
    assert_eq!(outcome.state(), HookState::ForeignPostRewrite);
    assert_eq!(
        outcome.state().describe(),
        "hook not installed: foreign post-rewrite"
    );
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "#!/bin/sh\necho mine\n"
    );
}

#[test]
fn core_hooks_path_decides_where_the_hook_goes() {
    let repo = repo();
    repo.git(&["config", "core.hooksPath", "shared-hooks"]);
    let outcome = install_post_rewrite(&repo.root).unwrap();
    let wanted = repo.root.join("shared-hooks/post-rewrite");
    assert_eq!(outcome, Install::Installed(wanted.clone()));
    assert!(wanted.exists());
    assert!(
        !hook_path(&repo).exists(),
        "the clone's own hooks are not used"
    );
}

#[test]
fn linked_worktrees_share_the_clones_hook() {
    let repo = repo();
    let linked = repo.root.with_file_name("linked");
    repo.git(&[
        "worktree",
        "add",
        "-q",
        "-b",
        "side",
        linked.to_str().unwrap(),
    ]);
    let outcome = install_post_rewrite(&linked).unwrap();
    assert_eq!(outcome, Install::Installed(hook_path(&repo)));
    assert_eq!(
        install_post_rewrite(&repo.root).unwrap(),
        Install::AlreadyInstalled(hook_path(&repo))
    );
}

/// git runs the installed script with the rewrite kind as `$1` and the pairs on stdin: a stand-in
/// `day` on the path records what the script handed it.
#[test]
fn git_runs_the_hook_with_the_kind_and_the_pairs() {
    let repo = repo();
    install_post_rewrite(&repo.root).unwrap();
    let bin = repo.root.with_file_name("bin");
    std::fs::create_dir(&bin).unwrap();
    let seen = repo.root.with_file_name("seen");
    let day = bin.join("day");
    std::fs::write(
        &day,
        format!("#!/bin/sh\necho \"$@\" > {0}\ncat >> {0}\n", seen.display()),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&day, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let old = repo.git(&["rev-parse", "HEAD"]);
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    let output = Command::new("git")
        .arg("-C")
        .arg(&repo.root)
        .args(["commit", "-q", "--amend", "-m", "amended"])
        .env("PATH", path)
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
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let new = repo.git(&["rev-parse", "HEAD"]);
    assert_ne!(old, new);
    let seen = std::fs::read_to_string(seen).unwrap();
    let mut lines = seen.lines();
    assert_eq!(lines.next(), Some("x intxt capture git-rewrite amend"));
    let pairs = parse_post_rewrite(&lines.collect::<Vec<_>>().join("\n")).unwrap();
    assert_eq!(pairs.len(), 1);
    assert_eq!(
        (pairs[0].old.as_str(), pairs[0].new.as_str()),
        (old.as_str(), new.as_str())
    );
}
