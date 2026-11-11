//! Test support: real git repositories in temporary directories, driven by the git CLI.

use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

/// Run git in `dir` with nothing taken from the machine: no global or system config, and a fixed
/// identity.
pub(crate) fn git_in(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
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
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

pub(crate) struct Repo {
    /// Holds the directory; the repository is `root` inside it.
    pub(crate) _dir: tempfile::TempDir,
    pub(crate) root: PathBuf,
}

impl Repo {
    pub(crate) fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap().join("repo");
        std::fs::create_dir(&root).unwrap();
        git_in(&root, &["init", "-q", "-b", "main"]);
        Self { _dir: dir, root }
    }

    pub(crate) fn git(&self, args: &[&str]) -> String {
        git_in(&self.root, args)
    }

    pub(crate) fn write(&self, path: &str, content: impl AsRef<[u8]>) {
        let file = self.root.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, content).unwrap();
    }

    pub(crate) fn commit_all(&self, message: &str) -> String {
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", message]);
        self.git(&["rev-parse", "HEAD"])
    }
}
