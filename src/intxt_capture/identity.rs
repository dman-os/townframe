//! Which worktree a path belongs to, and the identities capture gives it (ADR 039 §5, ADR 040 §2).
//!
//! git has no identity for a repository, a clone or a worktree, so capture mints UUIDv7s at first
//! sight and keeps them in the git directories, where they survive moves and deletions of the
//! checkout and are never changed:
//!
//! - the clone: `<common dir>/intxt-id`;
//! - a linked worktree: `<git dir>/intxt-id`, in `.git/worktrees/<name>/`;
//! - the main worktree, whose git dir is the common dir: `<common dir>/intxt-worktree-id`.

use crate::Error;
use crate::git::Git;
use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

/// A worktree and the identities of what it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// The checkout directory.
    pub root: PathBuf,
    /// This worktree's git directory: the common dir for the main worktree.
    pub git_dir: PathBuf,
    /// The clone's `.git` directory, shared by all its worktrees.
    pub common_dir: PathBuf,
    /// `gitClone/<uuid>`.
    pub clone_id: String,
    /// `worktree/<uuid>`.
    pub worktree_id: String,
    /// The clone's remote URLs, sorted and deduplicated: fetch hints for `gitRepo`.
    pub remotes: Vec<String>,
    /// The repository's root commits (there can be several), which let recorders on other
    /// machines match clones of one project. Empty before the first commit.
    pub roots: Vec<String>,
}

/// The UUID text kept in `path`, creating it when the file is missing. Creation is atomic (a complete
/// temporary file is hard-linked into place), so two processes racing at first sight agree on
/// the winner's id.
fn stored_id(path: &Path) -> Result<String, Error> {
    let read = |path: &Path| -> Result<String, Error> {
        let text = std::fs::read_to_string(path)?;
        let id = text.trim();
        Uuid::parse_str(id)
            .map_err(|_| Error::Format(format!("{}: not a UUID: {text:?}", path.display())))?;
        Ok(id.to_owned())
    };
    if path.exists() {
        return read(path);
    }
    let id = Uuid::now_v7();
    let temporary = path.with_extension(format!("{id}.tmp"));
    std::fs::File::create_new(&temporary)?.write_all(format!("{id}\n").as_bytes())?;
    let linked = std::fs::hard_link(&temporary, path);
    std::fs::remove_file(&temporary)?;
    match linked {
        Ok(()) => Ok(id.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => read(path),
        Err(error) => Err(error.into()),
    }
}

/// The worktree `path` belongs to: the deepest worktree root containing it (a nested repository
/// or a submodule wins over the one around it). `None` when the path is in no checkout. The path
/// need not exist; its nearest existing ancestor decides.
pub fn resolve(path: &Path) -> Result<Option<Identity>, Error> {
    let Some(dir) = path.ancestors().find(|ancestor| ancestor.is_dir()) else {
        return Ok(None);
    };
    let git = Git::new(dir);
    let output = git
        .command(&[
            "rev-parse",
            "--path-format=absolute",
            "--show-toplevel",
            "--git-dir",
            "--git-common-dir",
        ])
        .output()?;
    if !output.status.success() {
        // Not in a repository, or inside a git directory or a bare repository, which have no
        // worktree.
        return Ok(None);
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let [root, git_dir, common_dir] = text.lines().collect::<Vec<_>>()[..] else {
        return Err(Error::Format(format!("git rev-parse printed {text:?}")));
    };
    let (root, git_dir, common_dir) = (
        PathBuf::from(root),
        PathBuf::from(git_dir),
        PathBuf::from(common_dir),
    );

    let clone_id = stored_id(&common_dir.join("intxt-id"))?;
    let worktree_id = if git_dir == common_dir {
        stored_id(&common_dir.join("intxt-worktree-id"))?
    } else {
        stored_id(&git_dir.join("intxt-id"))?
    };

    let git = Git::new(&root);
    let remotes: BTreeSet<String> = git
        .ask(&["config", "--get-regexp", r"^remote\..*\.url$"])?
        .iter()
        .flat_map(|lines| lines.lines())
        .filter_map(|line| line.split_once(' '))
        .map(|(_, url)| url.to_owned())
        .collect();
    let roots = match git.ask(&["rev-parse", "-q", "--verify", "HEAD"])? {
        Some(_) => git
            .text(&["rev-list", "--max-parents=0", "HEAD"])?
            .lines()
            .map(str::to_owned)
            .collect(),
        None => Vec::new(),
    };
    Ok(Some(Identity {
        root,
        git_dir,
        common_dir,
        clone_id,
        worktree_id,
        remotes: remotes.into_iter().collect(),
        roots,
    }))
}
