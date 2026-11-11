//! A worktree's git state: `HEAD`, the refs, the reflog since the last observation, and the
//! commits those moves made (ADR 039 §3, ADR 040 §3 and §5).
//!
//! `HEAD`'s reflog is the account of what moved it: each entry's message says whether it was a
//! commit, an amend, a checkout, a reset or a rebase, and that spelling becomes the op's
//! [`OpKind`]. The reflog lives in the worktree's own git directory, so it is the worktree's
//! history, not the clone's. Refs that moved without moving `HEAD` (`git branch -f`, a fetch)
//! are found by comparing the refs with the stored ones.

use crate::Error;
use big_cabinet_core::datum::Extra;
use intxt_core::work::CommitFile;
use intxt_core::work::FileStatus;
use intxt_core::work::GitCommit;
use intxt_core::work::GitIdent;
use intxt_core::work::GitObservation;
use intxt_core::work::GitRef;
use intxt_core::work::OpKind;
use jiff::Timestamp;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::btree_map::Entry;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

/// A `git` runner in one directory. Optional locks are off, so observing never makes git rewrite
/// the index under a running agent.
pub(crate) struct Git {
    dir: PathBuf,
}

impl Git {
    pub(crate) fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_owned(),
        }
    }

    pub(crate) fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new("git");
        command
            .arg("-C")
            .arg(&self.dir)
            .env("GIT_OPTIONAL_LOCKS", "0")
            .args(args);
        command
    }

    /// The raw output; a non-zero exit is an error.
    pub(crate) fn run(&self, args: &[&str]) -> Result<Vec<u8>, Error> {
        let output = self.command(args).output()?;
        if output.status.success() {
            Ok(output.stdout)
        } else {
            Err(Error::Git {
                args: args.join(" "),
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            })
        }
    }

    pub(crate) fn text(&self, args: &[&str]) -> Result<String, Error> {
        Ok(String::from_utf8_lossy(&self.run(args)?).trim().to_owned())
    }

    /// The trimmed output, or `None` when git exits non-zero or prints nothing: for questions
    /// whose answer can be "no", such as the commit of an unborn `HEAD`.
    pub(crate) fn ask(&self, args: &[&str]) -> Result<Option<String>, Error> {
        let output = self.command(args).output()?;
        let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        Ok((output.status.success() && !text.is_empty()).then_some(text))
    }
}

/// Where `HEAD` points: the branch it is on (none when detached) and the commit (none when the
/// branch is unborn).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head {
    pub reference: Option<String>,
    pub target: Option<String>,
}

/// One line of a reflog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ReflogEntry {
    old: String,
    new: String,
    at: i64,
    message: String,
}

/// What a worktree's git observation remembers between hooks.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GitState {
    head: Option<Head>,
    /// The newest reflog entry already reported and its index, which is where the next
    /// observation resumes; none while the reflog was empty. Whether a baseline was taken at all
    /// is `head`. The entry itself, not only the index, is kept so that an expired or
    /// rewritten reflog is noticed.
    reflog: Option<(usize, ReflogEntry)>,
    refs: BTreeMap<String, String>,
}

/// One move of `HEAD` (or of a ref `HEAD` is not on), as an op records it.
#[derive(Debug, Clone, PartialEq)]
pub struct GitMove {
    pub at: Timestamp,
    pub kind: OpKind,
    pub observation: GitObservation,
}

/// What a worktree's git state did since the last observation.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GitObserved {
    pub head: Head,
    /// In order, oldest first. A run of rebase steps is one move.
    pub moves: Vec<GitMove>,
    /// sha → commit, for the commits the moves made and the one `HEAD` is at.
    pub commits: BTreeMap<String, GitCommit>,
    /// Branch name (`refs/heads/…`) → ref, for the branches that are new or moved.
    pub refs: BTreeMap<String, GitRef>,
    /// The stored reflog position was not in the reflog any more (it expired or was rewritten).
    /// The moves between are lost; `moves` ends with one synthetic `refMove` from the last
    /// observed `HEAD` to the current one, without a reflog message.
    pub reflog_gap: bool,
}

fn parse_reflog(path: &Path) -> Result<Vec<ReflogEntry>, Error> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    String::from_utf8_lossy(&bytes)
        .lines()
        .map(|line| {
            let format = || Error::Format(format!("reflog line {line:?}"));
            let (meta, message) = line.split_once('\t').unwrap_or((line, ""));
            let mut words = meta.split(' ');
            let (old, new) = (
                words.next().ok_or_else(format)?,
                words.next().ok_or_else(format)?,
            );
            let (_, stamp) = meta.rsplit_once("> ").ok_or_else(format)?;
            let at = stamp
                .split_whitespace()
                .next()
                .and_then(|seconds| seconds.parse().ok())
                .ok_or_else(format)?;
            Ok(ReflogEntry {
                old: old.to_owned(),
                new: new.to_owned(),
                at,
                message: message.to_owned(),
            })
        })
        .collect()
}

/// ADR 040's op kind from git's own account of a `HEAD` move.
fn classify(message: &str) -> OpKind {
    // What a move did, not which command ran: a fast-forward is a plain ref move whether it came
    // from `merge` or `pull`.
    let fast_forward = message.ends_with(": Fast-forward");
    if message.starts_with("commit (amend)") {
        OpKind::Amend
    } else if message.starts_with("commit") {
        OpKind::Commit
    } else if message.starts_with("checkout") {
        OpKind::Checkout
    } else if message.starts_with("reset") {
        OpKind::Reset
    } else if message.starts_with("rebase") || message.starts_with("pull --rebase") {
        OpKind::Rebase
    } else if message.starts_with("cherry-pick") {
        OpKind::CherryPick
    } else if message.starts_with("revert") {
        OpKind::Revert
    } else if (message.starts_with("merge") || message.starts_with("pull")) && !fast_forward {
        OpKind::Merge
    } else {
        OpKind::RefMove
    }
}

fn sha(text: &str) -> Option<String> {
    (!text.chars().all(|c| c == '0')).then(|| text.to_owned())
}

/// The commit `sha` as a datum: author and committer, message, and the paths it changed against
/// its first parent, with renames detected.
fn commit(git: &Git, sha: &str) -> Result<GitCommit, Error> {
    let raw = git.run(&[
        "show",
        "-s",
        "--format=%P%x00%an%x00%ae%x00%at%x00%cn%x00%ce%x00%ct%x00%B",
        sha,
    ])?;
    let raw = String::from_utf8_lossy(&raw);
    let fields: Vec<&str> = raw.splitn(8, '\0').collect();
    let [parents, an, ae, at, cn, ce, ct, message] = fields[..] else {
        return Err(Error::Format(format!("git show {sha}: {raw:?}")));
    };
    let parents: Vec<String> = parents.split_whitespace().map(str::to_owned).collect();
    let ident = |name: &str, email: &str, seconds: &str| -> Result<GitIdent, Error> {
        let at = seconds
            .parse()
            .ok()
            .and_then(|seconds| Timestamp::from_second(seconds).ok())
            .ok_or_else(|| Error::Format(format!("commit time {seconds:?}")))?;
        Ok(GitIdent {
            name: name.to_owned(),
            email: email.to_owned(),
            at,
            extra: Extra::default(),
        })
    };
    let mut args = vec!["diff-tree", "-r", "-M", "--raw", "-z", "--no-commit-id"];
    match parents.first() {
        Some(parent) => args.push(parent),
        None => args.push("--root"),
    }
    args.push(sha);
    let tree = git.run(&args)?;
    let tree = String::from_utf8_lossy(&tree);
    let mut tokens = tree.split('\0').filter(|token| !token.is_empty());
    let mut files = Vec::new();
    while let Some(meta) = tokens.next() {
        let status = meta
            .rsplit(' ')
            .next()
            .ok_or_else(|| Error::Format(format!("diff-tree entry {meta:?}")))?;
        let mut next = || {
            tokens
                .next()
                .map(str::to_owned)
                .ok_or_else(|| Error::Format(format!("diff-tree entry {meta:?} lacks a path")))
        };
        let (status, old_path, path) = match &status[..1] {
            "A" => (FileStatus::Added, None, next()?),
            "D" => (FileStatus::Deleted, None, next()?),
            "R" => (FileStatus::Renamed, Some(next()?), next()?),
            "C" => (FileStatus::Copied, Some(next()?), next()?),
            _ => (FileStatus::Modified, None, next()?),
        };
        files.push(CommitFile {
            path,
            status,
            old_path,
            extra: Extra::default(),
        });
    }
    Ok(GitCommit {
        parents,
        change_id: None,
        author: ident(an, ae, at)?,
        committer: ident(cn, ce, ct)?,
        message: message.trim_end().to_owned(),
        files,
        extra: Extra::default(),
    })
}

/// The moves `fresh` reflog entries make. The branch `HEAD` is on changes along the way: a
/// checkout names the branch it moves to, and a finished rebase returns to the one it started on.
/// `reference` is the branch before the first entry.
fn head_moves(
    fresh: &[ReflogEntry],
    mut reference: Option<String>,
    refs: &BTreeMap<String, String>,
) -> Result<Vec<GitMove>, Error> {
    let mut moves: Vec<GitMove> = Vec::new();
    let mut run_start = 0;
    while run_start < fresh.len() {
        let kind = classify(&fresh[run_start].message);
        let mut run_end = run_start + 1;
        if kind == OpKind::Rebase {
            while run_end < fresh.len() && classify(&fresh[run_end].message) == OpKind::Rebase {
                run_end += 1;
            }
        }
        let (first, last) = (&fresh[run_start], &fresh[run_end - 1]);
        let moved_ref = reference.clone();
        if kind == OpKind::Checkout {
            reference = last
                .message
                .rsplit_once(" to ")
                .map(|(_, to)| format!("refs/heads/{to}"))
                .filter(|name| refs.contains_key(name));
        } else if kind == OpKind::Rebase
            && let Some((_, returned)) = last.message.split_once("returning to ")
        {
            reference = Some(returned.to_owned());
        }
        moves.push(GitMove {
            at: Timestamp::from_second(last.at)
                .map_err(|_| Error::Format(format!("reflog time {}", last.at)))?,
            observation: GitObservation {
                head: Some(last.new.clone()),
                reference: if kind == OpKind::Checkout {
                    reference.clone()
                } else {
                    moved_ref
                },
                from: sha(&first.old),
                to: sha(&last.new),
                reflog: Some(last.message.clone()),
                extra: Extra::default(),
            },
            kind,
        });
        run_start = run_end;
    }

    Ok(moves)
}

/// Branches that moved while this worktree's `HEAD` was not on them.
fn ref_moves(
    git: &Git,
    common_dir: &Path,
    state: &GitState,
    refs: &BTreeMap<String, String>,
    head: &Head,
    moves: &[GitMove],
) -> Result<Vec<GitMove>, Error> {
    let on_head: BTreeSet<&String> = moves
        .iter()
        .filter_map(|mv| mv.observation.reference.as_ref())
        .collect();
    let mut moved_elsewhere = Vec::new();
    if state.head.is_some() {
        // A branch checked out in another worktree of the clone is moved by that worktree's
        // `HEAD`, which its own observation reports.
        let mut checked_out = None;
        for (name, target) in refs {
            let before = state.refs.get(name);
            if before == Some(target)
                || on_head.contains(name)
                || head.reference.as_ref() == Some(name)
            {
                continue;
            }
            if checked_out.is_none() {
                checked_out = Some(checked_out_branches(git)?);
            }
            if !checked_out.as_ref().is_some_and(|set| set.contains(name)) {
                let log = common_dir.join("logs").join(name);
                let last = parse_reflog(&log)?.pop();
                moved_elsewhere.push(GitMove {
                    at: last
                        .as_ref()
                        .and_then(|entry| Timestamp::from_second(entry.at).ok())
                        .unwrap_or_else(Timestamp::now),
                    kind: OpKind::RefMove,
                    observation: GitObservation {
                        head: head.target.clone(),
                        reference: Some(name.clone()),
                        from: before.cloned(),
                        to: Some(target.clone()),
                        reflog: last.map(|entry| entry.message),
                        extra: Extra::default(),
                    },
                });
            }
        }
    }

    Ok(moved_elsewhere)
}

/// The commits the moves made, and the one `HEAD` is at. A checkout or reset only arrives at its
/// commit; the others made `to --not from`.
fn made_commits(
    git: &Git,
    head: &Head,
    moves: &[GitMove],
) -> Result<BTreeMap<String, GitCommit>, Error> {
    let mut wanted: Vec<String> = head.target.iter().cloned().collect();
    let mut commits = BTreeMap::new();
    for mv in moves {
        let (Some(to), from) = (&mv.observation.to, &mv.observation.from) else {
            continue;
        };
        match (&mv.kind, from) {
            (OpKind::Checkout | OpKind::Reset, _) | (_, None) => wanted.push(to.clone()),
            (_, Some(from)) => {
                let listed = git.text(&["rev-list", to, "--not", from])?;
                wanted.extend(listed.lines().map(str::to_owned));
            }
        }
    }
    for sha in wanted {
        if let Entry::Vacant(slot) = commits.entry(sha) {
            let datum = commit(git, slot.key())?;
            slot.insert(datum);
        }
    }

    Ok(commits)
}

/// The branches checked out in any worktree of the clone.
fn checked_out_branches(git: &Git) -> Result<BTreeSet<String>, Error> {
    let listing = git.text(&["worktree", "list", "--porcelain"])?;
    Ok(listing
        .lines()
        .filter_map(|line| line.strip_prefix("branch "))
        .map(str::to_owned)
        .collect())
}

/// Observe `HEAD`, the refs and the reflog of the worktree at `root`, whose git directory is
/// `git_dir` and clone's `common_dir`.
///
/// The first observation (no stored reflog position) is a baseline: it reports where `HEAD` and
/// the refs are, and no moves.
pub fn observe(
    root: &Path,
    git_dir: &Path,
    common_dir: &Path,
    state: &GitState,
) -> Result<(GitObserved, GitState), Error> {
    let git = Git::new(root);
    let head = Head {
        target: git.ask(&["rev-parse", "-q", "--verify", "HEAD"])?,
        reference: git.ask(&["symbolic-ref", "-q", "HEAD"])?,
    };
    let refs: BTreeMap<String, String> = String::from_utf8_lossy(&git.run(&[
        "for-each-ref",
        "--format=%(refname)%00%(objectname)",
        "refs/heads",
    ])?)
    .lines()
    .filter_map(|line| line.split_once('\0'))
    .map(|(name, target)| (name.to_owned(), target.to_owned()))
    .collect();

    let entries = parse_reflog(&git_dir.join("logs/HEAD"))?;
    let mut reflog_gap = false;
    let fresh: &[ReflogEntry] = match &state.reflog {
        // No baseline yet: nothing before now is reported.
        _ if state.head.is_none() => &[],
        // A baseline taken over an empty reflog.
        None => &entries,
        // The stored entry, by identity: still at its index, or moved toward the front by
        // expiry, which only removes older entries. A search that stops at the old index finds
        // the right one among identical lines.
        Some((index, last)) => {
            let upto = (index + 1).min(entries.len());
            match entries[..upto].iter().rposition(|entry| entry == last) {
                Some(found) => &entries[found + 1..],
                None => {
                    reflog_gap = true;
                    &[]
                }
            }
        }
    };

    let moves = head_moves(
        fresh,
        state.head.as_ref().and_then(|head| head.reference.clone()),
        &refs,
    )?;
    let mut moves = moves;
    if reflog_gap {
        // The stored entry expired or the log was rewritten, so the moves between are unknown:
        // one move from where HEAD was last seen to where it is.
        moves.push(GitMove {
            at: Timestamp::now(),
            kind: OpKind::RefMove,
            observation: GitObservation {
                head: head.target.clone(),
                reference: head.reference.clone(),
                from: state.head.as_ref().and_then(|seen| seen.target.clone()),
                to: head.target.clone(),
                reflog: None,
                extra: Extra::default(),
            },
        });
    }
    moves.extend(ref_moves(&git, common_dir, state, &refs, &head, &moves)?);
    let commits = made_commits(&git, &head, &moves)?;

    let mut ref_datums = BTreeMap::new();
    for (name, target) in &refs {
        if state.refs.get(name) != Some(target) {
            let upstream = format!("{name}@{{upstream}}");
            ref_datums.insert(
                name.clone(),
                GitRef {
                    target: target.clone(),
                    base: git.ask(&["merge-base", target, &upstream])?,
                    extra: Extra::default(),
                },
            );
        }
    }

    let next = GitState {
        head: Some(head.clone()),
        reflog: entries
            .last()
            .map(|entry| (entries.len() - 1, entry.clone())),
        refs,
    };
    Ok((
        GitObserved {
            head,
            moves,
            commits,
            refs: ref_datums,
            reflog_gap,
        },
        next,
    ))
}
