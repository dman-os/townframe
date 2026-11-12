//! Resolving what a person names when packaging work (FDR 008 W2, W3): a range, a single commit,
//! a branch or a jj change become commit ids once, at the moment they are named, and the files
//! those commits wrote become the blob ids an intxt matches against the op log (ledger D61).

use crate::Error;
use crate::git::Git;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

/// The commits reachable from `tip` and not from `base`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Revisions {
    pub tip: String,
    /// What the recipient is assumed to hold: the merge base, which a thin pack deltas against.
    /// None when the commits reach the root.
    pub base: Option<String>,
}

/// A jj change and the commits it has been.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JjChange {
    /// The change's full id.
    pub change_id: String,
    /// Every commit the change has had, the current one first (`jj evolog`).
    pub commits: Vec<String>,
    /// The current commit's parents: what a thin pack deltas against.
    pub bases: Vec<String>,
    /// The git directory holding jj's commits, which a [`Git`] can read.
    pub git_dir: PathBuf,
}

fn revision(reason: impl Into<String>) -> Error {
    Error::Revision(reason.into())
}

/// The commit `name` names, or an error saying so.
fn commit_of(git: &Git, name: &str) -> Result<String, Error> {
    git.ask(&[
        "rev-parse",
        "--verify",
        "--quiet",
        &format!("{name}^{{commit}}"),
    ])?
    .ok_or_else(|| revision(format!("`{name}` is not a commit")))
}

/// The merge base of `tip` and `other`, which must be a proper ancestor of `tip`: a range that is
/// empty has nothing to package.
fn proper_base(git: &Git, tip: &str, other: &str, spec: &str) -> Result<String, Error> {
    let base = git
        .ask(&["merge-base", other, tip])?
        .ok_or_else(|| revision(format!("`{spec}` has no common ancestor with `{other}`")))?;
    if base == tip {
        return Err(revision(format!("`{spec}` names no commits")));
    }
    Ok(base)
}

/// `A..B` is the commits reachable from `B` and not from `A`, `..B` is not accepted, `A..` is
/// `A..HEAD`, and anything else is the one commit it names. The base is the merge base of the
/// range (the base of a single commit is its first parent), never the text of the range:
/// a revset read later would answer differently.
pub fn resolve_commits(git: &Git, spec: &str) -> Result<Revisions, Error> {
    if spec.contains("...") {
        return Err(revision(format!(
            "`{spec}`: a symmetric range names no single base; write `A..B`"
        )));
    }
    if let Some((from, to)) = spec.split_once("..") {
        if from.is_empty() {
            return Err(revision(format!("`{spec}` does not say where to start")));
        }
        let to = if to.is_empty() { "HEAD" } else { to };
        let tip = commit_of(git, to)?;
        let from = commit_of(git, from)?;
        let base = proper_base(git, &tip, &from, spec)?;
        return Ok(Revisions {
            tip,
            base: Some(base),
        });
    }
    let tip = commit_of(git, spec)?;
    let base = git.ask(&["rev-parse", "--verify", "--quiet", &format!("{tip}^")])?;
    Ok(Revisions { tip, base })
}

/// A branch and what it is compared against: its upstream when it has one, otherwise `main`
/// (or `master`). Returns the full ref name (`refs/heads/feat/x`) with the commits from the
/// merge base to where the branch points now.
pub fn resolve_branch(git: &Git, name: &str) -> Result<(String, Revisions), Error> {
    let reference = if name.starts_with("refs/heads/") {
        name.to_owned()
    } else {
        format!("refs/heads/{name}")
    };
    let tip = git
        .ask(&["rev-parse", "--verify", "--quiet", &reference])?
        .ok_or_else(|| revision(format!("`{name}` is not a local branch")))?;
    let upstream = git.ask(&[
        "rev-parse",
        "--verify",
        "--quiet",
        // `@{upstream}` takes the branch's short name.
        &format!(
            "{}@{{upstream}}",
            reference.trim_start_matches("refs/heads/")
        ),
    ])?;
    let target = match upstream {
        Some(upstream) => upstream,
        None => ["refs/heads/main", "refs/heads/master"]
            .into_iter()
            .filter(|candidate| *candidate != reference)
            .find_map(|candidate| {
                git.ask(&["rev-parse", "--verify", "--quiet", candidate])
                    .transpose()
            })
            .transpose()?
            .ok_or_else(|| {
                revision(format!(
                    "`{name}` has no upstream and there is no `main` or `master` to compare it against; name the commits with --commits"
                ))
            })?,
    };
    let base = proper_base(git, &tip, &target, name)?;
    Ok((
        reference,
        Revisions {
            tip,
            base: Some(base),
        },
    ))
}

/// The branch `HEAD` is on, as its full ref name; none when `HEAD` is detached.
pub fn head_branch(git: &Git) -> Result<Option<String>, Error> {
    git.ask(&["symbolic-ref", "-q", "HEAD"])
}

/// Whether `commit` is a commit of this repository that `of` reaches (or is). A commit the
/// repository does not have is not an ancestor of anything here.
pub fn is_ancestor(git: &Git, commit: &str, of: &str) -> Result<bool, Error> {
    if git.ask(&["cat-file", "-t", commit])?.as_deref() != Some("commit") {
        return Ok(false);
    }
    let status = git
        .command(&["merge-base", "--is-ancestor", commit, of])
        .status()?;
    match status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(Error::Git {
            args: format!("merge-base --is-ancestor {commit} {of}"),
            stderr: format!("exited with {status}"),
        }),
    }
}

/// The commits of `revisions`, newest first.
pub fn commits_of(git: &Git, revisions: &Revisions) -> Result<Vec<String>, Error> {
    let mut args = vec!["rev-list".to_owned(), revisions.tip.clone()];
    if let Some(base) = &revisions.base {
        args.push(format!("^{base}"));
    }
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    Ok(git.text(&args)?.lines().map(str::to_owned).collect())
}

/// What `commits` wrote, as (path, git blob id) pairs: each commit's tree against its first
/// parent's, or against nothing for a root commit, deletions and submodules left out. An
/// intxt matches these against the op log's `files` to find the sessions that produced the
/// content (ledger D61).
pub fn written_files(git: &Git, commits: &[String]) -> Result<BTreeSet<(String, String)>, Error> {
    if commits.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut input = commits.join("\n");
    input.push('\n');
    // `-m --first-parent` makes a merge's diff its first parent's too; `-z` keeps odd paths
    // intact. Records are `:<modes> <old> <new> <status>\0<path>\0`.
    let output = git.run_with_input(
        &[
            "diff-tree",
            "--stdin",
            "-r",
            "-z",
            "--no-renames",
            "--root",
            "--no-commit-id",
            "-m",
            "--first-parent",
        ],
        &[],
        input.as_bytes(),
    )?;
    let text = String::from_utf8_lossy(&output);
    let mut fields = text.split('\0').filter(|field| !field.is_empty());
    let mut written = BTreeSet::new();
    while let Some(header) = fields.next() {
        let path = fields
            .next()
            .ok_or_else(|| Error::Format(format!("git diff-tree ended after {header:?}")))?;
        let columns: Vec<&str> = header.trim_start_matches(':').split(' ').collect();
        let [_old_mode, new_mode, _old, new, _status] = columns[..] else {
            return Err(Error::Format(format!("git diff-tree printed {header:?}")));
        };
        // A deletion has an all-zero new id, a submodule a commit mode; neither is a blob.
        if new.bytes().all(|byte| byte == b'0') || new_mode == "160000" {
            continue;
        }
        written.insert((path.to_owned(), new.to_owned()));
    }
    Ok(written)
}

fn jj(root: &Path, args: &[&str]) -> Result<String, Error> {
    let output = Command::new("jj")
        .arg("--repository")
        .arg(root)
        .arg("--color=never")
        .args(args)
        .output()?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(revision(format!(
            "jj {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

/// The commits of jj change `change` in the repository at `root`, and where git can read them.
/// A divergent change (several current commits) is refused: it names no single change.
pub fn resolve_jj_change(root: &Path, change: &str) -> Result<JjChange, Error> {
    let lines = |text: String| -> Vec<String> { text.lines().map(str::to_owned).collect() };
    let template = ["--no-graph", "-T", r#"commit_id ++ "\n""#];
    let current = lines(jj(
        root,
        &[
            "log",
            "-r",
            change,
            "--no-graph",
            "-T",
            r#"change_id ++ "\n""#,
        ],
    )?);
    let [change_id] = current.as_slice() else {
        return Err(revision(format!(
            "`{change}` names {} changes; name one by its change id",
            current.len()
        )));
    };
    let commits = lines(jj(
        root,
        &[&["evolog", "-r", change][..], &template].concat(),
    )?);
    let bases = lines(jj(
        root,
        &[&["log", "-r", &format!("parents({change})")][..], &template].concat(),
    )?);
    let git_dir = PathBuf::from(jj(root, &["git", "root"])?);
    Ok(JjChange {
        change_id: change_id.clone(),
        commits,
        bases,
        git_dir,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Repo;

    fn branchy() -> (Repo, String, String, String) {
        let repo = Repo::new();
        repo.write("a.txt", "base\n");
        let base = repo.commit_all("base");
        repo.git(&["switch", "-q", "-c", "feat/x"]);
        repo.write("a.txt", "one\n");
        repo.write("b.txt", "b\n");
        let one = repo.commit_all("one");
        repo.git(&["rm", "-q", "b.txt"]);
        repo.write("c/d.txt", "d\n");
        let two = repo.commit_all("two");
        (repo, base, one, two)
    }

    #[test]
    fn a_range_resolves_to_its_tip_and_merge_base() {
        let (repo, base, one, two) = branchy();
        let git = Git::new(&repo.root);
        let expected = Revisions {
            tip: two.clone(),
            base: Some(base.clone()),
        };
        assert_eq!(resolve_commits(&git, "main..feat/x").unwrap(), expected);
        assert_eq!(resolve_commits(&git, "main..").unwrap(), expected);
        // The base is the merge base even when `main` has moved on since.
        repo.git(&["switch", "-q", "main"]);
        repo.write("m.txt", "m\n");
        repo.commit_all("main moved");
        assert_eq!(resolve_commits(&git, "main..feat/x").unwrap(), expected);
        assert_eq!(
            resolve_commits(&git, &one).unwrap(),
            Revisions {
                tip: one.clone(),
                base: Some(base.clone())
            }
        );
        assert_eq!(
            resolve_commits(&git, &base).unwrap(),
            Revisions {
                tip: base,
                base: None
            }
        );
        assert_eq!(commits_of(&git, &expected).unwrap(), [two, one]);
    }

    #[test]
    fn bad_ranges_are_refused_with_what_is_wrong() {
        let (repo, ..) = branchy();
        let git = Git::new(&repo.root);
        for (spec, reason) in [
            ("main...feat/x", "symmetric"),
            ("..feat/x", "does not say where to start"),
            ("feat/x..main", "names no commits"),
            ("nope..feat/x", "`nope` is not a commit"),
        ] {
            let error = resolve_commits(&git, spec).unwrap_err().to_string();
            assert!(error.contains(reason), "{spec}: {error}");
        }
    }

    #[test]
    fn a_branch_is_compared_with_its_upstream_or_main() {
        let (repo, base, _, two) = branchy();
        let git = Git::new(&repo.root);
        let (reference, revisions) = resolve_branch(&git, "feat/x").unwrap();
        assert_eq!(reference, "refs/heads/feat/x");
        assert_eq!(
            revisions,
            Revisions {
                tip: two.clone(),
                base: Some(base.clone())
            }
        );

        // With an upstream, the merge base with the upstream is the base.
        repo.git(&["switch", "-q", "-c", "feat/y", "feat/x"]);
        repo.write("y.txt", "y\n");
        let y = repo.commit_all("y");
        repo.git(&["branch", "-q", "--set-upstream-to=feat/x"]);
        let (_, revisions) = resolve_branch(&git, "feat/y").unwrap();
        assert_eq!(
            revisions,
            Revisions {
                tip: y,
                base: Some(two)
            }
        );

        let error = resolve_branch(&git, "main").unwrap_err().to_string();
        assert!(error.contains("no `main` or `master`"), "{error}");
        let error = resolve_branch(&git, "missing").unwrap_err().to_string();
        assert!(error.contains("not a local branch"), "{error}");
    }

    #[test]
    fn written_files_are_what_each_commit_wrote() {
        let (repo, base, one, two) = branchy();
        let git = Git::new(&repo.root);
        let blob = |commit: &str, path: &str| repo.git(&["rev-parse", &format!("{commit}:{path}")]);
        let written = written_files(&git, &[one.clone(), two.clone()]).unwrap();
        assert_eq!(
            written,
            [
                ("a.txt".to_owned(), blob(&one, "a.txt")),
                ("b.txt".to_owned(), blob(&one, "b.txt")),
                ("c/d.txt".to_owned(), blob(&two, "c/d.txt")),
            ]
            .into()
        );
        // A root commit is diffed against nothing.
        assert_eq!(
            written_files(&git, std::slice::from_ref(&base)).unwrap(),
            [("a.txt".to_owned(), blob(&base, "a.txt"))].into()
        );
        assert!(written_files(&git, &[]).unwrap().is_empty());
    }
}
