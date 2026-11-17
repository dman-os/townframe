//! Build the git object pack carried by an intxt (ADR 040 §4).

use crate::Error;
use crate::git::Git;

/// The commits and loose objects to include in a git pack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackRequest {
    /// Commit ids whose reachable history is included.
    pub tips: Vec<String>,
    /// Commit ids already held by the recipient. An empty list requests a full pack.
    pub bases: Vec<String>,
    /// Additional object ids, such as blobs recorded for uncommitted file states.
    pub objects: Vec<String>,
    /// Exclude all blobs larger than this many bytes when set, including extra objects.
    pub max_blob_bytes: Option<u64>,
}

/// The packed objects and whether the pack contains deltas against omitted bases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pack {
    pub bytes: Vec<u8>,
    pub thin: bool,
}

/// Build a full or thin pack from the requested revisions.
pub fn build(git: &Git, request: &PackRequest) -> Result<Pack, Error> {
    let mut revisions = String::new();
    for tip in &request.tips {
        revisions.push_str(tip);
        revisions.push('\n');
    }
    for base in &request.bases {
        revisions.push('^');
        revisions.push_str(base);
        revisions.push('\n');
    }
    // In --revs mode, a positive non-commit object is also a valid starting object. This keeps
    // loose blobs for uncommitted states in the same pack as the requested commit history.
    for object in &request.objects {
        revisions.push_str(object);
        revisions.push('\n');
    }

    let mut args = vec!["pack-objects", "--stdout", "--revs"];
    if !request.bases.is_empty() {
        args.push("--thin");
    }
    let filter = request
        .max_blob_bytes
        .map(|limit| format!("--filter=blob:limit={limit}"));
    if let Some(filter) = filter.as_deref() {
        // Git accepts blob filters together with --revs, so filtering stays in the pack-objects
        // operation and does not require first enumerating all reachable objects ourselves.
        args.push(filter);
    }

    Ok(Pack {
        bytes: git.run_with_input(&args, &[], revisions.as_bytes())?,
        thin: !request.bases.is_empty(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::Repo;
    use crate::testing::git_in;
    use std::path::Path;

    fn apply_pack(root: &Path, args: &[&str], pack: &Pack) {
        Git::new(root)
            .run_with_input(args, &[], &pack.bytes)
            .unwrap();
    }

    fn has_object(root: &Path, object: &str) -> bool {
        Git::new(root).run(&["cat-file", "-e", object]).is_ok()
    }

    #[test]
    fn full_pack_contains_branch_history_in_an_empty_repo() {
        let repo = Repo::new();
        repo.write("file.txt", "base\n");
        let base = repo.commit_all("base");
        repo.write("file.txt", "tip\n");
        let tip = repo.commit_all("tip");

        let pack = build(
            &Git::new(&repo.root),
            &PackRequest {
                tips: vec![tip.clone()],
                bases: Vec::new(),
                objects: Vec::new(),
                max_blob_bytes: None,
            },
        )
        .unwrap();
        assert!(!pack.thin);

        let target = tempfile::tempdir().unwrap();
        git_in(target.path(), &["init", "-q", "-b", "main"]);
        apply_pack(target.path(), &["unpack-objects"], &pack);

        assert!(has_object(target.path(), &format!("{base}^{{commit}}")));
        assert!(has_object(target.path(), &format!("{tip}^{{commit}}")));
    }

    #[test]
    fn thin_pack_uses_the_base_and_is_smaller_than_a_full_pack() {
        let repo = Repo::new();
        repo.write("large.bin", vec![b'a'; 1024 * 1024]);
        let base = repo.commit_all("base");
        let clone_dir = tempfile::tempdir().unwrap();
        let clone = clone_dir.path().join("clone");
        git_in(
            clone_dir.path(),
            &[
                "clone",
                "-q",
                repo.root.to_str().unwrap(),
                clone.to_str().unwrap(),
            ],
        );

        let mut changed = vec![b'a'; 1024 * 1024];
        changed[512 * 1024] = b'b';
        repo.write("large.bin", changed);
        let tip = repo.commit_all("tip");

        let full = build(
            &Git::new(&repo.root),
            &PackRequest {
                tips: vec![tip.clone()],
                bases: Vec::new(),
                objects: Vec::new(),
                max_blob_bytes: None,
            },
        )
        .unwrap();
        let thin = build(
            &Git::new(&repo.root),
            &PackRequest {
                tips: vec![tip.clone()],
                bases: vec![base],
                objects: Vec::new(),
                max_blob_bytes: None,
            },
        )
        .unwrap();
        assert!(!full.thin);
        assert!(thin.thin);
        eprintln!(
            "thin pack: {} bytes; full pack: {} bytes",
            thin.bytes.len(),
            full.bytes.len()
        );
        assert!(
            thin.bytes.len() < full.bytes.len(),
            "thin pack {} is not smaller than full pack {}",
            thin.bytes.len(),
            full.bytes.len()
        );

        apply_pack(&clone, &["index-pack", "--stdin", "--fix-thin"], &thin);
        assert!(has_object(&clone, &format!("{tip}^{{commit}}")));
    }

    #[test]
    fn full_pack_includes_an_extra_uncommitted_blob() {
        let repo = Repo::new();
        repo.write("committed.txt", "committed\n");
        let tip = repo.commit_all("tip");
        repo.write("uncommitted.txt", "never committed\n");
        let blob = repo.git(&["hash-object", "-w", "uncommitted.txt"]);

        let pack = build(
            &Git::new(&repo.root),
            &PackRequest {
                tips: vec![tip],
                bases: Vec::new(),
                objects: vec![blob.clone()],
                max_blob_bytes: None,
            },
        )
        .unwrap();
        let target = tempfile::tempdir().unwrap();
        git_in(target.path(), &["init", "-q", "-b", "main"]);
        apply_pack(target.path(), &["unpack-objects"], &pack);

        assert!(has_object(target.path(), &blob));
    }

    #[test]
    fn pack_blob_filter_leaves_out_large_blobs() {
        let repo = Repo::new();
        repo.write("small.txt", "small\n");
        repo.write("large.bin", vec![b'x'; 1024]);
        let tip = repo.commit_all("objects");
        let small = repo.git(&["rev-parse", "HEAD:small.txt"]);
        let large = repo.git(&["rev-parse", "HEAD:large.bin"]);

        let pack = build(
            &Git::new(&repo.root),
            &PackRequest {
                tips: vec![tip],
                bases: Vec::new(),
                objects: Vec::new(),
                max_blob_bytes: Some(100),
            },
        )
        .unwrap();
        let target = tempfile::tempdir().unwrap();
        git_in(target.path(), &["init", "-q", "-b", "main"]);
        apply_pack(target.path(), &["unpack-objects"], &pack);

        assert!(has_object(target.path(), &small));
        assert!(!has_object(target.path(), &large));
    }
}
