//! `add` writes a derived unified diff beside the pack of a commits entry (ADR 040 §4), and
//! leaves it out when the diff is too large to carry.

use super::git;
use super::init_repo;
use super::read_at;
use super::with_intxt_node;
use crate::client::NodeClient;
use crate::cmds::intxt::package::Named;
use crate::cmds::intxt::package::Packaging;
use method_core::Method;
use std::path::Path;

/// What `git diff` prints for `base..tip` in `repo`, as the patch blob is expected to hold it.
fn expected_diff(repo: &Path, base: &str, tip: &str) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["diff", "--no-color", "--no-ext-diff", "-M", base, tip])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}

/// Reads the bytes of the blob datum `reference` (`dtxt:datum:self/…`) names in the intxt's own
/// document, from the node's blob store.
async fn read_blob_text(
    client: &NodeClient,
    scratch: &Path,
    drawer: &big_cabinet_core::DocId,
    reference: &str,
) -> (String, String) {
    use crate::client::Raw;
    use big_blobs_core::BlobsScope;
    use big_blobs_core::MintBlobsCapability;
    use big_blobs_core::ReadBlob;
    use big_cabinet_core::authority::Rights;

    let blob_key = reference
        .strip_prefix("dtxt:datum:self/")
        .unwrap_or_else(|| panic!("{reference} names the intxt's own document"));
    let (_, blob) = read_at(client, drawer, &blob_key.parse().unwrap())
        .await
        .unwrap();
    let blob_id: big_blobs_core::BlobId = blob["digest"].as_str().unwrap().parse().unwrap();
    let capability = crate::cmds::intxt::call(
        client,
        MintBlobsCapability {
            scope: BlobsScope {
                read: [blob_id].into(),
                ..BlobsScope::default()
            },
            rights: Rights::USE,
            subject: None,
        },
    )
    .await
    .unwrap()
    .capability_id;
    let downloaded = scratch.join("patch-blob.bin");
    let reply = client
        .call_raw(
            ReadBlob::XRPC.nsid,
            ReadBlob::XRPC.method,
            &serde_json::to_value(ReadBlob {
                capability,
                blob_id,
            })
            .unwrap(),
            Raw {
                download: Some(&downloaded),
                ..Raw::default()
            },
        )
        .await
        .unwrap();
    assert!(reply.is_success(), "{reply:?}");
    (
        blob["mime"].as_str().unwrap().to_owned(),
        String::from_utf8(std::fs::read(downloaded).unwrap()).unwrap(),
    )
}

/// `add --branch` over `repo` into a new intxt: what it printed, the intxt, and its commits entry.
async fn add_branch(
    client: &NodeClient,
    node_root: &Path,
    repo: &Path,
) -> (String, big_cabinet_core::DocId, serde_json::Value) {
    let packaging = Packaging::new(client, repo, node_root);
    let drawer = packaging
        .create(&mut Vec::new(), "Patch", None, None)
        .await
        .unwrap();
    let mut out = Vec::new();
    packaging
        .add(
            &mut out,
            Some(&drawer.to_string()),
            Named::Branch("feat/x".to_owned()),
            false,
        )
        .await
        .unwrap();
    let tip = super::rev(repo, "feat/x");
    let (_, entry) = read_at(
        client,
        &drawer,
        &format!("org.example.intxt.work.gitCommitEntry/{tip}")
            .parse()
            .unwrap(),
    )
    .await
    .unwrap();
    (String::from_utf8(out).unwrap(), drawer, entry)
}

#[tokio::test(flavor = "multi_thread")]
async fn add_branch_writes_the_diff_beside_the_pack() {
    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let repo = init_repo(&base, &[("a.txt", "one\n")]);
        std::fs::write(repo.join("a.txt"), "two\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "edit a"]);
        std::fs::write(repo.join("b.txt"), "b\n").unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "add b"]);

        let (added, drawer, entry) = add_branch(client, &node_root, &repo).await;
        assert!(added.contains("  patch      "), "{added}");
        assert!(!added.contains("skipped"), "{added}");

        let patch = entry["patch"].as_str().unwrap();
        let (mime, diff) = read_blob_text(client, tmp.path(), &drawer, patch).await;
        assert_eq!(mime, "text/x-diff");
        assert_eq!(diff, expected_diff(&repo, "main", "feat/x"));
        assert!(diff.contains("@@ -1 +1 @@\n-one\n+two\n"), "{diff}");
        assert!(diff.contains("+++ b/b.txt\n@@ -0,0 +1 @@\n+b\n"), "{diff}");
        // The pack stays the truth: it is there beside the patch.
        assert!(entry["pack"].as_str().is_some(), "{entry}");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn add_branch_leaves_out_a_diff_over_the_limit_and_says_so() {
    with_intxt_node(async |tmp, client| {
        let node_root = tmp.path().join("node");
        let base = tmp.path().canonicalize().unwrap();
        let repo = init_repo(&base, &[("a.txt", "one\n")]);
        // A little over 8 MiB of added text: the diff is over the limit.
        std::fs::write(repo.join("big.txt"), "line\n".repeat(1_800_000)).unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-q", "-m", "add big"]);

        let (added, _, entry) = add_branch(client, &node_root, &repo).await;
        assert!(
            added.contains("  patch      skipped (the diff is over 8388608 bytes"),
            "{added}"
        );
        assert!(entry.get("patch").is_none(), "{entry}");
        assert!(entry["pack"].as_str().is_some(), "{entry}");
    })
    .await;
}
