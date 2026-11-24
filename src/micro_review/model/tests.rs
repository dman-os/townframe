//! The review model against the in-memory fixture node.
use super::*;
use crate::fixture::Fixture;
use intxt_core::generic::Intxt as IntxtMarker;
use intxt_core::generic::PrLink;
use intxt_core::generic::PrLinks;

fn review(fixture: &Fixture) -> Review<Fixture> {
    Review::new(fixture.clone())
}

fn found_titles(links: &Links) -> Vec<&str> {
    let Links::Named(rows) = links else {
        panic!("expected named links, got {links:?}")
    };
    rows.iter()
        .map(|row| match &row.outcome {
            LinkOutcome::Found { title, .. } => title.as_str(),
            LinkOutcome::NotFound => "(not an intxt)",
            LinkOutcome::Unreadable(_) => "(unreadable)",
        })
        .collect()
}

fn pull_with(body: &str) -> Pull {
    Pull {
        doc: Fixture::doc_id("2"),
        slot: "o/r/1".to_owned(),
        title: "t".to_owned(),
        state: PullState::Open,
        body: body.to_owned(),
        url: "https://github.com/o/r/pull/1".to_owned(),
    }
}

#[test]
fn the_block_is_hidden_from_the_body_and_counted_as_the_intxts_it_names() {
    let block = PrLinks {
        intxts: vec![
            PrLink::new(IntxtMarker::reference(&Fixture::doc_id("31"), [])),
            PrLink::new(IntxtMarker::reference(&Fixture::doc_id("32"), [])),
        ],
        ..Default::default()
    }
    .block();
    let reading = pull_with(&format!("First.\nStill first.\n\nSecond.\n\n{block}\n")).reading();
    assert_eq!(reading.paragraphs, ["First.\nStill first.", "Second."]);
    assert_eq!(reading.names_line().as_deref(), Some("names 2 intxts"));
    assert!(!reading.paragraphs.concat().contains("details"));
}

#[test]
fn a_description_without_a_block_shows_in_full_and_names_nothing() {
    let reading = pull_with("Just words.\r\n\r\nMore words.").reading();
    assert_eq!(reading.paragraphs, ["Just words.", "More words."]);
    assert_eq!(reading.names_line(), None);
    assert_eq!(pull_with("").reading().paragraphs, Vec::<String>::new());
}

#[test]
fn one_intxt_is_named_in_the_singular() {
    let body = PrLinks {
        intxts: vec![PrLink::new(IntxtMarker::reference(
            &Fixture::doc_id("31"),
            [],
        ))],
        ..Default::default()
    }
    .block();
    assert_eq!(
        pull_with(&body).reading().names_line().as_deref(),
        Some("names 1 intxt")
    );
}

#[test]
fn a_malformed_block_stays_in_the_text_and_names_nothing() {
    let body = "Words.\n\n<details>\n<summary>intxt</summary>\n\n```json\n{ \"$type\": \"org.example.intxt.prLinks\", \"intxts\": [{ \"ref\": 5 }] }\n```\n\n</details>";
    let reading = pull_with(body).reading();
    assert_eq!(reading.names_line(), None);
    assert!(reading.paragraphs.concat().contains("details"));
}

#[test]
fn a_teaser_is_one_line_cut_at_a_word_boundary() {
    assert_eq!(teaser("Short\n\ntext."), "Short text.");
    let long = "word ".repeat(60);
    let cut = teaser(&long);
    assert!(cut.ends_with("word…"), "{cut}");
    assert!(cut.chars().count() <= TEASER_CHARS + 1);
    let unbroken = "x".repeat(300);
    assert_eq!(teaser(&unbroken).chars().count(), TEASER_CHARS + 1);
}

#[tokio::test]
async fn entries_are_grouped_by_kind_in_the_order_the_intxt_was_written() {
    let Read::Ready(intxt) = review(&Fixture::sample())
        .intxt(&Fixture::doc_id("31"))
        .await
        .unwrap()
    else {
        panic!("the sample intxt is readable")
    };
    let groups = intxt.groups();
    assert_eq!(
        groups
            .iter()
            .map(|group| (group.heading, group.entries.len()))
            .collect::<Vec<_>>(),
        [("Commits", 2), ("Pull requests", 1)]
    );
    assert_eq!(entries_label(1), "1 entry");
    assert_eq!(entries_label(3), "3 entries");
}

#[tokio::test]
async fn the_list_shows_each_held_pull_with_its_repo_ref_title_and_state() {
    let pulls = review(&Fixture::sample()).pulls().await.unwrap();
    let shown: Vec<(String, String, &str)> = pulls
        .iter()
        .map(|read| match read {
            Read::Ready(pull) => (pull.repo_ref(), pull.title.clone(), pull.state_label()),
            Read::Unreadable(reason) => panic!("sample pulls are readable: {reason}"),
        })
        .collect();
    assert_eq!(
        shown,
        [
            (
                "dman-os/townframe#150".to_owned(),
                "intxt: read a PR and its intxts on the phone".to_owned(),
                "Open"
            ),
            (
                "dman-os/townframe#151".to_owned(),
                "chore: pin the review micro's lockfile".to_owned(),
                "Merged"
            ),
        ]
    );
}

#[tokio::test]
async fn a_pull_the_poll_names_but_the_document_lacks_is_an_unreadable_row() {
    let bare = Fixture::doc_id("5");
    let fixture = Fixture::empty()
        .doc(bare.clone(), [])
        .held_pull(bare, key(work::GithubPull::ID, "o/r/1"));
    let pulls = review(&fixture).pulls().await.unwrap();
    assert!(matches!(&pulls[..], [Read::Unreadable(reason)] if reason.contains("absent")));
}

#[tokio::test]
async fn the_list_is_one_poll_then_one_read_per_pull() {
    let fixture = Fixture::sample();
    review(&fixture).pulls().await.unwrap();
    assert_eq!(
        fixture.calls(),
        [
            "org.example.daybook.docs.pollDatums",
            "org.example.daybook.docs.readDatums",
            "org.example.daybook.docs.readDatums"
        ]
    );
}

#[tokio::test]
async fn the_block_reads_each_link_in_order_its_pointer_then_its_index() {
    let fixture = Fixture::sample();
    let review = review(&fixture);
    let Read::Ready(pull) = review.pulls().await.unwrap().remove(0) else {
        panic!("the first sample pull is readable")
    };
    fixture.forget_calls();
    let links = review.links(&pull.body).await.unwrap();
    assert_eq!(
        found_titles(&links),
        [
            "intxt: commits live with the intxt",
            "review micro: the PR on the phone",
            "(not an intxt)"
        ]
    );
    let Links::Named(rows) = &links else {
        unreachable!()
    };
    assert_eq!(
        rows[0].hinted_title.as_deref(),
        Some("intxt: commits live with the intxt")
    );
    assert!(matches!(
        &rows[1].outcome,
        LinkOutcome::Found { description: Some(text), entries: 1, .. } if text.contains("phone")
    ));
    // Each intxt is two reads, its drawer document for the index pointer and then the index
    // for the title and body; the document that is no intxt stops at the first.
    assert_eq!(
        fixture.calls(),
        ["org.example.daybook.docs.readDatums"; 5]
    );
}

#[tokio::test]
async fn a_description_without_a_block_names_nothing_and_asks_the_node_nothing() {
    let fixture = Fixture::sample();
    let links = review(&fixture).links("Just words.").await.unwrap();
    assert_eq!(links, Links::Named(vec![]));
    assert!(fixture.calls().is_empty());
}

#[tokio::test]
async fn a_malformed_block_is_an_error_not_an_empty_list() {
    let body = "<details>\n<summary>intxt</summary>\n\n```json\n{ \"$type\": \"org.example.intxt.prLinks\", \"intxts\": [{ \"ref\": 5 }] }\n```\n\n</details>";
    let fixture = Fixture::sample();
    let links = review(&fixture).links(body).await.unwrap();
    assert!(
        matches!(&links, Links::Malformed(why) if why.contains("malformed")),
        "{links:?}"
    );
    assert!(fixture.calls().is_empty());
}

#[tokio::test]
async fn a_drawer_the_node_does_not_hold_is_unreadable_and_an_unrelated_document_is_not_found() {
    let held_elsewhere = Fixture::doc_id("6");
    let body = PrLinks {
        intxts: vec![
            PrLink::new(IntxtMarker::reference(&Fixture::doc_id("41"), [])),
            PrLink::new(IntxtMarker::reference(&held_elsewhere, [])),
        ],
        ..Default::default()
    }
    .block();
    let Links::Named(rows) = review(&Fixture::sample()).links(&body).await.unwrap() else {
        panic!("the block is well formed")
    };
    assert_eq!(rows[0].outcome, LinkOutcome::NotFound);
    assert!(
        matches!(&rows[1].outcome, LinkOutcome::Unreadable(reason) if reason.contains("credential"))
    );
}

#[tokio::test]
async fn a_pull_reads_at_most_32_descriptors_in_order() {
    let drawers: Vec<DocId> = ('a'..='k')
        .flat_map(|second| {
            ['2', '3', '4', '5'].map(|first| Fixture::doc_id(&format!("{first}{second}")))
        })
        .take(MAX_LINKS_PER_PULL + 8)
        .collect();
    let body = PrLinks {
        intxts: drawers
            .iter()
            .map(|drawer| PrLink::new(IntxtMarker::reference(drawer, [])))
            .collect(),
        ..Default::default()
    }
    .block();
    let fixture = Fixture::empty();
    let Links::Named(rows) = review(&fixture).links(&body).await.unwrap() else {
        panic!("the block is well formed")
    };
    assert_eq!(
        rows.iter()
            .map(|row| row.drawer.clone())
            .collect::<Vec<_>>(),
        drawers[..MAX_LINKS_PER_PULL]
    );
    assert_eq!(fixture.calls().len(), MAX_LINKS_PER_PULL);
}

#[tokio::test]
async fn an_intxt_shows_its_title_description_and_entries_in_the_bodys_order() {
    let intxt = review(&Fixture::sample())
        .intxt(&Fixture::doc_id("31"))
        .await
        .unwrap();
    let Read::Ready(intxt) = intxt else {
        panic!("the sample intxt is readable")
    };
    assert_eq!(intxt.title, "intxt: commits live with the intxt");
    assert_eq!(
        intxt.description.as_deref(),
        Some("Commits travel with the intxt that names them.")
    );
    let shown: Vec<(&str, &str, bool)> = intxt
        .entries
        .iter()
        .map(|entry| {
            (
                entry.kind.name(),
                entry.label.as_str(),
                matches!(entry.changes, Changes::Blob(_)),
            )
        })
        .collect();
    assert_eq!(
        shown,
        [
            ("commits", "9a0e11c..4c1f0a7", true),
            ("commits", "77b2d03", false),
            ("pull request", "dman-os/townframe/150", false),
        ]
    );
}

#[tokio::test]
async fn a_commit_without_a_patch_says_its_diff_was_too_large_to_carry() {
    let Read::Ready(intxt) = review(&Fixture::sample())
        .intxt(&Fixture::doc_id("31"))
        .await
        .unwrap()
    else {
        panic!("the sample intxt is readable")
    };
    assert_eq!(intxt.entries[1].label, "77b2d03");
    assert_eq!(intxt.entries[1].changes, Changes::TooLarge);
    // Entries of other kinds have no changes to say anything of.
    assert_eq!(intxt.entries[2].changes, Changes::Absent);
}

#[tokio::test]
async fn a_patch_is_read_only_when_asked_for_and_resolves_self_and_concrete_references() {
    let fixture = Fixture::sample();
    let review = review(&fixture);
    let Read::Ready(intxt) = review.intxt(&Fixture::doc_id("31")).await.unwrap() else {
        panic!("the sample intxt is readable")
    };
    // Reading the intxt reads documents, never a blob.
    assert!(!fixture.calls().contains(&micro_review_manifest::READ_BLOB));
    let Changes::Blob(source) = &intxt.entries[0].changes else {
        panic!("the first commit carries a patch")
    };
    // `self` is the document the entry is in: the intxt's index document (D70), not its drawer.
    assert_eq!(source.doc, Fixture::doc_id("33"));
    assert!(
        review
            .patch(source)
            .await
            .unwrap()
            .starts_with("diff --git a/src/review.rs")
    );
    assert_eq!(
        fixture
            .calls()
            .iter()
            .filter(|nsid| **nsid == micro_review_manifest::READ_BLOB)
            .count(),
        1
    );

    // A concrete reference names its own document, where the blob datum may have moved to.
    let commits = Fixture::doc_id("5");
    let held = Fixture::empty().doc(commits.clone(), []).patch_blob(
        &commits,
        "moved",
        "diff --git a/x b/x\n",
    );
    let entry = (
        key(work::GitCommitEntry::ID, "abcdef0"),
        serde_json::json!({
            "tip": format!("{}/abcdef0", work::GitCommit::ID),
            "pack": "dtxt:datum:self/org.example.daybook.blob/pack",
            "patch": format!("dtxt:datum:{commits}/org.example.daybook.blob/moved"),
        }),
    );
    let drawer = Fixture::doc_id("6");
    let held = held.intxt(drawer.clone(), Fixture::doc_id("9"), "moved", None, vec![entry]);
    let Read::Ready(intxt) = Review::new(held.clone()).intxt(&drawer).await.unwrap() else {
        panic!("the intxt is readable")
    };
    let Changes::Blob(source) = &intxt.entries[0].changes else {
        panic!("the commit carries a patch")
    };
    assert_eq!(source.doc, commits);
    assert_eq!(
        Review::new(held).patch(source).await.unwrap(),
        "diff --git a/x b/x\n"
    );
}

#[tokio::test]
async fn a_patch_that_cannot_be_read_is_an_error_not_an_empty_diff() {
    let drawer = Fixture::doc_id("7");
    let missing = PatchSource {
        doc: drawer.clone(),
        key: key(big_blobs_core::Blob::ID, "nope"),
    };
    let fixture = Fixture::empty().doc(drawer, []);
    let error = review(&fixture).patch(&missing).await.unwrap_err();
    assert!(
        matches!(&error, CallError::Refused(reason) if reason.contains("absent")),
        "{error:?}"
    );
}

#[tokio::test]
async fn an_intxt_missing_its_index_title_or_an_entry_says_so_instead_of_showing_less() {
    let drawer = Fixture::doc_id("7");
    let index = Fixture::doc_id("8");
    let at = &drawer;
    let unreadable = |fixture: Fixture, says: &'static str| async move {
        let reading = review(&fixture).intxt(at).await.unwrap();
        assert!(
            matches!(&reading, Read::Unreadable(reason) if reason.contains(says)),
            "{says}: {reading:?}"
        );
    };
    let intxt = Fixture::empty().intxt(drawer.clone(), index.clone(), "t", None, vec![]);
    // The drawer document carries no marker: it is no intxt.
    unreadable(Fixture::empty().doc(drawer.clone(), []), "no intxt marker").await;
    // The marker is there and the pointer to the index document is not.
    let marker = [(
        key(IntxtMarker::ID, "main"),
        serde_json::to_value(IntxtMarker::default()).unwrap(),
    )];
    unreadable(Fixture::empty().doc(drawer.clone(), marker), "index pointer").await;
    // The index document is there without its title.
    unreadable(intxt.doc(index, []), "title").await;

    let Read::Ready(intxt) =
        review(&Fixture::sample().listing_a_missing_entry(&Fixture::doc_id("34")))
            .intxt(&Fixture::doc_id("32"))
            .await
            .unwrap()
    else {
        panic!("the title and body are there")
    };
    assert!(
        intxt
            .entries
            .iter()
            .any(|entry| matches!(&entry.kind, EntryKind::Unreadable(reason) if reason.contains("not in the document")))
    );
}

#[tokio::test]
async fn an_intxt_the_node_does_not_hold_is_an_unreadable_row() {
    let reading = review(&Fixture::sample())
        .intxt(&Fixture::doc_id("8"))
        .await
        .unwrap();
    assert!(matches!(reading, Read::Unreadable(reason) if reason.contains("credential")));
}
