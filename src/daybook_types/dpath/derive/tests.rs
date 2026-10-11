use super::*;

use crate::dpath::naming::identity_digest;

fn raw(doc_id: &str, dpath: &str) -> RawClaim {
    RawClaim {
        doc_id: doc_id.to_string(),
        dpath: dpath.to_string(),
        facet: DpathFacet::whole_document(),
        assigned_at: None,
    }
}

fn derive(spec: &[(&str, &str)]) -> DerivedTree {
    derive_tree(
        spec.iter().map(|(doc_id, dpath)| raw(doc_id, dpath)),
        &DpathNamePolicy::default(),
    )
}

fn paths(spec: &[(&str, &str)]) -> Vec<String> {
    derive(spec).paths()
}

/// The derived-name contract (FDR 001 Q1): `<stem>~<digest><.ext>`, where the
/// digest is a base58 (Bitcoin alphabet) encoding of the blake3 digest of the
/// claimant's identity (doc id + dpath).
fn expected_derived(segment: &str, doc_id: &str, dpath: &str, extension: Option<&str>) -> String {
    let digest = bs58::encode(blake3::hash(format!("{doc_id}\u{0}{dpath}").as_bytes()).as_bytes())
        .into_string();
    let stem = segment.strip_suffix(".md").unwrap_or(segment);
    match extension {
        Some(extension) => format!("{stem}~{}.{extension}", &digest[..8]),
        None => format!("{stem}~{}", &digest[..8]),
    }
}

fn collision(dir: &DerivedEntry) -> &[DerivedEntry] {
    match dir {
        DerivedEntry::Dir {
            kind: DerivedDirKind::Collision,
            children,
            ..
        } => children,
        other => panic!("expected a collision directory, got {other:?}"),
    }
}

#[test]
fn case1_single_claim_keeps_the_clean_name() {
    assert_eq!(
        paths(&[("A", "/inbox/hello.md")]),
        ["/inbox/", "/inbox/hello.md"]
    );
}

#[test]
fn case2_many_claimants_make_a_collision_directory() {
    let tree = derive(&[("A", "/inbox/hello.md"), ("B", "/inbox/hello.md")]);
    let dir = tree.lookup("/inbox/hello.md").unwrap();
    let children = collision(dir);
    assert_eq!(children.len(), 2);

    let mut names: Vec<&str> = children.iter().map(DerivedEntry::name).collect();
    names.sort_unstable();
    let mut expected = [
        expected_derived("hello.md", "A", "/inbox/hello.md", Some("md")),
        expected_derived("hello.md", "B", "/inbox/hello.md", Some("md")),
    ];
    expected.sort();
    assert_eq!(
        names,
        expected.iter().map(String::as_str).collect::<Vec<_>>()
    );

    // Conflicts are opinions: both claimants are retained in full.
    let docs: BTreeSet<&str> = children
        .iter()
        .filter_map(|child| match child {
            DerivedEntry::Claim { claim, .. } => Some(claim.doc_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(docs, BTreeSet::from(["A", "B"]));

    assert_eq!(
        tree.paths(),
        vec![
            "/inbox/".to_string(),
            "/inbox/hello.md/".to_string(),
            format!(
                "/inbox/hello.md/{}",
                expected_derived("hello.md", "B", "/inbox/hello.md", Some("md"))
            ),
            format!(
                "/inbox/hello.md/{}",
                expected_derived("hello.md", "A", "/inbox/hello.md", Some("md"))
            ),
        ]
    );
}

#[test]
fn case3_file_and_children_collide_into_a_spill_directory() {
    assert_eq!(
        paths(&[("A", "/hi/hello"), ("C", "/hi/hello/child")]),
        ["/hi/", "/hi/hello", "/hi/hello.d/", "/hi/hello.d/child"]
    );
}

#[test]
fn case4_literal_dot_d_claim_loses_its_name() {
    // FDR 001 §3 case 4: the real path `/a.d` is the spill slot of `/a`, so
    // the object claiming the literal dpath `/a.d` is demoted.
    assert_eq!(paths(&[("A", "/a"), ("D", "/a.d")]), ["/a", "/a.d.d"]);
}

#[test]
fn case5_prefix_only_paths_materialize_bare_directories() {
    let tree = derive(&[("A", "/a/b")]);
    assert_eq!(tree.paths(), ["/a/", "/a/b"]);
    assert!(matches!(
        tree.lookup("/a"),
        Some(DerivedEntry::Dir {
            kind: DerivedDirKind::Implicit,
            ..
        })
    ));
}

#[test]
fn case6_rules_compose() {
    assert_eq!(
        paths(&[("A", "/a"), ("C", "/a/b"), ("D", "/a.d")]),
        ["/a", "/a.d/", "/a.d/b", "/a.d.d"]
    );
}

#[test]
fn case7_dot_d_stacking_terminates_and_stacks() {
    assert_eq!(
        paths(&[("A", "/a"), ("E", "/a.d/x"), ("F", "/a.d.d")]),
        ["/a", "/a.d.d/", "/a.d.d/x", "/a.d.d.d"]
    );
}

#[test]
fn case8_collision_directory_nests_claimant_subtrees() {
    let tree = derive(&[
        ("G", "/inbox"),
        ("G", "/inbox/g1"),
        ("H", "/inbox"),
        ("H", "/inbox/h1"),
    ]);
    let children = collision(tree.lookup("/inbox").unwrap());
    assert_eq!(children.len(), 2);
    for child in children {
        match child {
            DerivedEntry::Dir {
                kind: DerivedDirKind::Claimant,
                children,
                ..
            } => assert_eq!(children.len(), 1),
            other => panic!("expected a claimant subtree, got {other:?}"),
        }
    }
    let docs: BTreeSet<&str> = tree
        .claims()
        .iter()
        .map(|claim| claim.doc_id.as_str())
        .collect();
    assert_eq!(docs, BTreeSet::from(["G", "H"]));
}

#[test]
fn by_id_is_an_ordinary_dpath_not_a_derived_surface() {
    let tree = derive(&[("A", "/by-id/x")]);
    assert_eq!(tree.paths(), ["/by-id/", "/by-id/x"]);
    assert!(matches!(
        tree.lookup("/by-id/x"),
        Some(DerivedEntry::Claim { claim, .. }) if claim.doc_id == "A"
    ));
}

#[test]
fn derived_names_are_identity_derived_and_extension_preserving() {
    let tree = derive(&[("A", "/inbox/hello.md"), ("B", "/inbox/hello.md")]);
    let children = collision(tree.lookup("/inbox/hello.md").unwrap());
    for child in children {
        let DerivedEntry::Claim { name, claim } = child else {
            panic!("expected a claim entry, got {child:?}");
        };
        assert_eq!(
            *name,
            expected_derived(
                "hello.md",
                &claim.doc_id,
                claim.dpath.as_str(),
                claim.dpath.extension()
            )
        );
    }

    // A collision of directories keeps derived names free of extensions.
    let tree = derive(&[
        ("A", "/dir"),
        ("A", "/dir/a.txt"),
        ("B", "/dir"),
        ("B", "/dir/b.txt"),
    ]);
    for child in collision(tree.lookup("/dir").unwrap()) {
        let name = child.name();
        assert!(name.starts_with("dir~"), "got {name}");
        assert!(!name.ends_with(".txt"), "got {name}");
    }
}

#[test]
fn digest_is_base58_and_omits_ambiguous_letters() {
    // Q1 changed from hex to base58 (Bitcoin alphabet: `0 O I l` omitted). The
    // derived name component stays filename-safe, and 8 base58 characters carry
    // ~41 bits of the blake3 digest where 8 hex characters carried ~32.
    let full = identity_digest("A\u{0}/inbox");
    assert_eq!(full.len(), 44, "blake3 32B encodes to 44 base58 chars");
    for character in full.chars() {
        assert!(
            character.is_ascii_alphanumeric() && !"0OIl".contains(character),
            "digest must be base58 (Bitcoin alphabet), got {character} in {full}"
        );
    }
}

#[test]
fn claims_are_ordered_by_assignment_then_doc_id() {
    let mut late = raw("a-doc", "/notes/a.md");
    late.assigned_at = Timestamp::from_second(1_700_000_000).ok();
    let mut early = raw("z-doc", "/notes/b.md");
    early.assigned_at = Timestamp::from_second(1_600_000_000).ok();
    let unknown = raw("m-doc", "/notes/c.md");

    let tree = derive_tree(vec![late, unknown, early], &DpathNamePolicy::default());
    let order: Vec<&str> = tree
        .claims()
        .iter()
        .map(|claim| claim.doc_id.as_str())
        .collect();
    // Earlier assignment first, then lexicographic doc id, unknown last.
    assert_eq!(order, ["z-doc", "a-doc", "m-doc"]);
}

#[test]
fn derivation_is_input_order_independent() {
    let spec = [
        ("A", "/a"),
        ("C", "/a/b"),
        ("D", "/a.d"),
        ("E", "/a.d/x"),
        ("B", "/inbox/hello.md"),
        ("A2", "/inbox/hello.md"),
    ];
    let forward = derive(&spec);
    let mut reversed = spec;
    reversed.reverse();
    let backward = derive(&reversed);
    assert_eq!(forward.paths(), backward.paths());
    assert_eq!(forward, backward);
    assert_eq!(forward.paths(), derive(&spec).paths());
}

#[test]
fn malformed_claims_are_rejected_not_silently_dropped() {
    let tree = derive(&[
        ("good", "/a/b"),
        ("bad", "relative/path"),
        ("dot", "/a/../b"),
    ]);
    assert_eq!(tree.paths(), ["/a/", "/a/b"]);
    assert_eq!(tree.claims().len(), 1);
    let rejections = tree.rejections();
    assert_eq!(rejections.len(), 2);
    assert!(matches!(
        &rejections[0],
        DpathRejection::MalformedDpath {
            error: DpathError::MissingLeadingSlash { .. },
            ..
        }
    ));
    assert!(matches!(
        &rejections[1],
        DpathRejection::MalformedDpath {
            error: DpathError::DotSegment { .. },
            ..
        }
    ));
}

#[test]
fn duplicate_claims_are_reported() {
    let tree = derive(&[("A", "/a/b"), ("A", "/a/b"), ("B", "/a/b")]);
    // One facet per (doc, dpath): the duplicate is not a second opinion.
    assert_eq!(tree.claims().len(), 2);
    assert_eq!(tree.rejections().len(), 1);
    assert!(matches!(
        &tree.rejections()[0],
        DpathRejection::DuplicateClaim { doc_id, .. } if doc_id == "A"
    ));
    // `/a/b` now has two claimants, so it is a collision directory.
    assert!(tree.lookup("/a/b").is_some_and(|entry| matches!(
        entry,
        DerivedEntry::Dir {
            kind: DerivedDirKind::Collision,
            ..
        }
    )));
}

#[test]
fn empty_claim_set_derives_an_empty_tree() {
    let tree = derive_tree(Vec::new(), &DpathNamePolicy::default());
    assert!(tree.root().is_empty());
    assert!(tree.paths().is_empty());
    assert!(tree.claims().is_empty());
    assert!(tree.rejections().is_empty());
}

#[test]
fn degenerate_name_policy_still_terminates() {
    // Totality never depends on the naming policy being sane.
    let policy = DpathNamePolicy {
        spill_suffix: "",
        ..DpathNamePolicy::default()
    };
    let tree = derive_tree([raw("A", "/a"), raw("D", "/a.d")], &policy);
    let paths = tree.paths();
    assert_eq!(paths.len(), 2);
    assert_ne!(paths[0], paths[1]);
}

#[test]
fn deep_claims_do_not_disturb_siblings() {
    let tree = derive(&[
        ("A", "/notes"),
        ("B", "/notes"),
        ("C", "/notes/deep/er/file.md"),
        ("D", "/other.md"),
    ]);
    let children = collision(tree.lookup("/notes").unwrap());
    let names: Vec<&str> = children.iter().map(DerivedEntry::name).collect();
    assert_eq!(names.len(), 3);
    assert_eq!(
        names
            .iter()
            .filter(|name| name.starts_with("notes~"))
            .count(),
        2
    );
    // The deeper claimant only lives *under* the claimed name, so it keeps
    // its segment name inside the collision directory.
    assert!(tree.lookup("/notes/deep/er/file.md").is_some());
    assert!(tree.lookup("/other.md").is_some());
}
