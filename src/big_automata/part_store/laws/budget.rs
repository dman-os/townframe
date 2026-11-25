//! The byte budget of a leaf page: the budget half of the law of
//! [`PartStoreServe::bucket_leaves`].
//!
//! A page is the longest prefix of the bucket's entries within the limit and the budget; its
//! first entry is always in, however wide; and it stops before the entry that does not fit
//! instead of skipping it for a narrower one. The cases below walk a bucket page by page under
//! each of those edges and judge every page against a naive count over the whole listing.

use super::crowd::live;
use super::*;
use crate::part_store::LeafEntry;
use crate::part_store::LeafPage;

/// `count` distinct keys of `width` bytes, told apart by a leading counter from `from`.
fn keys(from: u16, count: u16, width: usize) -> Vec<ObjKey> {
    (from..from + count)
        .map(|at| {
            let mut bytes = at.to_be_bytes().to_vec();
            bytes.resize(width, 0);
            ObjKey::new(&bytes)
        })
        .collect()
}

fn cost(entry: &LeafEntry) -> usize {
    LeafEntry::wire_bytes(entry.obj.as_bytes().len())
}

/// What the pages of `all` must be under `limit` and `budget`, by the law's words: each page
/// takes the next entries one by one until the limit or the entry that would take it past the
/// budget, and never fewer than one.
fn expected_pages(all: &[LeafEntry], limit: usize, budget: usize) -> Vec<Vec<LeafEntry>> {
    let mut pages = Vec::new();
    let mut rest = all;
    while !rest.is_empty() {
        let mut len = 1;
        let mut bytes = cost(&rest[0]);
        while len < limit.min(rest.len()) && bytes + cost(&rest[len]) <= budget {
            bytes += cost(&rest[len]);
            len += 1;
        }
        pages.push(rest[..len].to_vec());
        rest = &rest[len..];
    }
    pages
}

/// Law: a leaf page is the longest prefix of the bucket's entries within the limit and the byte
/// budget, with its first entry always in and stopping rather than skipping.
pub async fn leaf_pages_are_the_longest_prefix_within_the_byte_budget<B: Backend>(
    backend: &B,
) -> Res<()> {
    // A narrow key, and a wide one, both far below and far above the budgets used.
    let narrow = |count| keys(0, count, 10);
    let mixed = {
        // The bucket's order is by hash, so the wide keys land between narrow ones.
        let mut all = keys(0, 4, 10);
        all.extend(keys(100, 4, 2000));
        all
    };
    let two_narrow = 2 * LeafEntry::wire_bytes(10);
    for (name, members, limit, budget) in [
        (
            "a budget of two and a bit",
            narrow(9),
            usize::MAX,
            two_narrow + 1,
        ),
        (
            "a budget a page fills exactly",
            narrow(9),
            usize::MAX,
            3 * LeafEntry::wire_bytes(10),
        ),
        ("the limit binds first", narrow(10), 4, usize::MAX),
        (
            "the budget binds below the limit",
            keys(0, 20, 1000),
            1024,
            3 * LeafEntry::wire_bytes(1000) + 1,
        ),
        (
            "keys wider than the budget",
            keys(0, 3, 5000),
            usize::MAX,
            100,
        ),
        (
            "a wide key among narrow ones",
            mixed,
            usize::MAX,
            two_narrow + 1,
        ),
    ] {
        let mut harness = Harness::open_over(backend, members.clone()).await?;
        harness.commit(vec![grant(&[(0, 0)])]).await?;
        harness.commit(live(&members, 1)).await?;
        let principal = principal(0);
        let list = |after: Option<ObjKey>, limit, budget| {
            let (store, principal) = (&harness.store, &principal);
            async move {
                eyre::Ok(
                    store
                        .bucket_leaves(
                            principal,
                            &part(0),
                            &[(BuckId::ROOT, after)],
                            0,
                            limit,
                            budget,
                        )
                        .await?
                        .expect("the part is readable")
                        .remove(0),
                )
            }
        };
        let whole = list(None, usize::MAX, usize::MAX).await?;
        assert!(!whole.more, "{name}: the whole listing finishes the bucket");
        assert_eq!(whole.entries.len(), members.len(), "{name}");
        if name.starts_with("a wide key") {
            // Skipping would step over a wide key that has a narrow one behind it.
            assert!(
                whole.entries.iter().enumerate().any(|(at, entry)| {
                    at > 0
                        && entry.obj.as_bytes().len() == 2000
                        && whole.entries[at + 1..]
                            .iter()
                            .any(|later| later.obj.as_bytes().len() == 10)
                }),
                "the keys' order does not straddle a wide key: {whole:?}"
            );
        }

        let mut pages = Vec::new();
        let mut after = None;
        loop {
            let LeafPage { entries, more } = list(after.clone(), limit, budget).await?;
            assert!(
                !entries.is_empty(),
                "{name}: an empty page reads as the bucket's end"
            );
            after = more.then(|| entries.last().expect("checked above").obj.clone());
            pages.push(entries);
            if after.is_none() {
                break;
            }
            assert!(
                pages.len() <= members.len(),
                "{name}: the walk does not advance"
            );
        }
        let expected = expected_pages(&whole.entries, limit, budget);
        assert_eq!(pages, expected, "{name}: pages");
        for page in &pages {
            assert!(
                page.len() == 1 || page.iter().map(cost).sum::<usize>() <= budget,
                "{name}: a page past the first entry is over the budget"
            );
        }
        assert_eq!(
            pages.concat(),
            whole.entries,
            "{name}: nothing is skipped, repeated or reordered"
        );
    }
    Ok(())
}
