//! Law 5's byte budget: a leaf page stops at [`LEAF_PAGE_BYTE_BUDGET`] (or a smaller one the
//! responder was built with), a key wider than the budget still yields one entry, and stopping
//! strands nothing. The differential in [`super::differential`] runs the same law over random
//! stores.

use super::*;
use crate::fingerprint::Fingerprint;
use crate::part_store::LEAF_PAGE_BYTE_BUDGET;

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

/// A rig whose part 1 (readable by the asker) holds `keys`, each live.
async fn holding(keys: &[ObjKey]) -> Rig {
    let mut rig = Rig::new();
    rig.commit(vec![
        PartWrite::SetPayloads(
            keys.iter()
                .map(|key| (key.clone(), serde_json::json!(1)))
                .collect(),
        ),
        PartWrite::AddMembers(keys.iter().map(|key| (key.clone(), part(1))).collect()),
        grant(&[(1, 0)]),
    ])
    .await
    .expect("the writes change a row");
    rig
}

/// Every page of the root bucket, followed by `next_after` from the start, asking for `hint`
/// entries each. Panics when a page is empty before the bucket is finished, or the walk does
/// not advance.
async fn walk(responder: &RpcResponder<CountingStore>, hint: u32) -> Vec<LeafBucketPage> {
    let mut pages = Vec::<LeafBucketPage>::new();
    let mut after = None;
    loop {
        let mut page = responder
            .leaf_buckets(
                &asker(),
                leaf_request(part(1), vec![(BuckId::ROOT, after.clone())], 0, Some(hint)),
            )
            .await
            .unwrap()
            .unwrap();
        let page = page.remove(&BuckId::ROOT).expect("a page for the bucket");
        assert!(
            !page.entries.is_empty() || page.next_after.is_none(),
            "an empty page reads as the bucket's end: {page:?}"
        );
        after = page.next_after.clone();
        pages.push(page);
        if after.is_none() {
            return pages;
        }
        assert!(pages.len() < 10_000, "the walk does not advance");
    }
}

fn cost(page: &LeafBucketPage) -> usize {
    page.entries
        .iter()
        .map(|entry| LeafEntry::wire_bytes(entry.obj_key.as_bytes().len()))
        .sum()
}

/// What the walk of `all` (the bucket's keys in its order) must page into under `budget` and
/// `cap`, by the law's words: each page is the next entries, one by one, until the cap or the
/// entry that would take the page past the budget, and never fewer than one.
fn expected_pages(all: &[ObjKey], budget: usize, cap: usize) -> Vec<Vec<ObjKey>> {
    let mut pages = Vec::new();
    let mut rest = all;
    while !rest.is_empty() {
        let mut len = 1;
        let mut bytes = LeafEntry::wire_bytes(rest[0].as_bytes().len());
        while len < cap.min(rest.len()) {
            let next = LeafEntry::wire_bytes(rest[len].as_bytes().len());
            if bytes + next > budget {
                break;
            }
            bytes += next;
            len += 1;
        }
        pages.push(rest[..len].to_vec());
        rest = &rest[len..];
    }
    pages
}

fn keys_of(pages: &[LeafBucketPage]) -> Vec<Vec<ObjKey>> {
    pages
        .iter()
        .map(|page| page.entries.iter().map(|e| e.obj_key.clone()).collect())
        .collect()
}

/// The bucket's keys in its leaf order, as the oracle holds it.
fn in_order(rig: &Rig) -> Vec<ObjKey> {
    members(rig, BuckId::ROOT)
        .into_iter()
        .map(|entry| entry.obj)
        .collect()
}

/// The arithmetic is the postcard framing of an entry: a varint length and the key, the tag of
/// the fingerprint, and the fingerprint, whose varint is one to ten bytes. The bound is the
/// widest, reached by a fingerprint with its top bit set; a dead entry has no fingerprint.
#[test]
fn wire_bytes_is_the_widest_postcard_framing_of_an_entry() {
    for (key_len, expected) in [
        (0, 12),
        (1, 13),
        (32, 44),
        (127, 139),
        (128, 141),
        (16_383, 16_396),
        (16_384, 16_398),
    ] {
        assert_eq!(LeafEntry::wire_bytes(key_len), expected, "{key_len}");
        let key = ObjKey::new(&vec![7; key_len]);
        let encoded = |fp| {
            postcard::to_allocvec(&BucketObjPageEntry {
                obj_key: key.clone(),
                fp,
            })
            .expect("an entry encodes")
            .len()
        };
        assert_eq!(
            encoded(Some(Fingerprint::from_u64(u64::MAX))),
            expected,
            "the widest fingerprint costs the whole bound"
        );
        assert_eq!(
            encoded(Some(Fingerprint::from_u64(0))),
            expected - 9,
            "a small fingerprint costs less"
        );
        assert_eq!(
            encoded(None),
            expected - 10,
            "a dead entry has no fingerprint"
        );
    }
    assert!(
        LEAF_PAGE_BYTE_BUDGET / LeafEntry::wire_bytes(32) > MAX_LEAF_PAGE as usize,
        "the entry cap binds before the budget for ordinary keys"
    );
    assert!(
        LEAF_PAGE_BYTE_BUDGET / LeafEntry::wire_bytes(4096) < MAX_LEAF_PAGE as usize,
        "the budget binds first for wide keys"
    );
}

#[test]
fn law_5_a_page_stops_at_the_byte_budget_and_the_next_resumes_after_its_last_entry() {
    block_on(async {
        // 1013 bytes an entry: a budget of three and a bit entries, against a hint of 1024.
        let wide = keys(0, 20, 1000);
        let budget = 3 * LeafEntry::wire_bytes(1000) + 1;
        let rig = holding(&wide).await;
        let responder = rig.responder().with_leaf_byte_budget(budget);
        let pages = walk(&responder, 1024).await;

        assert_eq!(
            pages
                .iter()
                .map(|page| page.entries.len())
                .collect::<Vec<_>>(),
            [3, 3, 3, 3, 3, 3, 2],
            "the budget binds below the hint"
        );
        for page in &pages {
            assert!(cost(page) <= budget, "{} over {budget}", cost(page));
        }
        for (page, next) in pages.iter().zip(pages.iter().skip(1)) {
            assert_eq!(
                page.next_after.as_ref(),
                page.entries.last().map(|entry| &entry.obj_key),
                "a page that stopped for bytes resumes after its last entry"
            );
            assert_eq!(
                next.after, page.next_after,
                "and the next page answers from it"
            );
        }
        assert_eq!(pages.last().unwrap().next_after, None);
        let walked = keys_of(&pages).concat();
        assert_eq!(
            walked,
            in_order(&rig),
            "nothing is stranded, repeated or reordered"
        );
    })
}

#[test]
fn law_5_a_key_wider_than_the_budget_still_yields_one_entry_per_page() {
    block_on(async {
        let huge = keys(0, 3, 5000);
        let rig = holding(&huge).await;
        let responder = rig.responder().with_leaf_byte_budget(100);
        let pages = walk(&responder, 1024).await;
        assert_eq!(
            pages
                .iter()
                .map(|page| page.entries.len())
                .collect::<Vec<_>>(),
            [1, 1, 1],
            "each key is over the budget by itself and still goes out"
        );
        assert!(pages[0].next_after.is_some() && pages[1].next_after.is_some());
        assert_eq!(pages[2].next_after, None);
        assert_eq!(keys_of(&pages).concat(), in_order(&rig));
    })
}

/// A page stops before the entry that does not fit and the next starts with it: it does not
/// skip ahead to a narrower entry, which would put `next_after` past the wide one for good.
#[test]
fn law_5_a_page_stops_before_a_wide_entry_instead_of_skipping_it() {
    block_on(async {
        // A wide key between narrow ones, whichever order the hash puts them in: the same four
        // narrow and four wide keys, with the budget fitting the narrow ones together but not
        // a wide one beside them.
        let mut all = keys(0, 4, 10);
        all.extend(keys(100, 4, 2000));
        let budget = 2 * LeafEntry::wire_bytes(10) + 1;
        let rig = holding(&all).await;
        let order = in_order(&rig);
        let pages = walk(&rig.responder().with_leaf_byte_budget(budget), 1024).await;
        assert_eq!(keys_of(&pages), expected_pages(&order, budget, 1024));
        assert_eq!(keys_of(&pages).concat(), order, "no key is stepped over");
        assert!(
            order.iter().enumerate().any(|(at, key)| {
                at > 0
                    && key.as_bytes().len() == 2000
                    && order[at + 1..].iter().any(|key| key.as_bytes().len() == 10)
            }),
            "a wide key after the first, with a narrow one behind it, is what skipping would \
             step over: {order:?}"
        );
    })
}

#[test]
fn law_5_the_default_budget_binds_for_wide_keys_and_the_cap_for_ordinary_ones() {
    block_on(async {
        // 4109 bytes an entry: fifteen fit in 64 KiB, against a hint and a cap far above.
        let wide = keys(0, 20, 4096);
        let rig = holding(&wide).await;
        let pages = walk(&rig.responder(), u32::MAX).await;
        assert_eq!(
            pages
                .iter()
                .map(|page| page.entries.len())
                .collect::<Vec<_>>(),
            [15, 5]
        );
        assert!(cost(&pages[0]) <= LEAF_PAGE_BYTE_BUDGET);
        assert_eq!(keys_of(&pages).concat(), in_order(&rig));

        // Ordinary 32-byte keys: the entry cap ends the page well inside the budget.
        let ordinary = keys(0, 1100, 32);
        let rig = holding(&ordinary).await;
        let pages = walk(&rig.responder(), u32::MAX).await;
        assert_eq!(
            pages
                .iter()
                .map(|page| page.entries.len())
                .collect::<Vec<_>>(),
            [MAX_LEAF_PAGE as usize, 1100 - MAX_LEAF_PAGE as usize]
        );

        // A key wider than the whole budget goes out alone, and the rest after it.
        let mut mixed = keys(0, 3, 32);
        mixed.extend(keys(500, 1, 80 * 1024));
        let rig = holding(&mixed).await;
        let pages = walk(&rig.responder(), 1024).await;
        assert_eq!(keys_of(&pages).concat(), in_order(&rig));
        assert!(
            pages
                .iter()
                .any(|page| page.entries.len() == 1 && cost(page) > LEAF_PAGE_BYTE_BUDGET),
            "the oversized key is a page of its own"
        );
    })
}

#[test]
#[should_panic(expected = "a leaf byte budget is within")]
fn a_leaf_byte_budget_of_nothing_is_refused() {
    drop(Rig::new().responder().with_leaf_byte_budget(0));
}

#[test]
#[should_panic(expected = "a leaf byte budget is within")]
fn a_leaf_byte_budget_above_the_protocol_maximum_is_refused() {
    drop(
        Rig::new()
            .responder()
            .with_leaf_byte_budget(LEAF_PAGE_BYTE_BUDGET + 1),
    );
}
