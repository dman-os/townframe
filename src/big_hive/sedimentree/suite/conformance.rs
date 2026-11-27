//! Subduction's own `MinimizedTreeStore` conformance checks, run against the SQL-backed store.
//!
//! The checks populate and merge trees that storage never held, so they exercise the
//! generation rules (1 to 3) and a store that must never serve what the database does not hold;
//! the reads that do hit the database are checked by the differential.

use future_form::Sendable;
use futures::executor::block_on;
use proptest::collection::vec;
use proptest::prelude::*;
use subduction_core::minimized_tree_store::conformance::Handle;
use subduction_core::minimized_tree_store::conformance::Op;
use subduction_core::minimized_tree_store::conformance::TREES;
use subduction_core::minimized_tree_store::conformance::Tree;
use subduction_core::minimized_tree_store::conformance::check_ops;
use subduction_core::minimized_tree_store::conformance::check_shared_store;

use super::fresh_db;
use super::store_on;
use super::tree_id;

fn op() -> impl Strategy<Value = Op> {
    let handle = prop_oneof![Just(Handle::A), Just(Handle::B)];
    let tree = prop_oneof![Just(Tree::First), Just(Tree::Second), Just(Tree::Third)];
    (0..4_u8, handle, tree).prop_map(|(kind, handle, tree)| match kind {
        0 => Op::Get(handle, tree),
        1 => Op::Populate(handle, tree),
        2 => Op::Merge(handle, tree),
        _ => Op::Remove(handle, tree),
    })
}

#[test]
fn the_store_passes_the_shared_store_checks() {
    let store = store_on(&fresh_db());
    let mut next = 0u8;
    block_on(check_shared_store::<Sendable, _, _>(
        &store,
        &store.clone(),
        || {
            next += 1;
            tree_id(100 + next)
        },
    ));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn generated_operation_sequences_match_the_model(ops in vec(op(), 0..40)) {
        let ids: [_; TREES] = [tree_id(1), tree_id(2), tree_id(3)];
        let store = store_on(&fresh_db());
        block_on(check_ops::<Sendable, _>(&store, &store.clone(), ids, &ops));
    }
}
