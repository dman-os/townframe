use super::*;

#[tokio::test]
async fn memory_leaf_source_revisions() -> Res<()> {
    host_contract::assert_leaf_source_revision_contract(&MemoryPartStore::new()).await
}

/// The leaf page's byte budget, which the store applies beside the entry hint.
#[tokio::test(flavor = "multi_thread")]
async fn memory_leaf_page_byte_budget_contract() -> Res<()> {
    host_contract::assert_leaf_page_byte_budget_contract(&MemoryPartStore::new()).await
}
