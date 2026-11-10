use super::*;
use futures::FutureExt;
use std::collections::BTreeSet;

async fn state() -> Res<State> {
    let sql = sqlite_rs::SqliteRuntime::new(Default::default()).ephemeral(&[])?;
    sql.execute(
        "CREATE TABLE big_blobs_writer_claims (
             doc_id TEXT NOT NULL
           , drawer_id TEXT NOT NULL
           , root TEXT NOT NULL CHECK (root IN ('public', 'sensitive'))
           , name TEXT NOT NULL
           , inventory_id TEXT NOT NULL
           , digest TEXT NOT NULL
           , length_octets INTEGER NOT NULL
           , PRIMARY KEY (doc_id, drawer_id, root, name, inventory_id, digest)
         ) STRICT",
        (),
    )
    .await?;
    State::new(sql).await
}

async fn insert_claim(
    state: &State,
    inventory: &DocId,
    digest: BlobId,
    [document, drawer, root, name]: [&str; 4],
) -> Res<()> {
    state
        .sql
        .execute(
            "INSERT INTO big_blobs_writer_claims (
                 doc_id
               , drawer_id
               , root
               , name
               , inventory_id
               , digest
               , length_octets
             ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                document,
                drawer,
                root,
                name,
                inventory.to_string(),
                digest.to_string(),
                42_i64,
            ),
        )
        .await?;
    Ok(())
}

async fn owners(state: &State, inventory: &DocId, digest: BlobId) -> Res<BTreeSet<String>> {
    Ok(state
        .sql
        .query(
            "SELECT owner
               FROM big_blobs_producer_claims
              WHERE inventory_id = ? AND digest = ?",
            (inventory.to_string(), digest.to_string()),
        )
        .await?
        .all::<String>()?
        .into_iter()
        .collect())
}

#[tokio::test]
async fn producer_refresh_preserves_each_source_identity_and_unrelated_pairs() -> Res<()> {
    let state = state().await?;
    let inventory = DocId::new([111; 32]);
    let other_inventory = DocId::new([112; 32]);
    let digest = BlobId::new([113; 32]);
    let other_digest = BlobId::new([114; 32]);
    let document = DocId::new([115; 32]).to_string();
    let other_document = DocId::new([116; 32]).to_string();
    let drawer = DocId::new([117; 32]).to_string();
    let other_drawer = DocId::new([118; 32]).to_string();
    let sources: [[&str; 4]; 5] = [
        [&document, &drawer, "public", "slot:\"one\"\\two\n"],
        [&other_document, &drawer, "public", "slot:\"one\"\\two\n"],
        [&document, &other_drawer, "public", "slot:\"one\"\\two\n"],
        [&document, &drawer, "sensitive", "slot:\"one\"\\two\n"],
        [&document, &drawer, "public", "another-slot"],
    ];
    for source in sources {
        insert_claim(&state, &inventory, digest, source).await?;
    }
    insert_claim(&state, &other_inventory, digest, sources[0]).await?;
    insert_claim(&state, &inventory, other_digest, sources[0]).await?;
    state.refresh_producers(&inventory).await?;
    state.refresh_producers(&other_inventory).await?;
    let initial = state.state(&inventory, digest).await?.unwrap();
    let other_pair = state.state(&inventory, other_digest).await?;
    let other_scope = state.state(&other_inventory, digest).await?;
    let mut expected = sources
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<BTreeSet<_>, _>>()?;
    assert_eq!(owners(&state, &inventory, digest).await?, expected);
    for (index, source @ [document, drawer, root, name]) in sources.into_iter().enumerate() {
        state
            .sql
            .execute(
                "DELETE FROM big_blobs_writer_claims
                  WHERE inventory_id = ? AND digest = ?
                    AND doc_id = ? AND drawer_id = ? AND root = ? AND name = ?",
                (
                    inventory.to_string(),
                    digest.to_string(),
                    document,
                    drawer,
                    root,
                    name,
                ),
            )
            .await?;
        state.refresh_producers(&inventory).await?;
        assert!(expected.remove(&serde_json::to_string(&source)?));
        assert_eq!(owners(&state, &inventory, digest).await?, expected);
        let pair = state.state(&inventory, digest).await?.unwrap();
        assert_eq!(pair.writer, !expected.is_empty());
        assert_eq!(pair.generation, initial.generation + index as u64 + 1);
        assert_eq!(state.state(&inventory, other_digest).await?, other_pair);
        assert_eq!(state.state(&other_inventory, digest).await?, other_scope);
        let independent = BTreeSet::from([serde_json::to_string(&sources[0])?]);
        assert_eq!(owners(&state, &inventory, other_digest).await?, independent);
        assert_eq!(owners(&state, &other_inventory, digest).await?, independent);
    }
    Ok(())
}

#[tokio::test]
async fn producer_no_op_refresh_preserves_attempt_and_does_not_rearm_or_notify() -> Res<()> {
    let state = state().await?;
    let inventory = DocId::new([119; 32]);
    let digest = BlobId::new([120; 32]);
    let document = DocId::new([121; 32]).to_string();
    let drawer = DocId::new([122; 32]).to_string();
    insert_claim(
        &state,
        &inventory,
        digest,
        [&document, &drawer, "public", "slot"],
    )
    .await?;
    state.select(&inventory, "mirror", true).await?;
    state.authorize(&inventory, true).await?;
    state
        .pins(&inventory, 1, &BTreeMap::from([(digest, 42)]), true)
        .await?;
    state
        .observe(&inventory, digest, &PeerKey::new([123; 32]), 1, true, None)
        .await?;
    let mut changes = state.changes.subscribe();
    state.refresh_producers(&inventory).await?;
    assert!(changes.has_changed()?);
    changes.borrow_and_update();
    assert!(state.state(&inventory, digest).await?.unwrap().writer);
    let attempt = state.begin(&inventory, digest).await?.unwrap();
    let before = state.state(&inventory, digest).await?.unwrap();
    assert!(before.writer && before.fetching);
    state.settle(&inventory, digest, before.generation).await?;
    assert!(state.dirty().await?.is_empty());
    assert!(state.changed.notified().now_or_never().is_some());
    assert!(state.changed.notified().now_or_never().is_none());
    changes.borrow_and_update();
    for _ in 0..2 {
        state.refresh_producers(&inventory).await?;
        assert!(!changes.has_changed()?);
        assert_eq!(state.state(&inventory, digest).await?, Some(before.clone()));
        assert!(state.dirty().await?.is_empty());
        assert!(state.changed.notified().now_or_never().is_none());
    }
    assert!(state.complete(&attempt, true).await?);
    Ok(())
}
