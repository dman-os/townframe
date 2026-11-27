//! What must hold of the stored rows after any history, checked by reading the tables directly.

use std::collections::BTreeSet;

use sedimentree_core::blob::Blob;
use sedimentree_core::blob::has_meta::HasBlobMeta;
use sedimentree_core::crypto::digest::Digest;
use sedimentree_core::depth::CountLeadingZeroBytes;
use sedimentree_core::fragment::Fragment;
use sedimentree_core::loose_commit::LooseCommit;
use sedimentree_core::sedimentree::Sedimentree;
use sqlite_rs::Database;
use subduction_crypto::signed::Signed;

async fn column(db: &Database, sql: &str, name: &str) -> Vec<Vec<u8>> {
    db.query(sql, ())
        .await
        .unwrap()
        .rows
        .iter()
        .map(|row| row.get::<Vec<u8>>(name).unwrap())
        .collect()
}

/// Every table, row by row, for comparing a database with itself at another time.
pub(crate) async fn dump(db: &Database) -> Vec<String> {
    let mut lines = Vec::new();
    for (table, order) in [
        ("sedimentree_trees", "tree_id"),
        ("sedimentree_commits", "tree_id, commit_id"),
        ("sedimentree_fragments", "tree_id, fragment_head"),
    ] {
        let rows = db
            .query(&format!("SELECT * FROM {table} ORDER BY {order}"), ())
            .await
            .unwrap();
        for row in rows.rows {
            lines.push(format!("{table} {row:?}"));
        }
    }
    lines
}

/// The invariants of the module docs, from the file.
pub(crate) async fn assert_invariants(db: &Database) {
    // Rows have a live tree, and a dead tree has no rows (no foreign key says so).
    for table in ["sedimentree_commits", "sedimentree_fragments"] {
        let orphans = column(
            db,
            &format!(
                "SELECT tree_id FROM {table} WHERE tree_id NOT IN (SELECT tree_id FROM sedimentree_trees WHERE live = 1)"
            ),
            "tree_id",
        )
        .await;
        assert!(
            orphans.is_empty(),
            "{table} has rows of a tree that is not live"
        );
    }

    // `rev` is the hive's sequence: the latest is the table's maximum (uniqueness is the index's).
    let revs = db
        .query("SELECT rev FROM sedimentree_trees", ())
        .await
        .unwrap();
    let revs: BTreeSet<i64> = revs
        .rows
        .iter()
        .map(|row| row.get::<i64>("rev").unwrap())
        .collect();
    assert!(revs.iter().all(|rev| *rev > 0));

    for tree in column(
        db,
        "SELECT tree_id FROM sedimentree_trees WHERE live = 1",
        "tree_id",
    )
    .await
    {
        let hex = tree
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let commits = db
            .query(
                &format!("SELECT commit_id, digest, signed, blob FROM sedimentree_commits WHERE tree_id = x'{hex}'"),
                (),
            )
            .await
            .unwrap();
        let fragments = db
            .query(
                &format!("SELECT fragment_head, digest, signed, blob FROM sedimentree_fragments WHERE tree_id = x'{hex}'"),
                (),
            )
            .await
            .unwrap();
        let mut payload_commits = Vec::new();
        for row in &commits.rows {
            let payload = Signed::<LooseCommit>::try_decode(&row.get::<Vec<u8>>("signed").unwrap())
                .unwrap()
                .try_decode_trusted_payload()
                .unwrap();
            assert_eq!(
                payload.head().as_bytes().to_vec(),
                row.get::<Vec<u8>>("commit_id").unwrap()
            );
            assert_eq!(
                Digest::hash(&payload).as_bytes().to_vec(),
                row.get::<Vec<u8>>("digest").unwrap()
            );
            assert_eq!(
                Blob::new(row.get::<Vec<u8>>("blob").unwrap()).meta(),
                *payload.blob_meta()
            );
            payload_commits.push(payload);
        }
        let mut payload_fragments = Vec::new();
        for row in &fragments.rows {
            let payload = Signed::<Fragment>::try_decode(&row.get::<Vec<u8>>("signed").unwrap())
                .unwrap()
                .try_decode_trusted_payload()
                .unwrap();
            assert_eq!(
                payload.head().as_bytes().to_vec(),
                row.get::<Vec<u8>>("fragment_head").unwrap()
            );
            assert_eq!(
                Digest::hash(&payload).as_bytes().to_vec(),
                row.get::<Vec<u8>>("digest").unwrap()
            );
            assert_eq!(
                Blob::new(row.get::<Vec<u8>>("blob").unwrap()).meta(),
                payload.blob_meta()
            );
            payload_fragments.push(payload);
        }
        // Law 1: the rows are their own minimal form.
        let tree = Sedimentree::new(payload_fragments, payload_commits);
        assert!(
            tree.minimize(&CountLeadingZeroBytes) == tree,
            "the stored rows are not minimal"
        );
    }
}
