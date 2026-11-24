//! The driver's peer cursors in the same database as the part store.

use big_automata::driver::PeerCursors;
use big_automata::rev::Rev;
use sqlite_rs::Sql;

use crate::db::PartDb;
use crate::interlude::*;
use crate::keys::BlobKey;
use crate::sql;

/// [`PeerCursors`] over the `big_automata_peer_cursors` table. Clones share the database.
#[derive(Clone)]
pub struct SqlitePeerCursors<D: PartDb> {
    db: D,
}

impl<D: PartDb> SqlitePeerCursors<D> {
    /// `db` must have been registered with [`MIGRATIONS`](crate::MIGRATIONS) under
    /// [`MIGRATION_TABLE`](crate::MIGRATION_TABLE).
    pub fn new(db: D) -> Self {
        Self { db }
    }
}

impl<D: PartDb> PeerCursors for SqlitePeerCursors<D> {
    async fn read(&self, peer: &PeerKey, backend: &str, parts: &[PartKey]) -> Res<Vec<Rev>> {
        let reader = self.db.begin_read().await?;
        let mut found = BTreeMap::new();
        for chunk in sql::chunk_rows(parts.to_vec(), 1, 2) {
            let mut query = Sql::new(
                "SELECT part_key
                      , cursor
                   FROM big_automata_peer_cursors
                  WHERE peer = ",
            );
            query.bind(peer.blob()).push(" AND backend = ").bind(backend);
            query.push(" AND part_key IN ");
            query.bind_list(chunk.iter().map(|part| part.blob()));
            for (part, cursor) in query.query(&reader).await?.all::<(Vec<u8>, i64)>()? {
                found.insert(PartKey::from_blob(&part), sql::rev_from_sql(cursor));
            }
        }
        D::end_read(reader).await?;
        Ok(parts
            .iter()
            .map(|part| found.get(part).copied().unwrap_or(0))
            .collect())
    }

    /// One statement, so one atomic write: repeats of a part are coalesced to the highest
    /// first, and the upsert keeps the stored cursor when it is higher.
    async fn write(&self, peer: &PeerKey, backend: &str, writes: &[(PartKey, Rev)]) -> Res<()> {
        let mut coalesced = BTreeMap::<&PartKey, Rev>::new();
        for (part, cursor) in writes {
            let slot = coalesced.entry(part).or_default();
            *slot = (*slot).max(*cursor);
        }
        if coalesced.is_empty() {
            return Ok(());
        }
        let tx = self.db.begin_write().await?;
        let rows = coalesced
            .into_iter()
            .map(|(part, cursor)| {
                (
                    peer.blob(),
                    backend.to_owned(),
                    part.blob(),
                    sql::rev_to_sql(cursor),
                )
            })
            .collect();
        sql::execute_values(
            &tx,
            "INSERT INTO big_automata_peer_cursors (peer, backend, part_key, cursor) ",
            rows,
            4,
            " ON CONFLICT (peer, backend, part_key) DO UPDATE SET
                  cursor = MAX(cursor, excluded.cursor)",
        )
        .await?;
        D::commit(tx).await?;
        Ok(())
    }
}
