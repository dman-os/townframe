//! The driver's peer cursors in the same database as the part store.

use big_automata::driver::PeerCursors;
use big_automata::sync::PeerCursor;
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
    async fn read(
        &self,
        peer: &PeerKey,
        backend: &str,
        parts: &[PartKey],
    ) -> Res<Vec<Option<PeerCursor>>> {
        let reader = self.db.begin_read().await?;
        let mut found = BTreeMap::new();
        for chunk in sql::chunk_rows(parts.to_vec(), 1, 2) {
            let mut query = Sql::new(
                "SELECT part_key
                      , cursor
                      , generation
                   FROM big_automata_peer_cursors
                  WHERE peer = ",
            );
            query
                .bind(peer.blob())
                .push(" AND backend = ")
                .bind(backend);
            query.push(" AND part_key IN ");
            query.bind_list(chunk.iter().map(|part| part.blob()));
            for (part, cursor, generation) in query
                .query(&reader)
                .await?
                .all::<(Vec<u8>, i64, Vec<u8>)>()?
            {
                found.insert(
                    PartKey::from_blob(&part),
                    PeerCursor {
                        generation: sql::generation_from_sql(&generation),
                        rev: sql::rev_from_sql(cursor),
                    },
                );
            }
        }
        D::end_read(reader).await?;
        Ok(parts.iter().map(|part| found.get(part).copied()).collect())
    }

    /// Last write wins: the upsert replaces the row whatever its rev or generation. A statement
    /// applies its `VALUES` rows in order, and a long batch is several statements in order in
    /// one transaction, so the last entry for a part is the one stored and the write is atomic.
    async fn write(
        &self,
        peer: &PeerKey,
        backend: &str,
        writes: &[(PartKey, PeerCursor)],
    ) -> Res<()> {
        if writes.is_empty() {
            return Ok(());
        }
        let tx = self.db.begin_write().await?;
        let rows = writes
            .iter()
            .map(|(part, cursor)| {
                (
                    peer.blob(),
                    backend.to_owned(),
                    part.blob(),
                    sql::rev_to_sql(cursor.rev),
                    sql::generation_to_sql(cursor.generation),
                )
            })
            .collect();
        sql::execute_values(
            &tx,
            "INSERT INTO big_automata_peer_cursors (peer, backend, part_key, cursor, generation) ",
            rows,
            5,
            " ON CONFLICT (peer, backend, part_key) DO UPDATE SET
                  cursor = excluded.cursor
                , generation = excluded.generation",
        )
        .await?;
        D::commit(tx).await?;
        Ok(())
    }
}
