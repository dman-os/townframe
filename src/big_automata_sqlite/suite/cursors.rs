//! The peer cursor contract over the SQLite table, and what reopening adds.

use big_automata::driver::PeerCursors;
use big_automata::driver::table_laws::peer_cursor_table_laws;
use big_automata::part_store::Generation;
use big_automata::sync::PeerCursor;
use futures::executor::block_on;

use super::RUNTIME;
use crate::interlude::*;
use crate::*;

#[test]
fn the_table_obeys_the_peer_cursor_table_laws() -> Res<()> {
    block_on(async {
        let cursors = SqlitePeerCursors::new(RUNTIME.ephemeral(MIGRATIONS)?);
        peer_cursor_table_laws(&cursors).await
    })
}

/// Cursors are on disk: a reopened file reads what was written, generation included, and the
/// last write still wins after the reopen, a lower rev and another generation alike.
#[test]
fn cursors_survive_reopening() -> Res<()> {
    block_on(async {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("cursors.sqlite");
        let (peer, part) = (PeerKey::new(&[1]), PartKey::new(&[2]));
        let (old, new) = (Generation::mint(), Generation::mint());
        let at = |generation, rev| PeerCursor { generation, rev };
        let cursors = SqlitePeerCursors::new(RUNTIME.database(&path, MIGRATIONS, MIGRATION_TABLE));
        cursors
            .write(&peer, "main", &[(part.clone(), at(old, 7))])
            .await?;
        drop(cursors);

        let cursors = SqlitePeerCursors::new(RUNTIME.database(&path, MIGRATIONS, MIGRATION_TABLE));
        let read = async || {
            cursors
                .read(&peer, "main", std::slice::from_ref(&part))
                .await
        };
        assert_eq!(read().await?, vec![Some(at(old, 7))]);
        cursors
            .write(&peer, "main", &[(part.clone(), at(old, 3))])
            .await?;
        assert_eq!(read().await?, vec![Some(at(old, 3))]);
        cursors
            .write(&peer, "main", &[(part.clone(), at(new, 1))])
            .await?;
        assert_eq!(read().await?, vec![Some(at(new, 1))]);
        Ok(())
    })
}

/// The cursors and the part store share one database and one migration list without touching
/// each other's rows.
#[test]
fn cursors_share_a_database_with_the_part_store() -> Res<()> {
    block_on(async {
        let db = RUNTIME.ephemeral(MIGRATIONS)?;
        let store = SqlitePartStore::open(db.clone(), super::clock()).await?;
        let cursors = SqlitePeerCursors::new(db);
        let (peer, part) = (PeerKey::new(&[1]), PartKey::new(&[2]));
        let cursor = PeerCursor {
            generation: Generation::mint(),
            rev: 5,
        };
        cursors
            .write(&peer, "main", &[(part.clone(), cursor)])
            .await?;
        let mut tx = store.begin().await?;
        big_automata::part_store::laws::stage(
            &store,
            &mut tx,
            big_automata::part_store::laws::set(&[(0, 1)]),
        )
        .await?;
        tx.commit().await?;
        assert_eq!(
            cursors.read(&peer, "main", &[part]).await?,
            vec![Some(cursor)]
        );
        Ok(())
    })
}
