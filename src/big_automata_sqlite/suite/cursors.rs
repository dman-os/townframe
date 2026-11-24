//! The peer cursor contract over the SQLite table, and what reopening adds.

use big_automata::driver::PeerCursors;
use big_automata::driver::cursor_laws::CursorBackend;
use big_automata::driver::cursor_laws::{self};
use futures::executor::block_on;
use sqlite_rs::Database;

use super::RUNTIME;
use crate::interlude::*;
use crate::*;

struct Sqlite;

#[async_trait]
impl CursorBackend for Sqlite {
    type Cursors = SqlitePeerCursors<Database>;

    async fn open(&self) -> Res<Self::Cursors> {
        Ok(SqlitePeerCursors::new(RUNTIME.ephemeral(MIGRATIONS)?))
    }
}

macro_rules! cursor_laws {
    ($($law:ident),+ $(,)?) => {$(
        #[test]
        fn $law() -> Res<()> {
            block_on(cursor_laws::$law(&Sqlite))
        }
    )+};
}

cursor_laws!(
    unknown_parts_read_zero,
    writes_read_back_in_asked_order,
    cursors_never_lower,
    repeats_coalesce_to_the_highest,
    cursors_are_scoped,
    empty_calls_are_fine,
);

/// Cursors are on disk: a reopened file reads what was written, and a lower write after the
/// reopen still does not lower it.
#[test]
fn cursors_survive_reopening() -> Res<()> {
    block_on(async {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("cursors.sqlite");
        let (peer, part) = (PeerKey::new(&[1]), PartKey::new(&[2]));
        let cursors = SqlitePeerCursors::new(RUNTIME.database(&path, MIGRATIONS, MIGRATION_TABLE));
        cursors.write(&peer, "main", &[(part.clone(), 7)]).await?;
        drop(cursors);

        let cursors = SqlitePeerCursors::new(RUNTIME.database(&path, MIGRATIONS, MIGRATION_TABLE));
        assert_eq!(
            cursors
                .read(&peer, "main", std::slice::from_ref(&part))
                .await?,
            vec![7]
        );
        cursors.write(&peer, "main", &[(part.clone(), 3)]).await?;
        assert_eq!(
            cursors
                .read(&peer, "main", std::slice::from_ref(&part))
                .await?,
            vec![7]
        );
        cursors.write(&peer, "main", &[(part.clone(), 8)]).await?;
        assert_eq!(cursors.read(&peer, "main", &[part]).await?, vec![8]);
        Ok(())
    })
}

/// The cursors and the part store share one database and one migration list without touching
/// each other's rows.
#[test]
fn cursors_share_a_database_with_the_part_store() -> Res<()> {
    block_on(async {
        let db = RUNTIME.ephemeral(MIGRATIONS)?;
        let store = SqlitePartStore::open(db.clone()).await?;
        let cursors = SqlitePeerCursors::new(db);
        let (peer, part) = (PeerKey::new(&[1]), PartKey::new(&[2]));
        cursors.write(&peer, "main", &[(part.clone(), 5)]).await?;
        let mut tx = store.begin().await?;
        big_automata::part_store::laws::stage(
            &store,
            &mut tx,
            big_automata::part_store::laws::set(&[(0, 1)]),
        )
        .await?;
        tx.commit().await?;
        assert_eq!(cursors.read(&peer, "main", &[part]).await?, vec![5]);
        Ok(())
    })
}
