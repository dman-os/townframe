//! The laws every [`PeerCursors`] table obeys, as a check an implementation's own tests call
//! with a fresh, empty table (the memory table here, the SQLite one in its crate).
//!
//! 1. A part never written reads `None`, and reads answer in the order asked.
//! 2. A write is read back whole: the generation with the rev.
//! 3. **The last write wins**, whether its rev is higher, equal or lower, and whether its
//!    generation is the stored one or another. The table does not compare revs: the driver
//!    issues a part's writes in order and the machine never lowers a cursor, except by
//!    restarting a part (`PartLifecycle` law 10), whose first persisted cursor is below the one
//!    stored, in the same generation when a restore went unminted. A table that kept the
//!    higher rev would store the old cursor again and resume from it after the next
//!    reconnect, skipping what the peer's new log stamped below it.
//! 4. The entries of one write apply in order, so the last entry for a part is the one stored.
//! 5. Keys are (peer, backend, part): another peer's or backend's cursors do not meet.

use super::PeerCursors;
use crate::interlude::*;
use crate::part_store::Generation;
use crate::sync::PeerCursor;

/// Run every law above against `table`, which must be empty.
pub async fn peer_cursor_table_laws<K: PeerCursors>(table: &K) -> Res<()> {
    let peer = PeerKey::new(&[1; 32]);
    let other_peer = PeerKey::new(&[2; 32]);
    let parts = [PartKey::new(&[1]), PartKey::new(&[2]), PartKey::new(&[3])];
    let (old, new) = (Generation::mint(), Generation::mint());
    let at = |generation, rev| PeerCursor { generation, rev };
    let read = |peer: &PeerKey, backend: &str| {
        let parts = parts.clone();
        let peer = peer.clone();
        let backend = backend.to_owned();
        async move { table.read(&peer, &backend, &parts).await }
    };

    // 1
    assert_eq!(read(&peer, "b").await?, vec![None, None, None]);
    // 2
    table
        .write(&peer, "b", &[(parts[1].clone(), at(old, 5))])
        .await?;
    assert_eq!(read(&peer, "b").await?, vec![None, Some(at(old, 5)), None]);
    // 3
    for (written, why) in [
        (at(old, 9), "a higher rev"),
        (at(old, 9), "an equal rev"),
        (at(old, 3), "a lower rev"),
        (at(new, 2), "another generation"),
        (at(new, 1), "a lower rev of that generation"),
        (at(old, 8), "a lower rev, back in the first generation"),
    ] {
        table
            .write(&peer, "b", &[(parts[1].clone(), written)])
            .await?;
        assert_eq!(read(&peer, "b").await?[1], Some(written), "{why}");
    }
    // 4
    table
        .write(
            &peer,
            "b",
            &[
                (parts[0].clone(), at(new, 4)),
                (parts[0].clone(), at(old, 2)),
                (parts[2].clone(), at(old, 6)),
                (parts[2].clone(), at(old, 7)),
            ],
        )
        .await?;
    let read_back = read(&peer, "b").await?;
    assert_eq!(read_back[0], Some(at(old, 2)), "in order");
    assert_eq!(read_back[2], Some(at(old, 7)), "in order");
    // 5
    assert_eq!(read(&other_peer, "b").await?, vec![None, None, None]);
    assert_eq!(read(&peer, "c").await?, vec![None, None, None]);
    Ok(())
}
