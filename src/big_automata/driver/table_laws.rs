//! The laws every [`PeerCursors`] table obeys, as a check an implementation's own tests call
//! with a fresh, empty table (the memory table here, the SQLite one in its crate).
//!
//! 1. A part never written reads `None`, and reads answer in the order asked.
//! 2. A write is read back whole: the generation with the rev.
//! 3. Within a generation a cursor only advances: a lower rev than stored is ignored, in a later
//!    write and within one write alike, and an equal one changes nothing.
//! 4. A write of another generation replaces the stored cursor whatever its rev, lower
//!    included: the peer's log is a different one, and the part restarts from 0.
//! 5. Entries apply in order: a write that holds an old generation after a new one leaves the old
//!    one stored, and one that holds both revs of a generation keeps the higher.
//! 6. Keys are (peer, backend, part): another peer's or backend's cursors do not meet.

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
    table
        .write(&peer, "b", &[(parts[1].clone(), at(old, 3))])
        .await?;
    assert_eq!(read(&peer, "b").await?[1], Some(at(old, 5)), "a lower rev");
    table
        .write(&peer, "b", &[(parts[1].clone(), at(old, 5))])
        .await?;
    assert_eq!(read(&peer, "b").await?[1], Some(at(old, 5)), "an equal rev");
    table
        .write(
            &peer,
            "b",
            &[
                (parts[1].clone(), at(old, 9)),
                (parts[1].clone(), at(old, 7)),
            ],
        )
        .await?;
    assert_eq!(read(&peer, "b").await?[1], Some(at(old, 9)), "one write");
    // 6
    assert_eq!(read(&other_peer, "b").await?, vec![None, None, None]);
    assert_eq!(read(&peer, "c").await?, vec![None, None, None]);
    // 4
    table
        .write(&peer, "b", &[(parts[1].clone(), at(new, 2))])
        .await?;
    assert_eq!(
        read(&peer, "b").await?[1],
        Some(at(new, 2)),
        "a new generation"
    );
    table
        .write(&peer, "b", &[(parts[1].clone(), at(new, 1))])
        .await?;
    assert_eq!(read(&peer, "b").await?[1], Some(at(new, 2)));
    // 5
    table
        .write(
            &peer,
            "b",
            &[
                (parts[0].clone(), at(new, 4)),
                (parts[0].clone(), at(old, 8)),
            ],
        )
        .await?;
    assert_eq!(read(&peer, "b").await?[0], Some(at(old, 8)), "in order");
    Ok(())
}
