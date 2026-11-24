//! The contract every [`PeerCursors`] table obeys, as a harness the implementations' own tests
//! call (`MemoryPeerCursors` here, the SQLite table in `big_automata_sqlite`).
//!
//! | Law | Where |
//! | --- | --- |
//! | a part never written reads 0 | [`unknown_parts_read_zero`] |
//! | a write is read back, in the order the parts were asked, repeats included | [`writes_read_back_in_asked_order`] |
//! | a cursor never lowers, across writes and inside one write | [`cursors_never_lower`] |
//! | repeats of a part inside one write coalesce to the highest | [`repeats_coalesce_to_the_highest`] |
//! | cursors are scoped by (peer, backend, part) | [`cursors_are_scoped`] |
//! | empty reads and writes are fine | [`empty_calls_are_fine`] |

use crate::driver::PeerCursors;
use crate::interlude::*;
use crate::rev::Rev;

/// What an implementation supplies: a new, empty table.
#[async_trait]
pub trait CursorBackend: Send + Sync {
    type Cursors: PeerCursors;
    async fn open(&self) -> Res<Self::Cursors>;
}

fn part(index: u8) -> PartKey {
    PartKey::new(&[index])
}
fn peer(index: u8) -> PeerKey {
    PeerKey::new(&[200 + index])
}

async fn get<C: PeerCursors>(cursors: &C, peer: &PeerKey, backend: &str, index: u8) -> Res<Rev> {
    Ok(cursors.read(peer, backend, &[part(index)]).await?[0])
}

pub async fn unknown_parts_read_zero<B: CursorBackend>(backend: &B) -> Res<()> {
    let cursors = backend.open().await?;
    assert_eq!(
        cursors.read(&peer(0), "main", &[part(0), part(1)]).await?,
        vec![0, 0]
    );
    Ok(())
}

pub async fn writes_read_back_in_asked_order<B: CursorBackend>(backend: &B) -> Res<()> {
    let cursors = backend.open().await?;
    cursors
        .write(
            &peer(0),
            "main",
            &[(part(0), 3), (part(1), 7), (part(2), 5)],
        )
        .await?;
    assert_eq!(
        cursors
            .read(
                &peer(0),
                "main",
                &[part(2), part(0), part(3), part(1), part(0)]
            )
            .await?,
        vec![5, 3, 0, 7, 3]
    );
    Ok(())
}

pub async fn cursors_never_lower<B: CursorBackend>(backend: &B) -> Res<()> {
    let cursors = backend.open().await?;
    cursors.write(&peer(0), "main", &[(part(0), 5)]).await?;
    cursors.write(&peer(0), "main", &[(part(0), 3)]).await?;
    assert_eq!(get(&cursors, &peer(0), "main", 0).await?, 5);
    cursors
        .write(&peer(0), "main", &[(part(0), 9), (part(0), 4)])
        .await?;
    assert_eq!(get(&cursors, &peer(0), "main", 0).await?, 9);
    cursors.write(&peer(0), "main", &[(part(0), 0)]).await?;
    assert_eq!(get(&cursors, &peer(0), "main", 0).await?, 9);
    cursors.write(&peer(0), "main", &[(part(0), 10)]).await?;
    assert_eq!(get(&cursors, &peer(0), "main", 0).await?, 10);
    Ok(())
}

pub async fn repeats_coalesce_to_the_highest<B: CursorBackend>(backend: &B) -> Res<()> {
    let cursors = backend.open().await?;
    cursors
        .write(
            &peer(0),
            "main",
            &[
                (part(0), 2),
                (part(1), 1),
                (part(0), 9),
                (part(0), 5),
                (part(1), 4),
            ],
        )
        .await?;
    assert_eq!(
        cursors.read(&peer(0), "main", &[part(0), part(1)]).await?,
        vec![9, 4]
    );
    Ok(())
}

pub async fn cursors_are_scoped<B: CursorBackend>(backend: &B) -> Res<()> {
    let cursors = backend.open().await?;
    cursors.write(&peer(0), "main", &[(part(0), 4)]).await?;
    assert_eq!(get(&cursors, &peer(1), "main", 0).await?, 0, "another peer");
    assert_eq!(
        get(&cursors, &peer(0), "other", 0).await?,
        0,
        "another backend"
    );
    assert_eq!(get(&cursors, &peer(0), "main", 1).await?, 0, "another part");
    cursors.write(&peer(1), "main", &[(part(0), 1)]).await?;
    cursors.write(&peer(0), "other", &[(part(0), 2)]).await?;
    assert_eq!(get(&cursors, &peer(0), "main", 0).await?, 4);
    assert_eq!(get(&cursors, &peer(1), "main", 0).await?, 1);
    assert_eq!(get(&cursors, &peer(0), "other", 0).await?, 2);
    Ok(())
}

pub async fn empty_calls_are_fine<B: CursorBackend>(backend: &B) -> Res<()> {
    let cursors = backend.open().await?;
    cursors.write(&peer(0), "main", &[]).await?;
    assert_eq!(
        cursors.read(&peer(0), "main", &[]).await?,
        Vec::<Rev>::new()
    );
    Ok(())
}
