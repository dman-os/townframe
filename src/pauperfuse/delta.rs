//! Deltas: how a change is described, and how two reps are compared.
//!
//! A delta is the unit a backend reports and a store applies. Two of its
//! variants carry different meanings, which is worth keeping straight:
//!
//! - [`Delta::Added`], [`Delta::Changed`] and [`Delta::Removed`] are *state*
//!   deltas, and comparing two reps produces only these;
//! - [`Delta::Touched`] is a *reporting* delta: one backend's own metadata
//!   (its stat fingerprint, its availability) moved while the content identity
//!   did not. Cross-backend comparison never produces one, because metadata
//!   like a stat fingerprint is expected to differ between an fs and a doc
//!   store, and reporting that forever would make every diff noisy.

use crate::backend::BackendId;
use crate::entry::{Avail, Entry};
use crate::interlude::*;
use crate::path::RelPath;
use crate::store::{RepScan, VtreeStore};

/// How many deltas one page of a [`DiffWalk`] carries.
const DIFF_PAGE: usize = 256;

/// One observed change to one path.
///
/// Entries are carried inline rather than boxed: a delta is consumed as soon as
/// it is observed, and boxing would put an allocation in the path of every
/// scanned change.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delta {
    /// The path is new, or newly known.
    Added {
        /// Where.
        path: RelPath,
        /// What is there.
        entry: Entry,
    },
    /// The path's content identity changed.
    Changed {
        /// Where.
        path: RelPath,
        /// What was recorded.
        from: Entry,
        /// What is there now.
        to: Entry,
    },
    /// The path is gone.
    Removed {
        /// Where.
        path: RelPath,
        /// What was recorded, for whoever needs to know what is being lost.
        entry: Entry,
    },
    /// The content identity is unchanged; the recorded metadata moved.
    Touched {
        /// Where.
        path: RelPath,
        /// What is recorded now.
        entry: Entry,
    },
}

impl Delta {
    /// The path this delta is about.
    #[must_use]
    pub fn path(&self) -> &RelPath {
        match self {
            Self::Added { path, .. }
            | Self::Changed { path, .. }
            | Self::Removed { path, .. }
            | Self::Touched { path, .. } => path,
        }
    }

    /// The entry that holds after this delta.
    #[must_use]
    pub fn entry(&self) -> &Entry {
        match self {
            Self::Added { entry, .. }
            | Self::Removed { entry, .. }
            | Self::Touched { entry, .. } => entry,
            Self::Changed { to, .. } => to,
        }
    }

    /// Whether the source has present bytes at this path.
    ///
    /// This is the cheap half of planning: it says there is something to ask
    /// about, not that anything moves. Whether the target wants it is the
    /// target's answer ([`Backend::accept`](crate::backend::Backend::accept)),
    /// because only a backend can tell "I already hold these bytes under another
    /// name" from "I need them" (ADR 010 §2.3).
    ///
    /// A stub is not bytes: a source offering a stub has nothing to send, and a
    /// target holding a stub is the case of "there should be a file here" that
    /// the target's own answer covers (ADR 010 §2.4).
    #[must_use]
    pub fn needs_transfer(&self) -> bool {
        match self {
            Self::Added { entry, .. } | Self::Changed { to: entry, .. } => {
                entry.avail == Avail::Present
            }
            Self::Removed { .. } | Self::Touched { .. } => false,
        }
    }
}

/// The merge join of two reps, in canonical path order (ADR 010 §3.3).
///
/// Deltas describe how to turn the left rep into the right one. Pages are
/// pulled one at a time, so comparing a million-entry rep against another costs
/// one page of memory and no hashing, rendering, or random IO.
pub struct DiffWalk {
    left: Side,
    right: Side,
}

struct Side {
    scan: RepScan,
    pending: Option<(RelPath, Entry)>,
}

impl Side {
    fn new(scan: RepScan) -> Self {
        Self {
            scan,
            pending: None,
        }
    }

    /// Ensure the next recorded entry is in hand, if there is one.
    async fn fill(&mut self) -> Result<()> {
        if self.pending.is_none() {
            self.pending = self.scan.take_next().await?;
        }
        Ok(())
    }
}

impl DiffWalk {
    /// Compare `left` against `right` from the start of both.
    #[must_use]
    pub fn new(store: Arc<dyn VtreeStore>, left: &BackendId, right: &BackendId) -> Self {
        Self {
            left: Side::new(RepScan::new(Arc::clone(&store), left.clone())),
            right: Side::new(RepScan::new(store, right.clone())),
        }
    }

    /// The next page of deltas, at most `max` of them. Empty means done.
    ///
    /// The page boundary is not a cursor anyone has to persist: the walk's own
    /// position is the pair of paths it has consumed, and a caller that wants
    /// crash-resumable bulk work keeps the last emitted path instead (ADR 011
    /// §6).
    pub async fn next_page(&mut self, max: usize) -> Result<Vec<Delta>> {
        let mut out = Vec::new();
        while out.len() < max {
            self.left.fill().await?;
            self.right.fill().await?;
            match (self.left.pending.take(), self.right.pending.take()) {
                (None, None) => break,
                (Some((path, entry)), None) => out.push(Delta::Removed { path, entry }),
                (None, Some((path, entry))) => out.push(Delta::Added { path, entry }),
                (Some((left, left_entry)), Some((right, right_entry))) => match left.cmp(&right) {
                    Ordering::Less => {
                        self.right.pending = Some((right, right_entry));
                        out.push(Delta::Removed {
                            path: left,
                            entry: left_entry,
                        });
                    }
                    Ordering::Greater => {
                        self.left.pending = Some((left, left_entry));
                        out.push(Delta::Added {
                            path: right,
                            entry: right_entry,
                        });
                    }
                    Ordering::Equal => {
                        if !left_entry.agrees_with(&right_entry) {
                            out.push(Delta::Changed {
                                path: left,
                                from: left_entry,
                                to: right_entry,
                            });
                        }
                    }
                },
            }
        }
        Ok(out)
    }

    /// Every delta, for callers that want the whole comparison in hand.
    pub async fn collect(&mut self) -> Result<Vec<Delta>> {
        let mut all = Vec::new();
        loop {
            let page = self.next_page(DIFF_PAGE).await?;
            if page.is_empty() {
                return Ok(all);
            }
            all.extend(page);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::{StatFingerprint, TimeStamp, Token};
    use crate::store::mem::MemVtreeStore;

    fn stat(len: u64) -> StatFingerprint {
        StatFingerprint {
            len,
            mode: 0o644,
            mtime: TimeStamp { secs: 1, nanos: 0 },
        }
    }

    fn path(text: &str) -> RelPath {
        RelPath::try_new(text.split('/').map(OsString::from).collect()).expect(ERROR_PARSE)
    }

    fn file(digest: u8) -> Entry {
        Entry::file(Token::blake3([digest; 32]), stat(1))
    }

    async fn store_with(
        rep: &str,
        entries: &[(&str, Entry)],
    ) -> Result<(Arc<dyn VtreeStore>, BackendId)> {
        let store: Arc<dyn VtreeStore> = Arc::new(MemVtreeStore::default());
        let id = BackendId::new(rep);
        let deltas = entries
            .iter()
            .map(|(path_text, entry)| Delta::Added {
                path: path(path_text),
                entry: entry.clone(),
            })
            .collect::<Vec<_>>();
        store.apply(&id, &deltas).await?;
        Ok((store, id))
    }

    #[tokio::test]
    async fn diff_reports_additions_removals_and_changes_only() -> Result<()> {
        let (store, left) = store_with(
            "left",
            &[
                ("gone.txt", file(1)),
                ("same.txt", file(2)),
                ("edit.txt", file(3)),
            ],
        )
        .await?;
        let right = BackendId::new("right");
        store
            .apply(
                &right,
                &[
                    Delta::Added {
                        path: path("new.txt"),
                        entry: file(9),
                    },
                    Delta::Added {
                        path: path("same.txt"),
                        entry: file(2),
                    },
                    // Same content, different recorded metadata: not a delta.
                    Delta::Added {
                        path: path("edit.txt"),
                        entry: file(3).with_stat(stat(99)),
                    },
                ],
            )
            .await?;

        let deltas = DiffWalk::new(store, &left, &right).collect().await?;
        assert_eq!(
            deltas
                .iter()
                .map(Delta::path)
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["gone.txt", "new.txt"],
            "path order — and edit.txt is not a delta, despite its stat moving"
        );
        assert!(matches!(&deltas[0], Delta::Removed { .. }));
        assert!(matches!(&deltas[1], Delta::Added { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn an_empty_diff_is_empty() -> Result<()> {
        let (store, left) = store_with("left", &[("a.txt", file(1)), ("b.txt", file(2))]).await?;
        let right = BackendId::new("right");
        for path_text in ["a.txt", "b.txt"] {
            store
                .apply(
                    &right,
                    &[Delta::Added {
                        path: path(path_text),
                        entry: file(if path_text == "a.txt" { 1 } else { 2 }),
                    }],
                )
                .await?;
        }
        assert!(
            DiffWalk::new(store, &left, &right)
                .collect()
                .await?
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_stub_needs_filling_even_when_the_content_matches() -> Result<()> {
        let (store, left) = store_with("left", &[("a.txt", file(1).stubbed())]).await?;
        let right = BackendId::new("right");
        store
            .apply(
                &right,
                &[Delta::Added {
                    path: path("a.txt"),
                    entry: file(1),
                }],
            )
            .await?;

        let deltas = DiffWalk::new(store, &left, &right).collect().await?;
        assert_eq!(deltas.len(), 1);
        assert!(deltas[0].needs_transfer(), "{deltas:#?}");
        Ok(())
    }

    #[tokio::test]
    async fn paging_covers_the_same_deltas_as_collecting() -> Result<()> {
        let (store, left) = store_with(
            "left",
            &[("a.txt", file(1)), ("b.txt", file(2)), ("c.txt", file(3))],
        )
        .await?;
        let right = BackendId::new("right");
        let mut walk = DiffWalk::new(store, &left, &right);
        let mut paged = Vec::new();
        loop {
            // One delta per page: the walk must not lose its place.
            let page = walk.next_page(1).await?;
            if page.is_empty() {
                break;
            }
            assert_eq!(page.len(), 1);
            paged.extend(page);
        }
        assert_eq!(paged.len(), 3);
        assert!(
            paged
                .iter()
                .all(|delta| matches!(delta, Delta::Removed { .. }))
        );
        Ok(())
    }

    #[test]
    fn transfer_need_follows_the_availability() {
        // "There is present content here for you to consider" — not "transfer it".
        // Whether the target wants it is the target's answer (`Backend::accept`),
        // because the core cannot tell two identities apart (ADR 010 §2.3).
        assert!(
            Delta::Changed {
                path: path("a.txt"),
                from: file(1),
                to: file(2),
            }
            .needs_transfer()
        );
        assert!(
            Delta::Added {
                path: path("a.txt"),
                entry: file(1),
            }
            .needs_transfer()
        );
        assert!(
            !Delta::Changed {
                path: path("a.txt"),
                from: file(1),
                to: file(2).stubbed(),
            }
            .needs_transfer(),
            "a stub is a promise, not bytes to send"
        );
        assert!(
            !Delta::Removed {
                path: path("a.txt"),
                entry: file(1),
            }
            .needs_transfer()
        );
        assert!(
            !Delta::Touched {
                path: path("a.txt"),
                entry: file(1),
            }
            .needs_transfer()
        );
    }
}
