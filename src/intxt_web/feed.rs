//! The conversation: every item of an intxt in one list, newest first.
//!
//! The feed is derived from the [`IntxtView`], so every host that has the view has the same
//! feed. An item is placed by the time its own datums carry (see [`EntryView::at`] and the
//! comment and commit times); an item whose datums carry none goes after the dated ones, in the
//! order the intxt lists it, because inventing a time would put it where it never was.

use crate::view::EntryKind;
use crate::view::IntxtView;
use jiff::SignedDuration;
use jiff::Timestamp;
use std::cmp::Reverse;
use std::collections::BTreeSet;

/// Consecutive commit items by one author fold into one when each is within this long of the
/// previous.
pub const BURST_WINDOW: SignedDuration = SignedDuration::from_mins(30);

#[derive(Debug, Clone, PartialEq)]
pub struct FeedItem {
    /// The item's time, newest of a burst; none when its datums carry none.
    pub at: Option<Timestamp>,
    pub actor: Option<String>,
    pub what: FeedKind,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FeedKind {
    /// One commit, or a burst of them, newest first.
    Commits(Vec<FeedCommit>),
    /// A session, pull request, branch, generic datum or unreadable entry: an index into
    /// [`IntxtView::entries`].
    Entry(usize),
    /// A comment thread's root, with its replies counted: an index into [`IntxtView::threads`].
    Thread(usize),
    /// Turns of the session at this entry index that could not be read, said where they were.
    UnreadableTurns(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FeedCommit {
    pub sha: String,
    pub subject: String,
    pub at: Timestamp,
}

impl FeedItem {
    /// The oldest time the item covers: its own for most, the last commit's for a burst.
    pub fn oldest(&self) -> Option<Timestamp> {
        match &self.what {
            FeedKind::Commits(commits) => commits.last().map(|commit| commit.at),
            _ => self.at,
        }
    }
}

impl IntxtView {
    /// Every item of the intxt, newest first, bursts folded, undated items last. A commit that
    /// several revisions hold is one item.
    pub fn feed(&self) -> Vec<FeedItem> {
        let mut seen = BTreeSet::new();
        let mut items = Vec::new();
        for (index, entry) in self.entries.iter().enumerate() {
            for commit in &entry.commits {
                if seen.insert(commit.sha.as_str()) {
                    items.push(FeedItem {
                        at: Some(commit.at),
                        actor: Some(commit.author.clone()),
                        what: FeedKind::Commits(vec![FeedCommit {
                            sha: commit.sha.clone(),
                            subject: commit.subject().to_owned(),
                            at: commit.at,
                        }]),
                    });
                }
            }
            // A range's commits are its items; the range itself has no time or words of its own.
            if matches!(entry.kind, EntryKind::Commits | EntryKind::JjChange) {
                continue;
            }
            items.push(FeedItem {
                at: entry.at,
                actor: entry.actor.clone(),
                what: FeedKind::Entry(index),
            });
            if entry
                .session
                .as_ref()
                .is_some_and(|summary| summary.unreadable > 0)
            {
                items.push(FeedItem {
                    at: entry.at,
                    actor: None,
                    what: FeedKind::UnreadableTurns(index),
                });
            }
        }
        for (index, thread) in self.threads.iter().enumerate() {
            items.push(FeedItem {
                at: thread.root.at,
                actor: Some(thread.root.author.clone()),
                what: FeedKind::Thread(index),
            });
        }
        order(items)
    }
}

/// Newest first (ties keep the order given), commit bursts folded, undated items after, in the
/// order given.
pub fn order(items: Vec<FeedItem>) -> Vec<FeedItem> {
    let (mut dated, undated): (Vec<_>, Vec<_>) =
        items.into_iter().partition(|item| item.at.is_some());
    dated.sort_by_key(|item| Reverse(item.at));
    let mut folded: Vec<FeedItem> = Vec::with_capacity(dated.len() + undated.len());
    for item in dated {
        let folds = folded.last().is_some_and(|last| {
            matches!(last.what, FeedKind::Commits(_))
                && matches!(item.what, FeedKind::Commits(_))
                && last.actor == item.actor
                && last
                    .oldest()
                    .zip(item.at)
                    .is_some_and(|(oldest, at)| oldest.duration_since(at) <= BURST_WINDOW)
        });
        if !folds {
            folded.push(item);
            continue;
        }
        if let (
            Some(FeedItem {
                what: FeedKind::Commits(held),
                ..
            }),
            FeedKind::Commits(commits),
        ) = (folded.last_mut(), item.what)
        {
            held.extend(commits);
        }
    }
    folded.extend(undated);
    folded
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(actor: &str, at: &str) -> FeedItem {
        let at: Timestamp = at.parse().unwrap();
        FeedItem {
            at: Some(at),
            actor: Some(actor.to_owned()),
            what: FeedKind::Commits(vec![FeedCommit {
                sha: at.to_string(),
                subject: String::new(),
                at,
            }]),
        }
    }

    fn comment(at: Option<&str>) -> FeedItem {
        FeedItem {
            at: at.map(|at| at.parse().unwrap()),
            actor: Some("ada".to_owned()),
            what: FeedKind::Thread(0),
        }
    }

    fn sizes(items: &[FeedItem]) -> Vec<usize> {
        items
            .iter()
            .map(|item| match &item.what {
                FeedKind::Commits(commits) => commits.len(),
                _ => 0,
            })
            .collect()
    }

    #[test]
    fn commits_by_one_author_within_the_window_fold_into_one_item() {
        // Given oldest first and scrambled: the order is the time's, not the input's.
        let items = order(vec![
            commit("ada", "2026-11-23T10:50:00Z"),
            commit("ada", "2026-11-23T10:00:00Z"),
            commit("ada", "2026-11-23T10:25:00Z"),
        ]);
        assert_eq!(sizes(&items), [3]);
        assert_eq!(items[0].at, Some("2026-11-23T10:50:00Z".parse().unwrap()));
        assert_eq!(
            items[0].oldest(),
            Some("2026-11-23T10:00:00Z".parse().unwrap())
        );
    }

    #[test]
    fn the_window_is_measured_from_the_previous_commit_not_the_first() {
        // 25 minutes apart each: 50 minutes end to end, still one burst.
        let items = order(vec![
            commit("ada", "2026-11-23T10:00:00Z"),
            commit("ada", "2026-11-23T10:25:00Z"),
            commit("ada", "2026-11-23T10:50:00Z"),
        ]);
        assert_eq!(sizes(&items), [3]);
        // 31 minutes is a new item; exactly 30 is not.
        let items = order(vec![
            commit("ada", "2026-11-23T10:00:00Z"),
            commit("ada", "2026-11-23T10:30:00Z"),
            commit("ada", "2026-11-23T11:01:00Z"),
        ]);
        assert_eq!(sizes(&items), [1, 2]);
    }

    #[test]
    fn another_author_or_another_kind_in_between_breaks_a_burst() {
        let items = order(vec![
            commit("ada", "2026-11-23T10:00:00Z"),
            commit("grace", "2026-11-23T10:05:00Z"),
            commit("ada", "2026-11-23T10:10:00Z"),
            comment(Some("2026-11-23T10:12:00Z")),
            commit("ada", "2026-11-23T10:15:00Z"),
        ]);
        assert_eq!(sizes(&items), [1, 0, 1, 1, 1]);
    }

    #[test]
    fn items_without_a_time_come_last_in_the_order_given() {
        let items = order(vec![
            comment(None),
            commit("ada", "2026-11-23T10:00:00Z"),
            FeedItem {
                at: None,
                actor: None,
                what: FeedKind::Entry(3),
            },
            comment(Some("2026-11-24T10:00:00Z")),
        ]);
        let kinds: Vec<_> = items
            .iter()
            .map(|item| (item.at.is_some(), &item.what))
            .collect();
        assert_eq!(kinds.len(), 4);
        assert!(kinds[..2].iter().all(|(dated, _)| *dated));
        assert_eq!(items[0].what, FeedKind::Thread(0));
        assert_eq!(items[2].what, FeedKind::Thread(0));
        assert_eq!(items[3].what, FeedKind::Entry(3));
    }

    #[test]
    fn equal_times_keep_the_order_given() {
        let items = order(vec![
            FeedItem {
                at: comment(Some("2026-11-23T10:00:00Z")).at,
                actor: None,
                what: FeedKind::Entry(1),
            },
            FeedItem {
                at: comment(Some("2026-11-23T10:00:00Z")).at,
                actor: None,
                what: FeedKind::Entry(2),
            },
        ]);
        assert_eq!(items[0].what, FeedKind::Entry(1));
        assert_eq!(items[1].what, FeedKind::Entry(2));
    }
}
