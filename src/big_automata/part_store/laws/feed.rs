//! The commit feed law: the store's long-poll hub ([`PartStoreServe::long_poll`]) is told, per
//! commit, exactly the interests of the log rows that commit stamped, with the number of rows
//! on each, and nothing for a commit that stamped none.
//!
//! The harness parks probes before each commit: for every interest of the universe, one
//! waiter per minimum amount `1..=MAX_AMOUNT`, with a window that never runs out. After the
//! commit, the probes that are ready are exactly the minimums up to the interest's amount,
//! which the check recomputes from the log rows at the commit's rev, read through the
//! unauthorized [`RevSource`]: the feed against the log it describes.

use std::num::NonZeroUsize;

use super::*;
use crate::long_poll::Linger;
use crate::long_poll::Waiting;
use crate::part_store::LogInterest;

/// More rows on one interest than a single commit of the universe stamps.
const MAX_AMOUNT: u64 = 16;

/// Waiters parked before a commit.
pub(super) struct Probes {
    probes: Vec<(LogInterest, u64, Waiting<LogInterest>)>,
}

impl Probes {
    pub(super) fn park<S: PartStoreServe>(store: &S, objs: &[ObjKey], before: Rev) -> Self {
        let hub = store.long_poll();
        let interests = (0..PARTS)
            .map(|index| LogInterest::Part(part(index)))
            .chain(objs.iter().cloned().map(LogInterest::Object))
            .chain([LogInterest::Access])
            .collect::<Vec<_>>();
        let deadline = hub.now() + Duration::from_secs(2 * 3600);
        let probes = interests
            .into_iter()
            .flat_map(|interest| {
                let hub = &hub;
                (1..=MAX_AMOUNT).map(move |min_amount| {
                    let linger = Linger {
                        window: Duration::from_secs(3600),
                        min_amount,
                    };
                    let waiting =
                        hub.park(std::slice::from_ref(&interest), before, deadline, linger);
                    (interest.clone(), min_amount, waiting)
                })
            })
            .collect();
        Self { probes }
    }

    /// The commit went from `before` to `stamped`: compare what the probes saw with the log.
    pub(super) async fn check<S>(self, store: &S, before: Rev, stamped: Option<Rev>) -> Res<()>
    where
        S: RevSource<Key = LogKey, Value = LogEntry, Selector = LogSelector>,
    {
        let mut expected = BTreeMap::<LogInterest, u64>::new();
        if let Some(rev) = stamped {
            let mut page = Page::default();
            store
                .read(
                    &LogSelector::All,
                    before,
                    rev,
                    Limits {
                        max_entries: NonZeroUsize::new(1 << 20).expect(ERROR_IMPOSSIBLE),
                        max_bytes: None,
                    },
                    &mut page,
                )
                .await?;
            for entry in &page.entries {
                assert_eq!(entry.rev, rev, "the commit's rev stamps its rows");
                for interest in entry.key.interests() {
                    *expected.entry(interest).or_default() += 1;
                }
            }
        }
        for (interest, min_amount, waiting) in &self.probes {
            let amount = expected.get(interest).copied().unwrap_or(0);
            let told = waiting.outcome().is_some();
            assert_eq!(
                told,
                *min_amount <= amount,
                "the feed told {interest:?} of {amount} rows; a probe for {min_amount} was {}",
                if told { "woken" } else { "not woken" }
            );
        }
        Ok(())
    }
}
