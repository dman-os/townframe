//! Reading the log: a page of rows in `(rev, key)` order.
//!
//! A read is a set of *arms*, each an index range query (the whole member log, the whole access
//! log, a part's rows, an object's rows). Every arm is bounded by the page's size, so a page
//! costs the page and not the backlog behind it, and the arms' rows are merged in Rust.
//!
//! A page never splits a revision, and each arm is asked for `max_entries + 1` rows:
//!
//! - The merged first `k = max_entries + 1` rows are exactly the first `k` of the union of all
//!   arms, because no arm can hide a row that sorts before its own `k`th.
//! - A revision other than the last one fetched is therefore whole. The last one is whole only
//!   if fewer than `k` rows came back. Otherwise the rows before it number at most
//!   `max_entries` and it holds at least the one that made `k`, so it cannot fit and it ends
//!   the page. The exception is the first revision, which always goes in whole, however big:
//!   it is fetched again without a bound.

use std::num::NonZeroUsize;

use big_automata::part_store::LogEntry;
use big_automata::part_store::LogKey;
use big_automata::part_store::LogSelector;
use big_automata::part_store::Membership;
use big_automata::part_store::log_row_bytes;
use big_automata::rev::Entry;
use big_automata::rev::Limits;
use big_automata::rev::Page;
use big_automata::rev::Rev;
use sqlite_rs::QueryExecutor;

use crate::interlude::*;
use crate::keys::BlobKey;
use crate::sql;
use crate::sqlite::queries;

/// One index range of the log, carrying the rows whose rev is past `lo`.
pub enum Arm<'a> {
    AllMembers,
    AllAccess,
    /// A part route: the part's member rows.
    Part { part: &'a PartKey, lo: Rev },
    /// An object route: the object's member rows in every part.
    Object { obj: &'a ObjKey, lo: Rev },
    /// An object route of a principal: the object's member rows in the parts it may read.
    ReadableObject {
        obj: &'a ObjKey,
        lo: Rev,
        principal: &'a PeerKey,
    },
}

impl Arm<'_> {
    fn lo(&self) -> Rev {
        match self {
            Arm::AllMembers | Arm::AllAccess => 0,
            Arm::Part { lo, .. } | Arm::Object { lo, .. } | Arm::ReadableObject { lo, .. } => *lo,
        }
    }
}

type LogEntryRow = Entry<LogKey, LogEntry>;

fn member_value(live: i64, added_at: i64) -> Option<LogEntry> {
    (live == 1).then(|| {
        LogEntry::Member(Membership {
            added_at: sql::rev_from_sql(added_at),
        })
    })
}

/// The arm's rows in `(max(after, arm lo), hi]`, at most `limit`, in `(rev, key)` order.
async fn fetch(
    x: &impl QueryExecutor,
    arm: &Arm<'_>,
    after: Rev,
    hi: Rev,
    limit: usize,
) -> Res<Vec<LogEntryRow>> {
    let lo = after.max(arm.lo());
    if lo >= hi {
        return Ok(Vec::new());
    }
    let (lo, hi, limit) = (sql::rev_to_sql(lo), sql::rev_to_sql(hi), sql::limit(limit));
    Ok(match arm {
        Arm::AllMembers => queries::all_member_rows(x, lo, hi, limit)
            .await?
            .into_iter()
            .map(|row| LogEntryRow {
                key: LogKey::Member {
                    obj: ObjKey::from_blob(&row.obj_key),
                    part: PartKey::from_blob(&row.part_key),
                },
                rev: sql::rev_from_sql(row.rev),
                value: member_value(row.live, row.added_at),
            })
            .collect(),
        Arm::AllAccess => queries::all_access_rows(x, lo, hi, limit)
            .await?
            .into_iter()
            .map(|row| LogEntryRow {
                key: LogKey::Access {
                    part: PartKey::from_blob(&row.part_key),
                    principal: PeerKey::from_blob(&row.principal),
                },
                rev: sql::rev_from_sql(row.rev),
                value: row.level.map(|level| LogEntry::Access(sql::level_from_sql(level))),
            })
            .collect(),
        Arm::Part { part, .. } => queries::route_part_rows(x, part.blob(), lo, hi, limit)
            .await?
            .into_iter()
            .map(|row| LogEntryRow {
                key: LogKey::Member {
                    obj: ObjKey::from_blob(&row.obj_key),
                    part: (*part).clone(),
                },
                rev: sql::rev_from_sql(row.rev),
                value: member_value(row.live, row.added_at),
            })
            .collect(),
        Arm::Object { obj, .. } => queries::route_object_rows(x, obj.blob(), lo, hi, limit)
            .await?
            .into_iter()
            .map(|row| object_entry(obj, row.part_key, row.rev, row.live, row.added_at))
            .collect(),
        Arm::ReadableObject { obj, principal, .. } => {
            queries::route_readable_object_rows(x, principal.blob(), obj.blob(), lo, hi, limit)
                .await?
                .into_iter()
                .map(|row| object_entry(obj, row.part_key, row.rev, row.live, row.added_at))
                .collect()
        }
    })
}

fn object_entry(obj: &ObjKey, part: Vec<u8>, rev: i64, live: i64, added_at: i64) -> LogEntryRow {
    LogEntryRow {
        key: LogKey::Member {
            obj: obj.clone(),
            part: PartKey::from_blob(&part),
        },
        rev: sql::rev_from_sql(rev),
        value: member_value(live, added_at),
    }
}

/// The first `limit` rows of the arms together, in `(rev, key)` order, each row once (a row on
/// a part route and an object route comes back from both).
async fn fetch_merged(
    x: &impl QueryExecutor,
    arms: &[Arm<'_>],
    after: Rev,
    hi: Rev,
    limit: usize,
) -> Res<Vec<LogEntryRow>> {
    let mut rows = Vec::new();
    for arm in arms {
        rows.extend(fetch(x, arm, after, hi, limit).await?);
    }
    rows.sort_unstable_by(|left, right| (left.rev, &left.key).cmp(&(right.rev, &right.key)));
    rows.dedup_by(|right, left| left.rev == right.rev && left.key == right.key);
    rows.truncate(limit);
    Ok(rows)
}

/// The page of the arms' rows in `(after, until]`: its entries, and its `through`.
pub async fn read_arms(
    x: &impl QueryExecutor,
    arms: &[Arm<'_>],
    after: Rev,
    until: Rev,
    limits: Limits,
) -> Res<(Vec<LogEntryRow>, Rev)> {
    let max_entries = limits.max_entries.get();
    let max_bytes = limits.max_bytes.map_or(usize::MAX, NonZeroUsize::get);
    let k = max_entries.saturating_add(1);
    let fetched = fetch_merged(x, arms, after, until, k).await?;
    let complete = fetched.len() < k;

    let mut page = Vec::<LogEntryRow>::new();
    let mut bytes = 0_usize;
    let mut through = until;
    let mut remaining = fetched.into_iter().peekable();
    while let Some(first) = remaining.next() {
        let rev = first.rev;
        let mut revision = vec![first];
        while let Some(row) = remaining.next_if(|row| row.rev == rev) {
            revision.push(row);
        }
        let last = remaining.peek().is_none();
        if last && !complete {
            if !page.is_empty() {
                through = page.last().expect(ERROR_IMPOSSIBLE).rev;
                break;
            }
            // The first revision, whole: and whether anything follows decides `through`.
            revision = fetch_merged(x, arms, rev - 1, rev, usize::MAX).await?;
            let more = rev < until && !fetch_merged(x, arms, rev, until, 1).await?.is_empty();
            if more {
                through = rev;
            }
            page = revision;
            break;
        }
        let revision_bytes = revision.iter().fold(0_usize, |bytes, row| {
            bytes.saturating_add(log_row_bytes(&row.key, row.value.as_ref()))
        });
        let fits = page.is_empty()
            || (page.len() + revision.len() <= max_entries
                && bytes.saturating_add(revision_bytes) <= max_bytes);
        if !fits {
            through = page.last().expect(ERROR_IMPOSSIBLE).rev;
            break;
        }
        bytes = bytes.saturating_add(revision_bytes);
        page.extend(revision);
    }
    Ok((page, through))
}

/// An unauthorized read of what `selector` selects.
pub async fn read_selected(
    x: &impl QueryExecutor,
    selector: &LogSelector,
    after: Rev,
    until: Rev,
    limits: Limits,
    page: &mut Page<LogKey, LogEntry>,
) -> Res<()> {
    let arms = match selector {
        LogSelector::All => vec![Arm::AllMembers, Arm::AllAccess],
        LogSelector::Routes(routes) => routes
            .parts
            .iter()
            .map(|(part, lo)| Arm::Part { part, lo: *lo })
            .chain(
                routes
                    .objects
                    .iter()
                    .map(|(obj, lo)| Arm::Object { obj, lo: *lo }),
            )
            .collect(),
    };
    (page.entries, page.through) = read_arms(x, &arms, after, until, limits).await?;
    Ok(())
}
