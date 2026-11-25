//! Access rows, authorization against them, and dirt counts.

use super::State;
use crate::interlude::*;
use crate::part_store::AccessLevel;
use crate::part_store::AuthorizedPage;
use crate::part_store::LogEntry;
use crate::part_store::LogKey;
use crate::part_store::LogRoutes;
use crate::part_store::ObjPayload;
use crate::part_store::PartDirtyCount;
use crate::part_store::PartSummary;
use crate::part_store::ReadVerdict;
use crate::part_store::log_row_bytes;
use crate::rev::Limits;
use crate::rev::Rev;

fn access_key(part: &PartKey, principal: &PeerKey) -> LogKey {
    LogKey::Access {
        part: part.clone(),
        principal: principal.clone(),
    }
}

impl State {
    pub(super) fn grant_access(
        &mut self,
        rev: Rev,
        grants: &[(PartKey, PeerKey, AccessLevel)],
    ) -> bool {
        let mut stamped = false;
        for (part, principal, level) in grants {
            let key = access_key(part, principal);
            if let Some((_, Some(LogEntry::Access(current)))) = self.log.get(&key)
                && current == level
            {
                continue;
            }
            self.stamp(rev, key, Some(LogEntry::Access(*level)));
            stamped = true;
        }
        stamped
    }

    pub(super) fn revoke_access(&mut self, rev: Rev, revokes: &[(PartKey, PeerKey)]) -> bool {
        let mut stamped = false;
        for (part, principal) in revokes {
            let key = access_key(part, principal);
            if let Some((_, Some(_))) = self.log.get(&key) {
                self.stamp(rev, key, None);
                stamped = true;
            }
        }
        stamped
    }

    pub(super) fn permits(&self, principal: &PeerKey, part: &PartKey) -> bool {
        matches!(
            self.log.get(&access_key(part, principal)),
            Some((_, Some(LogEntry::Access(_))))
        )
    }

    fn read_verdict(&self, principal: &PeerKey, part: &PartKey) -> ReadVerdict {
        if !self.parts.contains_key(part) {
            ReadVerdict::UnknownPart
        } else if self.permits(principal, part) {
            ReadVerdict::Permitted
        } else {
            ReadVerdict::Denied
        }
    }

    /// An object route's verdict: whether the object is a live member of a part the principal
    /// may read.
    fn object_verdict(&self, principal: &PeerKey, obj: &ObjKey) -> ReadVerdict {
        if self
            .live_parts_of(obj)
            .any(|part| self.permits(principal, part))
        {
            ReadVerdict::Permitted
        } else {
            ReadVerdict::Denied
        }
    }

    /// Every route's verdict first, then the log rows they let through, all against this one
    /// state. A member row passes when the principal may read its part *now*, and it rides a
    /// permitted route: its part's route (permitted exactly when its part is readable), or its
    /// object's route when that route is permitted. Access rows ride no route.
    pub(super) fn read_authorized(
        &self,
        principal: &PeerKey,
        routes: &LogRoutes,
        after: Rev,
        until: Rev,
        limits: Limits,
        page: &mut AuthorizedPage,
    ) {
        let AuthorizedPage {
            parts,
            objects,
            rows,
            payloads,
        } = page;
        parts.clear();
        parts.extend(
            routes
                .parts
                .keys()
                .map(|part| self.read_verdict(principal, part)),
        );
        objects.clear();
        objects.extend(
            routes
                .objects
                .keys()
                .map(|obj| self.object_verdict(principal, obj)),
        );
        let readable_objects = routes
            .objects
            .keys()
            .zip(objects.iter())
            .filter(|(_, verdict)| **verdict == ReadVerdict::Permitted)
            .map(|(obj, _)| obj)
            .collect::<BTreeSet<_>>();
        assert!(
            after <= until,
            "read window ({after}, {until}] runs backwards"
        );
        if after == until {
            // The empty window: verdicts only. The okv read asserts a non-empty window, and
            // there is nothing for it to read anyway.
            assert!(
                until <= self.log.rev(),
                "read until {until} past the committed rev"
            );
            rows.entries.clear();
            rows.through = until;
        } else {
            self.log.read_into(
                |key, rev, _value| match key {
                    LogKey::Member { obj, part } => {
                        let (on_part, on_object) = routes.carriers(obj, part, rev);
                        (on_part || (on_object && readable_objects.contains(obj)))
                            && self.permits(principal, part)
                    }
                    LogKey::Access { .. } => false,
                },
                log_row_bytes,
                after,
                until,
                limits,
                rows,
            );
        }
        self.page_payloads(&rows.entries, payloads);
    }

    /// The payload of every object with a live row in `entries`, into `payloads` sorted by
    /// object. The keys are gathered and deduplicated first, so each payload is cloned once
    /// however many of the page's rows name its object.
    fn page_payloads(
        &self,
        entries: &[crate::rev::Entry<LogKey, LogEntry>],
        payloads: &mut Vec<(ObjKey, ObjPayload)>,
    ) {
        payloads.clear();
        payloads.extend(
            entries
                .iter()
                .filter(|entry| entry.value.is_some())
                .map(|entry| match &entry.key {
                    LogKey::Member { obj, .. } => (obj.clone(), ObjPayload::Null),
                    LogKey::Access { .. } => {
                        unreachable!("an authorized read carries no access row")
                    }
                }),
        );
        payloads.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        payloads.dedup_by(|(left, _), (right, _)| left == right);
        for (obj, payload) in payloads.iter_mut() {
            let row = self
                .objs
                .get(obj)
                .expect("a live membership always has a payload");
            payload.clone_from(&row.payload);
        }
    }

    /// Counts the part's member rows through the member index (every member with a row, live
    /// or removed), not by scanning the log window.
    pub(super) fn part_summary(
        &self,
        principal: &PeerKey,
        part: &PartKey,
        since: Rev,
    ) -> PartSummary {
        let member_changes = self
            .members
            .range((part.clone(), 0, ObjKey::new(&[]))..)
            .take_while(|(member_part, _, _)| member_part == part)
            .filter(|(_, _, obj)| {
                let key = LogKey::Member {
                    obj: obj.clone(),
                    part: part.clone(),
                };
                let (rev, _) = self.log.get(&key).expect(ERROR_IMPOSSIBLE);
                rev > since
            })
            .count() as u64;
        let access_changes = self
            .log
            .get(&access_key(part, principal))
            .map_or(0, |(rev, _)| u64::from(rev > since));
        PartSummary {
            generation: self.generation,
            latest_cursor: self.parts.get(part).map_or(0, |row| row.latest_cursor),
            member_count: self.member_count(part),
            deepest_bucket_level: BuckId::MAX_LEVEL,
            dirty: PartDirtyCount {
                member_changes,
                access_changes,
            },
        }
    }
}
