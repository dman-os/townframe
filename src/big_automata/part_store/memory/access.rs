//! Access rows, authorization against them, and dirt counts.

use super::State;
use crate::interlude::*;
use crate::part_store::AccessLevel;
use crate::part_store::LogEntry;
use crate::part_store::LogKey;
use crate::part_store::PartDirtyCount;
use crate::part_store::ReadVerdict;
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

    fn permits(&self, principal: &PeerKey, part: &PartKey) -> bool {
        matches!(
            self.log.get(&access_key(part, principal)),
            Some((_, Some(LogEntry::Access(_))))
        )
    }

    pub(super) fn read_verdict(&self, principal: &PeerKey, part: &PartKey) -> ReadVerdict {
        if !self.parts.contains_key(part) {
            ReadVerdict::UnknownPart
        } else if self.permits(principal, part) {
            ReadVerdict::Permitted
        } else {
            ReadVerdict::Denied
        }
    }

    pub(super) fn readable_parts_of(&self, principal: &PeerKey, obj: &ObjKey) -> Vec<PartKey> {
        self.live_parts_of(obj)
            .filter(|part| self.permits(principal, part))
            .cloned()
            .collect()
    }

    /// Counts the part's member rows through the member index (every member with a row, live
    /// or removed), not by scanning the log window.
    pub(super) fn part_dirty_count(
        &self,
        part: &PartKey,
        principal: Option<&PeerKey>,
        since: Rev,
    ) -> PartDirtyCount {
        let member_changes = self
            .members
            .range((part.clone(), 0, ObjKey::new(&[]))..)
            .take_while(|((member_part, _, _), _)| member_part == part)
            .filter(|((_, _, obj), _)| {
                let key = LogKey::Member {
                    obj: obj.clone(),
                    part: part.clone(),
                };
                let (rev, _) = self.log.get(&key).expect(ERROR_IMPOSSIBLE);
                rev > since
            })
            .count() as u64;
        let access_changes = principal.map_or(0, |principal| {
            self.log
                .get(&access_key(part, principal))
                .map_or(0, |(rev, _)| u64::from(rev > since))
        });
        PartDirtyCount {
            member_changes,
            access_changes,
        }
    }
}
