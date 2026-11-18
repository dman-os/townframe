//! Objects, payloads and membership, pending included.

use std::ops::Bound;

use super::MemberRow;
use super::ObjRow;
use super::State;
use crate::interlude::*;
use crate::part_store::LogEntry;
use crate::part_store::LogKey;
use crate::part_store::Membership;
use crate::part_store::ObjPayload;
use crate::part_store::ObjState;
use crate::part_store::PayloadDigest;
use crate::part_store::bucket_summary::BucketMemberKind;
use crate::rev::Rev;

/// Where an object's member rows start in the log: the smallest part key.
fn first_member_key(obj: &ObjKey) -> LogKey {
    LogKey::Member {
        obj: obj.clone(),
        part: PartKey::new(&[]),
    }
}

impl State {
    /// The object's member rows in the log, in part order, with each row's liveness.
    fn member_rows<'a>(&'a self, obj: &'a ObjKey) -> impl Iterator<Item = (&'a PartKey, bool)> {
        self.log
            .rows_in((Bound::Included(first_member_key(obj)), Bound::Unbounded))
            .map_while(move |(key, _rev, value)| match key {
                LogKey::Member { obj: row_obj, part } if row_obj == obj => {
                    Some((part, value.is_some()))
                }
                LogKey::Member { .. } | LogKey::Access { .. } => None,
            })
    }

    /// The member's place in its buckets as the log has it now.
    pub(super) fn member_kind(&self, obj: &ObjKey, part: &PartKey) -> BucketMemberKind {
        let key = LogKey::Member {
            obj: obj.clone(),
            part: part.clone(),
        };
        match self.log.get(&key) {
            None => BucketMemberKind::Absent,
            Some((_, None)) => BucketMemberKind::Dead,
            Some((_, Some(_))) => {
                BucketMemberKind::Live(self.objs.get(obj).expect(ERROR_IMPOSSIBLE).digest)
            }
        }
    }

    /// With content, the live parts; without, the pending ones (an object never has both).
    pub(super) fn obj_state(&self, obj: &ObjKey) -> Option<ObjState> {
        match self.objs.get(obj) {
            Some(row) => Some(ObjState {
                content: Some((row.payload.clone(), row.digest)),
                parts: self.live_parts_of(obj).cloned().collect(),
            }),
            None => {
                let pending = self
                    .pending
                    .range((obj.clone(), PartKey::new(&[]))..)
                    .take_while(|(pending_obj, _)| pending_obj == obj)
                    .map(|(_, part)| part.clone())
                    .collect::<BTreeSet<_>>();
                (!pending.is_empty()).then_some(ObjState {
                    content: None,
                    parts: pending,
                })
            }
        }
    }

    /// The object's content and the live parts `principal` may read, when there is one.
    pub(super) fn readable_obj_state(&self, principal: &PeerKey, obj: &ObjKey) -> Option<ObjState> {
        let row = self.objs.get(obj)?;
        let parts = self
            .live_parts_of(obj)
            .filter(|part| self.permits(principal, part))
            .cloned()
            .collect::<BTreeSet<_>>();
        (!parts.is_empty()).then(|| ObjState {
            content: Some((row.payload.clone(), row.digest)),
            parts,
        })
    }

    /// The parts `obj` is a live member of, into the reused scratch buffer.
    fn take_live_parts(&mut self, obj: &ObjKey) -> Vec<PartKey> {
        let mut parts = std::mem::take(&mut self.part_buf);
        parts.clear();
        parts.extend(self.live_parts_of(obj).cloned());
        parts
    }

    /// Stamp a membership live at `rev`, from absent or removed.
    fn go_live(&mut self, rev: Rev, obj: &ObjKey, part: &PartKey, digest: PayloadDigest) {
        let old = self.member_kind(obj, part);
        assert!(
            !matches!(old, BucketMemberKind::Live(_)),
            "a live membership went live again"
        );
        self.stamp(
            rev,
            LogKey::Member {
                obj: obj.clone(),
                part: part.clone(),
            },
            Some(LogEntry::Member(Membership { added_at: rev })),
        );
        self.members
            .entry((
                part.clone(),
                BuckId::deepest_from_obj_key(obj).index(),
                obj.clone(),
            ))
            .or_insert(MemberRow {
                first_added_at: rev,
            });
        self.bucket_transition(part, obj, rev, old, BucketMemberKind::Live(digest));
    }

    pub(super) fn set_payloads(
        &mut self,
        rev: Rev,
        payloads: Vec<(ObjKey, ObjPayload, PayloadDigest)>,
    ) -> bool {
        let mut stamped = false;
        for (obj, payload, digest) in payloads {
            let old = self.objs.get(&obj);
            if old.is_some_and(|row| row.payload == payload) {
                continue;
            }
            let old_digest = old.map(|row| row.digest);
            self.objs.insert(obj.clone(), ObjRow { payload, digest });

            // A content change re-stamps every live membership, keeping when it went live.
            let mut parts = self.take_live_parts(&obj);
            for part in &parts {
                let key = LogKey::Member {
                    obj: obj.clone(),
                    part: part.clone(),
                };
                let (_, value) = self.log.get(&key).expect(ERROR_IMPOSSIBLE);
                let membership = value.clone();
                self.stamp(rev, key, membership);
                let old_digest = old_digest.expect("a live member without a payload");
                self.bucket_transition(
                    part,
                    &obj,
                    rev,
                    BucketMemberKind::Live(old_digest),
                    BucketMemberKind::Live(digest),
                );
                stamped = true;
            }

            // The first payload promotes every pending membership, in the same buffer.
            parts.clear();
            parts.extend(
                self.pending
                    .range((obj.clone(), PartKey::new(&[]))..)
                    .take_while(|(pending_obj, _)| *pending_obj == obj)
                    .map(|(_, part)| part.clone()),
            );
            for part in &parts {
                self.pending.remove(&(obj.clone(), part.clone()));
                self.go_live(rev, &obj, part, digest);
                stamped = true;
            }
            self.part_buf = parts;
        }
        stamped
    }

    pub(super) fn add_members(&mut self, rev: Rev, members: &[(ObjKey, PartKey)]) -> bool {
        let mut stamped = false;
        for (obj, part) in members {
            self.parts.entry(part.clone()).or_default();
            match self.member_kind(obj, part) {
                BucketMemberKind::Live(_) => {}
                BucketMemberKind::Absent | BucketMemberKind::Dead => match self.objs.get(obj) {
                    Some(row) => {
                        let digest = row.digest;
                        self.go_live(rev, obj, part, digest);
                        stamped = true;
                    }
                    None => {
                        self.pending.insert((obj.clone(), part.clone()));
                    }
                },
            }
        }
        stamped
    }

    pub(super) fn remove_members(&mut self, rev: Rev, members: &[(ObjKey, PartKey)]) -> bool {
        let mut stamped = false;
        for (obj, part) in members {
            if self.pending.remove(&(obj.clone(), part.clone())) {
                continue;
            }
            let old = self.member_kind(obj, part);
            match old {
                BucketMemberKind::Live(_) => {
                    self.stamp(
                        rev,
                        LogKey::Member {
                            obj: obj.clone(),
                            part: part.clone(),
                        },
                        None,
                    );
                    self.bucket_transition(part, obj, rev, old, BucketMemberKind::Dead);
                    stamped = true;
                }
                BucketMemberKind::Absent | BucketMemberKind::Dead => {}
            }
        }
        stamped
    }

    /// The parts `obj` is a live member of.
    pub(super) fn live_parts_of<'a>(
        &'a self,
        obj: &'a ObjKey,
    ) -> impl Iterator<Item = &'a PartKey> {
        self.member_rows(obj)
            .filter(|(_, live)| *live)
            .map(|(part, _)| part)
    }
}
