//! The write batches.
//!
//! Each batch loads what it needs, decides exactly as `big_automata`'s memory store does (the
//! overlay maps below stand for its `State`, so a batch sees its own earlier effects), collects
//! the row changes in a [`Flush`], and writes them with multi-row upserts. Every batch returns
//! the log rows it stamped; the transaction collects them, and the store's long-poll hub is told
//! them after the commit.

use big_automata::part_store::AccessLevel;
use big_automata::part_store::LogKey;
use big_automata::part_store::ObjPayload;
use big_automata::part_store::PayloadDigest;
use big_automata::part_store::bucket_summary::BucketMemberKind;
use big_automata::rev::Rev;
use sqlite_rs::Executor;
use sqlite_rs::QueryExecutor;
use sqlite_rs::Sql;

use crate::buckets::Transition;
use crate::buckets::deepest_index;
use crate::interlude::*;
use crate::keys::BlobKey;
use crate::sql;

/// A member row as it is stored.
#[derive(Clone, Copy)]
struct Member {
    live: bool,
    added_at: Rev,
}

/// The row changes of one batch, written by [`Flush::run`].
#[derive(Default)]
struct Flush {
    /// Member rows to write at the batch's rev, last write per key.
    members: BTreeMap<(ObjKey, PartKey), Member>,
    transitions: Vec<Transition>,
    /// Per object, the change of its live membership count.
    counts: BTreeMap<ObjKey, i64>,
}

impl Flush {
    /// Stamp a member row and move the member between bucket states.
    fn stamp(
        &mut self,
        obj: &ObjKey,
        part: &PartKey,
        member: Member,
        old: BucketMemberKind,
        new: BucketMemberKind,
    ) {
        self.members.insert((obj.clone(), part.clone()), member);
        self.transitions.push(Transition {
            part: part.clone(),
            obj: obj.clone(),
            old,
            new,
        });
    }

    fn count(&mut self, obj: &ObjKey, delta: i64) {
        *self.counts.entry(obj.clone()).or_default() += delta;
    }

    /// The log rows this flush stamps: one member row per key of `members`.
    fn stamped(&self) -> Vec<LogKey> {
        self.members
            .keys()
            .map(|(obj, part)| LogKey::Member {
                obj: obj.clone(),
                part: part.clone(),
            })
            .collect()
    }

    async fn run(self, x: &impl Executor, rev: Rev) -> Res<()> {
        let touched_parts = self
            .members
            .keys()
            .map(|(_, part)| part.clone())
            .collect::<BTreeSet<_>>();
        let rows = self
            .members
            .into_iter()
            .map(|((obj, part), member)| {
                (
                    part.blob(),
                    deepest_index(&obj),
                    obj.blob(),
                    i64::from(member.live),
                    sql::rev_to_sql(rev),
                    sql::rev_to_sql(member.added_at),
                )
            })
            .collect();
        sql::execute_values(
            x,
            "INSERT INTO big_automata_members (
                   part_key
                 , buck
                 , obj_key
                 , live
                 , rev
                 , added_at
             ) ",
            rows,
            6,
            " ON CONFLICT (part_key, buck, obj_key) DO UPDATE SET
                  live = excluded.live
                , rev = excluded.rev
                , added_at = excluded.added_at",
        )
        .await?;
        crate::buckets::apply(x, rev, self.transitions).await?;
        // A part's cursor is the rev of its latest member row.
        let parts = touched_parts
            .into_iter()
            .map(|part| (part.blob(), sql::rev_to_sql(rev)))
            .collect();
        sql::execute_values(
            x,
            "INSERT INTO big_automata_parts (part_key, latest_cursor) ",
            parts,
            2,
            " ON CONFLICT (part_key) DO UPDATE SET latest_cursor = excluded.latest_cursor",
        )
        .await?;
        let counts = self
            .counts
            .into_iter()
            .filter(|(_, delta)| *delta != 0)
            .map(|(obj, delta)| (obj.blob(), delta))
            .collect();
        sql::execute_values(
            x,
            "WITH delta (obj_key, delta) AS (",
            counts,
            2,
            ") UPDATE big_automata_objs
                 SET part_count = part_count + (
                     SELECT delta.delta
                       FROM delta
                      WHERE delta.obj_key = big_automata_objs.obj_key
                 )
               WHERE obj_key IN (SELECT obj_key FROM delta)",
        )
        .await
    }
}

fn digest_from_blob(blob: &[u8]) -> PayloadDigest {
    PayloadDigest(blob.try_into().expect("a stored digest is 32 bytes"))
}

async fn load_digests(
    x: &impl QueryExecutor,
    objs: &BTreeSet<ObjKey>,
) -> Res<BTreeMap<ObjKey, PayloadDigest>> {
    let mut digests = BTreeMap::new();
    for chunk in sql::chunk_rows(objs.iter().collect(), 1, 0) {
        let mut query = Sql::new("SELECT obj_key, digest FROM big_automata_objs WHERE obj_key IN ");
        query.bind_list(chunk.iter().map(|obj| obj.blob()));
        for (obj, digest) in query.query(x).await?.all::<(Vec<u8>, Vec<u8>)>()? {
            digests.insert(ObjKey::from_blob(&obj), digest_from_blob(&digest));
        }
    }
    Ok(digests)
}

fn member_from_row(live: i64, added_at: i64) -> Member {
    Member {
        live: live == 1,
        added_at: sql::rev_from_sql(added_at),
    }
}

/// The member rows of exactly these `(object, part)` pairs.
async fn load_members(
    x: &impl QueryExecutor,
    pairs: &BTreeSet<(ObjKey, PartKey)>,
) -> Res<BTreeMap<(ObjKey, PartKey), Member>> {
    let mut members = BTreeMap::new();
    for chunk in sql::chunk_rows(pairs.iter().collect(), 3, 0) {
        let mut query = Sql::new(
            "SELECT part_key
                  , obj_key
                  , live
                  , added_at
               FROM big_automata_members
              WHERE (part_key, buck, obj_key) IN (",
        );
        sql::push_values(
            &mut query,
            chunk
                .iter()
                .map(|(obj, part)| (part.blob(), deepest_index(obj), obj.blob())),
        );
        query.push(")");
        for (part, obj, live, added_at) in query
            .query(x)
            .await?
            .all::<(Vec<u8>, Vec<u8>, i64, i64)>()?
        {
            members.insert(
                (ObjKey::from_blob(&obj), PartKey::from_blob(&part)),
                member_from_row(live, added_at),
            );
        }
    }
    Ok(members)
}

/// Every member row of these objects.
async fn load_obj_members(
    x: &impl QueryExecutor,
    objs: &BTreeSet<ObjKey>,
) -> Res<BTreeMap<ObjKey, BTreeMap<PartKey, Member>>> {
    let mut members = BTreeMap::<_, BTreeMap<_, _>>::new();
    for chunk in sql::chunk_rows(objs.iter().collect(), 1, 0) {
        let mut query = Sql::new(
            "SELECT obj_key
                  , part_key
                  , live
                  , added_at
               FROM big_automata_members
              WHERE obj_key IN ",
        );
        query.bind_list(chunk.iter().map(|obj| obj.blob()));
        for (obj, part, live, added_at) in query
            .query(x)
            .await?
            .all::<(Vec<u8>, Vec<u8>, i64, i64)>()?
        {
            members
                .entry(ObjKey::from_blob(&obj))
                .or_default()
                .insert(PartKey::from_blob(&part), member_from_row(live, added_at));
        }
    }
    Ok(members)
}

async fn load_pending(
    x: &impl QueryExecutor,
    objs: &BTreeSet<ObjKey>,
) -> Res<BTreeMap<ObjKey, BTreeSet<PartKey>>> {
    let mut pending = BTreeMap::<_, BTreeSet<_>>::new();
    for chunk in sql::chunk_rows(objs.iter().collect(), 1, 0) {
        let mut query =
            Sql::new("SELECT obj_key, part_key FROM big_automata_pending WHERE obj_key IN ");
        query.bind_list(chunk.iter().map(|obj| obj.blob()));
        for (obj, part) in query.query(x).await?.all::<(Vec<u8>, Vec<u8>)>()? {
            pending
                .entry(ObjKey::from_blob(&obj))
                .or_default()
                .insert(PartKey::from_blob(&part));
        }
    }
    Ok(pending)
}

async fn load_pending_pairs(
    x: &impl QueryExecutor,
    pairs: &BTreeSet<(ObjKey, PartKey)>,
) -> Res<BTreeSet<(ObjKey, PartKey)>> {
    let mut found = BTreeSet::new();
    for chunk in sql::chunk_rows(pairs.iter().collect(), 2, 0) {
        let mut query = Sql::new(
            "SELECT obj_key
                  , part_key
               FROM big_automata_pending
              WHERE (obj_key, part_key) IN (",
        );
        sql::push_values(
            &mut query,
            chunk.iter().map(|(obj, part)| (obj.blob(), part.blob())),
        );
        query.push(")");
        for (obj, part) in query.query(x).await?.all::<(Vec<u8>, Vec<u8>)>()? {
            found.insert((ObjKey::from_blob(&obj), PartKey::from_blob(&part)));
        }
    }
    Ok(found)
}

pub async fn ensure_parts(x: &impl Executor, parts: &[PartKey]) -> Res<()> {
    let rows = parts.iter().map(|part| (part.blob(), 0_i64)).collect();
    sql::execute_values(
        x,
        "INSERT OR IGNORE INTO big_automata_parts (part_key, latest_cursor) ",
        rows,
        2,
        "",
    )
    .await
}

pub async fn set_payloads(
    x: &impl Executor,
    rev: Rev,
    payloads: Vec<(ObjKey, ObjPayload, PayloadDigest)>,
) -> Res<Vec<LogKey>> {
    let objs = payloads
        .iter()
        .map(|(obj, _, _)| obj.clone())
        .collect::<BTreeSet<_>>();
    let mut digests = load_digests(x, &objs).await?;
    let mut members = load_obj_members(x, &objs).await?;
    let mut pending = load_pending(x, &objs).await?;
    let mut flush = Flush::default();
    // The payloads to write, with the live membership count of an object that has none yet:
    // the pending memberships its first payload promotes.
    let mut written = BTreeMap::<ObjKey, (ObjPayload, PayloadDigest, i64)>::new();
    let mut promoted = BTreeSet::new();
    for (obj, payload, digest) in payloads {
        let old = digests.get(&obj).copied();
        if old == Some(digest) {
            continue;
        }
        digests.insert(obj.clone(), digest);
        let rows = members.entry(obj.clone()).or_default();
        // A content change re-stamps every live membership, keeping when it went live.
        if let Some(old) = old {
            for (part, member) in rows.iter().filter(|(_, member)| member.live) {
                flush.stamp(
                    &obj,
                    part,
                    *member,
                    BucketMemberKind::Live(old),
                    BucketMemberKind::Live(digest),
                );
            }
        }
        // The first payload promotes every pending membership.
        let pending_parts = pending.remove(&obj).unwrap_or_default();
        let fresh_count = i64::try_from(pending_parts.len()).expect(ERROR_IMPOSSIBLE);
        if !pending_parts.is_empty() {
            promoted.insert(obj.clone());
        }
        for part in pending_parts {
            let before = match rows.get(&part) {
                None => BucketMemberKind::Absent,
                Some(_) => BucketMemberKind::Dead,
            };
            let member = Member {
                live: true,
                added_at: rev,
            };
            flush.stamp(&obj, &part, member, before, BucketMemberKind::Live(digest));
            rows.insert(part, member);
        }
        let carried = written
            .get(&obj)
            .map_or(fresh_count, |(_, _, count)| *count);
        written.insert(obj, (payload, digest, carried));
    }

    let rows = written
        .into_iter()
        .map(|(obj, (payload, digest, count))| {
            let text = serde_json::to_string(&payload).expect("a JSON value serializes");
            (obj.blob(), text, digest.0.to_vec(), count)
        })
        .collect();
    sql::execute_values(
        x,
        "INSERT INTO big_automata_objs (obj_key, payload, digest, part_count) ",
        rows,
        4,
        " ON CONFLICT (obj_key) DO UPDATE SET
              payload = excluded.payload
            , digest = excluded.digest",
    )
    .await?;
    for chunk in sql::chunk_rows(promoted.into_iter().collect(), 1, 0) {
        let mut delete = Sql::new("DELETE FROM big_automata_pending WHERE obj_key IN ");
        delete.bind_list(chunk.iter().map(|obj| obj.blob()));
        delete.execute(x).await?;
    }
    let stamped = flush.stamped();
    flush.run(x, rev).await?;
    Ok(stamped)
}

pub async fn add_members(
    x: &impl Executor,
    rev: Rev,
    members: &[(ObjKey, PartKey)],
) -> Res<Vec<LogKey>> {
    // Adding creates the part, live or pending.
    ensure_parts(
        x,
        &members
            .iter()
            .map(|(_, part)| part.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>(),
    )
    .await?;
    let pairs = members.iter().cloned().collect::<BTreeSet<_>>();
    let objs = members
        .iter()
        .map(|(obj, _)| obj.clone())
        .collect::<BTreeSet<_>>();
    let digests = load_digests(x, &objs).await?;
    let mut rows = load_members(x, &pairs).await?;
    let mut flush = Flush::default();
    let mut new_pending = BTreeSet::new();
    for (obj, part) in members {
        let key = (obj.clone(), part.clone());
        let before = match rows.get(&key) {
            Some(member) if member.live => continue,
            Some(_) => BucketMemberKind::Dead,
            None => BucketMemberKind::Absent,
        };
        match digests.get(obj) {
            Some(digest) => {
                let member = Member {
                    live: true,
                    added_at: rev,
                };
                flush.stamp(obj, part, member, before, BucketMemberKind::Live(*digest));
                flush.count(obj, 1);
                rows.insert(key, member);
            }
            None => {
                new_pending.insert(key);
            }
        }
    }
    let pending = new_pending
        .into_iter()
        .map(|(obj, part)| (obj.blob(), part.blob()))
        .collect();
    sql::execute_values(
        x,
        "INSERT OR IGNORE INTO big_automata_pending (obj_key, part_key) ",
        pending,
        2,
        "",
    )
    .await?;
    let stamped = flush.stamped();
    flush.run(x, rev).await?;
    Ok(stamped)
}

pub async fn remove_members(
    x: &impl Executor,
    rev: Rev,
    members: &[(ObjKey, PartKey)],
) -> Res<Vec<LogKey>> {
    let pairs = members.iter().cloned().collect::<BTreeSet<_>>();
    let objs = members
        .iter()
        .map(|(obj, _)| obj.clone())
        .collect::<BTreeSet<_>>();
    let digests = load_digests(x, &objs).await?;
    let mut rows = load_members(x, &pairs).await?;
    let mut pending = load_pending_pairs(x, &pairs).await?;
    let mut flush = Flush::default();
    let mut dropped_pending = Vec::new();
    for (obj, part) in members {
        let key = (obj.clone(), part.clone());
        // A pending membership leaves nothing.
        if pending.remove(&key) {
            dropped_pending.push((obj.blob(), part.blob()));
            continue;
        }
        let Some(member) = rows.get(&key).copied().filter(|member| member.live) else {
            continue;
        };
        let digest = digests.get(obj).expect("a live member has a payload");
        let removed = Member {
            live: false,
            added_at: member.added_at,
        };
        flush.stamp(
            obj,
            part,
            removed,
            BucketMemberKind::Live(*digest),
            BucketMemberKind::Dead,
        );
        flush.count(obj, -1);
        rows.insert(key, removed);
    }
    for chunk in sql::chunk_rows(dropped_pending, 2, 0) {
        let mut delete =
            Sql::new("DELETE FROM big_automata_pending WHERE (obj_key, part_key) IN (");
        sql::push_values(&mut delete, chunk);
        delete.push(")");
        delete.execute(x).await?;
    }
    let stamped = flush.stamped();
    flush.run(x, rev).await?;
    Ok(stamped)
}

/// The stored access row of each `(part, principal)`: `Some(None)` is a revocation tombstone.
async fn load_access(
    x: &impl QueryExecutor,
    pairs: &BTreeSet<(PartKey, PeerKey)>,
) -> Res<BTreeMap<(PartKey, PeerKey), Option<AccessLevel>>> {
    let mut rows = BTreeMap::new();
    for chunk in sql::chunk_rows(pairs.iter().collect(), 2, 0) {
        let mut query = Sql::new(
            "SELECT part_key
                  , principal
                  , level
               FROM big_automata_access
              WHERE (part_key, principal) IN (",
        );
        sql::push_values(
            &mut query,
            chunk
                .iter()
                .map(|(part, principal)| (part.blob(), principal.blob())),
        );
        query.push(")");
        for (part, principal, level) in query
            .query(x)
            .await?
            .all::<(Vec<u8>, Vec<u8>, Option<i64>)>()?
        {
            rows.insert(
                (PartKey::from_blob(&part), PeerKey::from_blob(&principal)),
                level.map(sql::level_from_sql),
            );
        }
    }
    Ok(rows)
}

/// Write access rows at `rev`, creating their parts.
async fn write_access(
    x: &impl Executor,
    rev: Rev,
    writes: BTreeMap<(PartKey, PeerKey), Option<AccessLevel>>,
) -> Res<()> {
    let parts = writes
        .keys()
        .map(|(part, _)| part.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    ensure_parts(x, &parts).await?;
    let rows = writes
        .into_iter()
        .map(|((part, principal), level)| {
            (
                part.blob(),
                principal.blob(),
                level.map(sql::level_to_sql),
                sql::rev_to_sql(rev),
            )
        })
        .collect();
    sql::execute_values(
        x,
        "INSERT INTO big_automata_access (part_key, principal, level, rev) ",
        rows,
        4,
        " ON CONFLICT (part_key, principal) DO UPDATE SET
              level = excluded.level
            , rev = excluded.rev",
    )
    .await
}

pub async fn grant_access(
    x: &impl Executor,
    rev: Rev,
    grants: &[(PartKey, PeerKey, AccessLevel)],
) -> Res<Vec<LogKey>> {
    let pairs = grants
        .iter()
        .map(|(part, principal, _)| (part.clone(), principal.clone()))
        .collect::<BTreeSet<_>>();
    let mut rows = load_access(x, &pairs).await?;
    let mut writes = BTreeMap::new();
    for (part, principal, level) in grants {
        let key = (part.clone(), principal.clone());
        if rows.get(&key) == Some(&Some(*level)) {
            continue;
        }
        rows.insert(key.clone(), Some(*level));
        writes.insert(key, Some(*level));
    }
    let stamped = writes
        .keys()
        .map(|(part, principal)| LogKey::Access {
            part: part.clone(),
            principal: principal.clone(),
        })
        .collect();
    write_access(x, rev, writes).await?;
    Ok(stamped)
}

pub async fn revoke_access(
    x: &impl Executor,
    rev: Rev,
    revokes: &[(PartKey, PeerKey)],
) -> Res<Vec<LogKey>> {
    let pairs = revokes.iter().cloned().collect::<BTreeSet<_>>();
    let mut rows = load_access(x, &pairs).await?;
    let mut writes = BTreeMap::new();
    for key in revokes {
        if matches!(rows.get(key), Some(Some(_))) {
            rows.insert(key.clone(), None);
            writes.insert(key.clone(), None);
        }
    }
    let stamped = writes
        .keys()
        .map(|(part, principal)| LogKey::Access {
            part: part.clone(),
            principal: principal.clone(),
        })
        .collect();
    write_access(x, rev, writes).await?;
    Ok(stamped)
}

/// Drop the payloads of objects that are members of no part. An object with no payload is
/// skipped. Holding a payload while a live member of a part is a caller bug and panics, before
/// anything is deleted; the transaction is then dropped, never committed.
pub async fn drop_payloads(x: &impl Executor, objs: &[ObjKey]) -> Res<()> {
    let objs = objs.iter().collect::<BTreeSet<_>>();
    for chunk in sql::chunk_rows(objs.iter().copied().collect(), 1, 0) {
        let mut query =
            Sql::new("SELECT obj_key, part_count FROM big_automata_objs WHERE obj_key IN ");
        query.bind_list(chunk.iter().map(|obj| obj.blob()));
        for (obj, parts) in query.query(x).await?.all::<(Vec<u8>, i64)>()? {
            assert!(
                parts == 0,
                "dropping the payload of {}, which is a live member of {parts} part(s)",
                ObjKey::from_blob(&obj)
            );
        }
    }
    for chunk in sql::chunk_rows(objs.into_iter().collect(), 1, 0) {
        let mut delete = Sql::new("DELETE FROM big_automata_objs WHERE obj_key IN ");
        delete.bind_list(chunk.iter().map(|obj| obj.blob()));
        delete.execute(x).await?;
    }
    Ok(())
}
