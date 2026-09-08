//! SQLite row selection for the part-store frontier.

use crate::keyed_frontier::{SqliteReadError, SqliteReadSource};
use big_sync_core::keyed_frontier::FrontierRevision;
use big_sync_core::{ObjId, PartId};
use sqlx::{QueryBuilder, Row, Sqlite};
use std::collections::BTreeMap;

/// Independent lower bounds for exact object and exact part routes.
///
/// A row selected by either map is included when it is newer than that map's
/// bound.  The OR predicate is intentionally built in SQL, rather than by
/// loading a broad source range and filtering it in memory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SqlitePartSelector {
    /// Select every member row in the scope, ignoring `objects`/`parts`.
    ///
    /// The value is the replay lower bound: the reader starts emitting rows
    /// newer than it. Used by local workers whose scope is "everything"
    /// (`All`): the part set is enumerated by the query itself at read time
    /// instead of being frozen at reader construction, so parts created
    /// later still match.
    pub(crate) all: Option<FrontierRevision>,
    pub(crate) objects: BTreeMap<ObjId, FrontierRevision>,
    pub(crate) parts: BTreeMap<PartId, FrontierRevision>,
}

impl SqlitePartSelector {
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.all.is_none() && self.objects.is_empty() && self.parts.is_empty()
    }
}

/// The current, collapsed SQLite row passed to the sibling decoder.
/// `payload_json == None` is meaningful: it is a retained deletion/tombstone
/// row and must not be discarded by a decoder merely because it has no value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SqliteFrontierRow {
    pub(crate) obj_ref: i64,
    pub(crate) part_ref: i64,
    pub(crate) revision: FrontierRevision,
    pub(crate) obj_id: ObjId,
    pub(crate) part_id: Option<PartId>,
    pub(crate) event_type: i64,
    pub(crate) payload_json: Option<String>,
}

fn id_blob(id: ObjId) -> Vec<u8> {
    id.0.into_bytes().to_vec()
}

fn part_blob(id: PartId) -> Vec<u8> {
    id.0.into_bytes().to_vec()
}

fn bytes32(bytes: Vec<u8>) -> [u8; 32] {
    bytes
        .try_into()
        .expect("SQLite part-store identifiers have exactly 32 bytes")
}

fn push_selector_predicate(
    query: &mut QueryBuilder<Sqlite>,
    selector: &SqlitePartSelector,
    scope_id: i64,
) {
    query.push(" AND (");
    if selector.all.is_some() {
        query.push("1");
        query.push(")");
        return;
    }
    let mut first = true;
    for (obj_id, lower_bound) in &selector.objects {
        if !first {
            query.push(" OR ");
        }
        first = false;
        query.push("(m.obj_ref IN (SELECT obj_ref FROM big_sync_objs WHERE scope_id = ");
        query.push_bind(scope_id);
        query.push(" AND obj_id = ");
        query.push_bind(id_blob(*obj_id));
        query.push(") AND m.txid > ");
        query.push_bind(i64::try_from(*lower_bound).expect("frontier revision fits SQLite"));
        query.push(")");
    }
    for (part_id, lower_bound) in &selector.parts {
        if !first {
            query.push(" OR ");
        }
        first = false;
        query.push("(m.maybe_part_ref IN (SELECT part_ref FROM big_sync_parts WHERE scope_id = ");
        query.push_bind(scope_id);
        query.push(" AND part_id = ");
        query.push_bind(part_blob(*part_id));
        query.push(") AND m.txid > ");
        query.push_bind(i64::try_from(*lower_bound).expect("frontier revision fits SQLite"));
        query.push(")");
    }
    if first {
        query.push("0");
    }
    query.push(")");
}

pub(crate) async fn part_query_rows<S>(
    source: &S,
    scope_id: i64,
    selector: &SqlitePartSelector,
    after: FrontierRevision,
    through: FrontierRevision,
    exact_revision: Option<FrontierRevision>,
    limit: Option<usize>,
) -> Result<Vec<SqliteFrontierRow>, SqliteReadError>
where
    S: SqliteReadSource,
{
    if selector.is_empty() || after >= through && exact_revision.is_none() {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::<Sqlite>::new(
        "SELECT m.obj_ref
             , m.maybe_part_ref
             , m.txid
             , o.obj_id
             , p.part_id
             , m.event_type
             , o.payload_json
          FROM big_sync_members m
          JOIN big_sync_objs o ON o.obj_ref = m.obj_ref
     LEFT JOIN big_sync_parts p ON p.part_ref = m.maybe_part_ref
         WHERE m.scope_id = ",
    );
    query.push_bind(scope_id);
    query.push(" AND m.txid > ");
    query.push_bind(i64::try_from(after).expect("frontier revision fits SQLite"));
    query.push(" AND m.txid <= ");
    query.push_bind(i64::try_from(through).expect("frontier revision fits SQLite"));
    if let Some(exact_revision) = exact_revision {
        query.push(" AND m.txid = ");
        query.push_bind(i64::try_from(exact_revision).expect("frontier revision fits SQLite"));
    }
    push_selector_predicate(&mut query, selector, scope_id);
    query.push(
        " ORDER BY m.txid\
                       , m.obj_ref\
                       , m.maybe_part_ref",
    );
    if let Some(limit) = limit {
        query.push(" LIMIT ");
        query.push_bind(i64::try_from(limit).expect("read limit fits SQLite"));
    }
    let rows = query.build().fetch_all(source.read_pool()).await?;
    rows.into_iter()
        .map(|row| {
            Ok(SqliteFrontierRow {
                obj_ref: row.try_get("obj_ref")?,
                part_ref: row.try_get("maybe_part_ref")?,
                revision: u64::try_from(row.try_get::<i64, _>("txid")?)
                    .expect("SQLite frontier revision is non-negative"),
                obj_id: ObjId::new(bytes32(row.try_get::<Vec<u8>, _>("obj_id")?)),
                part_id: row
                    .try_get::<Option<Vec<u8>>, _>("part_id")?
                    .map(|bytes| PartId::new(bytes32(bytes))),
                event_type: row.try_get("event_type")?,
                payload_json: row.try_get("payload_json")?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_keeps_object_and_part_bounds_independent() {
        let object = ObjId::new([1; 32]);
        let part = PartId::new([2; 32]);
        let selector = SqlitePartSelector {
            all: None,
            objects: BTreeMap::from([(object, 7)]),
            parts: BTreeMap::from([(part, 19)]),
        };
        assert_eq!(selector.objects[&object], 7);
        assert_eq!(selector.parts[&part], 19);
    }

    #[test]
    fn match_all_selector_is_never_empty() {
        let selector = SqlitePartSelector {
            all: Some(9),
            ..Default::default()
        };
        assert!(!selector.is_empty());
        assert!(SqlitePartSelector::default().is_empty());
    }
}
