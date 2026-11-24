//! The reads that need no authorization question, and the object states both audiences share.

use big_automata::part_store::ObjPayload;
use big_automata::part_store::ObjState;
use big_automata::part_store::PayloadDigest;
use big_automata::rev::Rev;
use sqlite_rs::QueryExecutor;
use sqlite_rs::Sql;

use crate::interlude::*;
use crate::keys::BlobKey;
use crate::sql;
use crate::sqlite::queries;

pub async fn latest_rev(x: &impl QueryExecutor) -> Res<Rev> {
    Ok(sql::rev_from_sql(queries::meta_rev(x).await?))
}

pub async fn member_count(x: &impl QueryExecutor, part: &PartKey) -> Res<u64> {
    Ok(queries::member_count(x, part.blob())
        .await?
        .map_or(0, |count| u64::try_from(count).expect(ERROR_IMPOSSIBLE)))
}

/// The objects' states, in the order asked.
///
/// With `principal`, only content readable through a part the principal may read: the live
/// parts it may read, and `None` for an object with none. Without, the object's own state:
/// with content, its live parts; without, its pending ones.
pub async fn obj_states(
    x: &impl QueryExecutor,
    principal: Option<&PeerKey>,
    objs: &[ObjKey],
) -> Res<Vec<Option<ObjState>>> {
    let asked = objs.iter().collect::<BTreeSet<_>>();
    let mut content = BTreeMap::new();
    let mut parts = BTreeMap::<ObjKey, BTreeSet<PartKey>>::new();
    let mut pending = BTreeMap::<ObjKey, BTreeSet<PartKey>>::new();
    for chunk in sql::chunk_rows(asked.into_iter().collect(), 1, 1) {
        let keys = || chunk.iter().map(|obj| obj.blob());

        let mut query = Sql::new(
            "SELECT obj_key
                  , payload
                  , digest
               FROM big_automata_objs
              WHERE obj_key IN ",
        );
        query.bind_list(keys());
        for (obj, payload, digest) in query
            .query(x)
            .await?
            .all::<(Vec<u8>, String, Vec<u8>)>()?
        {
            let payload = serde_json::from_str::<ObjPayload>(&payload)
                .expect("a stored payload is valid JSON");
            let digest = PayloadDigest(digest.try_into().expect("a stored digest is 32 bytes"));
            content.insert(ObjKey::from_blob(&obj), (payload, digest));
        }

        let mut query = match principal {
            Some(principal) => {
                let mut query = Sql::new(
                    "SELECT m.obj_key
                          , m.part_key
                       FROM big_automata_members AS m
                       JOIN big_automata_access AS a
                         ON a.part_key = m.part_key
                        AND a.level IS NOT NULL
                        AND a.principal = ",
                );
                query.bind(principal.blob());
                query.push(" WHERE m.live = 1 AND m.obj_key IN ");
                query
            }
            None => Sql::new(
                "SELECT m.obj_key
                      , m.part_key
                   FROM big_automata_members AS m
                  WHERE m.live = 1
                    AND m.obj_key IN ",
            ),
        };
        query.bind_list(keys());
        for (obj, part) in query.query(x).await?.all::<(Vec<u8>, Vec<u8>)>()? {
            parts
                .entry(ObjKey::from_blob(&obj))
                .or_default()
                .insert(PartKey::from_blob(&part));
        }

        if principal.is_none() {
            let mut query = Sql::new(
                "SELECT obj_key
                      , part_key
                   FROM big_automata_pending
                  WHERE obj_key IN ",
            );
            query.bind_list(keys());
            for (obj, part) in query.query(x).await?.all::<(Vec<u8>, Vec<u8>)>()? {
                pending
                    .entry(ObjKey::from_blob(&obj))
                    .or_default()
                    .insert(PartKey::from_blob(&part));
            }
        }
    }
    Ok(objs
        .iter()
        .map(|obj| match (content.get(obj), principal) {
            (Some(content), None) => Some(ObjState {
                content: Some(content.clone()),
                parts: parts.get(obj).cloned().unwrap_or_default(),
            }),
            (Some(content), Some(_)) => parts.get(obj).map(|parts| ObjState {
                content: Some(content.clone()),
                parts: parts.clone(),
            }),
            (None, None) => pending.get(obj).map(|parts| ObjState {
                content: None,
                parts: parts.clone(),
            }),
            (None, Some(_)) => None,
        })
        .collect())
}
