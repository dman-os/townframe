//! The declared-reference index: which datum values point at which other datums.
//!
//! The edges this store answers with are *written by the datum-set worker*, not by a worker of
//! their own. datum-set already reads every document once to project its predicate entries; from
//! that same read it extracts the declared references of every reference-tagged route and replaces
//! `big_cabinet_datum_ref_edges` for the route in the very transaction that settles the entries
//! (see [`super::datum_set`]). Folding extraction in removes the second document load, the per-tag
//! cursors, the parking table, and the separate `settled` watch the standalone worker used to
//! carry: there is now one projection pass and one settle per document revision.
//!
//! What remains here is the read surface: [`DatumRefStore::query`] answers
//! [`DatumRefQuery::Origin`], [`DatumRefQuery::OriginDoc`], [`DatumRefQuery::Target`], and
//! [`DatumRefQuery::TargetDoc`] straight from the edge table. Constructing the store still applies
//! its migration so a database that only ever answers edge queries — one where datum-set has not
//! yet run — finds the (idempotently shared) schema.
//!
//! The `big_cabinet_datum_ref_progress` and `big_cabinet_datum_ref_parked` tables in migration
//! `001_init.sql` are inert remnants of the retired standalone worker. They are left in place so
//! the migration's recorded identity does not change under databases that already applied it; no
//! code reads or writes them.

use automerge::ChangeHash;
use big_cabinet_core::DatumKey;
use big_cabinet_core::DocId;
use sqlite_rs::Database;
use sqlite_rs::Sql;
use utils_rs::prelude::Res;

use super::datum_set::DatumRoute;

mod queries {
    include!("datum_ref/sqlite/gen.rs");
}

/// The `target_doc` an edge table row stores for a key reference. A `#keyRef` names no document,
/// and no document id is empty, so the empty string marks the row as one whose `target_datum` is
/// the key itself. Sharing the table keeps "what references this" one indexed query on
/// `(target_doc, target_datum)` for both kinds of reference, and no join on a document id can
/// match a key reference's row.
pub(super) const KEY_REF_TARGET_DOC: &str = "";

/// What a recorded reference addresses.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DatumRefTarget {
    /// A datum in a document; a `self` reference resolves against the origin document.
    Route {
        route: DatumRoute,
        /// The frontier a `datumPin` pins the target to; empty follows the target's current
        /// frontier.
        heads: Vec<ChangeHash>,
    },
    /// A content-addressed datum named by its key alone (`#keyRef`, ADR 035 §2.9).
    Key(DatumKey),
}

impl DatumRefTarget {
    /// The `(target_doc, target_datum, ref_heads)` columns of the edge row for this target.
    pub(super) fn columns(&self) -> Res<(String, String, Vec<u8>)> {
        Ok(match self {
            Self::Route { route, heads } => (
                route.doc_id.to_string(),
                route.datum_key.to_string(),
                serde_json::to_vec(heads)?,
            ),
            Self::Key(key) => (
                KEY_REF_TARGET_DOC.to_owned(),
                key.to_string(),
                serde_json::to_vec(&Vec::<ChangeHash>::new())?,
            ),
        })
    }
}

/// One recorded reference: a position in one datum's value that addresses another datum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatumRefEdge {
    /// The document and datum the reference was found in.
    pub origin: DatumRoute,
    /// Where in the origin datum's value the reference sits, as a JSON pointer.
    pub path: String,
    pub target: DatumRefTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatumRefQuery {
    /// Every edge out of one datum route.
    Origin(DatumRoute),
    /// Every edge out of any datum in one document.
    OriginDoc(DocId),
    /// Every edge pointing at one datum route.
    Target(DatumRoute),
    /// Every edge pointing at any datum in one document.
    TargetDoc(DocId),
    /// Every `#keyRef` naming one key, whichever document holds the referrer.
    Key(DatumKey),
}

#[derive(Clone)]
pub struct DatumRefStore {
    sql: Database,
}

impl DatumRefStore {
    pub async fn new(sql: Database) -> Res<Self> {
        sql.apply_migrations(queries::MIGRATION_TABLE, queries::MIGRATIONS)
            .await?;
        Ok(Self { sql })
    }

    pub async fn query(&self, selector: DatumRefQuery) -> Res<Vec<DatumRefEdge>> {
        let mut query = Sql::new(
            r#"SELECT origin_doc
                 , origin_datum
                 , origin_path
                 , target_doc
                 , target_datum
                 , ref_heads
              FROM big_cabinet_datum_ref_edges
             WHERE "#,
        );
        match &selector {
            DatumRefQuery::Origin(route) => {
                query
                    .push("origin_doc = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND origin_datum = ")
                    .bind(route.datum_key.to_string());
            }
            DatumRefQuery::OriginDoc(doc_id) => {
                query.push("origin_doc = ").bind(doc_id.to_string());
            }
            DatumRefQuery::Target(route) => {
                query
                    .push("target_doc = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND target_datum = ")
                    .bind(route.datum_key.to_string());
            }
            DatumRefQuery::TargetDoc(doc_id) => {
                query.push("target_doc = ").bind(doc_id.to_string());
            }
            DatumRefQuery::Key(key) => {
                query
                    .push("target_doc = ")
                    .bind(KEY_REF_TARGET_DOC)
                    .push(" AND target_datum = ")
                    .bind(key.to_string());
            }
        }
        query.push(" ORDER BY origin_doc, origin_datum, origin_path, target_doc, target_datum");
        let rows = query.query(&self.sql).await?.rows;
        rows.into_iter()
            .map(|row| {
                let target_doc = row.get::<String>("target_doc")?;
                let target_datum = row.get::<String>("target_datum")?.parse()?;
                let target = if target_doc == KEY_REF_TARGET_DOC {
                    DatumRefTarget::Key(target_datum)
                } else {
                    DatumRefTarget::Route {
                        route: DatumRoute {
                            doc_id: target_doc.parse()?,
                            datum_key: target_datum,
                        },
                        heads: serde_json::from_slice(&row.get::<Vec<u8>>("ref_heads")?)?,
                    }
                };
                Ok(DatumRefEdge {
                    origin: DatumRoute {
                        doc_id: row.get::<String>("origin_doc")?.parse()?,
                        datum_key: row.get::<String>("origin_datum")?.parse()?,
                    },
                    path: row.get("origin_path")?,
                    target,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
