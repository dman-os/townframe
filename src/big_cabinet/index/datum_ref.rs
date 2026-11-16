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
        // Document references and key references live in two tables, because a key reference has
        // no target document. Both are read through the same seven columns: a key reference's
        // `target_doc` and `ref_heads` are NULL, which is how a row tells the two apart.
        const EDGES: &str = r#"SELECT origin_doc
                 , origin_datum
                 , origin_path
                 , target_doc
                 , target_datum
                 , ref_heads
              FROM big_cabinet_datum_ref_edges
             WHERE "#;
        const KEY_EDGES: &str = r#"SELECT origin_doc
                 , origin_datum
                 , origin_path
                 , NULL AS target_doc
                 , target_key AS target_datum
                 , NULL AS ref_heads
              FROM big_cabinet_datum_ref_key_edges
             WHERE "#;
        let mut query = Sql::new("");
        match &selector {
            DatumRefQuery::Origin(route) => {
                query
                    .push(EDGES)
                    .push("origin_doc = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND origin_datum = ")
                    .bind(route.datum_key.to_string())
                    .push(" UNION ALL ")
                    .push(KEY_EDGES)
                    .push("origin_doc = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND origin_datum = ")
                    .bind(route.datum_key.to_string());
            }
            DatumRefQuery::OriginDoc(doc_id) => {
                query
                    .push(EDGES)
                    .push("origin_doc = ")
                    .bind(doc_id.to_string())
                    .push(" UNION ALL ")
                    .push(KEY_EDGES)
                    .push("origin_doc = ")
                    .bind(doc_id.to_string());
            }
            DatumRefQuery::Target(route) => {
                query
                    .push(EDGES)
                    .push("target_doc = ")
                    .bind(route.doc_id.to_string())
                    .push(" AND target_datum = ")
                    .bind(route.datum_key.to_string());
            }
            DatumRefQuery::TargetDoc(doc_id) => {
                query
                    .push(EDGES)
                    .push("target_doc = ")
                    .bind(doc_id.to_string());
            }
            DatumRefQuery::Key(key) => {
                query
                    .push(KEY_EDGES)
                    .push("target_key = ")
                    .bind(key.to_string());
            }
        }
        query.push(" ORDER BY origin_doc, origin_datum, origin_path, target_doc, target_datum");
        let rows = query.query(&self.sql).await?.rows;
        rows.into_iter()
            .map(|row| {
                let target_datum = row.get::<String>("target_datum")?.parse()?;
                let target = match row.get::<Option<String>>("target_doc")? {
                    Some(doc_id) => DatumRefTarget::Route {
                        route: DatumRoute {
                            doc_id: doc_id.parse()?,
                            datum_key: target_datum,
                        },
                        heads: serde_json::from_slice(&row.get::<Vec<u8>>("ref_heads")?)?,
                    },
                    None => DatumRefTarget::Key(target_datum),
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
