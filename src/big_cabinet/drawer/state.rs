//! The local drawer projection: which drawers this node knows, what their rosters list, and the
//! two local drawers. SQLite, owned by DrawerService.
//!
//! It is a projection, not authority: the roster facets on the drawer docs are authoritative, and
//! these rows are what this service wrote plus what it read back from them. They exist so that
//! "which drawers hold doc X" — the question every DocsRepo access check asks — is one indexed
//! query rather than a scan of every drawer doc on the node.

use super::{DrawerId, DrawersError};
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::drawer::{Drawer, DrawerAccess as Access};
use sqlx::Row as _;
use sqlx_utils_rs::SqlCtx;
use std::collections::BTreeMap;
use std::sync::OnceLock;

use big_cabinet_core::drawer::protocol::{DrawerRecord, LocalDrawers};

pub(crate) struct State {
    sql: SqlCtx,
    local: OnceLock<LocalDrawers>,
    changes: tokio::sync::watch::Sender<u64>,
}

pub(crate) enum ScopeTarget<'a> {
    Drawer(&'a DrawerId),
    Doc(&'a DocId),
}

pub(crate) const SCOPED_DOCS_SQL: &str = r#"
WITH RECURSIVE levels(access, level) AS (
    VALUES ('Relay', 0), ('Read', 1), ('Write', 2), ('Admin', 3)
), reachable(drawer_id, level) AS (
    SELECT ?1, ?2
    UNION
    SELECT links.child_id
         , MIN(reachable.level, levels.level)
      FROM reachable
      JOIN big_cabinet_drawer_children AS links ON links.parent_id = reachable.drawer_id
      JOIN levels ON levels.access = links.access
      JOIN big_cabinet_drawers AS parent ON parent.drawer_id = reachable.drawer_id
     WHERE json_extract(parent.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
), entries(doc_id, level, via) AS (
    SELECT docs.doc_id
         , MIN(reachable.level, levels.level)
         , CASE WHEN reachable.drawer_id = ?1 THEN NULL ELSE reachable.drawer_id END
      FROM reachable
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = reachable.drawer_id
      JOIN big_cabinet_drawer_docs AS docs ON docs.drawer_id = reachable.drawer_id
      JOIN levels ON levels.access = docs.access
     WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
    UNION ALL
    SELECT reachable.drawer_id
         , reachable.level
         , CASE WHEN reachable.drawer_id = ?1 THEN NULL ELSE reachable.drawer_id END
      FROM reachable
      JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = reachable.drawer_id
     WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.selfDoc'
), ranked AS (
    SELECT doc_id
         , level
         , via
         , ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY level DESC, via IS NOT NULL, via) AS ordinal
      FROM entries
), scoped_docs AS (
    SELECT doc_id
         , level
         , via
      FROM ranked
     WHERE ordinal = 1
)
"#;

fn level(access: Access) -> i64 {
    match access {
        Access::Relay => 0,
        Access::Read => 1,
        Access::Write => 2,
        Access::Admin => 3,
    }
}

fn access_level(value: i64) -> Access {
    match value {
        0 => Access::Relay,
        1 => Access::Read,
        2 => Access::Write,
        3 => Access::Admin,
        _ => unreachable!("projected access is a known level"),
    }
}

fn storage(error: sqlx::Error) -> DrawersError {
    DrawersError::Storage(error.to_string())
}

fn access_text(access: Access) -> &'static str {
    access.as_str()
}

fn access_column(row: &sqlx::sqlite::SqliteRow, column: &str) -> Access {
    row.get::<String, _>(column)
        .parse()
        .expect("the access column is constrained to the drawer access vocabulary")
}

fn id_column(row: &sqlx::sqlite::SqliteRow, column: &str) -> DocId {
    row.get::<String, _>(column)
        .parse()
        .expect("an id column holds the text form of a doc id")
}

fn drawer_record(row: &sqlx::sqlite::SqliteRow) -> DrawerRecord {
    DrawerRecord {
        drawer_id: id_column(row, "drawer_id"),
        descriptor: serde_json::from_str(&row.get::<String, _>("descriptor_json"))
            .expect("a stored descriptor was written by this service"),
    }
}

impl State {
    pub(crate) async fn open(sql: SqlCtx) -> Result<Self, DrawersError> {
        for statement in [
            // The descriptor is stored verbatim, not decomposed: it is an open facet, and a
            // column per field (or a CHECK on the layout tag) would reject exactly the future
            // members carrying exists to survive. Interpretation happens at read time.
            "CREATE TABLE IF NOT EXISTS big_cabinet_drawers (
                 drawer_id TEXT PRIMARY KEY
               , descriptor_json TEXT NOT NULL
             ) STRICT",
            "CREATE TABLE IF NOT EXISTS big_cabinet_drawer_docs (
                 drawer_id TEXT NOT NULL
               , doc_id TEXT NOT NULL
               , access TEXT NOT NULL CHECK (access IN ('Relay', 'Read', 'Write', 'Admin'))
               , PRIMARY KEY (drawer_id, doc_id)
             ) STRICT",
            "CREATE INDEX IF NOT EXISTS big_cabinet_drawer_docs_by_doc
                 ON big_cabinet_drawer_docs (doc_id)",
            "CREATE TABLE IF NOT EXISTS big_cabinet_drawer_children (
                 parent_id TEXT NOT NULL
               , child_id TEXT NOT NULL
               , access TEXT NOT NULL CHECK (access IN ('Relay', 'Read', 'Write', 'Admin'))
               , PRIMARY KEY (parent_id, child_id)
             ) STRICT",
            "CREATE INDEX IF NOT EXISTS big_cabinet_drawer_children_by_child
                 ON big_cabinet_drawer_children (child_id)",
            "CREATE TABLE IF NOT EXISTS big_cabinet_drawer_local (
                 role TEXT PRIMARY KEY CHECK (role IN ('present', 'originated', 'registry'))
               , drawer_id TEXT NOT NULL
             ) STRICT",
        ] {
            sqlx::query(statement)
                .execute(&sql.write_pool)
                .await
                .map_err(storage)?;
        }
        let (changes, _) = tokio::sync::watch::channel(0);
        let state = Self {
            sql,
            local: OnceLock::new(),
            changes,
        };
        if let Some(local) = state.load_local().await? {
            state
                .local
                .set(local)
                .expect("a fresh state has no local drawers yet");
        }
        Ok(state)
    }

    pub(crate) fn subscribe_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.changes.subscribe()
    }

    pub(crate) fn notify_changed(&self) {
        self.changes.send_modify(|revision| *revision += 1);
    }

    /// The local drawers. Set by boot before any request is served.
    pub(crate) fn local(&self) -> LocalDrawers {
        self.local
            .get()
            .expect("the local drawers exist before DrawerService serves a request")
            .clone()
    }

    pub(crate) fn local_if_set(&self) -> Option<&LocalDrawers> {
        self.local.get()
    }

    async fn load_local(&self) -> Result<Option<LocalDrawers>, DrawersError> {
        let rows = sqlx::query(
            "SELECT role
                  , drawer_id
               FROM big_cabinet_drawer_local",
        )
        .fetch_all(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        let mut present = None;
        let mut originated = None;
        let mut registry = None;
        for row in rows {
            let id = id_column(&row, "drawer_id");
            match row.get::<String, _>("role").as_str() {
                "present" => present = Some(id),
                "originated" => originated = Some(id),
                "registry" => registry = Some(id),
                role => unreachable!("the role column is constrained: {role}"),
            }
        }
        match (present, originated, registry) {
            (Some(present), Some(originated), Some(registry)) => Ok(Some(LocalDrawers {
                present_id: present,
                originated_id: originated,
                registry_id: registry,
            })),
            (None, None, None) => Ok(None),
            _ => unreachable!("the local drawers are written together"),
        }
    }

    pub(crate) async fn set_local(&self, local: LocalDrawers) -> Result<(), DrawersError> {
        let mut transaction = self.sql.write_pool.begin().await.map_err(storage)?;
        for (role, id) in [
            ("present", &local.present_id),
            ("originated", &local.originated_id),
            ("registry", &local.registry_id),
        ] {
            sqlx::query(
                "INSERT INTO big_cabinet_drawer_local (role, drawer_id)
                 VALUES (?, ?)
                 ON CONFLICT (role) DO UPDATE SET drawer_id = excluded.drawer_id",
            )
            .bind(role)
            .bind(id.to_string())
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        }
        transaction.commit().await.map_err(storage)?;
        self.local
            .set(local)
            .expect("the local drawers are set once, at boot");
        Ok(())
    }

    pub(crate) async fn drawer(&self, id: &DrawerId) -> Result<Option<DrawerRecord>, DrawersError> {
        let row = sqlx::query(
            "SELECT drawer_id
                  , descriptor_json
               FROM big_cabinet_drawers
              WHERE drawer_id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(row.as_ref().map(drawer_record))
    }

    pub(crate) async fn drawers(&self) -> Result<Vec<DrawerRecord>, DrawersError> {
        let rows = sqlx::query(
            "SELECT drawer_id
                  , descriptor_json
               FROM big_cabinet_drawers
              ORDER BY drawer_id",
        )
        .fetch_all(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(rows.iter().map(drawer_record).collect())
    }

    pub(crate) async fn scoped_docs(
        &self,
        drawer_id: &DrawerId,
        access: Access,
    ) -> Result<BTreeMap<DocId, (Access, Option<DrawerId>)>, DrawersError> {
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(SCOPED_DOCS_SQL);
        query.push(" SELECT doc_id, level, via FROM scoped_docs ORDER BY doc_id");
        let rows = query
            .build()
            .bind(drawer_id.to_string())
            .bind(level(access))
            .fetch_all(&self.sql.read_pool)
            .await
            .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let via: Option<String> = row.get("via");
                (
                    id_column(&row, "doc_id"),
                    (
                        access_level(row.get("level")),
                        via.map(|id| id.parse().expect("projected drawer id is valid")),
                    ),
                )
            })
            .collect())
    }

    pub(crate) async fn scoped_doc(
        &self,
        drawer_id: &DrawerId,
        access: Access,
        doc_id: &DocId,
    ) -> Result<Option<Access>, DrawersError> {
        let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(SCOPED_DOCS_SQL);
        query.push(" SELECT level FROM scoped_docs WHERE doc_id = ?3");
        let found: Option<i64> = query
            .build_query_scalar()
            .bind(drawer_id.to_string())
            .bind(level(access))
            .bind(doc_id.to_string())
            .fetch_optional(&self.sql.read_pool)
            .await
            .map_err(storage)?;
        Ok(found.map(access_level))
    }

    pub(crate) async fn ancestors(
        &self,
        target: ScopeTarget<'_>,
    ) -> Result<BTreeMap<DrawerId, Access>, DrawersError> {
        let (target_id, document) = match target {
            ScopeTarget::Drawer(drawer_id) => (drawer_id, false),
            ScopeTarget::Doc(doc_id) => (doc_id, true),
        };
        let rows = sqlx::query(
            r#"WITH RECURSIVE levels(access, level) AS (
                   VALUES ('Relay', 0), ('Read', 1), ('Write', 2), ('Admin', 3)
               ), reached(drawer_id, level) AS (
                   SELECT drawer_id
                        , 3
                     FROM big_cabinet_drawers
                    WHERE drawer_id = ?1
                   UNION
                   SELECT docs.drawer_id
                        , levels.level
                     FROM big_cabinet_drawer_docs AS docs
                     JOIN levels ON levels.access = docs.access
                     JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = docs.drawer_id
                    WHERE ?2 AND docs.doc_id = ?1
                      AND json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
                   UNION
                   SELECT links.parent_id
                        , MIN(reached.level, levels.level)
                     FROM reached
                     JOIN big_cabinet_drawer_children AS links ON links.child_id = reached.drawer_id
                     JOIN levels ON levels.access = links.access
                     JOIN big_cabinet_drawers AS drawer ON drawer.drawer_id = links.parent_id
                    WHERE json_extract(drawer.descriptor_json, '$.layout.$type') = 'org.example.daybook.drawer.singleGroup'
               )
               SELECT drawer_id
                    , MAX(level) AS level
                 FROM reached
                GROUP BY drawer_id
                ORDER BY drawer_id"#,
        )
        .bind(target_id.to_string())
        .bind(document)
        .fetch_all(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| (id_column(&row, "drawer_id"), access_level(row.get("level"))))
            .collect())
    }

    /// Candidate drawer ids referenced by child links or by the `facet_set` index that have not
    /// yet been projected into `big_cabinet_drawers`.
    pub(crate) async fn unprojected_candidates(&self) -> Result<Vec<DrawerId>, DrawersError> {
        let mut candidates = std::collections::BTreeSet::new();
        let child_rows = sqlx::query(
            "SELECT DISTINCT child_id
               FROM big_cabinet_drawer_children
              WHERE child_id NOT IN (
                        SELECT drawer_id
                          FROM big_cabinet_drawers
                    )",
        )
        .fetch_all(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        for row in child_rows {
            if let Ok(id) = row.get::<String, _>("child_id").parse() {
                candidates.insert(id);
            }
        }
        let has_facet_set: Option<String> = sqlx::query_scalar(
            "SELECT name
               FROM sqlite_master
              WHERE type = 'table'
                AND name = 'big_cabinet_facet_set_entries'",
        )
        .fetch_optional(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        if has_facet_set.is_some() {
            let facet_rows = sqlx::query(
                "SELECT DISTINCT doc_id
                   FROM big_cabinet_facet_set_entries
                  WHERE facet_tag = ?
                    AND doc_id NOT IN (
                            SELECT drawer_id
                              FROM big_cabinet_drawers
                        )",
            )
            .bind(big_cabinet_core::drawer::DRAWER_FACET_TAG)
            .fetch_all(&self.sql.read_pool)
            .await
            .map_err(storage)?;
            for row in facet_rows {
                if let Ok(id) = row.get::<String, _>("doc_id").parse() {
                    candidates.insert(id);
                }
            }
        }
        Ok(candidates.into_iter().collect())
    }

    pub(crate) async fn put_drawer(
        &self,
        id: &DrawerId,
        descriptor: &Drawer,
    ) -> Result<(), DrawersError> {
        if self
            .drawer(id)
            .await?
            .is_some_and(|existing| &existing.descriptor == descriptor)
        {
            return Ok(());
        }
        sqlx::query(
            "INSERT INTO big_cabinet_drawers (drawer_id, descriptor_json)
             VALUES (?, ?)
             ON CONFLICT (drawer_id) DO UPDATE
                SET descriptor_json = excluded.descriptor_json",
        )
        .bind(id.to_string())
        .bind(serde_json::to_string(descriptor).expect("a descriptor serializes"))
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        self.notify_changed();
        Ok(())
    }

    pub(crate) async fn put_doc(
        &self,
        drawer: &DrawerId,
        doc: &DocId,
        access: Access,
    ) -> Result<(), DrawersError> {
        sqlx::query(
            "INSERT INTO big_cabinet_drawer_docs (drawer_id, doc_id, access)
             VALUES (?, ?, ?)
             ON CONFLICT (drawer_id, doc_id) DO UPDATE SET access = excluded.access",
        )
        .bind(drawer.to_string())
        .bind(doc.to_string())
        .bind(access_text(access))
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        self.notify_changed();
        Ok(())
    }

    pub(crate) async fn remove_doc(
        &self,
        drawer: &DrawerId,
        doc: &DocId,
    ) -> Result<(), DrawersError> {
        sqlx::query(
            "DELETE FROM big_cabinet_drawer_docs
              WHERE drawer_id = ?
                AND doc_id = ?",
        )
        .bind(drawer.to_string())
        .bind(doc.to_string())
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        self.notify_changed();
        Ok(())
    }

    pub(crate) async fn doc_entry(
        &self,
        drawer: &DrawerId,
        doc: &DocId,
    ) -> Result<Option<Access>, DrawersError> {
        let row = sqlx::query(
            "SELECT access
               FROM big_cabinet_drawer_docs
              WHERE drawer_id = ?
                AND doc_id = ?",
        )
        .bind(drawer.to_string())
        .bind(doc.to_string())
        .fetch_optional(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(row.map(|row| access_column(&row, "access")))
    }

    /// The roster's doc entries, as this node last saw them.
    pub(crate) async fn docs_of(
        &self,
        drawer: &DrawerId,
    ) -> Result<BTreeMap<DocId, Access>, DrawersError> {
        let rows = sqlx::query(
            "SELECT doc_id
                  , access
               FROM big_cabinet_drawer_docs
              WHERE drawer_id = ?
              ORDER BY doc_id",
        )
        .bind(drawer.to_string())
        .fetch_all(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .iter()
            .map(|row| (id_column(row, "doc_id"), access_column(row, "access")))
            .collect())
    }

    pub(crate) async fn put_child(
        &self,
        parent: &DrawerId,
        child: &DrawerId,
        access: Access,
    ) -> Result<(), DrawersError> {
        sqlx::query(
            "INSERT INTO big_cabinet_drawer_children (parent_id, child_id, access)
             VALUES (?, ?, ?)
             ON CONFLICT (parent_id, child_id) DO UPDATE SET access = excluded.access",
        )
        .bind(parent.to_string())
        .bind(child.to_string())
        .bind(access_text(access))
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        self.notify_changed();
        Ok(())
    }

    pub(crate) async fn remove_child(
        &self,
        parent: &DrawerId,
        child: &DrawerId,
    ) -> Result<(), DrawersError> {
        sqlx::query(
            "DELETE FROM big_cabinet_drawer_children
              WHERE parent_id = ?
                AND child_id = ?",
        )
        .bind(parent.to_string())
        .bind(child.to_string())
        .execute(&self.sql.write_pool)
        .await
        .map_err(storage)?;
        self.notify_changed();
        Ok(())
    }

    pub(crate) async fn child_entry(
        &self,
        parent: &DrawerId,
        child: &DrawerId,
    ) -> Result<Option<Access>, DrawersError> {
        let row = sqlx::query(
            "SELECT access
               FROM big_cabinet_drawer_children
              WHERE parent_id = ?
                AND child_id = ?",
        )
        .bind(parent.to_string())
        .bind(child.to_string())
        .fetch_optional(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(row.map(|row| access_column(&row, "access")))
    }

    pub(crate) async fn children_of(
        &self,
        parent: &DrawerId,
    ) -> Result<BTreeMap<DrawerId, Access>, DrawersError> {
        let rows = sqlx::query(
            "SELECT child_id
                  , access
               FROM big_cabinet_drawer_children
              WHERE parent_id = ?
              ORDER BY child_id",
        )
        .bind(parent.to_string())
        .fetch_all(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .iter()
            .map(|row| (id_column(row, "child_id"), access_column(row, "access")))
            .collect())
    }

    /// Replace a drawer's roster rows with what its doc says now: the rescan after a boot or a
    /// read that found the projection behind the doc.
    pub(crate) async fn replace_roster(
        &self,
        drawer: &DrawerId,
        docs: &BTreeMap<DocId, Access>,
        children: &BTreeMap<DrawerId, Access>,
    ) -> Result<(), DrawersError> {
        if &self.docs_of(drawer).await? == docs && &self.children_of(drawer).await? == children {
            return Ok(());
        }
        let mut transaction = self.sql.write_pool.begin().await.map_err(storage)?;
        let id = drawer.to_string();
        sqlx::query(
            "DELETE FROM big_cabinet_drawer_docs
              WHERE drawer_id = ?",
        )
        .bind(&id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        sqlx::query(
            "DELETE FROM big_cabinet_drawer_children
              WHERE parent_id = ?",
        )
        .bind(&id)
        .execute(&mut *transaction)
        .await
        .map_err(storage)?;
        for (table, source, target, entries) in [
            ("big_cabinet_drawer_docs", "drawer_id", "doc_id", docs),
            (
                "big_cabinet_drawer_children",
                "parent_id",
                "child_id",
                children,
            ),
        ] {
            let mut entries = entries.iter().peekable();
            while entries.peek().is_some() {
                let mut query = sqlx::QueryBuilder::<sqlx::Sqlite>::new(format!(
                    "INSERT INTO {table} ({source}, {target}, access) "
                ));
                query.push_values(
                    entries.by_ref().take(256),
                    |mut values, (target_id, access)| {
                        values
                            .push_bind(&id)
                            .push_bind(target_id.to_string())
                            .push_bind(access_text(*access));
                    },
                );
                query
                    .build()
                    .execute(&mut *transaction)
                    .await
                    .map_err(storage)?;
            }
        }
        transaction.commit().await.map_err(storage)?;
        self.notify_changed();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::drawer::GroupKey;

    #[tokio::test]
    async fn scoped_sql_terminates_cycles_and_selects_the_strongest_path() {
        let state = State::open(SqlCtx::memory().await.unwrap()).await.unwrap();
        let [root, left, right, leaf, doc] =
            [1_u8, 2, 3, 4, 5].map(|value| DocId::new([value; 32]));
        for id in [&root, &left, &right] {
            state
                .put_drawer(
                    id,
                    &Drawer::single_group(GroupKey([8; 32]), GroupKey([9; 32])),
                )
                .await
                .unwrap();
        }
        state.put_drawer(&leaf, &Drawer::self_doc()).await.unwrap();
        for (parent, child, access) in [
            (&root, &left, Access::Read),
            (&root, &right, Access::Write),
            (&left, &right, Access::Admin),
            (&right, &root, Access::Relay),
            (&right, &leaf, Access::Read),
        ] {
            state.put_child(parent, child, access).await.unwrap();
        }
        state.put_doc(&right, &doc, Access::Admin).await.unwrap();
        assert_eq!(
            state.scoped_docs(&root, Access::Admin).await.unwrap(),
            BTreeMap::from([
                (doc.clone(), (Access::Write, Some(right.clone()))),
                (leaf.clone(), (Access::Read, Some(leaf.clone()))),
            ]),
        );
        assert_eq!(
            state.ancestors(ScopeTarget::Doc(&doc)).await.unwrap(),
            BTreeMap::from([
                (root.clone(), Access::Write),
                (left.clone(), Access::Admin),
                (right.clone(), Access::Admin),
            ]),
        );
        assert_eq!(
            state.ancestors(ScopeTarget::Drawer(&leaf)).await.unwrap(),
            BTreeMap::from([
                (root.clone(), Access::Read),
                (left.clone(), Access::Read),
                (right.clone(), Access::Read),
                (leaf.clone(), Access::Admin),
            ]),
        );
        assert_eq!(
            state.scoped_docs(&root, Access::Relay).await.unwrap(),
            BTreeMap::from([
                (doc.clone(), (Access::Relay, Some(right.clone()))),
                (leaf.clone(), (Access::Relay, Some(leaf.clone()))),
            ]),
        );
        state.put_drawer(&right, &Drawer::self_doc()).await.unwrap();
        assert_eq!(
            state.scoped_docs(&root, Access::Admin).await.unwrap(),
            BTreeMap::from([(right.clone(), (Access::Write, Some(right)))]),
        );
        assert!(
            state
                .ancestors(ScopeTarget::Doc(&doc))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn scoped_facet_query_only_returns_readable_verified_members() {
        use crate::index::facet_set::{FacetRecord, FacetRoute, FacetSetStore};
        use big_cabinet_core::{FacetKey, FacetTag};

        let sql = SqlCtx::memory().await.unwrap();
        let state = State::open(sql.clone()).await.unwrap();
        let store = FacetSetStore::new(sql.clone()).await.unwrap();
        let [drawer_id, readable, relay, unrelated] =
            [1_u8, 2, 3, 4].map(|value| DocId::new([value; 32]));
        state
            .put_drawer(
                &drawer_id,
                &Drawer::single_group(GroupKey([8; 32]), GroupKey([9; 32])),
            )
            .await
            .unwrap();
        state
            .put_doc(&drawer_id, &readable, Access::Read)
            .await
            .unwrap();
        state
            .put_doc(&drawer_id, &relay, Access::Relay)
            .await
            .unwrap();
        let facet_key = FacetKey::from_text("org.example.note", "main").unwrap();
        let tag = FacetTag::new("org.example.note").unwrap();
        for doc_id in [&readable, &relay, &unrelated] {
            sqlx::query(
                "INSERT INTO big_cabinet_facet_set_entries
                      ( doc_id
                      , facet_key
                      , facet_tag
                      , doc_heads
                      , facet_heads
                      , clock
                      )
                 VALUES (?, ?, ?, ?, ?, 0)",
            )
            .bind(doc_id.to_string())
            .bind(facet_key.to_string())
            .bind(tag.as_str())
            .bind(b"[]".as_slice())
            .bind(b"[]".as_slice())
            .execute(&sql.write_pool)
            .await
            .unwrap();
        }
        let expected = vec![FacetRecord {
            route: FacetRoute {
                doc_id: readable.clone(),
                facet_key: facet_key.clone(),
            },
            doc_heads: Vec::new(),
            facet_heads: Vec::new(),
            clock: 0,
        }];
        assert_eq!(
            store
                .query_drawer(&drawer_id, Access::Admin, None, None, None)
                .await
                .unwrap(),
            expected,
        );
        assert_eq!(
            store
                .query_drawer(
                    &drawer_id,
                    Access::Read,
                    Some(&tag),
                    Some(&facet_key),
                    Some(&readable)
                )
                .await
                .unwrap(),
            expected,
        );
        assert!(
            store
                .query_drawer(&drawer_id, Access::Relay, None, None, None)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .query_drawer(&drawer_id, Access::Admin, None, None, Some(&unrelated))
                .await
                .unwrap()
                .is_empty()
        );
        state.remove_doc(&drawer_id, &readable).await.unwrap();
        assert!(
            store
                .query_drawer(&drawer_id, Access::Admin, None, None, None)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
