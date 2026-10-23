//! The local drawer projection: which drawers this node knows, what their rosters list, and the
//! two local drawers. SQLite, owned by DrawerService.
//!
//! It is a projection, not authority: the roster facets on the drawer docs are authoritative, and
//! these rows are what this service wrote plus what it read back from them. They exist so that
//! "which drawers hold doc X" — the question every DocsRepo access check asks — is one indexed
//! query rather than a scan of every drawer doc on the node.

use super::{DrawerId, DrawersError};
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::drawer::{Drawer, DrawerAccess as Access, DrawerGroups, LayoutKind};
use serde::{Deserialize, Serialize};
use sqlx::Row as _;
use sqlx_utils_rs::SqlCtx;
use std::collections::BTreeMap;
use std::sync::OnceLock;

/// The two drawers every doc created on this node is entered into (ADR 014).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalDrawers {
    /// Docs physically present on this node.
    pub present_id: DrawerId,
    /// Docs this node originated.
    pub originated_id: DrawerId,
    /// The known-drawers registry: an explicitly managed list, not a collection — remembering a
    /// drawer grants no access, and observing a tag never writes here.
    pub registry_id: DrawerId,
}

impl LocalDrawers {
    /// The drawers a new doc is placed into: the registry is not one — it records intent
    /// about *drawers*, and a doc does not belong in it by being created here.
    pub fn both(&self) -> [&DrawerId; 2] {
        [&self.present_id, &self.originated_id]
    }
}

/// A known drawer as this node carries it: the descriptor facet, verbatim. The facet is open —
/// a layout this build does not know, and fields it does not read, survive in `descriptor`
/// untouched. Operating code asks `layout_kind()` and refuses what it cannot interpret;
/// carrying code (listing, storing) never has to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrawerRecord {
    pub drawer_id: DrawerId,
    pub descriptor: Drawer,
}

impl DrawerRecord {
    /// The layout if it is one this build operates; `None` is a carried foreign layout.
    pub fn layout_kind(&self) -> Option<LayoutKind> {
        self.descriptor.layout.known()
    }

    /// Present exactly when the layout has groups, as the descriptor states them.
    pub fn groups(&self) -> Option<DrawerGroups> {
        self.descriptor.groups()
    }
}

pub(crate) struct State {
    sql: SqlCtx,
    local: OnceLock<LocalDrawers>,
    changes: tokio::sync::watch::Sender<u64>,
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

    /// Every drawer whose roster lists `doc`, with the entry's access: the reverse index the
    /// access policy walks.
    pub(crate) async fn drawers_of(
        &self,
        doc: &DocId,
    ) -> Result<Vec<(DrawerId, Access)>, DrawersError> {
        let rows = sqlx::query(
            "SELECT drawer_id
                  , access
               FROM big_cabinet_drawer_docs
              WHERE doc_id = ?
              ORDER BY drawer_id",
        )
        .bind(doc.to_string())
        .fetch_all(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .iter()
            .map(|row| (id_column(row, "drawer_id"), access_column(row, "access")))
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

    pub(crate) async fn parents_of(
        &self,
        child: &DrawerId,
    ) -> Result<Vec<(DrawerId, Access)>, DrawersError> {
        let rows = sqlx::query(
            "SELECT parent_id
                  , access
               FROM big_cabinet_drawer_children
              WHERE child_id = ?
              ORDER BY parent_id",
        )
        .bind(child.to_string())
        .fetch_all(&self.sql.read_pool)
        .await
        .map_err(storage)?;
        Ok(rows
            .iter()
            .map(|row| (id_column(row, "parent_id"), access_column(row, "access")))
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
        for (doc, access) in docs {
            sqlx::query(
                "INSERT INTO big_cabinet_drawer_docs (drawer_id, doc_id, access)
                 VALUES (?, ?, ?)",
            )
            .bind(&id)
            .bind(doc.to_string())
            .bind(access_text(*access))
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        }
        for (child, access) in children {
            sqlx::query(
                "INSERT INTO big_cabinet_drawer_children (parent_id, child_id, access)
                 VALUES (?, ?, ?)",
            )
            .bind(&id)
            .bind(child.to_string())
            .bind(access_text(*access))
            .execute(&mut *transaction)
            .await
            .map_err(storage)?;
        }
        transaction.commit().await.map_err(storage)?;
        self.notify_changed();
        Ok(())
    }
}
