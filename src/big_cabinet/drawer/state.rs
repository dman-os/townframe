//! The local drawer projection: which drawers this node knows, what their rosters list, and the
//! two local drawers. SQLite, owned by DrawerService.
//!
//! It is a projection, not authority: the roster datums on the drawer docs are authoritative, and
//! these rows are what this service wrote plus what it read back from them. They exist so that
//! "which drawers hold doc X" — the question every DocsRepo access check asks — is one indexed
//! query rather than a scan of every drawer doc on the node.

use super::sqlite;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::drawer::Drawer;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::MetaRoot;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawersError;
use sqlite_rs::Database;
use sqlite_rs::Sql;
use sqlite_rs::SqlError;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::OnceLock;

use big_cabinet_core::drawer::protocol::DrawerRecord;
use big_cabinet_core::drawer::protocol::LocalDrawers;

pub(crate) struct State {
    sql: Database,
    local: OnceLock<LocalDrawers>,
    changes: tokio::sync::watch::Sender<u64>,
}

pub(crate) enum ScopeTarget<'a> {
    Drawer(&'a DrawerId),
    Doc(&'a DocId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Reach {
    ordinary: Option<Access>,
    public: Option<Access>,
    sensitive: Option<Access>,
}

impl Reach {
    pub(crate) fn ordinary(access: Access) -> Self {
        Self {
            ordinary: Some(access),
            public: None,
            sensitive: None,
        }
    }

    pub(crate) fn apply(self, held: Access) -> Option<Access> {
        [
            self.ordinary.map(|ceiling| held.min(ceiling)),
            self.public
                .map(|ceiling| held.min(ceiling).max(Access::Read)),
            self.sensitive
                .map(|ceiling| held.min(ceiling))
                .filter(|access| *access >= Access::Read),
        ]
        .into_iter()
        .flatten()
        .max()
    }
}

fn scoped_roots(drawers: &BTreeMap<DrawerId, Access>) -> String {
    serde_json::to_string(
        &drawers
            .iter()
            .map(|(drawer, access)| (drawer.to_string(), level(*access)))
            .collect::<Vec<_>>(),
    )
    .expect("projected drawer roots serialize")
}

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

fn storage(error: SqlError) -> DrawersError {
    DrawersError::Storage(error.to_string())
}

fn access_column(value: &str) -> Access {
    value
        .parse()
        .expect("the access column is constrained to the drawer access vocabulary")
}

fn id_column(value: &str) -> DocId {
    value
        .parse()
        .expect("an id column holds the text form of a doc id")
}

/// A drawer's projected `metaDoc` references: per root, keyed by the `kind/name` slot — the
/// same spelling as the datum key after its tag, so a row and a roster datum name one slot.
pub(crate) type MetaDocRows = BTreeMap<MetaRoot, BTreeMap<String, DocId>>;

fn slot_name(kind: &str, name: &str) -> String {
    format!("{kind}/{name}")
}

fn root_column(value: &str) -> MetaRoot {
    value
        .parse()
        .expect("the root column is constrained to the metadata root vocabulary")
}

fn meta_doc_rows(rows: impl IntoIterator<Item = sqlite::MetaDocsRow>) -> MetaDocRows {
    let mut projected = MetaDocRows::new();
    for row in rows {
        projected
            .entry(root_column(&row.root))
            .or_default()
            .insert(slot_name(&row.kind, &row.name), id_column(&row.doc_id));
    }
    projected
}

fn drawer_record(drawer_id: &str, descriptor_json: &str) -> DrawerRecord {
    DrawerRecord {
        drawer_id: id_column(drawer_id),
        descriptor: serde_json::from_str(descriptor_json)
            .expect("a stored descriptor was written by this service"),
    }
}

impl State {
    pub(crate) async fn open(sql: Database) -> Result<Self, DrawersError> {
        // The descriptor is stored verbatim, not decomposed: it is an open datum, and a
        // column per field (or a CHECK on the layout tag) would reject exactly the future
        // members carrying exists to survive. Interpretation happens at read time.
        sql.apply_migrations(sqlite::MIGRATION_TABLE, sqlite::MIGRATIONS)
            .await
            .map_err(storage)?;
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
        let rows = sqlite::load_local(&self.sql).await.map_err(storage)?;
        let mut present = None;
        let mut originated = None;
        let mut registry = None;
        for row in rows {
            let id = id_column(&row.drawer_id);
            match row.role.as_str() {
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
        let transaction = self.sql.begin_write().await.map_err(storage)?;
        for (role, id) in [
            ("present", &local.present_id),
            ("originated", &local.originated_id),
            ("registry", &local.registry_id),
        ] {
            sqlite::set_local(&transaction, role.to_owned(), id.to_string())
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
        let row = sqlite::drawer(&self.sql, id.to_string())
            .await
            .map_err(storage)?;
        Ok(row.map(|row| drawer_record(&row.drawer_id, &row.descriptor_json)))
    }

    pub(crate) async fn drawers(&self) -> Result<Vec<DrawerRecord>, DrawersError> {
        let rows = sqlite::drawers(&self.sql).await.map_err(storage)?;
        Ok(rows
            .iter()
            .map(|row| drawer_record(&row.drawer_id, &row.descriptor_json))
            .collect())
    }

    pub(crate) async fn scoped_docs(
        &self,
        drawer_id: &DrawerId,
        access: Access,
    ) -> Result<BTreeMap<DocId, (Access, Option<DrawerId>)>, DrawersError> {
        let rows = sqlite::scoped_docs(&self.sql, drawer_id.to_string(), level(access))
            .await
            .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    id_column(&row.doc_id),
                    (access_level(row.level), row.via.as_deref().map(id_column)),
                )
            })
            .collect())
    }

    pub(crate) async fn scoped_doc_access(
        &self,
        drawers: &BTreeMap<DrawerId, Access>,
        documents: Option<&BTreeSet<DocId>>,
    ) -> Result<BTreeMap<DocId, Access>, DrawersError> {
        if drawers.is_empty() || documents.is_some_and(BTreeSet::is_empty) {
            return Ok(BTreeMap::new());
        }
        let roots = scoped_roots(drawers);
        let rows = match documents {
            Some(documents) => {
                sqlite::scoped_doc_candidates(
                    &self.sql,
                    roots,
                    serde_json::to_string(documents).expect("candidate document ids serialize"),
                )
                .await
            }
            None => sqlite::scoped_doc_access(&self.sql, roots).await,
        }
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| (id_column(&row.doc_id), access_level(row.level)))
            .collect())
    }

    pub(crate) async fn scoped_drawers(
        &self,
        drawers: &BTreeMap<DrawerId, Access>,
    ) -> Result<Vec<(DrawerRecord, Access)>, DrawersError> {
        if drawers.is_empty() {
            return Ok(Vec::new());
        }
        let rows = sqlite::scoped_drawers(&self.sql, scoped_roots(drawers))
            .await
            .map_err(storage)?;
        Ok(rows
            .iter()
            .map(|row| {
                (
                    drawer_record(&row.drawer_id, &row.descriptor_json),
                    access_level(row.level),
                )
            })
            .collect())
    }

    pub(crate) async fn ancestors(
        &self,
        target: ScopeTarget<'_>,
    ) -> Result<BTreeMap<DrawerId, Reach>, DrawersError> {
        let (target_id, document) = match target {
            ScopeTarget::Drawer(drawer_id) => (drawer_id, false),
            ScopeTarget::Doc(doc_id) => (doc_id, true),
        };
        let rows = sqlite::ancestors(&self.sql, document, target_id.to_string())
            .await
            .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    id_column(&row.drawer_id),
                    Reach {
                        ordinary: row.ordinary.map(access_level),
                        public: row.public.map(access_level),
                        sensitive: row.sensitive.map(access_level),
                    },
                )
            })
            .collect())
    }

    /// Drawer ids not yet projected into `big_cabinet_drawers`: those named by child links, plus
    /// the `announced` ids the caller found elsewhere (the docs carrying the drawer datum).
    pub(crate) async fn unprojected(
        &self,
        announced: &BTreeSet<DrawerId>,
    ) -> Result<Vec<DrawerId>, DrawersError> {
        let rows = sqlite::unprojected(
            &self.sql,
            serde_json::to_string(
                &announced
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
            )
            .expect("drawer ids serialize"),
        )
        .await
        .map_err(storage)?;
        Ok(rows.iter().map(|id| id_column(id)).collect())
    }

    pub(crate) async fn put_drawer(
        &self,
        id: &DrawerId,
        descriptor: &Drawer,
    ) -> Result<(), DrawersError> {
        let transaction = self.sql.begin_write().await.map_err(storage)?;
        if sqlite::drawer(&transaction, id.to_string())
            .await
            .map_err(storage)?
            .is_some_and(|row| {
                &drawer_record(&row.drawer_id, &row.descriptor_json).descriptor == descriptor
            })
        {
            transaction.rollback().await.map_err(storage)?;
            return Ok(());
        }
        sqlite::put_drawer(
            &transaction,
            id.to_string(),
            serde_json::to_string(descriptor).expect("a descriptor serializes"),
        )
        .await
        .map_err(storage)?;
        transaction.commit().await.map_err(storage)?;
        self.notify_changed();
        Ok(())
    }

    pub(crate) async fn put_doc(
        &self,
        drawer: &DrawerId,
        doc: &DocId,
        access: Access,
    ) -> Result<(), DrawersError> {
        sqlite::put_doc(
            &self.sql,
            drawer.to_string(),
            doc.to_string(),
            access.as_str().to_owned(),
        )
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
        sqlite::remove_doc(&self.sql, drawer.to_string(), doc.to_string())
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
        let access = sqlite::doc_entry(&self.sql, drawer.to_string(), doc.to_string())
            .await
            .map_err(storage)?;
        Ok(access.as_deref().map(access_column))
    }

    /// The roster's doc entries, as this node last saw them.
    pub(crate) async fn docs_of(
        &self,
        drawer: &DrawerId,
    ) -> Result<BTreeMap<DocId, Access>, DrawersError> {
        let rows = sqlite::docs_of(&self.sql, drawer.to_string())
            .await
            .map_err(storage)?;
        Ok(rows
            .iter()
            .map(|row| (id_column(&row.doc_id), access_column(&row.access)))
            .collect())
    }

    pub(crate) async fn put_child(
        &self,
        parent: &DrawerId,
        child: &DrawerId,
        access: Access,
    ) -> Result<(), DrawersError> {
        sqlite::put_child(
            &self.sql,
            parent.to_string(),
            child.to_string(),
            access.as_str().to_owned(),
        )
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
        sqlite::remove_child(&self.sql, parent.to_string(), child.to_string())
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
        let access = sqlite::child_entry(&self.sql, parent.to_string(), child.to_string())
            .await
            .map_err(storage)?;
        Ok(access.as_deref().map(access_column))
    }

    pub(crate) async fn children_of(
        &self,
        parent: &DrawerId,
    ) -> Result<BTreeMap<DrawerId, Access>, DrawersError> {
        let rows = sqlite::children_of(&self.sql, parent.to_string())
            .await
            .map_err(storage)?;
        Ok(rows
            .iter()
            .map(|row| (id_column(&row.child_id), access_column(&row.access)))
            .collect())
    }

    /// The metadata docs a drawer names on each of its metadata roots, keyed by their
    /// `kind/name` slot, as last projected.
    pub(crate) async fn meta_docs_of(
        &self,
        drawer: &DrawerId,
    ) -> Result<MetaDocRows, DrawersError> {
        let rows = sqlite::meta_docs(&self.sql, drawer.to_string())
            .await
            .map_err(storage)?;
        Ok(meta_doc_rows(rows))
    }

    /// Replace a drawer's roster rows with what its doc says now: the rescan after a boot or a
    /// read that found the projection behind the doc.
    pub(crate) async fn replace_roster(
        &self,
        drawer: &DrawerId,
        docs: &BTreeMap<DocId, Access>,
        children: &BTreeMap<DrawerId, Access>,
        meta_docs: &MetaDocRows,
    ) -> Result<(), DrawersError> {
        let transaction = self.sql.begin_write().await.map_err(storage)?;
        let id = drawer.to_string();
        let stored_docs: BTreeMap<_, _> = sqlite::docs_of(&transaction, id.clone())
            .await
            .map_err(storage)?
            .into_iter()
            .map(|row| (id_column(&row.doc_id), access_column(&row.access)))
            .collect();
        let stored_children: BTreeMap<_, _> = sqlite::children_of(&transaction, id.clone())
            .await
            .map_err(storage)?
            .into_iter()
            .map(|row| (id_column(&row.child_id), access_column(&row.access)))
            .collect();
        let stored_meta_docs = meta_doc_rows(
            sqlite::meta_docs(&transaction, id.clone())
                .await
                .map_err(storage)?,
        );
        if &stored_docs == docs && &stored_children == children && &stored_meta_docs == meta_docs {
            transaction.rollback().await.map_err(storage)?;
            return Ok(());
        }
        sqlite::clear_docs(&transaction, id.clone())
            .await
            .map_err(storage)?;
        sqlite::clear_children(&transaction, id.clone())
            .await
            .map_err(storage)?;
        sqlite::clear_meta_docs(&transaction, id.clone())
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
                let mut query = Sql::new("INSERT INTO ");
                query
                    .ident(table)
                    .push(" (")
                    .ident(source)
                    .push(", ")
                    .ident(target)
                    .push(", access) VALUES ");
                for (index, (target_id, access)) in entries.by_ref().take(256).enumerate() {
                    if index != 0 {
                        query.push(", ");
                    }
                    query
                        .push("(")
                        .bind(&id)
                        .push(", ")
                        .bind(target_id.to_string())
                        .push(", ")
                        .bind(access.as_str())
                        .push(")");
                }
                query.execute(&transaction).await.map_err(storage)?;
            }
        }
        for (root, slots) in meta_docs {
            let mut entries = slots.iter().peekable();
            while entries.peek().is_some() {
                let mut query = Sql::new("INSERT INTO ");
                query
                    .ident("big_cabinet_drawer_meta_docs")
                    .push(" (drawer_id, root, kind, name, doc_id) VALUES ");
                for (index, (slot, doc_id)) in entries.by_ref().take(256).enumerate() {
                    if index != 0 {
                        query.push(", ");
                    }
                    let (kind, name) = slot
                        .split_once('/')
                        .expect("a projected meta-doc slot is kind/name");
                    query
                        .push("(")
                        .bind(&id)
                        .push(", ")
                        .bind(root.as_str())
                        .push(", ")
                        .bind(kind)
                        .push(", ")
                        .bind(name)
                        .push(", ")
                        .bind(doc_id.to_string())
                        .push(")");
                }
                query.execute(&transaction).await.map_err(storage)?;
            }
        }
        transaction.commit().await.map_err(storage)?;
        self.notify_changed();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
