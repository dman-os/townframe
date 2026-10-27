//! The local drawer projection: which drawers this node knows, what their rosters list, and the
//! two local drawers. SQLite, owned by DrawerService.
//!
//! It is a projection, not authority: the roster facets on the drawer docs are authoritative, and
//! these rows are what this service wrote plus what it read back from them. They exist so that
//! "which drawers hold doc X" — the question every DocsRepo access check asks — is one indexed
//! query rather than a scan of every drawer doc on the node.

use super::DrawerId;
use super::DrawersError;
use super::sqlite;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::drawer::Drawer;
use big_cabinet_core::drawer::DrawerAccess as Access;
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

fn drawer_record(drawer_id: &str, descriptor_json: &str) -> DrawerRecord {
    DrawerRecord {
        drawer_id: id_column(drawer_id),
        descriptor: serde_json::from_str(descriptor_json)
            .expect("a stored descriptor was written by this service"),
    }
}

impl State {
    pub(crate) async fn open(sql: Database) -> Result<Self, DrawersError> {
        // The descriptor is stored verbatim, not decomposed: it is an open facet, and a
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
    ) -> Result<BTreeMap<DrawerId, Access>, DrawersError> {
        let (target_id, document) = match target {
            ScopeTarget::Drawer(drawer_id) => (drawer_id, false),
            ScopeTarget::Doc(doc_id) => (doc_id, true),
        };
        let rows = sqlite::ancestors(&self.sql, target_id.to_string(), document)
            .await
            .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|row| (id_column(&row.drawer_id), access_level(row.level)))
            .collect())
    }

    /// Drawer ids not yet projected into `big_cabinet_drawers`: those named by child links, plus
    /// the `announced` ids the caller found elsewhere (the docs carrying the drawer facet).
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

    /// Replace a drawer's roster rows with what its doc says now: the rescan after a boot or a
    /// read that found the projection behind the doc.
    pub(crate) async fn replace_roster(
        &self,
        drawer: &DrawerId,
        docs: &BTreeMap<DocId, Access>,
        children: &BTreeMap<DrawerId, Access>,
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
        if &stored_docs == docs && &stored_children == children {
            transaction.rollback().await.map_err(storage)?;
            return Ok(());
        }
        sqlite::clear_docs(&transaction, id.clone())
            .await
            .map_err(storage)?;
        sqlite::clear_children(&transaction, id.clone())
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
        transaction.commit().await.map_err(storage)?;
        self.notify_changed();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::drawer::GroupKey;
    use sqlite_rs::SqliteRuntime;

    #[tokio::test]
    async fn roster_batches_rollback_together_and_notify_only_after_commit() {
        let runtime = SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).unwrap();
        let state = State::open(sql.clone()).await.unwrap();
        let [drawer, old_doc, old_child] = [240_u8, 241, 242].map(|value| DocId::new([value; 32]));
        let before_docs = BTreeMap::from([(old_doc.clone(), Access::Read)]);
        let before_children = BTreeMap::from([(old_child.clone(), Access::Write)]);
        state
            .replace_roster(&drawer, &before_docs, &before_children)
            .await
            .unwrap();
        let mut changes = state.subscribe_changes();
        let snapshot = sql.begin_read().await.unwrap();
        let docs: BTreeMap<_, _> = (1_u16..=513)
            .map(|value| {
                let mut bytes = [0; 32];
                bytes[..2].copy_from_slice(&value.to_be_bytes());
                (DocId::new(bytes), Access::Admin)
            })
            .collect();
        let mut children: BTreeMap<_, _> = docs
            .keys()
            .take(257)
            .cloned()
            .map(|id| (id, Access::Read))
            .collect();
        *children.last_entry().unwrap().get_mut() = Access::Relay;
        sql.execute_batch(
            "CREATE TRIGGER fail_roster_child BEFORE INSERT ON big_cabinet_drawer_children
             WHEN NEW.access = 'Relay'
             BEGIN SELECT RAISE(ABORT, 'injected roster failure'); END;",
        )
        .await
        .unwrap();
        let error = state
            .replace_roster(&drawer, &docs, &children)
            .await
            .unwrap_err();
        assert!(
            matches!(error, DrawersError::Storage(ref message) if message.contains("injected roster failure"))
        );
        assert!(!changes.has_changed().unwrap());
        assert_eq!(state.docs_of(&drawer).await.unwrap(), before_docs);
        assert_eq!(state.children_of(&drawer).await.unwrap(), before_children);
        sql.execute_batch("DROP TRIGGER fail_roster_child")
            .await
            .unwrap();
        state
            .replace_roster(&drawer, &docs, &children)
            .await
            .unwrap();
        changes.changed().await.unwrap();
        assert_eq!(*changes.borrow_and_update(), 2);
        assert_eq!(state.docs_of(&drawer).await.unwrap(), docs);
        assert_eq!(state.children_of(&drawer).await.unwrap(), children);
        assert_eq!(
            sqlite::doc_entry(&snapshot, drawer.to_string(), old_doc.to_string())
                .await
                .unwrap(),
            Some(Access::Read.as_str().to_owned()),
        );
        assert_eq!(
            sqlite::child_entry(&snapshot, drawer.to_string(), old_child.to_string())
                .await
                .unwrap(),
            Some(Access::Write.as_str().to_owned()),
        );
        snapshot.rollback().await.unwrap();
        state
            .replace_roster(&drawer, &docs, &children)
            .await
            .unwrap();
        assert!(!changes.has_changed().unwrap());
        state
            .replace_roster(&drawer, &BTreeMap::new(), &BTreeMap::new())
            .await
            .unwrap();
        assert!(changes.has_changed().unwrap());
        assert!(state.docs_of(&drawer).await.unwrap().is_empty());
        assert!(state.children_of(&drawer).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn migrations_reopen_existing_projection_and_local_drawers() {
        let runtime = SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[]).unwrap();
        sql.execute_batch(sqlite::MIGRATIONS[0].sql).await.unwrap();
        let [present_id, originated_id, registry_id, announced_id] =
            [10_u8, 11, 12, 13].map(|value| DocId::new([value; 32]));
        let local = LocalDrawers {
            present_id,
            originated_id,
            registry_id,
        };
        let descriptor: Drawer = serde_json::from_value(serde_json::json!({
            "$type": "org.example.daybook.drawer",
            "layout": {"$type": "org.example.future.layout", "opaque": [1, 2, 3]}
        }))
        .unwrap();
        sqlite::put_drawer(
            &sql,
            local.present_id.to_string(),
            serde_json::to_string(&descriptor).unwrap(),
        )
        .await
        .unwrap();
        let state = State::open(sql.clone()).await.unwrap();
        assert_eq!(
            state
                .drawer(&local.present_id)
                .await
                .unwrap()
                .unwrap()
                .descriptor,
            descriptor
        );
        state.set_local(local.clone()).await.unwrap();
        state
            .put_child(&local.present_id, &local.originated_id, Access::Read)
            .await
            .unwrap();
        drop(state);
        let reopened = State::open(sql).await.unwrap();
        assert_eq!(reopened.local(), local);
        assert_eq!(
            reopened.drawers().await.unwrap(),
            vec![DrawerRecord {
                drawer_id: local.present_id.clone(),
                descriptor,
            }]
        );
        let mut expected = vec![local.originated_id.clone(), announced_id.clone()];
        expected.sort();
        assert_eq!(
            reopened
                .unprojected(&BTreeSet::from([local.present_id.clone(), announced_id]))
                .await
                .unwrap(),
            expected,
        );
    }

    #[tokio::test]
    async fn scoped_sql_terminates_cycles_and_selects_the_strongest_path() {
        let runtime = SqliteRuntime::new(Default::default());
        let state = State::open(runtime.ephemeral(&[]).unwrap()).await.unwrap();
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
        let roots = BTreeMap::from([(root.clone(), Access::Read), (right.clone(), Access::Admin)]);
        assert_eq!(
            state.scoped_doc_access(&roots, None).await.unwrap(),
            BTreeMap::from([(doc.clone(), Access::Admin), (leaf.clone(), Access::Read)]),
        );
        assert_eq!(
            state
                .scoped_doc_access(&roots, Some(&BTreeSet::from([leaf.clone(), root.clone()])))
                .await
                .unwrap(),
            BTreeMap::from([(leaf.clone(), Access::Read)]),
        );
        assert!(
            state
                .scoped_doc_access(&roots, Some(&BTreeSet::new()))
                .await
                .unwrap()
                .is_empty()
        );
        let mut expected = Vec::new();
        for (drawer, access) in [
            (&root, Access::Read),
            (&left, Access::Read),
            (&right, Access::Admin),
            (&leaf, Access::Read),
        ] {
            expected.push((state.drawer(drawer).await.unwrap().unwrap(), access));
        }
        expected.sort_by(|(left, _), (right, _)| left.drawer_id.cmp(&right.drawer_id));
        assert_eq!(state.scoped_drawers(&roots).await.unwrap(), expected);
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
}
