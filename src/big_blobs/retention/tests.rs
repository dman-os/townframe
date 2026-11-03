use super::*;
use automerge::ChangeHash;
use big_cabinet_core::authority::AuthorityMessage;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::DatumVersion;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsMessage;
use big_cabinet_core::doc::WriteReceipt;
use big_cabinet_core::drawer::GroupKey;
use big_sync::HostPartStore;
use big_sync::MemoryPartStore;
use futures::future::BoxFuture;
use service_utils_rs::InlineRoute;
use service_utils_rs::StopToken;
use sqlite_rs::SqliteRuntime;
use std::sync::Mutex;

#[derive(Default)]
struct State {
    documents: BTreeMap<DocId, DatumsRead>,
    read_errors: BTreeMap<DocId, DocsError>,
    write_errors: BTreeMap<DocId, DocsError>,
    writes: Vec<WriteDatums>,
    capabilities: std::collections::HashMap<CapId, (DocId, bool)>,
    mint_errors: BTreeMap<(DocId, bool), DocsError>,
}

#[derive(Default)]
struct DocsStub(Mutex<State>);

impl DocsStub {
    fn metadata(self: &Arc<Self>) -> MetadataCapabilities {
        let docs = Arc::clone(self);
        let (_, policy) = tokio::sync::watch::channel(0);
        MetadataCapabilities::new(
            Arc::new(move |scope: DocScope| {
                let document = scope
                    .read
                    .first()
                    .expect("metadata scope reads one document")
                    .clone();
                let write = scope.write.contains(&document);
                assert_eq!(
                    scope,
                    DocScope {
                        read: BTreeSet::from([document.clone()]),
                        write: if write {
                            BTreeSet::from([document.clone()])
                        } else {
                            BTreeSet::new()
                        },
                        ..Default::default()
                    }
                );
                let docs = Arc::clone(&docs);
                Box::pin(async move {
                    let mut state = docs.0.lock().unwrap();
                    if let Some(error) = state.mint_errors.get(&(document.clone(), write)) {
                        return Err(ClientError::Operation(error.clone()));
                    }
                    let capability =
                        CapId::from_bytes((state.capabilities.len() as u128 + 1).to_be_bytes());
                    assert!(
                        state
                            .capabilities
                            .insert(capability, (document, write))
                            .is_none()
                    );
                    Ok(capability)
                })
            }),
            policy,
        )
    }

    fn assert_scope(state: &State, capability: CapId, document: &DocId, write: bool) {
        assert_eq!(state.capabilities[&capability], (document.clone(), write));
    }

    fn datum(&self, document: &DocId, key: DatumKey, datum: DatumRead) {
        self.0
            .lock()
            .unwrap()
            .documents
            .entry(document.clone())
            .or_insert_with(|| DatumsRead {
                heads: vec![ChangeHash([1; 32])],
                datums: BTreeMap::new(),
            })
            .datums
            .insert(key, datum);
    }

    fn pin(&self, inventory: &DocId, id: BlobId) -> Option<BlobPin> {
        let state = self.0.lock().unwrap();
        match state
            .documents
            .get(inventory)?
            .datums
            .get(&DatumKey::from_text(BlobPin::ID, id.digest_str()).unwrap())?
        {
            DatumRead::Accepted { value, .. } => {
                Some(serde_json::from_value(value.clone()).unwrap())
            }
            _ => None,
        }
    }

    fn read(&self, request: ReadDatums) -> Result<DatumsRead, DocsError> {
        assert!(!request.optimistic);
        assert_eq!(request.at, At::Current);
        let state = self.0.lock().unwrap();
        Self::assert_scope(&state, request.capability, &request.doc_id, false);
        if let Some(error) = state.read_errors.get(&request.doc_id) {
            return Err(error.clone());
        }
        let mut read = state
            .documents
            .get(&request.doc_id)
            .cloned()
            .ok_or(DocsError::DocNotFound)?;
        if let Some(keys) = request.datums {
            read.datums.retain(|key, _| keys.contains(key));
            for key in keys {
                read.datums.entry(key).or_insert_with(|| DatumRead::Absent {
                    version: DatumVersion::default(),
                });
            }
        }
        Ok(read)
    }

    fn write(&self, request: WriteDatums) -> Result<WriteReceipt, DocsError> {
        let mut state = self.0.lock().unwrap();
        Self::assert_scope(&state, request.capability, &request.doc_id, true);
        state.writes.push(request.clone());
        if let Some(error) = state.write_errors.get(&request.doc_id) {
            return Err(error.clone());
        }
        let document = state
            .documents
            .get_mut(&request.doc_id)
            .ok_or(DocsError::DocNotFound)?;
        let WriteMode::IfDocUnchanged { heads } = request.mode else {
            panic!("writer pins require CAS");
        };
        if document.heads != heads {
            return Err(DocsError::Conflict {
                heads: document.heads.clone(),
                datums: BTreeMap::new(),
            });
        }
        for (key, value) in request.set {
            document.datums.insert(key, accepted(value));
        }
        for key in request.remove {
            document.datums.insert(key, absent());
        }
        Ok(WriteReceipt {
            doc_id: request.doc_id,
            heads: document.heads.clone(),
        })
    }
}

impl InlineRoute<DocsProtocol> for DocsStub {
    fn route_inline(self: Arc<Self>, message: DocsMessage) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            match message {
                DocsMessage::ReadDatums(message) => message
                    .tx
                    .send(self.read(message.inner.params).map(Into::into))
                    .await
                    .unwrap(),
                DocsMessage::WriteDatums(message) => message
                    .tx
                    .send(self.write(message.inner.params).map(Into::into))
                    .await
                    .unwrap(),
                _ => panic!("writer distribution only reads and writes signed datums"),
            }
        })
    }
}

struct UnusedAuthority;

impl InlineRoute<AuthorityProtocol> for UnusedAuthority {
    fn route_inline(self: Arc<Self>, _: AuthorityMessage) -> BoxFuture<'static, ()> {
        Box::pin(async {
            panic!("internal retention does not invoke the public blob API");
        })
    }
}

fn capability() -> CapId {
    CapId::from_bytes([7; 16])
}
fn id(byte: u8) -> DocId {
    DocId::new([byte; 32])
}
fn accepted(value: serde_json::Value) -> DatumRead {
    DatumRead::Accepted {
        value,
        version: DatumVersion {
            heads: vec![ChangeHash([1; 32])],
            clock: 1,
        },
    }
}
fn absent() -> DatumRead {
    DatumRead::Absent {
        version: DatumVersion {
            heads: vec![ChangeHash([2; 32])],
            clock: 2,
        },
    }
}
fn reference(name: &str) -> DatumKey {
    DatumKey::from_text(DrawerMetaDoc::ID, format!("blobInventory/{name}")).unwrap()
}

struct Harness {
    sql: Database,
    docs: Arc<DocsStub>,
    service: Arc<BlobService>,
    parts: Arc<MemoryPartStore>,
    stop: StopToken,
}

impl Harness {
    async fn new() -> Res<Self> {
        let runtime = SqliteRuntime::new(Default::default());
        let sql = runtime.ephemeral(&[])?;
        sql.apply_migrations(
            "test_drawers",
            &[
                sqlite_rs::Migration {
                    version: 1,
                    name: "drawers",
                    sql: include_str!("../../big_cabinet/drawer/sqlite/migrations/001_drawers.sql"),
                },
                sqlite_rs::Migration {
                    version: 2,
                    name: "metadata",
                    sql: include_str!(
                        "../../big_cabinet/drawer/sqlite/migrations/003_meta_doc_roots.sql"
                    ),
                },
            ],
        )
        .await?;
        let (blobs, stop) = crate::boot(crate::Config {
            storage: crate::Storage::Memory,
            sql: sql.clone(),
            authority: Client::direct(Arc::new(UnusedAuthority)),
            namespace: Namespace {
                namespace_id: Uuid7::new(),
                capability: capability(),
            },
        })
        .await?;
        Ok(Self {
            sql,
            docs: Arc::new(DocsStub::default()),
            service: blobs.service,
            parts: Arc::new(MemoryPartStore::new()),
            stop,
        })
    }

    async fn retention(&self) -> Res<Retention> {
        Retention::new(
            self.sql.clone(),
            Arc::clone(&self.service),
            Client::direct(Arc::clone(&self.docs)),
            self.docs.metadata(),
            Arc::clone(&self.parts) as SharedPartStore,
        )
        .await
    }

    async fn drawer(
        &self,
        drawer: &DocId,
        sensitive: &DocId,
        document: &DocId,
        child: bool,
    ) -> Res<()> {
        let descriptor = Drawer::single_group(
            GroupKey([1; 32]),
            GroupKey([2; 32]),
            GroupKey([3; 32]),
            sensitive.clone(),
        );
        self.sql
            .execute(
                "INSERT INTO big_cabinet_drawers (drawer_id, descriptor_json) VALUES (?, ?)",
                (drawer.to_string(), serde_json::to_string(&descriptor)?),
            )
            .await?;
        self.docs.datum(
            drawer,
            DatumKey::from_text(Drawer::ID, "main")?,
            accepted(serde_json::to_value(descriptor)?),
        );
        let tag = if child {
            self.sql
                .execute(
                    "INSERT INTO big_cabinet_drawers (drawer_id, descriptor_json) VALUES (?, ?)",
                    (
                        document.to_string(),
                        serde_json::to_string(&Drawer::self_doc())?,
                    ),
                )
                .await?;
            self.sql.execute("INSERT INTO big_cabinet_drawer_children (parent_id, child_id, access) VALUES (?, ?, 'Admin')", (drawer.to_string(), document.to_string())).await?;
            DrawerChildEntry::ID
        } else {
            self.sql.execute("INSERT INTO big_cabinet_drawer_docs (drawer_id, doc_id, access) VALUES (?, ?, 'Admin')", (drawer.to_string(), document.to_string())).await?;
            DrawerDocEntry::ID
        };
        self.docs.datum(
            drawer,
            DatumKey::from_text(tag, document.to_string())?,
            accepted(serde_json::json!({"access": "Admin"})),
        );
        self.docs.0.lock().unwrap().documents.insert(
            sensitive.clone(),
            DatumsRead {
                heads: vec![ChangeHash([1; 32])],
                datums: BTreeMap::new(),
            },
        );
        Ok(())
    }

    async fn target(
        &self,
        drawer: &DocId,
        root: MetaRoot,
        root_doc: &DocId,
        name: &str,
        inventory: &DocId,
    ) -> Res<()> {
        self.sql.execute("INSERT OR REPLACE INTO big_cabinet_drawer_meta_docs (drawer_id, root, kind, name, doc_id) VALUES (?, ?, 'blobInventory', ?, ?)", (drawer.to_string(), root.as_str(), name, inventory.to_string())).await?;
        self.docs.datum(
            root_doc,
            reference(name),
            accepted(serde_json::json!({"docId": inventory})),
        );
        self.docs
            .0
            .lock()
            .unwrap()
            .documents
            .entry(inventory.clone())
            .or_insert_with(|| DatumsRead {
                heads: vec![ChangeHash([1; 32])],
                datums: BTreeMap::new(),
            });
        Ok(())
    }

    async fn pair(&self) -> Res<(RetainedRepresentation, [crate::service::ImportedBlob; 2])> {
        let plain = self
            .service
            .import_stream(futures::stream::iter([Ok(bytes::Bytes::from_static(
                b"writer plaintext",
            ))]))
            .await?;
        let cipher = self
            .service
            .import_stream(futures::stream::iter([Ok(bytes::Bytes::from_static(
                b"opaque encrypted representation with framing",
            ))]))
            .await?;
        Ok((
            RetainedRepresentation {
                blob_key: DatumKey::from_text(big_blobs_core::Blob::ID, "main")?,
                cipher_key: DatumKey::from_text(big_blobs_core::CipherBlob::ID, "main")?,
                plaintext: plain.info.blob_id,
                ciphertext: cipher.info.blob_id,
                length_octets: cipher.info.length_octets,
            },
            [plain, cipher],
        ))
    }

    async fn assert_parts(&self, id: BlobId, inventories: &[DocId]) -> Res<()> {
        let actual: BTreeSet<_> = self.parts.obj_parts(id.into()).await?.into_iter().collect();
        let expected: BTreeSet<_> = inventories
            .iter()
            .map(crate::workers::inventory_part)
            .collect();
        assert_eq!(actual, expected);
        Ok(())
    }
}

#[tokio::test]
async fn named_inventory_roots_distribute_only_their_representation_idempotently() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let (drawer, sensitive, document) = (id(1), id(2), id(3));
    h.drawer(&drawer, &sensitive, &document, false).await?;
    let (a, b, plain, plain_backup) = (id(4), id(5), id(6), id(7));
    h.target(&drawer, MetaRoot::Public, &drawer, "plaintext", &a)
        .await?;
    h.target(&drawer, MetaRoot::Public, &drawer, "backup", &b)
        .await?;
    h.target(
        &drawer,
        MetaRoot::Sensitive,
        &sensitive,
        "ciphertext",
        &plain,
    )
    .await?;
    h.target(
        &drawer,
        MetaRoot::Sensitive,
        &sensitive,
        "backup",
        &plain_backup,
    )
    .await?;
    let (pair, _) = h.pair().await?;
    retention
        .replace(
            &document,
            std::slice::from_ref(&pair),
            BTreeMap::new(),
            false,
        )
        .await?;
    assert_eq!(
        retention.flush().await?,
        BTreeSet::from([a.clone(), b.clone(), plain.clone(), plain_backup.clone()])
    );
    h.assert_parts(pair.ciphertext, &[a.clone(), b.clone()])
        .await?;
    h.assert_parts(pair.plaintext, &[plain.clone(), plain_backup.clone()])
        .await?;
    for inventory in [&a, &b] {
        assert_eq!(
            h.docs
                .pin(inventory, pair.ciphertext)
                .unwrap()
                .length_octets,
            pair.length_octets
        );
        assert!(h.docs.pin(inventory, pair.plaintext).is_none());
    }
    for inventory in [&plain, &plain_backup] {
        assert_eq!(
            h.docs.pin(inventory, pair.plaintext).unwrap().length_octets,
            16
        );
        assert!(h.docs.pin(inventory, pair.ciphertext).is_none());
    }
    let writes = h.docs.0.lock().unwrap().writes.len();
    retention
        .replace(&document, &[pair], BTreeMap::new(), false)
        .await?;
    assert!(retention.flush().await?.is_empty());
    assert_eq!(h.docs.0.lock().unwrap().writes.len(), writes);
    h.stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn cross_root_inventory_sharing_combines_pins_without_combining_claims() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let (drawer, sensitive, document, inventory) = (id(1), id(2), id(3), id(4));
    h.drawer(&drawer, &sensitive, &document, false).await?;
    for name in ["public", "backup"] {
        h.target(&drawer, MetaRoot::Public, &drawer, name, &inventory)
            .await?;
    }
    h.target(
        &drawer,
        MetaRoot::Sensitive,
        &sensitive,
        "private",
        &inventory,
    )
    .await?;
    let (pair, _) = h.pair().await?;
    retention
        .replace(
            &document,
            std::slice::from_ref(&pair),
            BTreeMap::new(),
            false,
        )
        .await?;
    assert_eq!(
        retention.flush().await?,
        BTreeSet::from([inventory.clone()])
    );
    for digest in [pair.ciphertext, pair.plaintext] {
        assert!(h.docs.pin(&inventory, digest).is_some());
        h.assert_parts(digest, std::slice::from_ref(&inventory))
            .await?;
    }
    assert_eq!(
        h.sql
            .query(
                "SELECT root, count(*) FROM big_blobs_writer_claims GROUP BY root ORDER BY root",
                (),
            )
            .await?
            .all::<(String, u64)>()?,
        vec![("public".into(), 2), ("sensitive".into(), 1)]
    );
    for name in ["public", "backup"] {
        h.docs.datum(&drawer, reference(name), absent());
        h.sql.execute(
            "DELETE FROM big_cabinet_drawer_meta_docs WHERE drawer_id = ? AND root = 'public' AND name = ?",
            (drawer.to_string(), name),
        ).await?;
        retention.refresh_targets(&document).await?;
        retention.flush().await?;
        assert_eq!(
            h.docs.pin(&inventory, pair.ciphertext).is_some(),
            name == "public"
        );
        assert!(h.docs.pin(&inventory, pair.plaintext).is_some());
        h.assert_parts(pair.plaintext, std::slice::from_ref(&inventory))
            .await?;
    }
    h.assert_parts(pair.ciphertext, &[]).await?;
    drop(retention);
    let mut retention = h.retention().await?;
    retention.flush().await?;
    assert!(h.docs.pin(&inventory, pair.plaintext).is_some());
    h.assert_parts(pair.plaintext, std::slice::from_ref(&inventory))
        .await?;
    h.docs.datum(&sensitive, reference("private"), absent());
    h.sql
        .execute(
            "DELETE FROM big_cabinet_drawer_meta_docs WHERE drawer_id = ? AND root = 'sensitive'",
            (drawer.to_string(),),
        )
        .await?;
    retention.refresh_targets(&document).await?;
    retention.flush().await?;
    for digest in [pair.ciphertext, pair.plaintext] {
        assert!(h.docs.pin(&inventory, digest).is_none());
        h.assert_parts(digest, &[]).await?;
        assert!(h.service.stat(&digest).await?.is_some());
    }
    h.stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn inventory_removal_and_replacement_leave_other_inventory_claims_intact() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let (drawer, sensitive, document, a, b, c) = (id(1), id(2), id(3), id(4), id(5), id(6));
    h.drawer(&drawer, &sensitive, &document, false).await?;
    h.target(&drawer, MetaRoot::Public, &drawer, "a", &a)
        .await?;
    h.target(&drawer, MetaRoot::Public, &drawer, "b", &b)
        .await?;
    let (pair, _) = h.pair().await?;
    retention
        .replace(
            &document,
            std::slice::from_ref(&pair),
            BTreeMap::new(),
            false,
        )
        .await?;
    retention.flush().await?;
    h.docs.datum(&drawer, reference("a"), absent());
    h.sql
        .execute(
            "DELETE FROM big_cabinet_drawer_meta_docs WHERE name = 'a'",
            (),
        )
        .await?;
    retention.refresh_targets(&document).await?;
    retention.flush().await?;
    assert!(h.docs.pin(&a, pair.ciphertext).is_none());
    assert!(h.docs.pin(&b, pair.ciphertext).is_some());
    h.assert_parts(pair.ciphertext, std::slice::from_ref(&b))
        .await?;
    h.target(&drawer, MetaRoot::Public, &drawer, "b", &c)
        .await?;
    retention.refresh_targets(&document).await?;
    retention.flush().await?;
    assert!(h.docs.pin(&b, pair.ciphertext).is_none());
    assert!(h.docs.pin(&c, pair.ciphertext).is_some());
    h.assert_parts(pair.ciphertext, &[c]).await?;
    h.stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn self_doc_child_targets_parent_inventories_not_its_own_or_ancestors() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let (parent, sensitive, child, inventory) = (id(1), id(2), id(3), id(4));
    h.drawer(&parent, &sensitive, &child, true).await?;
    h.target(&parent, MetaRoot::Public, &parent, "node", &inventory)
        .await?;
    let own = id(5);
    h.target(&child, MetaRoot::Public, &child, "own", &own)
        .await?;
    let (grandparent, grand_sensitive, grand_inventory) = (id(6), id(7), id(8));
    h.drawer(&grandparent, &grand_sensitive, &parent, false)
        .await?;
    h.target(
        &grandparent,
        MetaRoot::Public,
        &grandparent,
        "ancestor",
        &grand_inventory,
    )
    .await?;
    let (pair, _) = h.pair().await?;
    retention
        .replace(&child, std::slice::from_ref(&pair), BTreeMap::new(), false)
        .await?;
    retention.flush().await?;
    assert!(h.docs.pin(&inventory, pair.ciphertext).is_some());
    assert!(h.docs.pin(&own, pair.ciphertext).is_none());
    assert!(h.docs.pin(&grand_inventory, pair.ciphertext).is_none());
    h.assert_parts(pair.ciphertext, &[inventory]).await?;
    h.stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn denied_writer_effects_survive_restart_and_pin_uncertainty() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let (drawer, sensitive, document, inventory) = (id(1), id(2), id(3), id(4));
    h.drawer(&drawer, &sensitive, &document, false).await?;
    h.target(&drawer, MetaRoot::Public, &drawer, "node", &inventory)
        .await?;
    let (pair, _) = h.pair().await?;
    h.docs
        .0
        .lock()
        .unwrap()
        .write_errors
        .insert(inventory.clone(), DocsError::Denied);
    retention
        .replace(
            &document,
            std::slice::from_ref(&pair),
            BTreeMap::new(),
            false,
        )
        .await?;
    assert!(retention.flush().await?.is_empty());
    h.assert_parts(pair.ciphertext, &[]).await?;
    assert!(h.docs.pin(&inventory, pair.ciphertext).is_none());
    drop(retention);
    let (_, _, path) = h
        .sql
        .query("PRAGMA database_list", ())
        .await?
        .one::<(i64, String, String)>()?;
    let runtime = SqliteRuntime::new(Default::default());
    let sql = runtime.database(path, &[], "_reopened_retention");
    let mut retention = Retention::new(
        sql,
        Arc::clone(&h.service),
        Client::direct(Arc::clone(&h.docs)),
        h.docs.metadata(),
        Arc::clone(&h.parts) as SharedPartStore,
    )
    .await?;
    h.docs.0.lock().unwrap().write_errors.clear();
    let key = DatumKey::from_text(BlobPin::ID, pair.ciphertext.digest_str())?;
    h.docs.datum(
        &inventory,
        key.clone(),
        DatumRead::Pending {
            reason: "signature pending".into(),
            version: DatumVersion::default(),
            value: None,
        },
    );
    assert!(retention.flush().await?.is_empty());
    h.assert_parts(pair.ciphertext, &[]).await?;
    h.docs.datum(&inventory, key, absent());
    assert_eq!(
        retention.flush().await?,
        BTreeSet::from([inventory.clone()])
    );
    h.assert_parts(pair.ciphertext, std::slice::from_ref(&inventory))
        .await?;
    assert!(h.docs.pin(&inventory, pair.ciphertext).is_some());
    h.stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn denied_removal_retains_bytes_until_inventory_write_is_authorized() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let (drawer, sensitive, document, inventory) = (id(1), id(2), id(3), id(4));
    h.drawer(&drawer, &sensitive, &document, false).await?;
    h.target(&drawer, MetaRoot::Sensitive, &sensitive, "node", &inventory)
        .await?;
    let (pair, uploads) = h.pair().await?;
    retention
        .replace(
            &document,
            std::slice::from_ref(&pair),
            BTreeMap::new(),
            false,
        )
        .await?;
    retention.flush().await?;
    for upload in &uploads {
        h.service.adopt_import(upload, &document).await?;
    }
    h.docs.datum(&sensitive, reference("node"), absent());
    h.sql
        .execute("DELETE FROM big_cabinet_drawer_meta_docs", ())
        .await?;
    h.docs
        .0
        .lock()
        .unwrap()
        .write_errors
        .insert(inventory.clone(), DocsError::Denied);
    retention
        .replace(&document, &[], BTreeMap::new(), false)
        .await?;
    assert!(retention.flush().await?.is_empty());
    drop(retention);
    let mut retention = h.retention().await?;
    assert!(retention.flush().await?.is_empty());
    h.assert_parts(pair.plaintext, std::slice::from_ref(&inventory))
        .await?;
    assert!(h.docs.pin(&inventory, pair.plaintext).is_some());
    assert!(h.service.ownership.contains(&pair.plaintext).await?);
    assert_eq!(
        h.sql
            .query(
                "SELECT owner FROM big_blobs_byte_owners WHERE digest = ?",
                (pair.plaintext.to_string(),),
            )
            .await?
            .all::<String>()?,
        vec![format!("writer:retention:effect:{inventory}")]
    );
    let hash = iroh_blobs::Hash::from(pair.plaintext.to_bytes32());
    assert!(
        h.service
            .store()
            .tags()
            .get(format!("pin:{hash}"))
            .await?
            .is_some()
    );
    assert_eq!(
        h.sql
            .query(
                "SELECT inventory_id, digest FROM big_blobs_writer_effects",
                ()
            )
            .await?
            .all::<(String, String)>()?,
        vec![(inventory.to_string(), pair.plaintext.to_string())]
    );
    h.docs.0.lock().unwrap().write_errors.clear();
    assert_eq!(
        retention.flush().await?,
        BTreeSet::from([inventory.clone()])
    );
    h.assert_parts(pair.plaintext, &[]).await?;
    assert!(h.docs.pin(&inventory, pair.plaintext).is_none());
    assert!(!h.service.ownership.contains(&pair.plaintext).await?);
    assert!(
        h.service
            .store()
            .tags()
            .get(format!("pin:{hash}"))
            .await?
            .is_none()
    );
    assert!(
        h.sql
            .query(
                "SELECT inventory_id, digest FROM big_blobs_writer_effects",
                ()
            )
            .await?
            .all::<(String, String)>()?
            .is_empty()
    );
    h.stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn metadata_mint_denials_preserve_claims_and_writer_effects() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let (drawer, sensitive, document, inventory) = (id(1), id(2), id(3), id(4));
    h.drawer(&drawer, &sensitive, &document, false).await?;
    h.target(&drawer, MetaRoot::Sensitive, &sensitive, "node", &inventory)
        .await?;
    let (pair, _) = h.pair().await?;
    retention
        .replace(
            &document,
            std::slice::from_ref(&pair),
            BTreeMap::new(),
            false,
        )
        .await?;
    retention.flush().await?;
    drop(retention);
    h.docs.datum(&sensitive, reference("node"), absent());
    h.sql
        .execute("DELETE FROM big_cabinet_drawer_meta_docs", ())
        .await?;
    h.docs.0.lock().unwrap().mint_errors.insert(
        (sensitive.clone(), false),
        DocsError::Access("sensitive read unavailable".into()),
    );
    let mut retention = h.retention().await?;
    retention
        .replace(&document, &[], BTreeMap::new(), false)
        .await?;
    assert!(retention.flush().await?.is_empty());
    h.assert_parts(pair.plaintext, std::slice::from_ref(&inventory))
        .await?;
    assert!(h.docs.pin(&inventory, pair.plaintext).is_some());
    assert_eq!(
        h.sql
            .query("SELECT inventory_id FROM big_blobs_writer_claims", ())
            .await?
            .all::<String>()?,
        vec![inventory.to_string()]
    );
    h.docs.0.lock().unwrap().mint_errors.clear();
    retention.refresh_targets(&document).await?;
    h.docs
        .0
        .lock()
        .unwrap()
        .mint_errors
        .insert((inventory.clone(), true), DocsError::Denied);
    assert!(retention.flush().await?.is_empty());
    h.assert_parts(pair.plaintext, std::slice::from_ref(&inventory))
        .await?;
    assert!(h.docs.pin(&inventory, pair.plaintext).is_some());
    assert_eq!(
        h.sql
            .query("SELECT inventory_id FROM big_blobs_writer_effects", ())
            .await?
            .all::<String>()?,
        vec![inventory.to_string()]
    );
    h.docs.0.lock().unwrap().mint_errors.clear();
    assert_eq!(
        retention.flush().await?,
        BTreeSet::from([inventory.clone()])
    );
    h.assert_parts(pair.plaintext, &[]).await?;
    assert!(h.docs.pin(&inventory, pair.plaintext).is_none());
    h.stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn all_direct_drawers_and_shared_source_claims_reconcile_independently() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let (drawer, sensitive, document, inventory) = (id(1), id(2), id(3), id(4));
    let (other_drawer, other_sensitive, other_inventory) = (id(5), id(6), id(7));
    let (shared_drawer, shared_sensitive, shared_document) = (id(8), id(9), id(10));
    h.drawer(&drawer, &sensitive, &document, false).await?;
    h.drawer(&other_drawer, &other_sensitive, &document, false)
        .await?;
    h.drawer(&shared_drawer, &shared_sensitive, &shared_document, false)
        .await?;
    h.target(&drawer, MetaRoot::Public, &drawer, "node", &inventory)
        .await?;
    h.target(
        &other_drawer,
        MetaRoot::Public,
        &other_drawer,
        "node",
        &other_inventory,
    )
    .await?;
    h.target(
        &shared_drawer,
        MetaRoot::Public,
        &shared_drawer,
        "node",
        &inventory,
    )
    .await?;
    let (pair, _) = h.pair().await?;
    for source in [&document, &shared_document] {
        retention
            .replace(source, std::slice::from_ref(&pair), BTreeMap::new(), false)
            .await?;
    }
    retention.flush().await?;
    h.assert_parts(
        pair.ciphertext,
        &[inventory.clone(), other_inventory.clone()],
    )
    .await?;
    h.docs.datum(
        &drawer,
        DatumKey::from_text(DrawerDocEntry::ID, document.to_string())?,
        absent(),
    );
    h.sql
        .execute(
            "DELETE FROM big_cabinet_drawer_docs WHERE drawer_id = ?",
            (drawer.to_string(),),
        )
        .await?;
    retention.refresh_targets(&document).await?;
    retention.flush().await?;
    h.assert_parts(
        pair.ciphertext,
        &[inventory.clone(), other_inventory.clone()],
    )
    .await?;
    assert!(h.docs.pin(&inventory, pair.ciphertext).is_some());
    retention
        .replace(&document, &[], BTreeMap::new(), false)
        .await?;
    retention.flush().await?;
    h.assert_parts(pair.ciphertext, std::slice::from_ref(&inventory))
        .await?;
    assert!(h.docs.pin(&inventory, pair.ciphertext).is_some());
    assert!(h.docs.pin(&other_inventory, pair.ciphertext).is_none());
    retention
        .replace(&shared_document, &[], BTreeMap::new(), false)
        .await?;
    retention.flush().await?;
    h.assert_parts(pair.ciphertext, &[]).await?;
    assert!(h.docs.pin(&inventory, pair.ciphertext).is_none());
    h.stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn missing_pending_denied_and_unadmitted_references_preserve_previous_claims() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let (drawer, sensitive, document, inventory) = (id(1), id(2), id(3), id(4));
    h.drawer(&drawer, &sensitive, &document, false).await?;
    h.target(&drawer, MetaRoot::Sensitive, &sensitive, "node", &inventory)
        .await?;
    let (pair, _) = h.pair().await?;
    retention
        .replace(
            &document,
            std::slice::from_ref(&pair),
            BTreeMap::new(),
            false,
        )
        .await?;
    retention.flush().await?;
    let original =
        h.docs.0.lock().unwrap().documents[&sensitive].datums[&reference("node")].clone();
    h.sql
        .execute("DELETE FROM big_cabinet_drawer_meta_docs", ())
        .await?;
    let states = [
        DatumRead::Absent {
            version: DatumVersion::default(),
        },
        DatumRead::Pending {
            reason: "backing pending".into(),
            version: DatumVersion::default(),
            value: None,
        },
        DatumRead::Rejected {
            reason: "unadmitted".into(),
        },
        DatumRead::Undecodable {
            reason: "unknown encoding".into(),
            version: DatumVersion::default(),
        },
        accepted(serde_json::json!({"docId": id(9)})),
    ];
    for state in states {
        h.docs.datum(&sensitive, reference("node"), state);
        retention
            .replace(&document, &[], BTreeMap::new(), false)
            .await?;
        assert!(retention.flush().await?.is_empty());
        h.assert_parts(pair.plaintext, std::slice::from_ref(&inventory))
            .await?;
        assert!(h.docs.pin(&inventory, pair.plaintext).is_some());
    }
    h.docs.datum(&sensitive, reference("node"), original);
    for error in [
        DocsError::Denied,
        DocsError::DocNotFound,
        DocsError::AuthorityUnavailable("offline".into()),
    ] {
        h.docs
            .0
            .lock()
            .unwrap()
            .read_errors
            .insert(sensitive.clone(), error);
        retention.refresh_targets(&document).await?;
        assert!(retention.flush().await?.is_empty());
        h.assert_parts(pair.plaintext, std::slice::from_ref(&inventory))
            .await?;
    }
    h.docs.0.lock().unwrap().read_errors.clear();
    h.docs.datum(&sensitive, reference("node"), absent());
    retention
        .replace(&document, &[], BTreeMap::new(), false)
        .await?;
    retention.flush().await?;
    h.assert_parts(pair.plaintext, &[]).await?;
    assert!(h.docs.pin(&inventory, pair.plaintext).is_none());
    let hash = iroh_blobs::Hash::from(pair.plaintext.to_bytes32());
    assert!(
        h.service
            .store()
            .tags()
            .get(format!("pin:{hash}"))
            .await?
            .is_none()
    );
    h.stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn retention_does_not_adopt_or_release_another_upload_owner() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let import =
        |bytes: &'static [u8]| futures::stream::iter([Ok(bytes::Bytes::from_static(bytes))]);
    let first = h.service.import_stream(import(b"shared upload")).await?;
    let other = h.service.import_stream(import(b"other upload")).await?;
    let second = h.service.import_stream(import(b"shared upload")).await?;
    let a = first.info.blob_id;
    let b = other.info.blob_id;
    assert_eq!(second.info, first.info);
    assert!(h.service.adopt_import(&first, &id(20)).await.is_err());
    retention
        .replace(&id(20), &[], BTreeMap::from([(a, 13), (b, 12)]), false)
        .await?;
    retention.flush().await?;
    assert_eq!(h.sql.query(
        "SELECT COUNT(*) FROM big_blobs_byte_owners WHERE digest = ? AND owner GLOB 'upload:*'",
        (a.to_string(),),
    ).await?.all::<i64>()?, vec![2]);
    h.service.adopt_import(&first, &id(20)).await?;
    h.service.adopt_import(&first, &id(20)).await?;
    assert_eq!(h.sql.query(
        "SELECT COUNT(*) FROM big_blobs_byte_owners WHERE digest = ? AND owner GLOB 'upload:*'",
        (a.to_string(),),
    ).await?.all::<i64>()?, vec![1]);
    retention
        .replace(&id(20), &[], BTreeMap::from([(b, 12)]), false)
        .await?;
    retention.flush().await?;
    assert!(h.service.ownership.contains(&a).await?);
    assert!(h.service.ownership.contains(&b).await?);
    h.service.release_import(&first).await?;
    assert!(h.service.ownership.contains(&a).await?);
    h.service.release_import(&second).await?;
    assert!(!h.service.ownership.contains(&a).await?);
    assert!(
        h.service
            .store()
            .tags()
            .get(format!("held:{}", iroh_blobs::Hash::from(a.to_bytes32())))
            .await?
            .is_none()
    );
    h.service.release_import(&other).await?;
    assert!(h.service.ownership.contains(&b).await?);
    retention
        .replace(&id(20), &[], BTreeMap::new(), false)
        .await?;
    retention.flush().await?;
    assert!(!h.service.ownership.contains(&b).await?);
    h.stop.stop().await;
    Ok(())
}

#[tokio::test]
async fn metadata_pins_without_bytes_are_not_byte_owners() -> Res<()> {
    let h = Harness::new().await?;
    let mut retention = h.retention().await?;
    let missing = BlobId::new([91; 32]);
    retention
        .replace(&id(21), &[], BTreeMap::from([(missing, 100)]), false)
        .await?;
    retention.flush().await?;
    assert!(!h.service.ownership.contains(&missing).await?);
    let hash = iroh_blobs::Hash::from(missing.to_bytes32());
    for prefix in ["owned", "pin"] {
        assert!(
            h.service
                .store()
                .tags()
                .get(format!("{prefix}:{hash}"))
                .await?
                .is_none()
        );
    }
    let mut restarted = h.retention().await?;
    restarted.flush().await?;
    assert!(!h.service.ownership.contains(&missing).await?);
    h.stop.stop().await;
    Ok(())
}
