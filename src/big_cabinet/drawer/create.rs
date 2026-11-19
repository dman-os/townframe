//! CreateDrawer: a new single-group drawer (three Keyhive groups, its two metadata roots and the
//! metadata docs they name), or an existing doc declared a self-doc drawer.

use super::principal::hex_digest;
use super::*;
use crate::doc::pipeline::digest;
use big_cabinet_core::doc::CreateDoc;
use big_cabinet_core::doc::WriteDatums;
use big_cabinet_core::drawer::Drawer;
use big_cabinet_core::drawer::DrawerRoster;
use big_cabinet_core::drawer::MetaRoot;
use ledger::Op;

fn datum_value(datum: &impl Serialize) -> serde_json::Value {
    serde_json::to_value(datum).expect("a drawer datum serializes")
}

/// The DocsRepo command the sensitive metadata root of `command` is created under: derived
/// from the drawer's own command so a replay lands on the same doc, and distinct from it so
/// the two creations are two commands with two ledgers.
pub(super) fn sensitive_root_command(command: &CommandId) -> CommandId {
    let suffix = digest(&("sensitiveRoot", command.as_str()));
    CommandId::try_from(format!("sensitiveRoot/{}", hex_digest(&suffix)))
        .expect("a derived command id is within bounds")
}

/// The record a descriptor states. Carrying, not operating: a foreign layout is stored as it
/// arrived, and operating code refuses it at `layout_kind()`, not here.
pub(crate) fn record_of(drawer_id: &DrawerId, descriptor: &Drawer) -> DrawerRecord {
    DrawerRecord {
        drawer_id: drawer_id.clone(),
        descriptor: descriptor.clone(),
    }
}

impl Core {
    /// Whether Keyhive shows `doc` delegated to the drawer's meta group, which is the only
    /// evidence that admits a `metaDoc` reference. A writer can put any doc id in the slot; without
    /// this check the reference would hand every drawer writer Cabinet reach to that doc.
    pub(crate) async fn meta_doc_admitted(
        &self,
        meta: &BigKeyhiveGroup,
        doc: &DocId,
    ) -> Result<bool, DrawersError> {
        let key = keyhive_key(doc)?;
        Ok(self
            .big_repo
            .keyhive()
            .direct_access_on(
                &meta.id().into(),
                big_repo::keyhive_core::principal::identifier::Identifier::from(key),
            )
            .await
            .is_some_and(|access| access >= KeyhiveAccess::Admin))
    }
}

impl Core {
    /// Declare `doc` a self-doc drawer by writing the descriptor on it with the caller's own
    /// capability: DocsRepo enforces Write, signs the datum as the caller, and its command ledger
    /// makes the command idempotent. The drawer ledger row ensures that if a crash falls after the
    /// descriptor write and before the projection record or the creator's Admin grant, boot
    /// finishes both from the committed descriptor even if the caller never retries; if the crash
    /// fell before the descriptor write, boot drops the row.
    async fn create_self_doc(
        &self,
        context: &RequestContext,
        capability: CapId,
        command: CommandId,
        doc_id: DocId,
    ) -> Result<DrawerCreated, DrawersError> {
        let caller = self.invocation(context, capability).await?;
        let actor = &caller.signer;
        if self.state.drawer(&doc_id).await?.is_some() {
            return Err(DrawersError::AlreadyDrawer(doc_id));
        }
        if self.doc_level(context, &caller.authority, &doc_id).await? < Some(Access::Write) {
            return Err(DrawersError::Denied);
        }
        let owner_id = caller.originating_actor;
        let request_digest = digest(&("createSelfDoc", actor.actor_id, owner_id, &doc_id));
        self.ledger
            .run(
                &command,
                request_digest,
                actor.actor_id,
                Op::CreateSelfDoc {
                    doc_id: doc_id.clone(),
                    owner_id,
                },
                |_| async {
                    let receipt = self
                        .as_signer(
                            context,
                            caller.via,
                            DocScope {
                                write: [doc_id.clone()].into(),
                                ..Default::default()
                            },
                            |authorization| {
                                let (command, doc_id) = (&command, &doc_id);
                                async move {
                                    self.docs()
                                        .call_with(
                                            context.child(),
                                            authorization.metadata(),
                                            WriteDatums {
                                                capability: authorization.authority,
                                                command: Some(command.clone()),
                                                doc_id: doc_id.clone(),
                                                mode: WriteMode::AtCurrent,
                                                set: BTreeMap::from([(
                                                    drawer_key(),
                                                    datum_value(&Drawer::self_doc()),
                                                )]),
                                                remove: Default::default(),
                                            },
                                        )
                                        .await
                                        .map_err(docs_error)
                                }
                            },
                        )
                        .await?;
                    self.state.put_drawer(&doc_id, &Drawer::self_doc()).await?;
                    if let Some(owner_id) = owner_id {
                        self.grant(
                            context,
                            owner_id,
                            drawer_object(Access::Admin, &doc_id),
                            None,
                        )
                        .await?;
                    }
                    Ok(DrawerCreated {
                        drawer_id: doc_id,
                        heads: receipt.heads,
                    })
                },
            )
            .await
    }

    pub(crate) async fn resume_create_self_doc(
        &self,
        context: &RequestContext,
        row: &ledger::Row,
    ) -> Result<(), DrawersError> {
        let Op::CreateSelfDoc { doc_id, owner_id } = &row.op else {
            unreachable!(
                "resume_create_self_doc runs under a create-self-doc row: {:?}",
                row.op
            )
        };
        let read = self
            .as_signer(
                context,
                self.issuer,
                DocScope {
                    read: [doc_id.clone()].into(),
                    ..Default::default()
                },
                |Authorization {
                     authority: capability,
                     ..
                 }| async move {
                    self.docs()
                        .call(
                            context.child(),
                            big_cabinet_core::doc::ReadDatums {
                                capability,
                                doc_id: doc_id.clone(),
                                at: big_cabinet_core::doc::At::Current,
                                datums: Some(std::collections::BTreeSet::from([drawer_key()])),
                                optimistic: false,
                            },
                        )
                        .await
                        .map_err(docs_error)
                },
            )
            .await?;
        match read.datums.get(&drawer_key()) {
            Some(big_cabinet_core::doc::DatumRead::Accepted { value, .. }) => {
                let descriptor: Drawer =
                    serde_json::from_value(value.clone()).map_err(|error| {
                        DrawersError::RosterUnreadable(format!("{doc_id} descriptor: {error}"))
                    })?;
                self.state.put_drawer(doc_id, &descriptor).await?;
                if let Some(owner_id) = owner_id {
                    self.grant(
                        context,
                        *owner_id,
                        drawer_object(Access::Admin, doc_id),
                        None,
                    )
                    .await?;
                }
                self.ledger
                    .finish(
                        &row.command,
                        &DrawerCreated {
                            drawer_id: doc_id.clone(),
                            heads: read.heads,
                        },
                    )
                    .await
            }
            _ => self.ledger.forget(&row.command).await,
        }
    }

    /// A new single-group drawer for an already authorized `actor`, run under the ledger so a
    /// crash at any point resumes: before the groups are recorded they are made again (two groups
    /// made but not recorded are leaked — Keyhive has no group deletion, and an unreferenced group
    /// is inert); after, the recorded ones are reused.
    async fn create_single_group(
        &self,
        context: &RequestContext,
        command: &CommandId,
        actor: Uuid7,
        owner_id: Option<Uuid7>,
    ) -> Result<DrawerCreated, DrawersError> {
        let request_digest = digest(&("createDrawer", actor, owner_id));
        self.ledger
            .run(
                command,
                request_digest,
                actor,
                Op::Create {
                    groups: None,
                    sensitive_root: None,
                    owner_id,
                },
                |op| self.create_collection(context, command, op),
            )
            .await
    }

    /// Everything after the row exists: make or look up the three groups, make or look up the
    /// sensitive metadata root, then create the drawer doc as this service under `command`
    /// (DocsRepo's own ledger replays it if a crash already created it) — that doc *is* the
    /// public metadata root — put the public metadata group in charge of it, record the drawer,
    /// and grant `actor` Admin.
    pub(crate) async fn create_collection(
        &self,
        context: &RequestContext,
        command: &CommandId,
        op: Op,
    ) -> Result<DrawerCreated, DrawersError> {
        let Op::Create {
            groups: recorded_groups,
            sensitive_root,
            owner_id,
        } = op
        else {
            unreachable!("create_collection runs under a create row: {op:?}")
        };
        let (meta, _docs, sensitive, groups) = match recorded_groups {
            Some(groups) => (
                self.group(groups.meta_group).await?,
                self.group(groups.docs_group).await?,
                self.group(groups.sensitive_group).await?,
                groups,
            ),
            None => {
                let meta = self
                    .big_repo
                    .create_group_with_parents(Vec::new())
                    .await
                    .map_err(keyhive_error)?;
                let docs = self
                    .big_repo
                    .create_group_with_parents(Vec::new())
                    .await
                    .map_err(keyhive_error)?;
                let sensitive = self
                    .big_repo
                    .create_group_with_parents(Vec::new())
                    .await
                    .map_err(keyhive_error)?;
                let groups = DrawerGroups {
                    meta_group: group_key(&meta),
                    docs_group: group_key(&docs),
                    sensitive_group: group_key(&sensitive),
                };
                self.ledger
                    .record(
                        command,
                        &Op::Create {
                            groups: Some(groups),
                            sensitive_root: None,
                            owner_id,
                        },
                    )
                    .await?;
                (meta, docs, sensitive, groups)
            }
        };
        let sensitive_root = match sensitive_root {
            Some(sensitive_root) => sensitive_root,
            None => {
                let sensitive_root = self
                    .create_metadata_root(context, command, &sensitive)
                    .await?;
                self.ledger
                    .record(
                        command,
                        &Op::Create {
                            groups: Some(groups),
                            sensitive_root: Some(sensitive_root.clone()),
                            owner_id,
                        },
                    )
                    .await?;
                #[cfg(test)]
                self.crash_at(super::CrashPoint::MetadataDocRecorded)?;
                sensitive_root
            }
        };
        let descriptor = Drawer::single_group(
            groups.meta_group,
            groups.docs_group,
            groups.sensitive_group,
            sensitive_root,
        );
        let receipt = self
            .as_signer(
                context,
                self.issuer,
                DocScope {
                    create: [METADATA_PLACEMENT.into()].into(),
                    ..Default::default()
                },
                |Authorization {
                     authority: capability,
                     ..
                 }| {
                    let descriptor = &descriptor;
                    async move {
                        self.docs()
                            .call(
                                context.child(),
                                CreateDoc {
                                    capability,
                                    command: command.clone(),
                                    placement: METADATA_PLACEMENT.into(),
                                    datums: BTreeMap::from([
                                        (drawer_key(), datum_value(descriptor)),
                                        (roster_key(), datum_value(&DrawerRoster::inline())),
                                    ]),
                                },
                            )
                            .await
                            .map_err(docs_error)
                    }
                },
            )
            .await?;
        self.big_repo
            .grant_doc_access(receipt.doc_id.clone(), meta, KeyhiveAccess::Admin)
            .await
            .map_err(keyhive_error)?;
        #[cfg(test)]
        self.crash_at(super::CrashPoint::MetadataDocPublished)?;
        self.state.put_drawer(&receipt.doc_id, &descriptor).await?;
        if let Some(owner_id) = owner_id {
            self.grant(
                context,
                owner_id,
                drawer_object(Access::Admin, &receipt.doc_id),
                None,
            )
            .await?;
        }
        Ok(DrawerCreated {
            drawer_id: receipt.doc_id,
            heads: receipt.heads,
        })
    }

    /// The sensitive metadata root: a metadata doc carrying no descriptor, roster, entries or
    /// child entries, created as this service under a command derived from the drawer's own so
    /// a replay lands on the same doc while the two creations stay two commands with two
    /// ledgers. The sensitive metadata group takes Admin on it the way the public metadata group
    /// takes Admin on the drawer doc; principal access is attenuated by membership level.
    async fn create_metadata_root(
        &self,
        context: &RequestContext,
        command: &CommandId,
        sensitive: &BigKeyhiveGroup,
    ) -> Result<DocId, DrawersError> {
        let receipt = self
            .as_signer(
                context,
                self.issuer,
                DocScope {
                    create: [METADATA_PLACEMENT.into()].into(),
                    ..Default::default()
                },
                |Authorization {
                     authority: capability,
                     ..
                 }| async move {
                    self.docs()
                        .call(
                            context.child(),
                            CreateDoc {
                                capability,
                                command: sensitive_root_command(command),
                                placement: METADATA_PLACEMENT.into(),
                                datums: BTreeMap::new(),
                            },
                        )
                        .await
                        .map_err(docs_error)
                },
            )
            .await?;
        #[cfg(test)]
        self.crash_at(super::CrashPoint::MetadataDocCreated)?;
        self.big_repo
            .grant_doc_access(
                receipt.doc_id.clone(),
                sensitive.clone(),
                KeyhiveAccess::Admin,
            )
            .await
            .map_err(keyhive_error)?;
        #[cfg(test)]
        self.crash_at(super::CrashPoint::MetadataDocGranted)?;
        Ok(receipt.doc_id)
    }

    pub(crate) async fn resume_create(
        &self,
        context: &RequestContext,
        row: &ledger::Row,
    ) -> Result<(), DrawersError> {
        let created = self
            .create_collection(context, &row.command, row.op.clone())
            .await?;
        self.ledger.finish(&row.command, &created).await
    }

    pub(crate) async fn create_meta_doc(
        &self,
        context: &RequestContext,
        command: &CommandId,
        op: Op,
    ) -> Result<MetaDocCreated, DrawersError> {
        let Op::CreateMetaDoc {
            drawer_id,
            kind,
            name,
            root,
            doc_id,
        } = op
        else {
            unreachable!("create_meta_doc runs under a create-meta-doc row: {op:?}")
        };
        let slot = meta_doc_key(&kind, &name)?;
        let record = self
            .state
            .drawer(&drawer_id)
            .await?
            .ok_or_else(|| DrawersError::UnknownDrawer(drawer_id.clone()))?;
        let groups = record.groups().ok_or_else(|| match record.layout_kind() {
            None => DrawersError::ForeignLayout(
                record
                    .descriptor
                    .layout
                    .tag()
                    .unwrap_or("unknown")
                    .to_owned(),
            ),
            Some(_) => DrawersError::SelfDoc(drawer_id.clone()),
        })?;
        // The reference lands on the root doc: Public names the drawer doc itself, Sensitive
        // names the separate sensitive root the descriptor carries. The group granted on the
        // new doc and the group whose delegation admits the reference are that root's own.
        let root_doc = match root {
            MetaRoot::Public => drawer_id.clone(),
            MetaRoot::Sensitive => record
                .descriptor
                .sensitive_root()
                .ok_or_else(|| DrawersError::UnknownDrawer(drawer_id.clone()))?,
        };
        let named_group = match root {
            MetaRoot::Public => groups.meta_group,
            MetaRoot::Sensitive => groups.sensitive_group,
        };
        let group = self.group(named_group).await?;
        let doc_id = match doc_id {
            Some(doc_id) => doc_id,
            None => {
                let receipt = self
                    .as_signer(
                        context,
                        self.issuer,
                        DocScope {
                            create: [METADATA_PLACEMENT.into()].into(),
                            ..Default::default()
                        },
                        |Authorization {
                             authority: capability,
                             ..
                         }| async move {
                            self.docs()
                                .call(
                                    context.child(),
                                    CreateDoc {
                                        capability,
                                        command: command.clone(),
                                        placement: METADATA_PLACEMENT.into(),
                                        datums: BTreeMap::new(),
                                    },
                                )
                                .await
                                .map_err(docs_error)
                        },
                    )
                    .await?;
                #[cfg(test)]
                self.crash_at(super::CrashPoint::MetadataDocCreated)?;
                self.big_repo
                    .grant_doc_access(receipt.doc_id.clone(), group, KeyhiveAccess::Admin)
                    .await
                    .map_err(keyhive_error)?;
                #[cfg(test)]
                self.crash_at(super::CrashPoint::MetadataDocGranted)?;
                self.ledger
                    .record(
                        command,
                        &Op::CreateMetaDoc {
                            drawer_id: drawer_id.clone(),
                            kind: kind.clone(),
                            name: name.clone(),
                            root,
                            doc_id: Some(receipt.doc_id.clone()),
                        },
                    )
                    .await?;
                #[cfg(test)]
                self.crash_at(super::CrashPoint::MetadataDocRecorded)?;
                receipt.doc_id
            }
        };
        let entry = DrawerMetaDoc::new(doc_id.clone());
        let receipt = self
            .write_roster(
                context,
                &root_doc,
                None,
                BTreeMap::from([(slot, datum_value(&entry))]),
                std::collections::BTreeSet::new(),
            )
            .await?;
        #[cfg(test)]
        self.crash_at(super::CrashPoint::MetadataDocPublished)?;
        Ok(MetaDocCreated {
            drawer_id: drawer_id.clone(),
            kind,
            name,
            root,
            doc_id,
            heads: receipt.heads,
        })
    }

    pub(crate) async fn resume_create_meta_doc(
        &self,
        context: &RequestContext,
        row: &ledger::Row,
    ) -> Result<(), DrawersError> {
        let created = self
            .create_meta_doc(context, &row.command, row.op.clone())
            .await?;
        self.ledger.finish(&row.command, &created).await
    }
}

impl DrawerService {
    async fn create(
        &self,
        context: &RequestContext,
        params: CreateDrawer,
    ) -> Result<DrawerCreated, DrawersError> {
        match params.layout {
            CreateLayout::SelfDoc { doc_id } => {
                self.core
                    .create_self_doc(context, params.capability, params.command, doc_id)
                    .await
            }
            CreateLayout::SingleGroup => {
                let caller = self.core.invocation(context, params.capability).await?;
                drop(
                    self.core
                        .authorize_create(context, &caller.authority)
                        .await?,
                );
                let owner_id = caller.originating_actor;
                self.core
                    .create_single_group(context, &params.command, caller.signer.actor_id, owner_id)
                    .await
            }
        }
    }

    /// Create the two local drawers on first boot and record them; later boots find them. Each
    /// goes through the ledger under a fixed command like a request, so a boot interrupted between
    /// the groups and the record resumes it, and one interrupted between the two drawers or before
    /// `set_local` replays the finished one.
    pub(super) async fn ensure_local_drawers(&self) -> Result<(), DrawersError> {
        if self.core.state.local_if_set().is_some() {
            return Ok(());
        }
        let context = RequestContext::root();
        let actor = self.core.actor().actor_id;
        let mut created = Vec::with_capacity(2);
        for command in ["local drawer present", "local drawer originated"] {
            let command = CommandId::try_from(command.to_owned())
                .expect("the local drawer commands are within bounds");
            created.push(
                self.core
                    .create_single_group(&context, &command, actor, Some(actor))
                    .await?
                    .drawer_id,
            );
        }
        // The known-drawers and node config drawers are fresh self-doc drawers: metadata docs that are
        // their own drawers, whose entries are bookkeeping/config datums on them, and a foreign
        // layout arriving from a peer is carried rather than operated, as anywhere else.
        let mut self_docs = Vec::with_capacity(2);
        for command in ["local known drawers", "local drawer config"] {
            let command = CommandId::try_from(command.to_owned())
                .expect("the local self-doc drawer commands are within bounds");
            let receipt = self
                .core
                .as_signer(
                    &context,
                    self.core.issuer,
                    DocScope {
                        create: [METADATA_PLACEMENT.into()].into(),
                        ..Default::default()
                    },
                    |Authorization {
                         authority: capability,
                         ..
                     }| {
                        let (command, context) = (&command, &context);
                        async move {
                            self.core
                                .docs()
                                .call(
                                    context.child(),
                                    CreateDoc {
                                        capability,
                                        command: command.clone(),
                                        placement: METADATA_PLACEMENT.into(),
                                        datums: BTreeMap::from([(
                                            drawer_key(),
                                            datum_value(&Drawer::self_doc()),
                                        )]),
                                    },
                                )
                                .await
                                .map_err(docs_error)
                        }
                    },
                )
                .await?;
            self.core
                .state
                .put_drawer(&receipt.doc_id, &Drawer::self_doc())
                .await?;
            self.core
                .grant(
                    &context,
                    actor,
                    drawer_object(Access::Admin, &receipt.doc_id),
                    None,
                )
                .await?;
            self_docs.push(receipt.doc_id);
        }
        let [present, originated]: [DrawerId; 2] =
            created.try_into().expect("two local drawers were created");
        let [known_drawers_id, config_id]: [DrawerId; 2] = self_docs
            .try_into()
            .expect("two local self-doc drawers were created");
        self.core
            .state
            .set_local(LocalDrawers {
                present_id: present,
                originated_id: originated,
                known_drawers_id,
                config_id,
            })
            .await
    }
}

impl Handler<CreateDrawer> for DrawerService {
    async fn call(
        &self,
        request: Envelope<CreateDrawer>,
    ) -> Result<ResponseEnvelope<DrawerCreated>, DrawersError> {
        Ok(self.create(&request.context, request.params).await?.into())
    }
}

impl DrawerService {
    async fn create_meta_doc(
        &self,
        context: &RequestContext,
        params: CreateMetaDoc,
    ) -> Result<MetaDocCreated, DrawersError> {
        let caller = self.core.invocation(context, params.capability).await?;
        let actor = &caller.signer;
        self.core
            .authorize(context, &caller.authority, &params.drawer_id, Access::Write)
            .await?;
        let slot = meta_doc_key(&params.kind, &params.name)?;
        drop(slot);
        let request_digest = digest(&(
            "createMetaDoc",
            actor.actor_id,
            &params.drawer_id,
            &params.kind,
            &params.name,
            params.root,
        ));
        let op = Op::CreateMetaDoc {
            drawer_id: params.drawer_id.clone(),
            kind: params.kind.clone(),
            name: params.name.clone(),
            root: params.root,
            doc_id: None,
        };
        self.core
            .ledger
            .run(&params.command, request_digest, actor.actor_id, op, |op| {
                self.core.create_meta_doc(context, &params.command, op)
            })
            .await
    }
}

impl Handler<CreateMetaDoc> for DrawerService {
    async fn call(
        &self,
        request: Envelope<CreateMetaDoc>,
    ) -> Result<ResponseEnvelope<MetaDocCreated>, DrawersError> {
        Ok(self
            .create_meta_doc(&request.context, request.params)
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use automerge::ChangeHash;

    fn command() -> CommandId {
        CommandId::try_from("cmd-1".to_owned()).expect("a command id")
    }

    utils_rs::table_tests! {
        create_drawer_wire_shape,
        (request, expected),
        {
            let value = serde_json::to_value(&request).expect("serialize");
            assert_eq!(value, expected);
            let back: CreateDrawer = serde_json::from_value(value).expect("deserialize");
            assert_eq!(back, request);
        }
    }

    create_drawer_wire_shape! {
        self_doc: (
            CreateDrawer {
                capability: CapId::from_bytes([1; 16]),
                command: command(),
                layout: CreateLayout::SelfDoc { doc_id: DocId::new([2; 32]) },
            },
            serde_json::json!({
                "capability": CapId::from_bytes([1; 16]),
                "command": "cmd-1",
                "layout": "selfDoc",
                "docId": DocId::new([2; 32]).to_string(),
            })
        ),
        single_group: (
            CreateDrawer {
                capability: CapId::from_bytes([1; 16]),
                command: command(),
                layout: CreateLayout::SingleGroup,
            },
            serde_json::json!({
                "capability": CapId::from_bytes([1; 16]),
                "command": "cmd-1",
                "layout": "singleGroup",
            })
        ),
    }

    #[test]
    fn a_layout_is_required() {
        let error = serde_json::from_value::<CreateDrawer>(serde_json::json!({
            "capability": CapId::from_bytes([1; 16]),
            "command": "cmd-1",
        }))
        .unwrap_err();
        assert!(error.to_string().contains("layout"), "{error}");
        let error = serde_json::from_value::<CreateDrawer>(serde_json::json!({
            "capability": CapId::from_bytes([1; 16]),
            "command": "cmd-1",
            "layout": "selfDoc",
        }))
        .unwrap_err();
        assert!(error.to_string().contains("doc"), "{error}");
    }

    #[test]
    fn a_receipt_names_the_drawer_and_its_heads() {
        let created = DrawerCreated {
            drawer_id: DocId::new([3; 32]),
            heads: vec![ChangeHash([4; 32])],
        };
        let value = serde_json::to_value(&created).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "drawerId": DocId::new([3; 32]).to_string(),
                "heads": [ChangeHash([4; 32])],
            })
        );
        assert_eq!(
            serde_json::from_value::<DrawerCreated>(value).expect("deserialize"),
            created
        );
    }
}
