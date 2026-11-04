use super::*;
use big_cabinet_core::doc::DatumRead;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataRootStatus {
    Authoritative,
    Missing,
    Denied,
    Pending { reason: String },
    Rejected { reason: String },
    Undecodable { reason: String },
}

impl MetadataRootStatus {
    fn from_read_error(error: DrawersError) -> Result<Self, DrawersError> {
        Ok(match error {
            DrawersError::Docs(DocsError::DocNotFound) => Self::Missing,
            DrawersError::Denied | DrawersError::Docs(DocsError::Denied | DocsError::Access(_)) => {
                Self::Denied
            }
            DrawersError::AuthorityUnavailable(reason)
            | DrawersError::DocsUnavailable(reason)
            | DrawersError::Docs(
                DocsError::AuthorityUnavailable(reason) | DocsError::Pending { reason },
            ) => Self::Pending { reason },
            error @ DrawersError::Docs(DocsError::UnknownHeads { .. }) => Self::Pending {
                reason: error.to_string(),
            },
            other => return Err(other),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DrawerMetadataObservation {
    pub drawer_id: DrawerId,
    pub descriptor: Drawer,
    pub children: BTreeMap<DrawerId, Access>,
    pub meta_docs: BTreeMap<MetaRoot, BTreeMap<String, DocId>>,
    pub roots: BTreeMap<MetaRoot, MetadataRootStatus>,
}

impl Drawers {
    pub fn metadata_observer(&self) -> DrawerService {
        DrawerService {
            core: Arc::clone(&self.core),
        }
    }

    pub async fn observe_metadata(
        &self,
        context: &RequestContext,
        drawer_id: &DrawerId,
    ) -> Result<DrawerMetadataObservation, DrawersError> {
        self.core.observe_metadata(context, drawer_id).await
    }
}

impl DrawerService {
    pub async fn observe_metadata(
        &self,
        context: &RequestContext,
        drawer_id: &DrawerId,
    ) -> Result<DrawerMetadataObservation, DrawersError> {
        self.core.observe_metadata(context, drawer_id).await
    }
}

impl DrawerMetadataObservation {
    fn projected_meta_docs(&self, mut prior: MetaDocRows) -> MetaDocRows {
        for (root, status) in &self.roots {
            if matches!(status, MetadataRootStatus::Authoritative) {
                prior.remove(root);
            } else if !matches!(status, MetadataRootStatus::Pending { .. }) {
                continue;
            }
            if let Some(slots) = self.meta_docs.get(root)
                && !slots.is_empty()
            {
                prior
                    .entry(*root)
                    .or_default()
                    .extend(slots.iter().map(|(slot, doc)| (slot.clone(), doc.clone())));
            }
        }
        prior
    }
}

impl Core {
    pub(super) async fn observe_metadata(
        &self,
        context: &RequestContext,
        drawer_id: &DrawerId,
    ) -> Result<DrawerMetadataObservation, DrawersError> {
        context.validate()?;
        let datums = self.read_all_datums(context, drawer_id).await?;
        let descriptor = Self::metadata_descriptor(drawer_id, &datums)?;
        let record = create::record_of(drawer_id, &descriptor);
        let mut observation = DrawerMetadataObservation {
            drawer_id: drawer_id.clone(),
            descriptor,
            children: BTreeMap::new(),
            meta_docs: BTreeMap::new(),
            roots: BTreeMap::new(),
        };
        match record.layout_kind() {
            Some(LayoutKind::SingleGroup) => {
                let RosterRows {
                    docs,
                    children,
                    meta_docs,
                } = Self::read_roster(drawer_id, datums)?;
                let groups = record
                    .groups()
                    .expect("a single-group layout states its groups");
                let sensitive_root = record
                    .descriptor
                    .sensitive_root()
                    .expect("a single-group layout states its sensitive root");
                let (sensitive_status, sensitive) =
                    self.read_metadata_root(context, &sensitive_root).await?;
                observation.children = children;
                observation
                    .roots
                    .insert(MetaRoot::Public, MetadataRootStatus::Authoritative);
                observation
                    .roots
                    .insert(MetaRoot::Sensitive, sensitive_status);
                observation.meta_docs = self
                    .admit_meta_docs(
                        groups,
                        [
                            (MetaRoot::Public, meta_docs),
                            (MetaRoot::Sensitive, sensitive),
                        ],
                        &mut observation.roots,
                    )
                    .await?;
                let prior = self.state.meta_docs_of(drawer_id).await?;
                let projected = observation.projected_meta_docs(prior);
                self.state
                    .replace_roster(drawer_id, &docs, &observation.children, &projected)
                    .await?;
            }
            Some(LayoutKind::SelfDoc) => {
                self.state
                    .replace_roster(
                        drawer_id,
                        &BTreeMap::new(),
                        &BTreeMap::new(),
                        &BTreeMap::new(),
                    )
                    .await?;
            }
            None => {}
        }
        self.state
            .put_drawer(drawer_id, &observation.descriptor)
            .await?;
        Ok(observation)
    }

    fn metadata_descriptor(
        drawer_id: &DrawerId,
        datums: &BTreeMap<DatumKey, DatumRead>,
    ) -> Result<Drawer, DrawersError> {
        let value = match datums.get(&drawer_key()) {
            Some(DatumRead::Accepted { value, .. }) => value.clone(),
            Some(DatumRead::Absent { .. }) | None => {
                return Err(DrawersError::RosterUnreadable(format!(
                    "{drawer_id} carries no drawer descriptor"
                )));
            }
            Some(other) => {
                return Err(DrawersError::RosterUnreadable(format!(
                    "{drawer_id} descriptor: {other:?}"
                )));
            }
        };
        serde_json::from_value(value).map_err(|error| {
            DrawersError::RosterUnreadable(format!("{drawer_id} descriptor: {error}"))
        })
    }

    async fn read_metadata_root(
        &self,
        context: &RequestContext,
        root: &DocId,
    ) -> Result<(MetadataRootStatus, BTreeMap<String, DrawerMetaDoc>), DrawersError> {
        let local = ed25519_dalek::VerifyingKey::from_bytes(
            &self
                .big_repo
                .local_peer_id()
                .to_bytes32()
                .map_err(keyhive_error)?,
        )
        .map_err(keyhive_error)?;
        let document =
            ed25519_dalek::VerifyingKey::from_bytes(&root.to_bytes32().map_err(keyhive_error)?)
                .map_err(keyhive_error)?;
        if !self
            .big_repo
            .keyhive()
            .agent_access_on(&local.into(), document.into())
            .await
            .is_some_and(|access| access >= KeyhiveAccess::Read)
        {
            return Ok((MetadataRootStatus::Denied, BTreeMap::new()));
        }
        let datums = match self.read_all_datums(context, root).await {
            Ok(datums) => datums,
            Err(error) => {
                return Ok((MetadataRootStatus::from_read_error(error)?, BTreeMap::new()));
            }
        };
        for (key, datum) in &datums {
            let status = match datum {
                DatumRead::Accepted { .. } | DatumRead::Absent { .. } => continue,
                DatumRead::Pending { reason, .. } => MetadataRootStatus::Pending {
                    reason: format!("{key} on {root}: {reason}"),
                },
                DatumRead::Rejected { reason } => MetadataRootStatus::Rejected {
                    reason: format!("{key} on {root}: {reason}"),
                },
                DatumRead::Undecodable { reason, .. } => MetadataRootStatus::Undecodable {
                    reason: format!("{key} on {root}: {reason}"),
                },
            };
            return Ok((status, BTreeMap::new()));
        }
        match Self::read_roster(root, datums) {
            Ok(roster) => Ok((MetadataRootStatus::Authoritative, roster.meta_docs)),
            Err(DrawersError::RosterUnreadable(reason)) => {
                Ok((MetadataRootStatus::Undecodable { reason }, BTreeMap::new()))
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_root_unavailable_reads_are_pending_and_unexpected_errors_propagate() {
        let unknown = DrawersError::Docs(DocsError::UnknownHeads {
            heads: vec![automerge::ChangeHash([9; 32])],
        });
        for (error, reason) in [
            (unknown.clone(), unknown.to_string()),
            (
                DrawersError::Docs(DocsError::AuthorityUnavailable(
                    "docs authority offline".into(),
                )),
                "docs authority offline".into(),
            ),
            (
                DrawersError::AuthorityUnavailable("drawer authority offline".into()),
                "drawer authority offline".into(),
            ),
            (
                DrawersError::DocsUnavailable("docs offline".into()),
                "docs offline".into(),
            ),
            (
                DrawersError::Docs(DocsError::Pending {
                    reason: "materializing".into(),
                }),
                "materializing".into(),
            ),
        ] {
            assert_eq!(
                MetadataRootStatus::from_read_error(error).unwrap(),
                MetadataRootStatus::Pending { reason }
            );
        }
        for (error, status) in [
            (
                DrawersError::Docs(DocsError::DocNotFound),
                MetadataRootStatus::Missing,
            ),
            (DrawersError::Denied, MetadataRootStatus::Denied),
            (
                DrawersError::Docs(DocsError::Denied),
                MetadataRootStatus::Denied,
            ),
            (
                DrawersError::Docs(DocsError::Access("policy denied".into())),
                MetadataRootStatus::Denied,
            ),
        ] {
            assert_eq!(MetadataRootStatus::from_read_error(error).unwrap(), status);
        }
        for error in [
            DrawersError::Docs(DocsError::Repository("store failed".into())),
            DrawersError::Storage("projection failed".into()),
            DrawersError::Docs(DocsError::InvalidCommand),
        ] {
            assert_eq!(
                MetadataRootStatus::from_read_error(error.clone()).unwrap_err(),
                error
            );
        }
    }

    #[test]
    fn metadata_projection_preserves_unavailable_roots_but_clears_authoritative_empty() {
        let prior = BTreeMap::from([
            (
                MetaRoot::Public,
                BTreeMap::from([("notes/old".to_owned(), DocId::new([1; 32]))]),
            ),
            (
                MetaRoot::Sensitive,
                BTreeMap::from([("notes/main".to_owned(), DocId::new([2; 32]))]),
            ),
        ]);
        let current = BTreeMap::from([("notes/new".to_owned(), DocId::new([3; 32]))]);
        let mut observation = DrawerMetadataObservation {
            drawer_id: DocId::new([4; 32]),
            descriptor: Drawer::self_doc(),
            children: BTreeMap::new(),
            meta_docs: BTreeMap::from([(MetaRoot::Public, current.clone())]),
            roots: BTreeMap::from([(MetaRoot::Public, MetadataRootStatus::Authoritative)]),
        };
        let expected = BTreeMap::from([
            (MetaRoot::Public, current.clone()),
            (MetaRoot::Sensitive, prior[&MetaRoot::Sensitive].clone()),
        ]);
        assert_eq!(observation.projected_meta_docs(prior.clone()), expected);
        let mut unavailable = observation.clone();
        unavailable.meta_docs.clear();
        unavailable.roots = [MetaRoot::Public, MetaRoot::Sensitive]
            .into_iter()
            .map(|root| {
                (
                    root,
                    MetadataRootStatus::Pending {
                        reason: "backing delegation has not arrived".into(),
                    },
                )
            })
            .collect();
        assert_eq!(unavailable.projected_meta_docs(prior.clone()), prior);
        for status in [
            MetadataRootStatus::Missing,
            MetadataRootStatus::Denied,
            MetadataRootStatus::Pending {
                reason: "pending".into(),
            },
            MetadataRootStatus::Rejected {
                reason: "rejected".into(),
            },
            MetadataRootStatus::Undecodable {
                reason: "undecodable".into(),
            },
        ] {
            observation
                .roots
                .insert(MetaRoot::Sensitive, status.clone());
            assert_eq!(
                observation.projected_meta_docs(prior.clone()),
                expected,
                "{status:?}"
            );
        }
        observation.roots.insert(
            MetaRoot::Public,
            MetadataRootStatus::Pending {
                reason: "another slot has no local backing delegation".into(),
            },
        );
        let mut partial = prior.clone();
        partial
            .get_mut(&MetaRoot::Public)
            .unwrap()
            .extend(current.clone());
        assert_eq!(observation.projected_meta_docs(prior.clone()), partial);
        observation
            .roots
            .insert(MetaRoot::Public, MetadataRootStatus::Authoritative);
        observation
            .roots
            .insert(MetaRoot::Sensitive, MetadataRootStatus::Authoritative);
        assert_eq!(
            observation.projected_meta_docs(prior.clone()),
            BTreeMap::from([(MetaRoot::Public, current)])
        );
        observation.meta_docs.clear();
        assert_eq!(observation.projected_meta_docs(prior), BTreeMap::new());
    }
}
