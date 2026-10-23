//! RememberDrawer, ForgetDrawer, ListKnownDrawers, SetDrawerPolicy: the known-drawers
//! registry.
//!
//! The registry is an explicitly managed list, kept as `knownEntry/<drawerId>` facets on the
//! local registry drawer — a self-doc drawer whose own doc is the durable home, so the list
//! survives like every other drawer fact and a peer that syncs the doc sees what this node
//! remembered. It is bookkeeping, not membership: remembering grants no access to the drawer,
//! mirroring intent certifies no fetched or retained bytes, and forgetting edits nothing but
//! the local registry. Nothing scans tags into it — only these operations write it.

use super::*;
use crate::doc::At;
use big_cabinet_core::drawer::KnownDrawerEntry;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RememberDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    /// Intent to fetch and retain the drawer's content. Absent or `false` means none —
    /// remembering is not mirroring.
    #[serde(default)]
    pub mirror: bool,
}

impl Operation for RememberDrawer {
    type Output = ();
    type Error = DrawersError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.drawers.rememberDrawer")
            .with_schemas("Void", "DrawersError");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForgetDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
}

impl Operation for ForgetDrawer {
    type Output = ();
    type Error = DrawersError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.drawers.forgetDrawer")
            .with_schemas("Void", "DrawersError");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListKnownDrawers {
    pub capability: CapId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnownDrawer {
    pub drawer_id: DrawerId,
    pub mirror: bool,
}

impl Operation for ListKnownDrawers {
    type Output = Vec<KnownDrawer>;
    type Error = DrawersError;
    const XRPC: XrpcOperation =
        XrpcOperation::query("org.example.daybook.drawers.listKnownDrawers")
            .with_schemas("KnownDrawer[]", "DrawersError");
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetDrawerPolicy {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub mirror: bool,
}

impl Operation for SetDrawerPolicy {
    type Output = ();
    type Error = DrawersError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.drawers.setDrawerPolicy")
            .with_schemas("Void", "DrawersError");
}

impl Core {
    /// The registry's `knownEntry` facets, read as this service at current heads.
    pub(crate) async fn known_entries(
        &self,
        context: &RequestContext,
    ) -> Result<BTreeMap<DrawerId, KnownDrawerEntry>, DrawersError> {
        let registry = self.state.local().registry_id;
        let read = self
            .docs()
            .call(
                context.child(),
                crate::doc::ReadFacets {
                    capability: self.issuer,
                    doc_id: registry,
                    at: At::Current,
                    facets: None,
                    optimistic: false,
                },
            )
            .await
            .map_err(docs_error)?;
        let mut known = BTreeMap::new();
        for (key, facet) in read.facets {
            if key.tag().as_str() != DRAWER_KNOWN_ENTRY_FACET_TAG {
                continue;
            }
            let value = match facet {
                crate::doc::FacetRead::Accepted { value, .. } => value,
                crate::doc::FacetRead::Absent { .. } => continue,
                other => {
                    return Err(DrawersError::RosterUnreadable(format!(
                        "{key} on the registry: {other:?}"
                    )));
                }
            };
            let Some(drawer_id) = entry_target(&key) else {
                return Err(DrawersError::RosterUnreadable(format!(
                    "{key} on the registry does not name a drawer"
                )));
            };
            let entry: KnownDrawerEntry = serde_json::from_value(value).map_err(|error| {
                DrawersError::RosterUnreadable(format!("{key} on the registry: {error}"))
            })?;
            known.insert(drawer_id, entry);
        }
        Ok(known)
    }
}

impl DrawerService {
    async fn remember(
        &self,
        context: &RequestContext,
        params: RememberDrawer,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        core.resolve(context, params.capability).await?;
        let registry = core.state.local().registry_id;
        let entry = KnownDrawerEntry {
            mirror: params.mirror.then_some(true),
            extra: Default::default(),
        };
        core.write_roster(
            context,
            &registry,
            None,
            BTreeMap::from([(
                known_entry_key(&params.drawer_id),
                serde_json::to_value(entry).expect("a known entry serializes"),
            )]),
            Default::default(),
        )
        .await?;
        // A remembered drawer enters the known set of records when this node carries its
        // descriptor; remembering alone never creates one.
        drop(core.ensure_drawer(context, &params.drawer_id).await?);
        Ok(())
    }

    async fn forget(
        &self,
        context: &RequestContext,
        params: ForgetDrawer,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        core.resolve(context, params.capability).await?;
        let registry = core.state.local().registry_id;
        core.write_roster(
            context,
            &registry,
            None,
            Default::default(),
            std::collections::BTreeSet::from([known_entry_key(&params.drawer_id)]),
        )
        .await?;
        Ok(())
    }

    async fn list_known(
        &self,
        context: &RequestContext,
        params: ListKnownDrawers,
    ) -> Result<Vec<KnownDrawer>, DrawersError> {
        let core = &self.core;
        core.resolve(context, params.capability).await?;
        Ok(core
            .known_entries(context)
            .await?
            .into_iter()
            .map(|(drawer_id, entry)| KnownDrawer {
                drawer_id,
                mirror: entry.mirror.unwrap_or(false),
            })
            .collect())
    }

    async fn set_policy(
        &self,
        context: &RequestContext,
        params: SetDrawerPolicy,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        core.resolve(context, params.capability).await?;
        if !core
            .known_entries(context)
            .await?
            .contains_key(&params.drawer_id)
        {
            return Err(DrawersError::UnknownDrawer(params.drawer_id.clone()));
        }
        self.remember(
            context,
            RememberDrawer {
                capability: params.capability,
                drawer_id: params.drawer_id,
                mirror: params.mirror,
            },
        )
        .await
    }
}

impl Handler<RememberDrawer> for DrawerService {
    async fn call(
        &self,
        request: Envelope<RememberDrawer>,
    ) -> Result<ResponseEnvelope<()>, DrawersError> {
        Ok(self
            .remember(&request.context, request.params)
            .await?
            .into())
    }
}

impl Handler<ForgetDrawer> for DrawerService {
    async fn call(
        &self,
        request: Envelope<ForgetDrawer>,
    ) -> Result<ResponseEnvelope<()>, DrawersError> {
        Ok(self.forget(&request.context, request.params).await?.into())
    }
}

impl Handler<ListKnownDrawers> for DrawerService {
    async fn call(
        &self,
        request: Envelope<ListKnownDrawers>,
    ) -> Result<ResponseEnvelope<Vec<KnownDrawer>>, DrawersError> {
        Ok(self
            .list_known(&request.context, request.params)
            .await?
            .into())
    }
}

impl Handler<SetDrawerPolicy> for DrawerService {
    async fn call(
        &self,
        request: Envelope<SetDrawerPolicy>,
    ) -> Result<ResponseEnvelope<()>, DrawersError> {
        Ok(self
            .set_policy(&request.context, request.params)
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wire_spells_intent_not_membership() {
        let remember = RememberDrawer {
            capability: CapId::from_bytes([1; 16]),
            drawer_id: DocId::new([2; 32]),
            mirror: true,
        };
        let value = serde_json::to_value(&remember).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "capability": CapId::from_bytes([1; 16]),
                "drawerId": DocId::new([2; 32]).to_string(),
                "mirror": true,
            })
        );
        assert_eq!(
            serde_json::from_value::<RememberDrawer>(value).expect("deserialize"),
            remember
        );
        let entry = KnownDrawerEntry {
            mirror: None,
            extra: Default::default(),
        };
        assert_eq!(serde_json::to_value(&entry).unwrap(), serde_json::json!({}));
        let entry = KnownDrawerEntry {
            mirror: Some(true),
            extra: Default::default(),
        };
        assert_eq!(
            serde_json::to_value(&entry).unwrap(),
            serde_json::json!({"mirror": true})
        );
    }
}
