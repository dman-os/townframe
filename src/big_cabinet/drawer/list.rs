//! ListDrawers: every drawer this node knows that the caller can at least relay.

use super::*;

impl DrawerService {
    /// In id order, which is how the projection lists them.
    async fn list(
        &self,
        context: &RequestContext,
        params: ListDrawers,
    ) -> Result<Vec<ListedDrawer>, DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.capability).await?;
        core.sync_discovered(context).await?;
        let DomainAuthority::Scoped(scope) = &caller.authority else {
            return Err(DrawersError::Denied);
        };
        let records = core.state.scoped_drawers(&scope.drawers).await?;
        if records.is_empty() {
            return Ok(Vec::new());
        }
        core.as_signer(
            context,
            core.issuer,
            DocScope {
                read: records
                    .iter()
                    .map(|(record, _)| record.drawer_id.clone())
                    .collect(),
                ..Default::default()
            },
            |capability| async move {
                let mut listed = Vec::with_capacity(records.len());
                for (record, access) in records {
                    let read = core
                        .docs()
                        .call(
                            context.child(),
                            big_cabinet_core::doc::ReadFacets {
                                capability,
                                doc_id: record.drawer_id.clone(),
                                at: big_cabinet_core::doc::At::Current,
                                facets: Some(std::collections::BTreeSet::from([drawer_key()])),
                                optimistic: false,
                            },
                        )
                        .await
                        .map_err(docs_error)?;
                    let value = match read.facets.into_iter().next() {
                        Some((_, big_cabinet_core::doc::FacetRead::Accepted { value, .. })) => {
                            value
                        }
                        _ => {
                            return Err(DrawersError::RosterUnreadable(format!(
                                "{} carries no drawer descriptor",
                                record.drawer_id
                            )));
                        }
                    };
                    let descriptor: Drawer = serde_json::from_value(value).map_err(|error| {
                        DrawersError::RosterUnreadable(format!(
                            "{} descriptor: {error}",
                            record.drawer_id
                        ))
                    })?;
                    listed.push(ListedDrawer {
                        drawer_id: record.drawer_id,
                        layout: descriptor.layout.clone(),
                        descriptor,
                        access,
                    });
                }
                Ok(listed)
            },
        )
        .await
    }
}

impl Handler<ListDrawers> for DrawerService {
    async fn call(
        &self,
        request: Envelope<ListDrawers>,
    ) -> Result<ResponseEnvelope<Vec<ListedDrawer>>, DrawersError> {
        Ok(self.list(&request.context, request.params).await?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_carries_only_capability() {
        let request = ListDrawers {
            capability: CapId::from_bytes([1; 16]),
        };
        let value = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({ "capability": CapId::from_bytes([1; 16]) })
        );
        assert_eq!(
            serde_json::from_value::<ListDrawers>(value).expect("deserialize"),
            request
        );
    }

    #[test]
    fn a_listing_spells_the_layout_and_the_level() {
        let listed = ListedDrawer {
            drawer_id: DocId::new([2; 32]),
            layout: Drawer::single_group(GroupKey([3; 32]), GroupKey([4; 32])).layout,
            descriptor: Drawer::single_group(GroupKey([3; 32]), GroupKey([4; 32])),
            access: Access::Admin,
        };
        let value = serde_json::to_value(&listed).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "drawerId": DocId::new([2; 32]).to_string(),
                "layout": {
                    "$type": "org.example.daybook.drawer.singleGroup",
                    "metaGroup": GroupKey([3; 32]).to_string(),
                    "docsGroup": GroupKey([4; 32]).to_string(),
                },
                "descriptor": {
                    "layout": {
                        "$type": "org.example.daybook.drawer.singleGroup",
                        "metaGroup": GroupKey([3; 32]).to_string(),
                        "docsGroup": GroupKey([4; 32]).to_string(),
                    },
                },
                "access": "Admin",
            })
        );
        assert_eq!(
            serde_json::from_value::<ListedDrawer>(value).expect("deserialize"),
            listed
        );
    }
}
