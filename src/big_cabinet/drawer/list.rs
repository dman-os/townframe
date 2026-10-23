//! ListDrawers: every drawer this node knows that the caller can at least relay.

use super::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListDrawers {
    pub capability: CapId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedDrawer {
    pub drawer_id: DrawerId,
    pub layout: DrawerLayout,
    /// The descriptor facet, as [`DrawerView`] carries it.
    pub descriptor: Drawer,
    pub access: Access,
}

impl Operation for ListDrawers {
    type Output = Vec<ListedDrawer>;
    type Error = DrawersError;
    const XRPC: XrpcOperation = XrpcOperation::query("org.example.daybook.drawers.listDrawers")
        .with_schemas("ListedDrawers", "DrawersError");
}

impl DrawerService {
    /// In id order, which is how the projection lists them.
    async fn list(
        &self,
        context: &RequestContext,
        params: ListDrawers,
    ) -> Result<Vec<ListedDrawer>, DrawersError> {
        let core = &self.core;
        let actor = core.resolve(context, params.capability).await?;
        core.sync_discovered(context).await?;
        let mut listed = Vec::new();
        for record in core.state.drawers().await? {
            if let Some(access) = core
                .effective(context, actor.uuid, &record.drawer_id)
                .await?
            {
                let descriptor = core.descriptor(context, &record.drawer_id).await?;
                listed.push(ListedDrawer {
                    drawer_id: record.drawer_id,
                    layout: record.descriptor.layout.clone(),
                    descriptor,
                    access,
                });
            }
        }
        Ok(listed)
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
    fn a_request_carries_only_the_capability() {
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
