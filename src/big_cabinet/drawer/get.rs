//! GetDrawer: a drawer's layout and roster as this node sees them.

use super::*;

impl DrawerService {
    /// Relay suffices: ADR 014 lets a relay enumerate the roster. The access check comes before
    /// the record lookup so that an unauthorized caller cannot tell a drawer from any other doc.
    async fn get(
        &self,
        context: &RequestContext,
        params: GetDrawer,
    ) -> Result<DrawerView, DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.authorization).await?;
        let access = core
            .level(context, &caller.authority, &params.drawer_id)
            .await?
            .ok_or(DrawersError::Denied)?;
        let record = core
            .ensure_drawer(context, &params.drawer_id)
            .await?
            .ok_or_else(|| DrawersError::UnknownDrawer(params.drawer_id.clone()))?;
        let (record, descriptor) = core.rescan(context, &record).await?;
        Ok(DrawerView {
            docs: core.state.docs_of(&record.drawer_id).await?,
            children: core.state.children_of(&record.drawer_id).await?,
            drawer_id: record.drawer_id,
            layout: record.descriptor.layout.clone(),
            descriptor,
            access,
        })
    }
}

impl Handler<GetDrawer> for DrawerService {
    async fn call(
        &self,
        request: Envelope<GetDrawer>,
    ) -> Result<ResponseEnvelope<DrawerView>, DrawersError> {
        Ok(self.get(&request.context, request.params).await?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_names_the_drawer() {
        let request = GetDrawer {
            authorization: Authorization::identity(CapId::from_bytes([1; 16])),
            drawer_id: DocId::new([2; 32]),
        };
        let value = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "authorization": Authorization::identity(CapId::from_bytes([1; 16])),
                "drawerId": DocId::new([2; 32]).to_string(),
            })
        );
        assert_eq!(
            serde_json::from_value::<GetDrawer>(value).expect("deserialize"),
            request
        );
    }

    #[test]
    fn a_view_spells_entries_by_id() {
        let view = DrawerView {
            drawer_id: DocId::new([2; 32]),
            layout: Drawer::self_doc().layout,
            descriptor: Drawer::self_doc(),
            access: Access::Relay,
            docs: BTreeMap::from([(DocId::new([3; 32]), Access::Read)]),
            children: BTreeMap::from([(DocId::new([4; 32]), Access::Write)]),
        };
        let value = serde_json::to_value(&view).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "drawerId": DocId::new([2; 32]).to_string(),
                "layout": { "$type": "org.example.daybook.drawer.selfDoc", "empty": true },
                "descriptor": {
                    "layout": { "$type": "org.example.daybook.drawer.selfDoc", "empty": true },
                },
                "access": "Relay",
                "docs": { DocId::new([3; 32]).to_string(): "Read" },
                "children": { DocId::new([4; 32]).to_string(): "Write" },
            })
        );
        assert_eq!(
            serde_json::from_value::<DrawerView>(value).expect("deserialize"),
            view
        );
    }
}
