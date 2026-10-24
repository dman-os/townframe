//! RevokeDrawer: withdraw every level a local actor holds on a drawer.

use super::*;

impl DrawerService {
    async fn revoke(
        &self,
        context: &RequestContext,
        params: RevokeDrawer,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.capability).await?;
        core.authorize(context, &caller.authority, &params.drawer_id, Access::Admin)
            .await?;
        core.grants
            .revoke(
                context,
                params.subject_id,
                &Access::ALL.map(|access| drawer_object(access, &params.drawer_id)),
            )
            .await
            .map_err(grant_error)?;
        core.state.notify_changed();
        Ok(())
    }
}

impl Handler<RevokeDrawer> for DrawerService {
    async fn call(
        &self,
        request: Envelope<RevokeDrawer>,
    ) -> Result<ResponseEnvelope<()>, DrawersError> {
        Ok(self.revoke(&request.context, request.params).await?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_names_the_subject_without_a_level() {
        let subject = Uuid7::new();
        let request = RevokeDrawer {
            capability: CapId::from_bytes([1; 16]),
            drawer_id: DocId::new([2; 32]),
            subject_id: subject,
        };
        let value = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "capability": CapId::from_bytes([1; 16]),
                "drawerId": DocId::new([2; 32]).to_string(),
                "subjectId": subject.to_string(),
            })
        );
        assert_eq!(
            serde_json::from_value::<RevokeDrawer>(value).expect("deserialize"),
            request
        );
    }
}
