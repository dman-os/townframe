//! GrantDrawer: a standing grant to a local actor on a drawer, in this service's ledger namespace.

use super::*;

impl DrawerService {
    async fn grant(
        &self,
        context: &RequestContext,
        params: GrantDrawer,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.capability).await?;
        core.authorize(context, &caller.authority, &params.drawer_id, Access::Admin)
            .await?;
        if !caller.rights.contains(Rights::DELEGATE) {
            return Err(DrawersError::Denied);
        }
        core.grant(
            context,
            params.subject_id,
            drawer_object(params.access, &params.drawer_id),
            Some(params.capability),
        )
        .await
    }
}

impl Handler<GrantDrawer> for DrawerService {
    async fn call(
        &self,
        request: Envelope<GrantDrawer>,
    ) -> Result<ResponseEnvelope<()>, DrawersError> {
        Ok(self.grant(&request.context, request.params).await?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_spells_the_level() {
        let subject = Uuid7::new();
        let request = GrantDrawer {
            capability: CapId::from_bytes([1; 16]),
            drawer_id: DocId::new([2; 32]),
            subject_id: subject,
            access: Access::Write,
        };
        let value = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "capability": CapId::from_bytes([1; 16]),
                "drawerId": DocId::new([2; 32]).to_string(),
                "subjectId": subject.to_string(),
                "access": "Write",
            })
        );
        assert_eq!(
            serde_json::from_value::<GrantDrawer>(value).expect("deserialize"),
            request
        );
    }
}
