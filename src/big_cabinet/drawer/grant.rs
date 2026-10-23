//! GrantDrawer: a standing grant to a local actor on a drawer, in this service's ledger namespace.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub subject_id: Uuid7,
    pub access: Access,
}

impl Operation for GrantDrawer {
    type Output = ();
    type Error = DrawersError;
    const XRPC: XrpcOperation = XrpcOperation::procedure("org.example.daybook.drawers.grantDrawer")
        .with_schemas("Void", "DrawersError");
}

impl DrawerService {
    async fn grant(
        &self,
        context: &RequestContext,
        params: GrantDrawer,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        let actor = core.resolve(context, params.capability).await?;
        core.authorize(context, &actor, &params.drawer_id, Access::Admin)
            .await?;
        core.grant(
            context,
            params.subject_id,
            drawer_object(params.access, &params.drawer_id),
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
