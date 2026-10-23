//! RevokeDrawer: withdraw every level a local actor holds on a drawer.

use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeDrawer {
    pub capability: CapId,
    pub drawer_id: DrawerId,
    pub subject_id: Uuid7,
}

impl Operation for RevokeDrawer {
    type Output = ();
    type Error = DrawersError;
    const XRPC: XrpcOperation =
        XrpcOperation::procedure("org.example.daybook.drawers.revokeDrawer")
            .with_schemas("Void", "DrawersError");
}

impl DrawerService {
    /// Every level is its own object, so all four are withdrawn; the authority service's revoke
    /// is a delete by key, so a level never granted is a no-op rather than an error.
    async fn revoke(
        &self,
        context: &RequestContext,
        params: RevokeDrawer,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        let actor = core.resolve(context, params.capability).await?;
        core.authorize(context, &actor, &params.drawer_id, Access::Admin)
            .await?;
        for access in Access::ALL {
            core.revoke(
                context,
                params.subject_id,
                drawer_object(access, &params.drawer_id),
            )
            .await?;
        }
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
