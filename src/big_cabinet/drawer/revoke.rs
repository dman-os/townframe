//! RevokeDrawer: withdraw every level a local actor holds on a drawer.

use super::*;

impl DrawerService {
    /// Every level is its own object, so all four are withdrawn; the authority service's revoke
    /// is a delete by key, so a level never granted is a no-op rather than an error.
    async fn revoke(
        &self,
        context: &RequestContext,
        params: RevokeDrawer,
    ) -> Result<(), DrawersError> {
        let core = &self.core;
        let caller = core.invocation(context, params.authorization).await?;
        core.authorize(context, &caller.authority, &params.drawer_id, Access::Admin)
            .await?;
        for access in Access::ALL {
            core.authority
                .call(
                    context.child(),
                    RevokeAccess {
                        via: core.issuer,
                        subject_id: params.subject_id,
                        object: drawer_object(access, &params.drawer_id),
                    },
                )
                .await
                .map_err(authority_error)?;
        }
        // One wake after every level is gone: notifying per level would let an observer
        // recheck between revokes and see a partial withdrawal as a hard access failure.
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
            authorization: Authorization::identity(CapId::from_bytes([1; 16])),
            drawer_id: DocId::new([2; 32]),
            subject_id: subject,
        };
        let value = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            value,
            serde_json::json!({
                "authorization": Authorization::identity(CapId::from_bytes([1; 16])),
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
