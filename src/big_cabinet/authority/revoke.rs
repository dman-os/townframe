//! RevokeAccess: withdraw grants in an issuing actor's namespace.

use super::operation;
use super::*;

/// Withdraw grants of `object` to `subject`. Needs `delegate` on `via`. The
/// issuer's actor capability withdraws every such grant in its namespace; an
/// object capability withdraws those written through it or through
/// capabilities derived from it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeAccess {
    pub via: CapId,
    pub subject: Uuid7,
    pub object: String,
}

operation!(RevokeAccess => (), procedure, "revokeAccess", "Void");

impl AuthorityService {
    async fn revoke(&self, request: RevokeAccess) -> Result<(), AuthorityError> {
        let mut transaction = self.write().await?;
        let via = cap::walk(&mut transaction, &request.via).await?;
        let (issuer, granted_via) = self
            .grant_scope(&mut transaction, &via, &request.object)
            .await?;
        match granted_via {
            None => {
                store::delete_grants(
                    &mut transaction,
                    issuer,
                    request.subject,
                    &request.object,
                    None,
                )
                .await?;
            }
            Some(_) => {
                let rows =
                    store::grants(&mut transaction, issuer, request.subject, &request.object)
                        .await?;
                for granted in rows.iter().filter_map(|row| row.via.as_ref()) {
                    if cap::descends_from(&mut transaction, granted, &via.cap.id).await? {
                        store::delete_grants(
                            &mut transaction,
                            issuer,
                            request.subject,
                            &request.object,
                            Some(granted),
                        )
                        .await?;
                    }
                }
            }
        }
        transaction.commit().await?;
        Ok(())
    }
}

impl Handler<RevokeAccess> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<RevokeAccess>,
    ) -> Result<ResponseEnvelope<()>, AuthorityServiceError> {
        Ok(self.revoke(request.params).await?.into())
    }
}
