//! GrantAccess: record a grant in an issuing actor's namespace.

use super::operation;
use super::store::GrantRow;
use super::*;

/// Record that `subject` may hold `rights` over `object` in the namespace of
/// the actor that names it. Needs `delegate` on `via`:
///
/// - through the issuer's own actor capability, any object it may name, with
///   rights inside its capability's object rights; the grant stands until the
///   issuer revokes it;
/// - through an object capability, exactly that capability's object, with
///   rights inside its rights; the grant stands while `via` stays live, so
///   retracting the capability a grant was written through withdraws it.
///
/// Granting again through the same `via` replaces the rights it recorded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrantAccess {
    pub via: CapId,
    pub subject: Uuid7,
    pub object: String,
    pub rights: Rights,
}

operation!(GrantAccess => (), procedure, "grantAccess", "Void");

impl AuthorityService {
    async fn grant(&self, request: GrantAccess) -> Result<(), AuthorityError> {
        let mut transaction = self.write().await?;
        let via = cap::walk(&mut transaction, &request.via).await?;
        let (issuer, granted_via) = self
            .grant_scope(&mut transaction, &via, &request.object)
            .await?;
        via.bound(request.rights)?;
        if !store::actor_exists(&mut transaction, request.subject).await? {
            return Err(AuthorityError::ActorNotFound);
        }
        store::put_grant(
            &mut transaction,
            issuer,
            request.subject,
            &request.object,
            &GrantRow {
                via: granted_via,
                rights: request.rights,
            },
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

impl Handler<GrantAccess> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<GrantAccess>,
    ) -> Result<ResponseEnvelope<()>, AuthorityServiceError> {
        Ok(self.grant(request.params).await?.into())
    }
}
