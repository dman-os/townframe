//! CheckGrant: the standing rights a subject holds over an object.

use super::operation;
use super::*;

/// The rights `subject` holds over `object` in the issuer's namespace: the
/// union of its standing grants, exact object match only. `via` must be the
/// issuer's own actor capability with `use`, because only the issuer
/// interprets its objects and decides what to mint from the answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckGrant {
    pub via: CapId,
    pub subject: Uuid7,
    pub object: String,
}

operation!(CheckGrant => Rights, query, "checkGrant", "Rights");

impl AuthorityService {
    async fn check_grant(&self, request: CheckGrant) -> Result<Rights, AuthorityError> {
        let mut transaction = self.read().await?;
        let via = cap::walk(&mut transaction, &request.via).await?;
        let issuer = actor::recognize(&mut transaction, &via, Rights::USE).await?;
        let mut held = Rights::empty();
        for row in store::grants(&mut transaction, issuer, request.subject, &request.object).await?
        {
            let standing = match row.via {
                None => true,
                Some(granted) => match cap::walk(&mut transaction, &granted).await {
                    Ok(_) => true,
                    // Retracting the capability a grant was written through
                    // withdraws the grant.
                    Err(AuthorityError::InvalidCapability) => false,
                    Err(error) => return Err(error),
                },
            };
            if standing {
                held = held | row.rights;
            }
        }
        Ok(held)
    }
}

impl Handler<CheckGrant> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<CheckGrant>,
    ) -> Result<ResponseEnvelope<Rights>, AuthorityServiceError> {
        Ok(self.check_grant(request.params).await?.into())
    }
}
