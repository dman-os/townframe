//! MintCapability: mint an object capability in an actor's namespace.

use super::operation;
use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MintCapability {
    pub issuer: CapId,
    pub object: String,
    pub rights: Rights,
}

operation!(MintCapability => Capability, procedure, "mintCapability", "Capability");

impl AuthorityService {
    async fn mint(&self, request: MintCapability) -> Result<Capability, AuthorityError> {
        let mut transaction = self.write().await?;
        let issuer = cap::walk(&mut transaction, &request.issuer).await?;
        let namespace = actor::recognize(&mut transaction, &issuer, Rights::NARROW).await?;
        let child = issuer.child(request.object, request.rights, namespace)?;
        store::insert_cap(&mut transaction, &child).await?;
        transaction.commit().await?;
        Ok(child.wire())
    }
}

impl Handler<MintCapability> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<MintCapability>,
    ) -> Result<ResponseEnvelope<Capability>, AuthorityServiceError> {
        Ok(self.mint(request.params).await?.into())
    }
}
