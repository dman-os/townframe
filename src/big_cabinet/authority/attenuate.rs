//! AttenuateCapability: derive a narrower capability over the same object.

use super::operation;
use super::*;

/// Derive an object capability with fewer rights and the same object and
/// issuer. Needs `narrow`. A holder can hand on less than it holds, never
/// something else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttenuateCapability {
    pub capability: CapId,
    pub rights: Rights,
}

operation!(AttenuateCapability => Capability, procedure, "attenuateCapability", "Capability");

impl AuthorityService {
    async fn attenuate(&self, request: AttenuateCapability) -> Result<Capability, AuthorityError> {
        let mut transaction = self.write().await?;
        let held = cap::walk(&mut transaction, &request.capability).await?;
        held.require(Rights::NARROW)?;
        let child = held.child(held.cap.object.clone(), request.rights, held.cap.issuer)?;
        store::insert_cap(&mut transaction, &child).await?;
        transaction.commit().await?;
        Ok(child.wire())
    }
}

impl Handler<AttenuateCapability> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<AttenuateCapability>,
    ) -> Result<ResponseEnvelope<Capability>, AuthorityServiceError> {
        Ok(self.attenuate(request.params).await?.into())
    }
}
