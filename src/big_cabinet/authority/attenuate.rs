//! AttenuateCapability: derive a narrower capability over the same object.

use super::*;

impl AuthorityService {
    async fn attenuate(&self, request: AttenuateCapability) -> Result<Capability, AuthorityError> {
        let mut state = self.state.lock().await;
        let held = cap::walk(&state, &request.capability)?;
        held.require(Rights::NARROW)?;
        held.bound(request.rights)?;
        let child = state.child(
            held.cap.object.clone(),
            request.rights,
            held.cap.issuer,
            held.cap.kind,
            [request.capability],
        )?;
        let capability = child.wire();
        state.insert(child);
        Ok(capability)
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
