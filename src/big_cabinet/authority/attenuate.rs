//! AttenuateCapability: derive a narrower capability over the same object.

use super::*;

impl cap::State {
    pub(super) fn attenuate(
        &mut self,
        request: AttenuateCapability,
    ) -> Result<Capability, AuthorityError> {
        let held = cap::walk(self, &request.capability)?;
        held.require(Rights::NARROW)?;
        held.bound(request.rights)?;
        let child = self.child(
            held.cap.object.clone(),
            request.rights,
            held.cap.issuer,
            held.cap.kind,
            [request.capability],
        )?;
        let capability = child.wire();
        self.insert(child);
        Ok(capability)
    }
}

impl Handler<AttenuateCapability> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<AttenuateCapability>,
    ) -> Result<ResponseEnvelope<Capability>, AuthorityServiceError> {
        let attenuated = cap::ask(&self.ledger, |reply| {
            cap::Command::Attenuate(request.params, reply)
        })
        .await?;
        Ok(attenuated.into())
    }
}
