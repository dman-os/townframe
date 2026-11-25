//! AttenuateCapability: derive a narrower capability over the same object, optionally
//! introduced to one of the holder's own descendants.

use super::*;

impl cap::State {
    pub(super) fn attenuate(
        &mut self,
        request: AttenuateCapability,
    ) -> Result<Capability, AuthorityError> {
        let held = cap::walk(self, &request.capability)?;
        held.require(Rights::NARROW)?;
        held.bound(request.rights)?;
        let mut dependencies = vec![request.capability];
        let subject = match request.subject {
            Some(named) if held.cap.subject != Some(named) => {
                Some(self.introduce(&held, named, &mut dependencies)?)
            }
            _ => held.cap.subject,
        };
        let child = self.child(
            held.cap.object.clone(),
            request.rights,
            held.cap.issuer,
            held.cap.kind,
            subject,
            dependencies,
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
