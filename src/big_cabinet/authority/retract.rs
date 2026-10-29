//! RetractCapability: retract a capability and everything derived from it.

use super::*;

impl cap::State {
    /// Retract `request.capability` on the authority of `request.via`, which must be the
    /// capability itself or one it descends from.
    pub(super) fn retract_via(
        &mut self,
        request: &RetractCapability,
    ) -> Result<(), AuthorityError> {
        cap::walk(self, &request.via)?;
        let target = cap::walk(self, &request.capability)?;
        cap::may_retract(&target, &request.via)?;
        self.retract(request.capability);
        Ok(())
    }
}

impl Handler<RetractCapability> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<RetractCapability>,
    ) -> Result<ResponseEnvelope<()>, AuthorityServiceError> {
        cap::ask(&self.ledger, |reply| {
            cap::Command::Retract(request.params, reply)
        })
        .await?;
        Ok(().into())
    }
}
