use super::*;

impl cap::State {
    pub(super) fn mint(&mut self, request: MintCapability) -> Result<Capability, AuthorityError> {
        let issuer = cap::walk(self, &request.issuer)?;
        let namespace = issuer.namespace(Rights::NARROW)?;
        issuer.bound(request.rights)?;
        if let Some(parent) = &request.parent {
            let source = cap::walk(self, parent)?;
            source.require(Rights::NARROW)?;
            source.bound(request.rights)?;
        }
        let child = self.child(
            request.object,
            request.rights,
            namespace,
            cap::Kind::Object,
            std::iter::once(request.issuer)
                .chain(request.parent)
                .chain(request.dependencies),
        )?;
        let capability = child.wire();
        self.insert(child);
        Ok(capability)
    }
}

impl Handler<MintCapability> for AuthorityService {
    async fn call(
        &self,
        request: Envelope<MintCapability>,
    ) -> Result<ResponseEnvelope<Capability>, AuthorityServiceError> {
        let minted = cap::ask(&self.ledger, |reply| {
            cap::Command::Mint(request.params, reply)
        })
        .await?;
        Ok(minted.into())
    }
}
