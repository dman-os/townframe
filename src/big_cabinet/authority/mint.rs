use super::*;

impl AuthorityService {
    async fn mint(&self, request: MintCapability) -> Result<Capability, AuthorityError> {
        let mut state = self.state.lock().await;
        let issuer = cap::walk(&state, &request.issuer)?;
        let namespace = issuer.namespace(Rights::NARROW)?;
        issuer.bound(request.rights)?;
        if let Some(parent) = &request.parent {
            let source = cap::walk(&state, parent)?;
            source.require(Rights::NARROW)?;
            source.bound(request.rights)?;
        }
        let child = state.child(
            request.object,
            request.rights,
            namespace,
            cap::Kind::Object,
            std::iter::once(request.issuer)
                .chain(request.parent)
                .chain(request.dependencies),
        )?;
        let capability = child.wire();
        state.insert(child);
        Ok(capability)
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
