use super::*;

impl cap::State {
    pub(super) fn mint(&mut self, request: MintCapability) -> Result<Capability, AuthorityError> {
        let issuer = cap::walk(self, &request.issuer)?;
        let namespace = issuer.namespace(Rights::NARROW)?;
        issuer.bound(request.rights)?;
        // The handle inherits the subject of the capability it derives from: the parent, or the
        // issuer when there is none.
        let mut inherited = issuer.cap.subject;
        let mut delegator = &issuer;
        let source = match &request.parent {
            Some(parent) => {
                let source = cap::walk(self, parent)?;
                source.require(Rights::NARROW)?;
                source.bound(request.rights)?;
                Some(source)
            }
            None => None,
        };
        if let Some(source) = &source {
            inherited = source.cap.subject;
            delegator = source;
        }
        let mut dependencies: Vec<CapId> = std::iter::once(request.issuer)
            .chain(request.parent)
            .chain(request.dependencies)
            .collect();
        let subject = match request.subject {
            Some(named) if inherited != Some(named) => {
                Some(self.introduce(delegator, named, &mut dependencies)?)
            }
            _ => inherited,
        };
        let child = self.child(
            request.object,
            request.rights,
            namespace,
            cap::Kind::Object,
            subject,
            dependencies,
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
