use crate::authority::DomainAuthority;
use big_cabinet_core::authority::Actor;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::AuthorityServiceError;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Capability;
use big_cabinet_core::authority::InspectCapability;
use big_cabinet_core::authority::MintCapability;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::ResolveActor;
use big_cabinet_core::authority::ResolveAuthority;
use big_cabinet_core::authority::RetractCapability;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::Uuid7;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::StopToken;

type Error = ClientError<AuthorityServiceError>;

/// The object a domain capability carries. `signer` is the actor capability the mint bound
/// the handle to; it is also a dependency of the handle, so a handle whose signer has been
/// retracted is itself invalid and never reaches [`DomainCapabilities::scoped`].
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BoundScope<S> {
    scope: S,
    signer: CapId,
    originating_actor: Option<Uuid7>,
}

pub(crate) struct Invocation<S> {
    pub(crate) authority: DomainAuthority<S>,
    pub(crate) signer: Actor,
    pub(crate) rights: Rights,
    pub(crate) originating_actor: Option<Uuid7>,
}

#[derive(Clone)]
pub(crate) struct DomainCapabilities {
    authority: Client<AuthorityProtocol>,
    namespace: Namespace,
    prefix: &'static str,
}

impl DomainCapabilities {
    pub(crate) fn new(
        authority: Client<AuthorityProtocol>,
        namespace: Namespace,
        prefix: &'static str,
    ) -> Self {
        Self {
            authority,
            namespace,
            prefix,
        }
    }

    pub(crate) fn stop_token(&self) -> StopToken {
        let (stop, tasks) = StopToken::new();
        let cancellation = tasks.clone();
        let authority = self.authority.clone();
        let capability = self.namespace.capability;
        tasks
            .spawn(async move {
                cancellation.cancelled().await;
                match authority
                    .call(
                        RequestContext::root(),
                        RetractCapability {
                            via: capability,
                            capability,
                        },
                    )
                    .await
                {
                    Ok(())
                    | Err(ClientError::Operation(AuthorityServiceError::InvalidCapability)) => {}
                    Err(error) => panic!("failed to retire domain namespace: {error}"),
                }
            })
            .expect("domain retirement task starts before shutdown");
        stop
    }

    async fn scoped<S: DeserializeOwned>(
        &self,
        context: &RequestContext,
        cap: Capability,
    ) -> Result<Invocation<S>, Error> {
        if cap.issuer_id != self.namespace.namespace_id || !cap.rights.contains(Rights::USE) {
            return Err(ClientError::Operation(AuthorityServiceError::Unauthorized));
        }
        let object = cap
            .object
            .strip_prefix(self.prefix)
            .ok_or(ClientError::Operation(AuthorityServiceError::Unauthorized))?;
        let bound: BoundScope<S> = serde_json::from_str(object)
            .map_err(|_| ClientError::Operation(AuthorityServiceError::Unauthorized))?;
        let signer = self
            .authority
            .call(
                context.child(),
                ResolveActor {
                    capability: bound.signer,
                },
            )
            .await?;
        Ok(Invocation {
            authority: DomainAuthority::Scoped(bound.scope),
            signer,
            rights: cap.rights,
            originating_actor: bound.originating_actor,
        })
    }

    pub(crate) async fn resolve<S: DeserializeOwned>(
        &self,
        context: &RequestContext,
        capability: CapId,
    ) -> Result<Invocation<S>, Error> {
        let cap = self
            .authority
            .call(context.child(), InspectCapability { capability })
            .await?;
        self.scoped(context, cap).await
    }

    pub(crate) async fn source<S: DeserializeOwned>(
        &self,
        context: &RequestContext,
        authorization: Authorization,
    ) -> Result<Invocation<S>, Error> {
        let resolved = self
            .authority
            .call(
                context.child(),
                ResolveAuthority {
                    capability: authorization.authority,
                },
            )
            .await?;
        let signer = self
            .authority
            .call(
                context.child(),
                ResolveActor {
                    capability: authorization.signer,
                },
            )
            .await?;
        match resolved.actor {
            Some(actor) => Ok(Invocation {
                authority: DomainAuthority::Identity(actor.actor_id),
                originating_actor: Some(actor.actor_id),
                signer,
                rights: resolved.capability.rights,
            }),
            None => {
                let mut source = self.scoped(context, resolved.capability).await?;
                source.signer = signer;
                Ok(source)
            }
        }
    }

    pub(crate) async fn mint<S: Serialize>(
        &self,
        context: &RequestContext,
        source: CapId,
        caller: &Invocation<S>,
        scope: S,
        rights: Rights,
        mut dependencies: Vec<CapId>,
    ) -> Result<Capability, Error> {
        if !caller.rights.contains(Rights::NARROW) || !rights.is_subset_of(caller.rights) {
            return Err(ClientError::Operation(AuthorityServiceError::Escalation));
        }
        dependencies.push(caller.signer.capability);
        let object = format!(
            "{}{}",
            self.prefix,
            serde_json::to_string(&BoundScope {
                scope,
                signer: caller.signer.capability,
                originating_actor: caller.originating_actor,
            })
            .expect("domain capability data serializes")
        );
        self.authority
            .call(
                context.child(),
                MintCapability {
                    issuer: self.namespace.capability,
                    parent: Some(source),
                    dependencies,
                    object,
                    rights,
                },
            )
            .await
    }

    /// Retract a handle this domain minted. The handle is its own `via`, so this needs no
    /// more authority than holding it.
    pub(crate) async fn retract(
        &self,
        context: &RequestContext,
        capability: CapId,
    ) -> Result<(), Error> {
        self.authority
            .call(
                context.child(),
                RetractCapability {
                    via: capability,
                    capability,
                },
            )
            .await
    }
}
