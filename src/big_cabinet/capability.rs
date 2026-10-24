use crate::authority::{
    Actor, AuthorityProtocol, AuthorityServiceError, Authorization, CapId, Capability,
    DomainAuthority, InspectCapability, MintCapability, Namespace, ResolveActor, ResolveAuthority,
    RetractCapability, Rights,
};
use big_cabinet_core::dmeta::Uuid7;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use service_utils_rs::{Client, ClientError, RequestContext, StopToken};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Error = ClientError<AuthorityServiceError>;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BoundScope<S> {
    scope: S,
    binding_id: Uuid7,
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
    bindings: Arc<Mutex<HashMap<Uuid7, Actor>>>,
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
            bindings: Arc::new(Mutex::new(HashMap::new())),
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

    fn scoped<S: DeserializeOwned>(&self, cap: Capability) -> Result<Invocation<S>, Error> {
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
            .bindings
            .lock()
            .unwrap()
            .get(&bound.binding_id)
            .cloned()
            .ok_or(ClientError::Operation(
                AuthorityServiceError::InvalidCapability,
            ))?;
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
        self.scoped(
            self.authority
                .call(context.child(), InspectCapability { capability })
                .await?,
        )
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
                let mut source = self.scoped(resolved.capability)?;
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
        let binding_id = Uuid7::new();
        let object = format!(
            "{}{}",
            self.prefix,
            serde_json::to_string(&BoundScope {
                scope,
                binding_id,
                originating_actor: caller.originating_actor,
            })
            .expect("domain capability data serializes")
        );
        assert!(
            self.bindings
                .lock()
                .unwrap()
                .insert(binding_id, caller.signer.clone())
                .is_none()
        );
        let result = self
            .authority
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
            .await;
        if result.is_err() {
            self.bindings.lock().unwrap().remove(&binding_id);
        }
        result
    }
}
