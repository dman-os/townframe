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
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use service_utils_rs::StopToken;

type Error = ClientError<AuthorityServiceError>;

/// The object a domain capability carries. Who the handle is bound to is not part of it: that
/// is the capability's `subject`, which the authority records and enforces.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BoundScope<S> {
    scope: S,
    originating_actor: Option<Uuid7>,
}

pub struct Invocation<S> {
    pub authority: DomainAuthority<S>,
    /// The handle that decided this call's scope and rights (`Authorization::authority`): what a
    /// mint derives from.
    pub handle: CapId,
    /// The handle the call signs through (`Authorization::signer`, `SignDatumClaim { via }`), so
    /// the call's authority to sign is exactly that handle's.
    pub via: CapId,
    /// The public identity of the signing handle's subject, the actor `via` signs as. On a path
    /// that resolves an [`Authorization`] ([`DomainCapabilities::source`]) it is also the
    /// presenter: the actor holding the credential, which a scoped handle must be bound to.
    pub signer: Actor,
    pub rights: Rights,
    pub originating_actor: Option<Uuid7>,
}

#[derive(Clone)]
pub struct DomainCapabilities {
    authority: Client<AuthorityProtocol>,
    namespace: Namespace,
    prefix: &'static str,
}

impl DomainCapabilities {
    pub fn new(
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

    pub fn stop_token(&self) -> StopToken {
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
                    Ok(()) | Err(ClientError::Method(AuthorityServiceError::InvalidCapability)) => {
                    }
                    Err(error) => panic!("failed to retire domain namespace: {error}"),
                }
            })
            .expect("domain retirement task starts before shutdown");
        stop
    }

    /// What a handle this domain minted says, without resolving anyone: its scope and the
    /// actor it is bound to.
    fn bound<S: DeserializeOwned>(
        &self,
        cap: &Capability,
    ) -> Result<(BoundScope<S>, Uuid7), Error> {
        if cap.issuer_id != self.namespace.namespace_id || !cap.rights.contains(Rights::USE) {
            return Err(ClientError::Method(AuthorityServiceError::Unauthorized));
        }
        let object = cap
            .object
            .strip_prefix(self.prefix)
            .ok_or(ClientError::Method(AuthorityServiceError::Unauthorized))?;
        let bound: BoundScope<S> = serde_json::from_str(object)
            .map_err(|_| ClientError::Method(AuthorityServiceError::Unauthorized))?;
        let subject = cap.subject.ok_or(ClientError::Method(
            AuthorityServiceError::UnrecognizedActor,
        ))?;
        Ok((bound, subject))
    }

    /// The public identity `signing` signs as. The handle must be bound to an actor and carry
    /// `USE`, as `SignDatumClaim` will require when the signature is made. `known` is an actor
    /// the caller already resolved, reused when it is the subject.
    async fn signing_actor(
        &self,
        context: &RequestContext,
        signing: &Capability,
        known: Option<&Actor>,
    ) -> Result<Actor, Error> {
        let subject = signing.subject.ok_or(ClientError::Method(
            AuthorityServiceError::UnrecognizedActor,
        ))?;
        if !signing.rights.contains(Rights::USE) {
            return Err(ClientError::Method(AuthorityServiceError::Unauthorized));
        }
        match known {
            Some(actor) if actor.actor_id == subject => Ok(actor.clone()),
            _ => {
                self.authority
                    .call(context.child(), ResolveActor { actor_id: subject })
                    .await
            }
        }
    }

    async fn inspect(
        &self,
        context: &RequestContext,
        capability: CapId,
    ) -> Result<Capability, Error> {
        self.authority
            .call(context.child(), InspectCapability { capability })
            .await
    }

    /// A bare handle, presented with no separate signer: it is its own `handle` and signs as the
    /// actor it is bound to.
    pub async fn resolve<S: DeserializeOwned>(
        &self,
        context: &RequestContext,
        capability: CapId,
    ) -> Result<Invocation<S>, Error> {
        self.resolve_signed(context, capability, capability).await
    }

    /// A handle this domain minted, signing through `signer` instead of through itself. The
    /// handle decides scope and rights, and `signer` (live, `USE`, bound to an actor) decides
    /// whose signature the call carries, exactly as wide as the handle the caller presented.
    /// Unlike [`Self::source`] the authority must be a scoped handle, never an identity.
    pub async fn resolve_signed<S: DeserializeOwned>(
        &self,
        context: &RequestContext,
        capability: CapId,
        signer: CapId,
    ) -> Result<Invocation<S>, Error> {
        let cap = self.inspect(context, capability).await?;
        let (bound, _) = self.bound(&cap)?;
        let signing = if signer == capability {
            cap.clone()
        } else {
            self.inspect(context, signer).await?
        };
        let signer_actor = self.signing_actor(context, &signing, None).await?;
        Ok(Invocation {
            authority: DomainAuthority::Scoped(bound.scope),
            handle: capability,
            via: signer,
            signer: signer_actor,
            rights: cap.rights,
            originating_actor: bound.originating_actor,
        })
    }

    /// Resolve an [`Authorization`]: `authority` decides the scope and rights, and `signer` is
    /// the handle the call signs through. The signer must be live, carry `USE` and be bound to
    /// an actor; naming a handle there is how a service acts for an authenticated caller without
    /// any introduction, since signing is exactly as wide as the handle the caller presented.
    ///
    /// A scoped handle is its holder's: it was minted bound to one actor (its `subject`) and only
    /// that actor may present it. The presenter is the actor the `signer` handle is bound to, so a
    /// scoped `authority` whose subject is anyone else is `Unauthorized`, and a leaked handle id
    /// is a dead number under another actor's signer. An identity `authority` is nobody's handle
    /// (it may be presented under a descendant's signer), so it is not checked here.
    pub async fn source<S: DeserializeOwned>(
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
        let signing = if authorization.signer == authorization.authority {
            resolved.capability.clone()
        } else {
            self.inspect(context, authorization.signer).await?
        };
        let signer = self
            .signing_actor(context, &signing, resolved.actor.as_ref())
            .await?;
        match resolved.actor {
            Some(actor) => Ok(Invocation {
                authority: DomainAuthority::Identity(actor.actor_id),
                handle: authorization.authority,
                via: authorization.signer,
                originating_actor: Some(actor.actor_id),
                signer,
                rights: resolved.capability.rights,
            }),
            None => {
                let (bound, bound_subject) = self.bound(&resolved.capability)?;
                if bound_subject != signer.actor_id {
                    return Err(ClientError::Method(AuthorityServiceError::Unauthorized));
                }
                Ok(Invocation {
                    authority: DomainAuthority::Scoped(bound.scope),
                    handle: authorization.authority,
                    via: authorization.signer,
                    signer,
                    rights: resolved.capability.rights,
                    originating_actor: bound.originating_actor,
                })
            }
        }
    }

    /// Mint a handle for `scope` from the handle `caller` presented as its authority. The new
    /// handle is bound to `subject`, or to that handle's own subject when it is `None`. Naming
    /// another actor is an introduction, which the authority permits only for a descendant of
    /// the presented handle's subject and only when the handle carries `Rights::DELEGATE`.
    pub async fn mint<S: Serialize>(
        &self,
        context: &RequestContext,
        caller: &Invocation<S>,
        subject: Option<Uuid7>,
        scope: S,
        rights: Rights,
        dependencies: Vec<CapId>,
    ) -> Result<Capability, Error> {
        if !caller.rights.contains(Rights::NARROW) || !rights.is_subset_of(caller.rights) {
            return Err(ClientError::Method(AuthorityServiceError::Escalation));
        }
        let object = format!(
            "{}{}",
            self.prefix,
            serde_json::to_string(&BoundScope {
                scope,
                originating_actor: caller.originating_actor,
            })
            .expect("domain capability data serializes")
        );
        self.authority
            .call(
                context.child(),
                MintCapability {
                    issuer: self.namespace.capability,
                    parent: Some(caller.handle),
                    dependencies,
                    object,
                    rights,
                    subject,
                },
            )
            .await
    }

    /// Retract a handle this domain minted. The handle is its own `via`, so this needs no
    /// more authority than holding it.
    pub async fn retract(&self, context: &RequestContext, capability: CapId) -> Result<(), Error> {
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
