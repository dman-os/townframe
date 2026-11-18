//! Answers a Clay micro's `dispatch.local` requests through the node's dispatch, as the micro's
//! own actor (Clay on the node §3).
//!
//! A micro calls node methods with `wasi:http/client`, the nodule transport byte for byte. Its
//! engine admits only the `dispatch.local` authority and hands every such request to a
//! [`DispatchEgress`], which this crate implements over [`big_nodule::egress`]: the same code that
//! answers a nodule's outgoing calls, so a micro and a nodule are authorized by one path (the
//! capability must be live, permit the NSID and be bound to the signer's actor, and the call is
//! [`big_dispatch_core::Origin::Remote`]).
//!
//! The identity is the composition's, never the guest's. A [`MicroEgress`] is built for one micro
//! from the actor and capability the host holds for it; whatever `authorization` the guest set is
//! replaced, so a guest neither holds capability bytes nor can present another's.

use big_cabinet_core::authority::Authorization;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::OpenedActor;
use big_cabinet_core::authority::Rights;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchProtocol;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::MintDispatchCapability;
use big_nodule::egress;
use clay_guest_session::egress::DispatchEgress;
use clay_guest_session::egress::EgressBody;
use futures::future::BoxFuture;
use service_client_native_rs::Client;
use service_core_rs::xrpc::bearer_header_value;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use std::collections::BTreeSet;

#[cfg(test)]
mod e2e;

/// One micro's `dispatch.local` egress: calls are signed by the micro's actor and present the
/// micro's capability.
#[derive(Clone)]
pub struct MicroEgress {
    dispatch: Client<DispatchProtocol>,
    /// The micro's own actor capability, from the host's incarnation table. It is the signer of
    /// every call, so dispatch binds the call to this actor.
    signer: CapId,
    /// The capability the host presents for the micro, or `None` for a micro with no granted
    /// methods, whose every call is then refused for want of a credential.
    capability: Option<CapId>,
}

impl MicroEgress {
    /// Gives `actor` (the micro's actor, opened by the host) a capability for exactly `methods`
    /// (NSIDs), minted by `minter` and bound to the actor, so dispatch refuses it under any
    /// other. A micro granted no methods is minted nothing and can call nothing: an empty scope
    /// is not a way to say "everything".
    ///
    /// INTERIM: the permission model (per-micro `uses` intersected with the cartridge's grants,
    /// re-minted when a grant changes, Clay on the node §2) is not implemented. Until it is,
    /// the composition names the granted methods itself when it installs a micro. This
    /// constructor is the seam that model replaces: it will mint from the granted `uses`
    /// and re-mint on revocation instead of taking a fixed list.
    pub async fn install(
        dispatch: Client<DispatchProtocol>,
        minter: Authorization,
        actor: &OpenedActor,
        methods: BTreeSet<String>,
    ) -> Result<Self, ClientError<DispatchError>> {
        let capability = if methods.is_empty() {
            None
        } else {
            let minted = dispatch
                .call_with(
                    RequestContext::root(),
                    minter.metadata(),
                    MintDispatchCapability {
                        scope: DispatchScope {
                            operations: methods,
                            ..DispatchScope::default()
                        },
                        rights: Rights::USE,
                        subject: Some(actor.actor.actor_id),
                    },
                )
                .await?;
            Some(minted.capability_id)
        };
        Ok(Self {
            dispatch,
            signer: actor.capability,
            capability,
        })
    }
}

impl DispatchEgress for MicroEgress {
    fn send(
        &self,
        mut request: http::Request<EgressBody>,
    ) -> BoxFuture<'static, http::Response<EgressBody>> {
        let dispatch = self.dispatch.clone();
        let (caller, capability) = (
            egress::WorkloadCaller {
                signer: self.signer,
                missing_authority: None,
            },
            self.capability,
        );
        Box::pin(async move {
            // The engine already drops a guest's credential; the composition owns the header
            // regardless, so no engine can leak one through.
            request.headers_mut().remove(http::header::AUTHORIZATION);
            if let Some(capability) = capability {
                request.headers_mut().insert(
                    http::header::AUTHORIZATION,
                    http::HeaderValue::from_str(&bearer_header_value(&Authorization::id_text(
                        capability,
                    )))
                    .expect("a bearer capability id is a valid header value"),
                );
            }
            let (status, body) = egress::call_http(&dispatch, Some(&caller), request).await;
            egress::http_response(status, body)
        })
    }
}
