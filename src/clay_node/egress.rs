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
use big_nodule::egress::WorkloadCaller;
use clay_guest_session::egress::DispatchEgress;
use clay_guest_session::egress::EgressBody;
use futures::future::BoxFuture;
use service_client_native_rs::Client;
use service_core_rs::xrpc::bearer_header_value;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use std::sync::Arc;
use std::sync::Mutex;

/// What the host signs as and presents for one micro.
#[derive(Clone, Copy)]
struct Binding {
    /// The micro's own actor capability, the signer of every call, so dispatch binds the call to
    /// this actor.
    signer: CapId,
    /// The capability the host presents for the micro, or `None` for a micro with no granted
    /// methods (or none minted), whose every call is then refused for want of a credential.
    capability: Option<CapId>,
}

/// One micro's `dispatch.local` egress: calls are signed by the micro's actor and present the
/// micro's capability. Clones share the binding, so the egress a host handed to the micro's
/// incarnation follows [`Self::rebind`], and the binding is read at each call, so a re-mint
/// reaches a micro that is running.
#[derive(Clone)]
pub struct MicroEgress {
    dispatch: Client<DispatchProtocol>,
    binding: Arc<Mutex<Binding>>,
}

impl MicroEgress {
    /// The egress of a micro whose handles the node's `micros` nodule minted: `signer` is the
    /// micro's actor capability and `capability` its dispatch handle, as `micros.handles` answers
    /// them. The host mints nothing for it.
    pub fn attached(
        dispatch: Client<DispatchProtocol>,
        signer: CapId,
        capability: Option<CapId>,
    ) -> Self {
        Self {
            dispatch,
            binding: Arc::new(Mutex::new(Binding { signer, capability })),
        }
    }

    /// Gives `actor` (the micro's actor, opened by the host) a capability for exactly `scope`,
    /// minted by `minter` and bound to the actor, so dispatch refuses it under any other. A scope
    /// that permits nothing mints nothing: the micro then can call nothing, and an empty scope is
    /// not a way to say "everything".
    ///
    /// INTERIM, for a micro that is in no cartridge ([`crate::MicroInstall`]): a cartridge's micro
    /// is minted by the node ([`Self::attached`]).
    pub async fn install(
        dispatch: Client<DispatchProtocol>,
        minter: Authorization,
        actor: &OpenedActor,
        scope: DispatchScope,
    ) -> Result<Self, ClientError<DispatchError>> {
        if scope.operations.is_empty() && scope.authorities.is_empty() {
            return Ok(Self::attached(dispatch, actor.capability, None));
        }
        let minted = dispatch
            .call_with(
                RequestContext::root(),
                minter.metadata(),
                MintDispatchCapability {
                    capability: None,
                    scope,
                    rights: Rights::USE,
                    subject: Some(actor.actor.actor_id),
                },
            )
            .await?;
        Ok(Self::attached(
            dispatch,
            actor.capability,
            Some(minted.capability_id),
        ))
    }

    /// Signs as `signer` and presents `capability` from the next call on. This is how a re-mint
    /// the node made reaches a running micro: its incarnation holds this egress, not a capability.
    /// The node retracts the handles it replaced.
    pub fn rebind(&self, signer: CapId, capability: Option<CapId>) {
        *self.binding.lock().expect("binding lock") = Binding { signer, capability };
    }

    /// Presents no capability from the next call on: the micro's handles were taken away.
    pub fn unbind(&self) {
        self.binding.lock().expect("binding lock").capability = None;
    }

    /// The capability the host presents for the micro, `None` for a micro that can call nothing.
    pub fn capability(&self) -> Option<CapId> {
        self.binding.lock().expect("binding lock").capability
    }
}

impl DispatchEgress for MicroEgress {
    fn send(
        &self,
        mut request: http::Request<EgressBody>,
    ) -> BoxFuture<'static, http::Response<EgressBody>> {
        let dispatch = self.dispatch.clone();
        let Binding { signer, capability } = *self.binding.lock().expect("binding lock");
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
            // A micro is not a nodule's routine, so no refusal is refined to a missing authority.
            let caller = WorkloadCaller {
                signer,
                missing_authority: None,
            };
            let (status, body) = egress::call_http(&dispatch, Some(&caller), request).await;
            egress::http_response(status, body)
        })
    }
}
