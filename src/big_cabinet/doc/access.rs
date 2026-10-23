//! Who may read, write, and create documents.
//!
//! DocsRepo does not know what a drawer is. It asks a [`DocAccess`] policy, owned above
//! it, whether an actor may act on a document, and hands that policy the document it created so the
//! policy can place it (for DrawerService: enter it in the named drawer and the local drawers).
//! Containment — "doc X is in drawer D, so write on D covers X" — is the policy's decision, because
//! the policy is the one that named D.
//!
//! [`LedgerAccess`] is the policy this crate ships: standing grants in the authority ledger on
//! objects DocsRepo names itself (`read document <id>`, `write document <id>`,
//! `create document in <placement>`). It is a complete policy for an embedding without drawers and
//! for tests, not a stand-in.

use crate::authority::{
    AuthorityProtocol, AuthorityServiceError, CapId, CheckGrant, GrantAccess, Rights,
};
use crate::doc::{DocsError, DocsProtocol, DocsService, MintDocCapability};
use big_cabinet_core::dmeta::{DocId, Uuid7};
use service_utils_rs::{Client, ClientError, RequestContext};
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;

pub type AccessFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, AccessError>> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessRequest<'a> {
    Read(&'a DocId),
    Write(&'a DocId),
    Create { placement: &'a str },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AccessError {
    #[error("access policy is unavailable: {0}")]
    Unavailable(String),
    #[error("access policy refused: {0}")]
    Refused(String),
}

pub trait DocAccess: Send + Sync + 'static {
    /// Hand the policy a client to the booted [`DocsProtocol`] before boot recovery replays any
    /// interrupted placements.
    fn attach_docs(&self, _docs: Client<DocsProtocol>) {}

    /// Whether `actor` may perform `request`. A pure policy read: nothing is minted or recorded.
    fn check<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Uuid7,
        request: AccessRequest<'a>,
    ) -> AccessFuture<'a, bool>;

    /// Place a document `actor` just created. Called after the document's identity and initial
    /// content are durable and before the creation is acknowledged, and replayed after a crash
    /// between the two, so it must be idempotent for the same arguments.
    fn place<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Option<Uuid7>,
        document: &'a DocId,
        placement: &'a str,
    ) -> AccessFuture<'a, ()>;

    /// Every document in `drawer_id` that `actor` may read (`Read` or stronger) through that
    /// drawer, or `Ok(None)` when `actor` does not hold at least `Read` on `drawer_id`.
    fn readable_in_drawer<'a>(
        &'a self,
        _context: &'a RequestContext,
        _actor: Uuid7,
        _drawer_id: &'a DocId,
    ) -> AccessFuture<'a, Option<BTreeSet<DocId>>> {
        Box::pin(async {
            Err(AccessError::Refused(
                "drawers are not configured on this repository".into(),
            ))
        })
    }

    fn drawer_access<'a>(
        &'a self,
        _context: &'a RequestContext,
        _actor: Uuid7,
        _drawer_id: &'a DocId,
    ) -> AccessFuture<'a, Option<big_cabinet_core::drawer::DrawerAccess>> {
        Box::pin(async { Ok(None) })
    }

    fn scoped_docs<'a>(
        &'a self,
        _context: &'a RequestContext,
        _drawer_id: &'a DocId,
        _access: big_cabinet_core::drawer::DrawerAccess,
        _doc_id: Option<&'a DocId>,
    ) -> AccessFuture<'a, std::collections::BTreeMap<DocId, big_cabinet_core::drawer::DrawerAccess>>
    {
        Box::pin(async {
            Err(AccessError::Refused(
                "drawers are not configured on this repository".into(),
            ))
        })
    }

    /// A watch receiver that ticks whenever drawer membership or standing grants change, or `None`
    /// when the policy has no drawer membership changes to report.
    fn subscribe_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        None
    }
}

pub struct LedgerAccess {
    authority: Client<AuthorityProtocol>,
    issuer: CapId,
}

impl LedgerAccess {
    /// `issuer` is DocsRepo's own actor capability: the ledger answers grant checks only to
    /// the actor that named the objects, and that actor's capability must carry `use` and
    /// `delegate`.
    pub fn new(authority: Client<AuthorityProtocol>, issuer: CapId) -> Self {
        Self { authority, issuer }
    }

    pub fn object(request: AccessRequest<'_>) -> String {
        match request {
            AccessRequest::Read(document) => format!("read document {document}"),
            AccessRequest::Write(document) => format!("write document {document}"),
            AccessRequest::Create { placement } => format!("create document in {placement}"),
        }
    }
}

fn ledger_error(error: ClientError<AuthorityServiceError>) -> AccessError {
    match error {
        ClientError::Operation(error) => AccessError::Refused(error.to_string()),
        ClientError::Transport { message, .. } => AccessError::Unavailable(message),
    }
}

impl DocAccess for LedgerAccess {
    fn check<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Uuid7,
        request: AccessRequest<'a>,
    ) -> AccessFuture<'a, bool> {
        Box::pin(async move {
            let rights = self
                .authority
                .call(
                    context.child(),
                    CheckGrant {
                        via: self.issuer,
                        subject_id: actor,
                        object: Self::object(request),
                    },
                )
                .await
                .map_err(ledger_error)?;
            Ok(rights.contains(Rights::USE))
        })
    }

    fn place<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Option<Uuid7>,
        document: &'a DocId,
        _placement: &'a str,
    ) -> AccessFuture<'a, ()> {
        Box::pin(async move {
            let Some(actor) = actor else { return Ok(()); };
            for request in [
                AccessRequest::Read(document),
                AccessRequest::Write(document),
            ] {
                self.authority
                    .call(
                        context.child(),
                        GrantAccess {
                            via: self.issuer,
                            subject_id: actor,
                            object: Self::object(request),
                            rights: Rights::USE,
                        },
                    )
                    .await
                    .map_err(ledger_error)?;
            }
            Ok(())
        })
    }
}

impl DocsService {
    async fn mint_capability(
        &self,
        context: &RequestContext,
        request: MintDocCapability,
    ) -> Result<crate::authority::Capability, DocsError> {
        let caller = self.invocation(context, request.authorization).await?;
        if !caller.rights.contains(Rights::NARROW) || !request.rights.is_subset_of(caller.rights) {
            return Err(DocsError::Denied);
        }
        for doc in request.scope.read.union(&request.scope.write) {
            self.authorize(context, &caller.authority, AccessRequest::Read(doc))
                .await?;
        }
        for doc in &request.scope.write {
            self.authorize(context, &caller.authority, AccessRequest::Write(doc))
                .await?;
        }
        for placement in &request.scope.create {
            self.authorize(
                context,
                &caller.authority,
                AccessRequest::Create { placement },
            )
            .await?;
        }
        for (drawer, needed) in &request.scope.drawers {
            let held = match &caller.authority {
                crate::authority::DomainAuthority::Identity(actor) => self
                    .access
                    .drawer_access(context, *actor, drawer)
                    .await
                    .map_err(super::access_error)?,
                crate::authority::DomainAuthority::Scoped(scope) => {
                    scope.drawers.get(drawer).copied()
                }
            };
            if held < Some(*needed) {
                return Err(DocsError::Denied);
            }
        }
        self.authority
            .call(
                context.child(),
                crate::authority::MintCapability {
                    issuer: self.issuer,
                    object: format!(
                        "DocsRepo/scope/v1/{}",
                        serde_json::to_string(&request.scope).expect("document scope serializes")
                    ),
                    rights: request.rights,
                },
            )
            .await
            .map_err(super::authority_error)
    }
}

impl service_utils_rs::Handler<MintDocCapability> for DocsService {
    async fn call(
        &self,
        request: service_utils_rs::Envelope<MintDocCapability>,
    ) -> Result<service_utils_rs::ResponseEnvelope<crate::authority::Capability>, DocsError> {
        Ok(self
            .mint_capability(&request.context, request.params)
            .await?
            .into())
    }
}
