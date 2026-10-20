//! Who may read, write, and create documents.
//!
//! DocumentsRepo does not know what a drawer is. It asks a [`DocumentAccess`] policy, owned above
//! it, whether an actor may act on a document, and hands that policy the document it created so the
//! policy can place it (for DrawerRepo: enter it in the named drawer and grant node authority).
//! Containment — "doc X is in drawer D, so write on D covers X" — is the policy's decision, because
//! the policy is the one that named D.
//!
//! [`LedgerAccess`] is the policy this crate ships: standing grants in the authority ledger on
//! objects DocumentsRepo names itself (`read document <id>`, `write document <id>`,
//! `create document in <placement>`). It is a complete policy for an embedding without drawers and
//! for tests, not a stand-in.

use crate::authority::{
    AuthorityProtocol, AuthorityServiceError, CapId, CheckGrant, GrantAccess, Rights,
};
use big_cabinet_core::dmeta::{DocumentId, Uuid7};
use service_utils_rs::{Client, ClientError, RequestContext};
use std::future::Future;
use std::pin::Pin;

pub type AccessFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, AccessError>> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessRequest<'a> {
    Read(&'a DocumentId),
    Write(&'a DocumentId),
    Create { placement: &'a str },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AccessError {
    #[error("access policy is unavailable: {0}")]
    Unavailable(String),
    #[error("access policy refused: {0}")]
    Refused(String),
}

pub trait DocumentAccess: Send + Sync + 'static {
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
        actor: Uuid7,
        document: &'a DocumentId,
        placement: &'a str,
    ) -> AccessFuture<'a, ()>;
}

pub struct LedgerAccess {
    authority: Client<AuthorityProtocol>,
    issuer: CapId,
}

impl LedgerAccess {
    /// `issuer` is DocumentsRepo's own actor capability: the ledger answers grant checks only to
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

impl DocumentAccess for LedgerAccess {
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
                        subject: actor,
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
        actor: Uuid7,
        document: &'a DocumentId,
        _placement: &'a str,
    ) -> AccessFuture<'a, ()> {
        Box::pin(async move {
            for request in [
                AccessRequest::Read(document),
                AccessRequest::Write(document),
            ] {
                self.authority
                    .call(
                        context.child(),
                        GrantAccess {
                            via: self.issuer,
                            subject: actor,
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
