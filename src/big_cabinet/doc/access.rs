//! Who may read, write, and create documents.
//!
//! DocsRepo does not know what a drawer is. It asks a [`DocAccess`] policy, owned above
//! it, whether an actor may act on a document, and hands that policy the document it created so the
//! policy can place it (for DrawerService: enter it in the named drawer and the local drawers).
//! Containment — "doc X is in drawer D, so write on D covers X" — is the policy's decision, because
//! the policy is the one that named D.
//!
//! [`LedgerAccess`] is the policy this crate ships: standing grants in the local domain ledger on
//! objects DocsRepo names itself (`read document <id>`, `write document <id>`,
//! `create document in <placement>`). It is a complete policy for an embedding without drawers and
//! for tests, not a stand-in.

use crate::authority::DomainAuthority;
use crate::doc::DocsService;
use crate::grants::GrantError;
use crate::grants::Grants;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::Rights;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::DocScope;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::MintDocCapability;
use service_utils_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

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
    fn prove<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Uuid7,
        scope: &'a DocScope,
    ) -> AccessFuture<'a, Vec<CapId>>;

    /// Whether `actor` may perform `request`.
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

    fn scoped_docs<'a>(
        &'a self,
        _context: &'a RequestContext,
        _drawers: &'a BTreeMap<DocId, big_cabinet_core::drawer::DrawerAccess>,
        _documents: Option<&'a BTreeSet<DocId>>,
    ) -> AccessFuture<'a, BTreeMap<DocId, big_cabinet_core::drawer::DrawerAccess>> {
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
    pub grants: Arc<Grants>,
}

impl LedgerAccess {
    pub async fn open(
        sql: Database,
        authority: Client<AuthorityProtocol>,
        namespace: Namespace,
    ) -> Result<Self, AccessError> {
        Ok(Self {
            grants: Arc::new(
                Grants::open(sql, authority, namespace)
                    .await
                    .map_err(ledger_error)?,
            ),
        })
    }

    pub fn object(request: AccessRequest<'_>) -> String {
        match request {
            AccessRequest::Read(document) => format!("read document {document}"),
            AccessRequest::Write(document) => format!("write document {document}"),
            AccessRequest::Create { placement } => format!("create document in {placement}"),
        }
    }
}

fn ledger_error(error: GrantError) -> AccessError {
    match error {
        GrantError::Storage(error) => AccessError::Unavailable(error.to_string()),
        GrantError::Authority(ClientError::Operation(error)) => {
            AccessError::Refused(error.to_string())
        }
        GrantError::Authority(ClientError::Transport { message, .. }) => {
            AccessError::Unavailable(message)
        }
    }
}

impl DocAccess for LedgerAccess {
    fn prove<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Uuid7,
        scope: &'a DocScope,
    ) -> AccessFuture<'a, Vec<CapId>> {
        Box::pin(async move {
            if !scope.drawers.is_empty() {
                return Err(AccessError::Refused(
                    "drawers are not configured on this repository".into(),
                ));
            }
            let objects: Vec<_> = scope
                .read
                .union(&scope.write)
                .map(|document| Self::object(AccessRequest::Read(document)))
                .chain(
                    scope
                        .write
                        .iter()
                        .map(|document| Self::object(AccessRequest::Write(document))),
                )
                .chain(
                    scope
                        .create
                        .iter()
                        .map(|placement| Self::object(AccessRequest::Create { placement })),
                )
                .collect();
            let proofs = self
                .grants
                .matching(context, actor, &objects)
                .await
                .map_err(ledger_error)?;
            let mut dependencies = Vec::new();
            for proof in proofs {
                if !proof.rights.contains(Rights::USE) {
                    return Err(AccessError::Refused("document scope is not granted".into()));
                }
                dependencies.extend(proof.dependencies);
            }
            dependencies.sort_unstable_by_key(|capability| *capability.as_bytes());
            dependencies.dedup();
            Ok(dependencies)
        })
    }

    fn check<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Uuid7,
        request: AccessRequest<'a>,
    ) -> AccessFuture<'a, bool> {
        Box::pin(async move {
            let proof = self
                .grants
                .matching(context, actor, &[Self::object(request)])
                .await
                .map_err(ledger_error)?
                .pop()
                .expect("one requested object has one grant proof");
            Ok(proof.rights.contains(Rights::USE))
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
            let Some(actor) = actor else {
                return Ok(());
            };
            for request in [
                AccessRequest::Read(document),
                AccessRequest::Write(document),
            ] {
                self.grants
                    .grant(context, actor, Self::object(request), Rights::USE, None)
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
    ) -> Result<big_cabinet_core::authority::Capability, DocsError> {
        let caller = self
            .capabilities
            .source::<big_cabinet_core::doc::DocScope>(context, request.authorization)
            .await
            .map_err(super::authority_error)?;
        if !caller.rights.contains(Rights::NARROW) || !request.rights.is_subset_of(caller.rights) {
            return Err(DocsError::Denied);
        }
        let dependencies = match &caller.authority {
            DomainAuthority::Identity(actor) => self
                .access
                .prove(context, *actor, &request.scope)
                .await
                .map_err(super::access_error)?,
            DomainAuthority::Scoped(scope) => {
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
                    if scope.drawers.get(drawer) < Some(needed) {
                        return Err(DocsError::Denied);
                    }
                }
                Vec::new()
            }
        };
        self.capabilities
            .mint(
                context,
                request.authorization.authority,
                &caller,
                request.scope,
                request.rights,
                dependencies,
            )
            .await
            .map_err(super::authority_error)
    }
}

impl service_utils_rs::Handler<MintDocCapability> for DocsService {
    async fn call(
        &self,
        request: service_utils_rs::Envelope<MintDocCapability>,
    ) -> Result<
        service_utils_rs::ResponseEnvelope<big_cabinet_core::authority::Capability>,
        DocsError,
    > {
        Ok(self
            .mint_capability(&request.context, request.params)
            .await?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use big_cabinet_core::drawer::DrawerAccess;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    struct BoundedAccess {
        drawers: BTreeMap<DocId, DrawerAccess>,
        documents: BTreeSet<DocId>,
        result: BTreeMap<DocId, DrawerAccess>,
        calls: AtomicUsize,
    }

    impl DocAccess for BoundedAccess {
        fn prove<'a>(
            &'a self,
            _context: &'a RequestContext,
            _actor: Uuid7,
            _scope: &'a DocScope,
        ) -> AccessFuture<'a, Vec<CapId>> {
            panic!("resource filtering must not prove identity grants")
        }

        fn check<'a>(
            &'a self,
            _context: &'a RequestContext,
            _actor: Uuid7,
            _request: AccessRequest<'a>,
        ) -> AccessFuture<'a, bool> {
            panic!("resource filtering must not check identity grants")
        }

        fn place<'a>(
            &'a self,
            _context: &'a RequestContext,
            _actor: Option<Uuid7>,
            _document: &'a DocId,
            _placement: &'a str,
        ) -> AccessFuture<'a, ()> {
            panic!("resource filtering must not place documents")
        }

        fn scoped_docs<'a>(
            &'a self,
            _context: &'a RequestContext,
            drawers: &'a BTreeMap<DocId, DrawerAccess>,
            documents: Option<&'a BTreeSet<DocId>>,
        ) -> AccessFuture<'a, BTreeMap<DocId, DrawerAccess>> {
            Box::pin(async move {
                assert_eq!(drawers, &self.drawers);
                assert_eq!(documents, Some(&self.documents));
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(self.result.clone())
            })
        }
    }

    #[tokio::test]
    async fn scoped_document_filter_batches_only_unresolved_candidates() {
        let [
            read,
            write,
            member,
            relay,
            denied,
            first,
            second,
            relay_root,
        ] = [1_u8, 2, 3, 4, 5, 6, 7, 8].map(|value| DocId::new([value; 32]));
        let drawers = BTreeMap::from([
            (first.clone(), DrawerAccess::Read),
            (second.clone(), DrawerAccess::Write),
        ]);
        let mut policy = BoundedAccess {
            drawers: drawers.clone(),
            documents: BTreeSet::from([member.clone(), relay.clone(), denied.clone()]),
            result: BTreeMap::from([
                (member.clone(), DrawerAccess::Write),
                (relay.clone(), DrawerAccess::Relay),
            ]),
            calls: AtomicUsize::new(0),
        };
        let mut scope = DocScope {
            read: BTreeSet::from([read.clone()]),
            write: BTreeSet::from([write.clone()]),
            drawers,
            ..Default::default()
        };
        scope.drawers.insert(relay_root, DrawerAccess::Relay);
        let authority = DomainAuthority::Scoped(scope);
        let context = RequestContext::root();
        let direct = BTreeSet::from([read.clone(), write.clone()]);
        assert_eq!(
            DocsService::allowed_docs(&policy, &context, &authority, &direct, DrawerAccess::Read)
                .await
                .unwrap(),
            direct,
        );
        let writable = BTreeSet::from([write.clone()]);
        assert_eq!(
            DocsService::allowed_docs(
                &policy,
                &context,
                &authority,
                &writable,
                DrawerAccess::Write
            )
            .await
            .unwrap(),
            writable,
        );
        assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
        let candidates =
            BTreeSet::from([read.clone(), write.clone(), member.clone(), relay, denied]);
        assert_eq!(
            DocsService::allowed_docs(
                &policy,
                &context,
                &authority,
                &candidates,
                DrawerAccess::Read
            )
            .await
            .unwrap(),
            BTreeSet::from([read.clone(), write.clone(), member.clone()]),
        );
        assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            DocsService::allowed_docs(
                &policy,
                &context,
                &DomainAuthority::Identity(Uuid7::new()),
                &candidates,
                DrawerAccess::Read,
            )
            .await,
            Err(DocsError::Denied),
        ));
        assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
        policy.drawers = BTreeMap::from([(second, DrawerAccess::Write)]);
        policy.documents = candidates.difference(&writable).cloned().collect();
        assert!(
            policy
                .result
                .insert(read.clone(), DrawerAccess::Read)
                .is_none()
        );
        assert_eq!(
            DocsService::allowed_docs(
                &policy,
                &context,
                &authority,
                &candidates,
                DrawerAccess::Write,
            )
            .await
            .unwrap(),
            BTreeSet::from([write, member]),
        );
        assert_eq!(policy.calls.load(Ordering::SeqCst), 2);
        let read_only = DomainAuthority::Scoped(DocScope {
            read: BTreeSet::from([read]),
            write: writable.clone(),
            drawers: BTreeMap::from([(first, DrawerAccess::Read)]),
            ..Default::default()
        });
        assert_eq!(
            DocsService::allowed_docs(
                &policy,
                &context,
                &read_only,
                &candidates,
                DrawerAccess::Write,
            )
            .await
            .unwrap(),
            writable,
        );
        let empty_candidates = BTreeSet::new();
        assert!(
            DocsService::allowed_docs(
                &policy,
                &context,
                &authority,
                &empty_candidates,
                DrawerAccess::Read,
            )
            .await
            .unwrap()
            .is_empty()
        );
        assert_eq!(policy.calls.load(Ordering::SeqCst), 2);
    }
}
