//! The [`DocAccess`] policy for an embedding with drawers: a doc is reachable through the drawers
//! that list it, and a placement is a drawer.
//!
//! Built before DocsRepo (DocsRepo is booted with it) and completed by [`super::boot`], which
//! hands it the docs client it needs to publish entries. The service and the policy are one
//! [`Core`]: the same ledger namespace, the same projection, the same actor.

use super::{
    Core, DrawerId, DrawersError, METADATA_PLACEMENT, authority_error, drawer_object,
    ledger::Ledger, parse_placement, state::State,
};
use crate::authority::{AuthorityProtocol, CapId, Namespace, ResolveActor};
use crate::doc::{AccessError, AccessFuture, AccessRequest, DocAccess, DocsProtocol};
use big_cabinet_core::dmeta::{DocId, Uuid7};
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_repo::SharedBigRepo;
use service_utils_rs::{Client, RequestContext};
use sqlx_utils_rs::SqlCtx;
use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

pub struct DrawerAccess {
    pub(crate) core: Arc<Core>,
}

fn access_error(error: DrawersError) -> AccessError {
    match error {
        DrawersError::AuthorityUnavailable(_)
        | DrawersError::DocsUnavailable(_)
        | DrawersError::Storage(_)
        | DrawersError::Keyhive(_) => AccessError::Unavailable(error.to_string()),
        other => AccessError::Refused(other.to_string()),
    }
}

impl DrawerAccess {
    /// `issuer` is DrawerService's own actor capability, carrying `use` and `delegate`. `sql` is
    /// the projection and ledger store; DocsRepo may share the same `SqlCtx`.
    pub async fn open(
        big_repo: SharedBigRepo,
        authority: Client<AuthorityProtocol>,
        issuer: CapId,
        namespace: Namespace,
        sql: SqlCtx,
    ) -> Result<Arc<Self>, DrawersError> {
        let actor = authority
            .call(RequestContext::root(), ResolveActor { capability: issuer })
            .await
            .map_err(authority_error)?;
        let state = State::open(sql.clone()).await?;
        let grants = Arc::new(
            crate::grants::Grants::open(sql.clone(), authority.clone(), namespace)
                .await
                .map_err(super::grant_error)?,
        );
        let ledger = Ledger::open(sql).await?;
        let actor_cell = OnceLock::new();
        actor_cell.set(actor).expect("fresh cell");
        Ok(Arc::new(Self {
            core: Arc::new(Core {
                big_repo,
                capabilities: super::DomainCapabilities::new(
                    authority.clone(),
                    namespace,
                    "DrawerService/scope/v2/",
                ),
                grants,
                authority,
                issuer,
                namespace,
                actor: actor_cell,
                docs: OnceLock::new(),
                state,
                ledger,
            }),
        }))
    }

    pub fn grants(&self) -> &Arc<crate::grants::Grants> {
        &self.core.grants
    }

    /// The object a standing grant on a drawer names; the embedder grants these to its actors.
    pub fn object(access: Access, drawer: &DrawerId) -> String {
        drawer_object(access, drawer)
    }
}

impl DocAccess for DrawerAccess {
    fn attach_docs(&self, docs: Client<DocsProtocol>) {
        drop(self.core.docs.set(docs));
    }

    fn prove<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Uuid7,
        scope: &'a crate::doc::DocScope,
    ) -> AccessFuture<'a, Vec<CapId>> {
        Box::pin(async move {
            if actor == self.core.actor().actor_id {
                return Ok(vec![self.core.namespace.capability]);
            }
            let mut dependencies = Vec::new();
            for (doc, needed) in scope
                .read
                .difference(&scope.write)
                .map(|doc| (doc, Access::Read))
                .chain(scope.write.iter().map(|doc| (doc, Access::Write)))
            {
                let (held, proof) = self
                    .core
                    .reach(context, actor, doc)
                    .await
                    .map_err(access_error)?
                    .ok_or_else(|| AccessError::Refused("document is not reachable".into()))?;
                if held < needed {
                    return Err(AccessError::Refused("insufficient document access".into()));
                }
                dependencies.extend(proof);
            }
            for placement in &scope.create {
                let drawer = parse_placement(placement)
                    .ok_or_else(|| AccessError::Refused("invalid drawer placement".into()))?;
                let (held, proof) = self
                    .core
                    .effective(context, actor, &drawer)
                    .await
                    .map_err(access_error)?
                    .ok_or_else(|| AccessError::Refused("placement is not reachable".into()))?;
                if held < Access::Write {
                    return Err(AccessError::Refused("insufficient placement access".into()));
                }
                dependencies.extend(proof);
            }
            for (drawer, needed) in &scope.drawers {
                let (held, proof) = self
                    .core
                    .effective(context, actor, drawer)
                    .await
                    .map_err(access_error)?
                    .ok_or_else(|| AccessError::Refused("drawer is not reachable".into()))?;
                if held < *needed {
                    return Err(AccessError::Refused("insufficient drawer access".into()));
                }
                dependencies.extend(proof);
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
            // The service's own roster writes and metadata doc creations: it checked the caller.
            if actor == self.core.actor().actor_id {
                return Ok(true);
            }
            let verdict = match request {
                AccessRequest::Read(doc) => self
                    .core
                    .reach(context, actor, doc)
                    .await
                    .map(|reached| reached.is_some_and(|(held, _)| held >= Access::Read)),
                AccessRequest::Write(doc) => self
                    .core
                    .reach(context, actor, doc)
                    .await
                    .map(|reached| reached.is_some_and(|(held, _)| held >= Access::Write)),
                AccessRequest::Create { placement } => match parse_placement(placement) {
                    Some(drawer) => self
                        .core
                        .effective(context, actor, &drawer)
                        .await
                        .map(|reached| reached.is_some_and(|(held, _)| held >= Access::Write)),
                    None => Ok(false),
                },
            };
            verdict.map_err(access_error)
        })
    }

    /// Enter the new doc in the drawer it was created into and in both local drawers, at `Admin`:
    /// a doc created in a drawer belongs to it. Idempotent — DocsRepo replays a placement a crash
    /// interrupted — because `enter` grants and publishes only what is not already there.
    fn place<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Option<Uuid7>,
        document: &'a DocId,
        placement: &'a str,
    ) -> AccessFuture<'a, ()> {
        Box::pin(async move {
            let core = &self.core;
            let mut targets = Vec::with_capacity(3);
            match parse_placement(placement) {
                Some(drawer) => targets.push(drawer),
                None => {
                    assert_eq!(
                        placement, METADATA_PLACEMENT,
                        "a placement that is not a drawer is refused by `check` before it is placed"
                    );
                    assert_eq!(
                        actor,
                        Some(core.actor().actor_id),
                        "only DrawerService creates with the metadata placement"
                    );
                }
            }
            // The local drawers' own metadata docs are created before the local drawers exist.
            if let Some(local) = core.state.local_if_set() {
                targets.extend(local.both().into_iter().cloned());
            }
            targets.sort();
            targets.dedup();
            for drawer in &targets {
                core.enter(context, drawer, document, Access::Admin)
                    .await
                    .map_err(access_error)?;
            }
            Ok(())
        })
    }

    fn readable_in_drawer<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Uuid7,
        drawer_id: &'a DocId,
    ) -> AccessFuture<'a, Option<BTreeSet<DocId>>> {
        Box::pin(async move {
            let level = if actor == self.core.actor().actor_id {
                Some(Access::Admin)
            } else {
                self.core
                    .effective(context, actor, drawer_id)
                    .await
                    .map_err(access_error)?
                    .map(|(access, _)| access)
            };
            let Some(level) = level.filter(|level| *level >= Access::Read) else {
                return Ok(None);
            };
            // The level above is this call's authorization; the projection query uses it
            // directly so a concurrent revoke cannot turn a second effective() into Denied
            // after we already observed Read.
            let docs = self
                .core
                .state
                .scoped_docs(drawer_id, level)
                .await
                .map_err(access_error)?;
            Ok(Some(
                docs.into_iter()
                    .filter_map(|(doc_id, (access, _))| (access >= Access::Read).then_some(doc_id))
                    .collect(),
            ))
        })
    }

    fn drawer_access<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Uuid7,
        drawer_id: &'a DocId,
    ) -> AccessFuture<'a, Option<Access>> {
        Box::pin(async move {
            if actor == self.core.actor().actor_id {
                return Ok(Some(Access::Admin));
            }
            self.core
                .effective(context, actor, drawer_id)
                .await
                .map(|proof| proof.map(|(access, _)| access))
                .map_err(access_error)
        })
    }

    fn scoped_docs<'a>(
        &'a self,
        _context: &'a RequestContext,
        drawer_id: &'a DocId,
        access: Access,
        doc_id: Option<&'a DocId>,
    ) -> AccessFuture<'a, std::collections::BTreeMap<DocId, Access>> {
        Box::pin(async move {
            match doc_id {
                Some(doc_id) => Ok(self
                    .core
                    .state
                    .scoped_doc(drawer_id, access, doc_id)
                    .await
                    .map_err(access_error)?
                    .map(|held| std::collections::BTreeMap::from([(doc_id.clone(), held)]))
                    .unwrap_or_default()),
                None => Ok(self
                    .core
                    .state
                    .scoped_docs(drawer_id, access)
                    .await
                    .map_err(access_error)?
                    .into_iter()
                    .map(|(doc, (held, _))| (doc, held))
                    .collect()),
            }
        })
    }

    fn subscribe_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.core.state.subscribe_changes())
    }
}
