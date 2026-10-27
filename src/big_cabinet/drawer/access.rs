//! The [`DocAccess`] policy for an embedding with drawers: a doc is reachable through the drawers
//! that list it, and a placement is a drawer.

use super::Core;
use super::METADATA_PLACEMENT;
use super::Projection;
use super::authority_error;
use super::ledger::Ledger;
use super::parse_placement;
use super::state::State;
use crate::doc::AccessError;
use crate::doc::AccessFuture;
use crate::doc::AccessRequest;
use crate::doc::DocAccess;
use big_cabinet_core::authority::AuthorityProtocol;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::authority::Namespace;
use big_cabinet_core::authority::ResolveActor;
use big_cabinet_core::dmeta::DocId;
use big_cabinet_core::dmeta::Uuid7;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::drawer::DrawerAccess as Access;
use big_cabinet_core::drawer::protocol::DrawerId;
use big_cabinet_core::drawer::protocol::DrawersError;
use big_cabinet_core::drawer::protocol::drawer_object;
use big_repo::SharedBigRepo;
use service_utils_rs::Client;
use service_utils_rs::ContextError;
use service_utils_rs::RequestContext;
use service_utils_rs::StopToken;
use sqlite_rs::Database;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub struct DrawerAccess {
    pub(crate) core: Arc<Projection>,
}

pub struct DrawerProvider {
    core: Arc<Projection>,
    receiver: mpsc::Receiver<PolicyCommand>,
}

#[must_use]
pub struct DrawerWorker {
    pub(crate) core: Arc<Core>,
    task: tokio::task::JoinHandle<()>,
    #[cfg(test)]
    tasks: service_utils_rs::Spawner,
}

#[cfg(test)]
pub(crate) struct PlacementGate {
    reached: oneshot::Sender<DocId>,
    release: oneshot::Receiver<()>,
}

#[cfg(test)]
impl DrawerWorker {
    pub(super) fn pause_placement(&self) -> (oneshot::Receiver<DocId>, oneshot::Sender<()>) {
        let (reached, placement) = oneshot::channel();
        let (release, resume) = oneshot::channel();
        assert!(
            self.core
                .placement_gate
                .lock()
                .unwrap()
                .replace(PlacementGate {
                    reached,
                    release: resume,
                })
                .is_none()
        );
        (placement, release)
    }

    pub(super) fn tasks(&self) -> service_utils_rs::Spawner {
        self.tasks.clone()
    }
}

impl Drop for DrawerWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(crate) enum PolicyOperation {
    Refresh(Option<DocId>),
    Place {
        actor: Option<Uuid7>,
        document: DocId,
        placement: String,
    },
}

pub(crate) struct PolicyCommand {
    context: RequestContext,
    operation: PolicyOperation,
    reply: oneshot::Sender<Result<(), DrawersError>>,
}

impl DrawerProvider {
    pub fn start(self, docs: Client<DocsProtocol>) -> (DrawerWorker, StopToken) {
        let core = Arc::new(Core {
            projection: self.core,
            docs,
            #[cfg(test)]
            placement_gate: std::sync::Mutex::new(None),
            #[cfg(test)]
            crash: std::sync::Mutex::new(None),
        });
        let (stop, tasks) = StopToken::new();
        let owner = tasks.clone();
        let worker_core = Arc::clone(&core);
        let mut receiver = self.receiver;
        let task = tasks
            .spawn(async move {
                loop {
                    let mut command = tokio::select! {
                        biased;
                        _ = owner.cancelled() => break,
                        command = receiver.recv() => match command {
                            Some(command) => command,
                            None => break,
                        },
                    };
                    let result = tokio::select! {
                        biased;
                        _ = owner.cancelled() => Err(ContextError::Cancelled.into()),
                        _ = command.context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
                        _ = command.context.expired() => Err(ContextError::DeadlineExceeded.into()),
                        _ = command.reply.closed() => Err(ContextError::Cancelled.into()),
                        result = worker_core.policy_command(&command.context, command.operation) => result,
                    };
                    if command.reply.send(result).is_err() {
                        command.context.cancel();
                    }
                }
            })
            .expect("drawer command worker starts before shutdown");
        (
            DrawerWorker {
                core,
                task,
                #[cfg(test)]
                tasks,
            },
            stop,
        )
    }
}

impl Projection {
    async fn command(
        &self,
        context: &RequestContext,
        operation: PolicyOperation,
    ) -> Result<(), DrawersError> {
        context.validate()?;
        let (reply, completion) = oneshot::channel();
        let command = PolicyCommand {
            context: context.child(),
            operation,
            reply,
        };
        let call = async {
            self.commands.send(command).await.map_err(|_| {
                DrawersError::DocsUnavailable("drawer command worker stopped".into())
            })?;
            completion.await.map_err(|_| {
                DrawersError::DocsUnavailable("drawer command worker stopped".into())
            })?
        };
        tokio::select! {
            biased;
            _ = context.cancellation.cancelled() => Err(ContextError::Cancelled.into()),
            _ = context.expired() => Err(ContextError::DeadlineExceeded.into()),
            result = call => result,
        }
    }

    pub(crate) async fn effective(
        &self,
        context: &RequestContext,
        actor: Uuid7,
        drawer: &DrawerId,
    ) -> Result<Option<(Access, Vec<CapId>)>, DrawersError> {
        let mut bounds = self
            .state
            .ancestors(super::state::ScopeTarget::Drawer(drawer))
            .await?;
        if bounds.len() <= 1 {
            self.command(context, PolicyOperation::Refresh(None))
                .await?;
            bounds = self
                .state
                .ancestors(super::state::ScopeTarget::Drawer(drawer))
                .await?;
        }
        bounds.insert(drawer.clone(), Access::Admin);
        self.granted(context, actor, bounds).await
    }

    pub(crate) async fn reach(
        &self,
        context: &RequestContext,
        actor: Uuid7,
        doc: &DocId,
    ) -> Result<Option<(Access, Vec<CapId>)>, DrawersError> {
        let mut bounds = self
            .state
            .ancestors(super::state::ScopeTarget::Doc(doc))
            .await?;
        if bounds.is_empty() {
            self.command(context, PolicyOperation::Refresh(Some(doc.clone())))
                .await?;
            bounds = self
                .state
                .ancestors(super::state::ScopeTarget::Doc(doc))
                .await?;
        }
        self.granted(context, actor, bounds).await
    }
}

impl Core {
    async fn policy_command(
        &self,
        context: &RequestContext,
        operation: PolicyOperation,
    ) -> Result<(), DrawersError> {
        context.validate()?;
        match operation {
            PolicyOperation::Refresh(document) => {
                self.sync_discovered(context).await?;
                if let Some(document) = document
                    && self.state.drawer(&document).await?.is_none()
                {
                    drop(self.ensure_drawer(context, &document).await?);
                }
            }
            PolicyOperation::Place {
                actor,
                document,
                placement,
            } => {
                #[cfg(test)]
                {
                    let gate = self.placement_gate.lock().unwrap().take();
                    if let Some(gate) = gate {
                        gate.reached
                            .send(document.clone())
                            .expect("placement observer remains open");
                        gate.release.await.expect("placement gate remains open");
                    }
                }
                let mut targets = Vec::with_capacity(3);
                match parse_placement(&placement) {
                    Some(drawer) => targets.push(drawer),
                    None => {
                        assert_eq!(
                            placement, METADATA_PLACEMENT,
                            "a placement that is not a drawer is refused by `check` before it is placed"
                        );
                        assert_eq!(
                            actor,
                            Some(self.actor().actor_id),
                            "only DrawerService creates with the metadata placement"
                        );
                    }
                }
                if let Some(local) = self.state.local_if_set() {
                    targets.extend(local.both().into_iter().cloned());
                }
                targets.sort();
                targets.dedup();
                for drawer in &targets {
                    self.enter(context, drawer, &document, Access::Admin)
                        .await?;
                }
            }
        }
        Ok(())
    }
}

fn access_error(error: DrawersError) -> AccessError {
    match error {
        DrawersError::Context(_)
        | DrawersError::AuthorityUnavailable(_)
        | DrawersError::DocsUnavailable(_)
        | DrawersError::Storage(_)
        | DrawersError::Keyhive(_) => AccessError::Unavailable(error.to_string()),
        other => AccessError::Refused(other.to_string()),
    }
}

impl DrawerAccess {
    /// `issuer` is DrawerService's own actor capability, carrying `use` and `delegate`. `sql` is
    /// the projection and ledger store; DocsRepo may share the same `Database`. `facet_set` is
    /// where drawer docs that arrived by sync are discovered: a doc carrying the drawer facet is
    /// a drawer this node can project without anyone having remembered it.
    pub async fn open(
        big_repo: SharedBigRepo,
        authority: Client<AuthorityProtocol>,
        issuer: CapId,
        namespace: Namespace,
        sql: Database,
        facet_set: Arc<crate::index::FacetSetStore>,
    ) -> Result<(Arc<Self>, DrawerProvider, StopToken), DrawersError> {
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
        let (commands, receiver) = mpsc::channel(64);
        let core = Arc::new(Projection {
            big_repo,
            capabilities: super::DomainCapabilities::new(
                authority,
                namespace,
                "DrawerService/scope/v2/",
            ),
            grants,
            issuer,
            namespace,
            actor,
            state,
            facet_set,
            ledger,
            commands,
        });
        let stop = core.capabilities.stop_token();
        Ok((
            Arc::new(Self {
                core: Arc::clone(&core),
            }),
            DrawerProvider { core, receiver },
            stop,
        ))
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
    fn prove<'a>(
        &'a self,
        context: &'a RequestContext,
        actor: Uuid7,
        scope: &'a big_cabinet_core::doc::DocScope,
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
            self.core
                .command(
                    context,
                    PolicyOperation::Place {
                        actor,
                        document: document.clone(),
                        placement: placement.to_owned(),
                    },
                )
                .await
                .map_err(access_error)
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

    fn scoped_docs<'a>(
        &'a self,
        _context: &'a RequestContext,
        drawers: &'a BTreeMap<DocId, Access>,
        documents: Option<&'a BTreeSet<DocId>>,
    ) -> AccessFuture<'a, BTreeMap<DocId, Access>> {
        Box::pin(async move {
            self.core
                .state
                .scoped_doc_access(drawers, documents)
                .await
                .map_err(access_error)
        })
    }

    fn subscribe_changes(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.core.state.subscribe_changes())
    }
}
