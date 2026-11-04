//! The routing table, modelled on wash-runtime's `Router`/`DynamicRouter`: an immutable
//! snapshot `NSID -> provider entries` replaced as a whole, so a lookup never blocks on a
//! registration and never sees a half-applied one.
//!
//! The swap primitive is `RwLock<Arc<Table>>`: `arc-swap` is not a workspace dependency and the
//! lock is held only to clone or replace an `Arc`, never across a build or an await. Writers
//! build the next table while holding the write lock, which also serializes them; the table is
//! small (one row per implemented operation) and registrations only follow config changes.

use async_trait::async_trait;
use big_cabinet_core::authority::Authorization;
use big_cabinet_core::datum::operation::OperationKind;
use big_cabinet_core::dmeta::Uuid7;
use big_nodule_core::dispatch::DispatchError;
use big_nodule_core::dispatch::DispatchScope;
use big_nodule_core::dispatch::InvocationContext;
use big_nodule_core::dispatch::NotReadyReason;
use big_nodule_core::inventory::Readiness;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::RwLock;

/// What a provider answers when it refuses a call, mapped by dispatch onto
/// [`DispatchError`]: a malformed input, or the provider's own XRPC error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderError {
    InvalidInput(String),
    Failed { status: u16, body: Value },
}

/// The caller as dispatch verified it, handed to the provider so it can run its own checks or
/// forward the authority (a Wasm provider carries `authorization` in the authority header).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Caller {
    /// The capability the caller presented and the signer it was presented with.
    pub authorization: Authorization,
    /// The actor that signed the call.
    pub actor: Uuid7,
    /// The scope of the minted handle presented, `None` for the operator's identity.
    pub scope: Option<DispatchScope>,
}

/// A native service registered as the provider of operations. Its input and output are the
/// JSON dispatch exchanges with its caller: the edge a dynamic by-NSID call pays anyway.
#[async_trait]
pub trait Provider: Send + Sync {
    /// Runs `operation`. One provider serves all of a nodule's operations, so the NSID arrives
    /// per call (a Wasm provider builds `/xrpc/<operation>` from it).
    async fn call(
        &self,
        caller: &Caller,
        operation: &str,
        context: &InvocationContext,
        input: Value,
    ) -> Result<Value, ProviderError>;
}

/// How a ready provider is reached. A kind is only constructible once its provider can be
/// called, which is why [`ProviderState::Ready`] carries it instead of an entry holding a kind
/// beside a readiness flag.
#[derive(Clone)]
pub enum ProviderKind {
    /// Native code registered in-process.
    BuiltIn(Arc<dyn Provider>),
    /// A nodule handler hosted by an executor. `kind` is the executor's provider type tag
    /// ([`crate::executor::NoduleExecutor::kind`]), which dispatch needs for type-dependent
    /// grants.
    Executor {
        kind: &'static str,
        provider: Arc<dyn Provider>,
    },
}

impl ProviderKind {
    /// The provider to call, whichever way it is hosted.
    pub fn provider(&self) -> &Arc<dyn Provider> {
        match self {
            Self::BuiltIn(provider) | Self::Executor { provider, .. } => provider,
        }
    }
}

impl PartialEq for ProviderKind {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::BuiltIn(a), Self::BuiltIn(b)) => Arc::ptr_eq(a, b),
            (
                Self::Executor {
                    kind: a_kind,
                    provider: a,
                },
                Self::Executor {
                    kind: b_kind,
                    provider: b,
                },
            ) => a_kind == b_kind && Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl fmt::Debug for ProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BuiltIn(_) => f.write_str("BuiltIn"),
            Self::Executor { kind, .. } => write!(f, "Executor({kind})"),
        }
    }
}

/// Whether a provider can be called now: ready with the way to reach it, or not ready with the
/// inventory reason a caller is told.
#[derive(Debug, Clone, PartialEq)]
pub enum ProviderState {
    Ready(ProviderKind),
    NotReady(Readiness),
}

impl ProviderState {
    /// The readiness a caller is told, and the nodule record reports, for this state.
    pub fn readiness(&self) -> Readiness {
        match self {
            Self::Ready(_) => Readiness::Ready,
            Self::NotReady(readiness) => *readiness,
        }
    }
}

/// One provider's claim on one operation. `operation_kind` is `None` when the contract is
/// foreign and unresolved (ADR 027 §2).
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderEntry {
    pub provider_id: String,
    pub operation_kind: Option<OperationKind>,
    pub state: ProviderState,
}

#[derive(Default)]
struct Table {
    /// Claims per NSID, sorted by provider id. More than one is an ambiguity.
    routes: BTreeMap<String, Vec<Arc<ProviderEntry>>>,
    /// Reverse index: the NSIDs each provider currently claims.
    providers: BTreeMap<String, Vec<String>>,
}

#[derive(Default)]
pub struct Router {
    table: RwLock<Arc<Table>>,
}

impl Router {
    fn snapshot(&self) -> Arc<Table> {
        Arc::clone(&self.table.read().expect("routing table lock"))
    }

    /// The provider serving `nsid`: unknown when nothing claims it, not ready when the claim is
    /// ambiguous or its provider is not serving. Ambiguity is never resolved by order.
    pub fn resolve(&self, nsid: &str) -> Result<Arc<ProviderEntry>, DispatchError> {
        let table = self.snapshot();
        match table.routes.get(nsid).map(Vec::as_slice) {
            None | Some([]) => Err(DispatchError::UnknownOperation),
            Some([entry]) => Ok(Arc::clone(entry)),
            Some(_) => Err(DispatchError::NotReady {
                reason: NotReadyReason::Ambiguous,
            }),
        }
    }

    /// The NSIDs `provider_id` currently claims, in order.
    pub fn operations_of(&self, provider_id: &str) -> Vec<String> {
        self.snapshot()
            .providers
            .get(provider_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Replace everything `provider_id` claims with `operations` (NSID to operation kind), all
    /// in `state`, in one swap. Registering what is already registered swaps nothing.
    pub fn register(
        &self,
        provider_id: &str,
        operations: impl IntoIterator<Item = (String, Option<OperationKind>)>,
        state: ProviderState,
    ) {
        assert!(
            !matches!(&state, ProviderState::NotReady(readiness) if readiness.is_ready()),
            "a provider that is ready is registered with the way to call it"
        );
        let operations: BTreeMap<String, Option<OperationKind>> = operations.into_iter().collect();
        let mut slot = self.table.write().expect("routing table lock");
        let current = &**slot;
        let unchanged = match current.providers.get(provider_id) {
            None => operations.is_empty(),
            Some(claimed) => {
                claimed.len() == operations.len()
                    && claimed.iter().all(|nsid| {
                        let held = current.routes[nsid]
                            .iter()
                            .find(|entry| entry.provider_id == provider_id)
                            .expect("reverse index names a claimed route");
                        operations
                            .get(nsid)
                            .is_some_and(|kind| *kind == held.operation_kind && state == held.state)
                    })
            }
        };
        if unchanged {
            return;
        }
        let mut routes = current.routes.clone();
        let mut providers = current.providers.clone();
        for nsid in providers.remove(provider_id).unwrap_or_default() {
            let claims = routes.get_mut(&nsid).expect("reverse index names a route");
            claims.retain(|entry| entry.provider_id != provider_id);
            if claims.is_empty() {
                routes.remove(&nsid);
            }
        }
        if !operations.is_empty() {
            for (nsid, operation_kind) in &operations {
                let claims = routes.entry(nsid.clone()).or_default();
                let at = claims.partition_point(|entry| entry.provider_id.as_str() < provider_id);
                claims.insert(
                    at,
                    Arc::new(ProviderEntry {
                        provider_id: provider_id.to_owned(),
                        operation_kind: *operation_kind,
                        state: state.clone(),
                    }),
                );
            }
            providers.insert(provider_id.to_owned(), operations.into_keys().collect());
        }
        *slot = Arc::new(Table { routes, providers });
    }

    /// Drop everything `provider_id` claims. Unbinding an unknown provider is a no-op.
    pub fn unbind(&self, provider_id: &str) {
        self.register(provider_id, [], ProviderState::NotReady(Readiness::Refused));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Echo;

    #[async_trait]
    impl Provider for Echo {
        async fn call(
            &self,
            _: &Caller,
            _: &str,
            _: &InvocationContext,
            input: Value,
        ) -> Result<Value, ProviderError> {
            Ok(input)
        }
    }

    fn ready() -> ProviderState {
        ProviderState::Ready(ProviderKind::BuiltIn(Arc::new(Echo)))
    }

    fn ops(nsids: &[&str]) -> Vec<(String, Option<OperationKind>)> {
        nsids.iter().map(|n| ((*n).to_owned(), None)).collect()
    }

    fn provider_of(router: &Router, nsid: &str) -> String {
        router.resolve(nsid).unwrap().provider_id.clone()
    }

    #[test]
    fn registered_operations_resolve_with_their_state_and_kind() {
        let router = Router::default();
        assert_eq!(
            router.resolve("a.b.c").unwrap_err(),
            DispatchError::UnknownOperation
        );
        let state = ready();
        router.register(
            "p",
            [("a.b.c".to_owned(), Some(OperationKind::Query))],
            state.clone(),
        );
        let entry = router.resolve("a.b.c").unwrap();
        assert_eq!(entry.provider_id, "p");
        assert_eq!(entry.operation_kind, Some(OperationKind::Query));
        assert_eq!(entry.state, state);
    }

    #[test]
    fn register_replaces_the_providers_entries_atomically() {
        let router = Router::default();
        router.register("p", ops(&["a.one", "a.two"]), ready());
        router.register(
            "p",
            ops(&["a.two", "a.three"]),
            ProviderState::NotReady(Readiness::Disabled),
        );
        assert_eq!(
            router.resolve("a.one").unwrap_err(),
            DispatchError::UnknownOperation
        );
        for nsid in ["a.two", "a.three"] {
            assert_eq!(
                router.resolve(nsid).unwrap().state,
                ProviderState::NotReady(Readiness::Disabled)
            );
        }
        assert_eq!(router.operations_of("p"), ["a.three", "a.two"]);
    }

    #[test]
    fn unbind_is_idempotent_and_leaves_other_providers_alone() {
        let router = Router::default();
        router.unbind("never-registered");
        router.register("p", ops(&["a.one"]), ready());
        router.register("q", ops(&["b.one"]), ready());
        router.unbind("p");
        router.unbind("p");
        assert_eq!(
            router.resolve("a.one").unwrap_err(),
            DispatchError::UnknownOperation
        );
        assert!(router.operations_of("p").is_empty());
        assert_eq!(provider_of(&router, "b.one"), "q");
    }

    #[test]
    fn two_providers_for_one_nsid_is_ambiguous_never_first_wins() {
        let router = Router::default();
        router.register("p", ops(&["x.op"]), ready());
        router.register("q", ops(&["x.op", "q.only"]), ready());
        assert_eq!(
            router.resolve("x.op").unwrap_err(),
            DispatchError::NotReady {
                reason: NotReadyReason::Ambiguous
            }
        );
        assert_eq!(provider_of(&router, "q.only"), "q");
        // Removing either claimant resolves the other, whatever order they arrived in.
        router.unbind("p");
        assert_eq!(provider_of(&router, "x.op"), "q");
        router.register("p", ops(&["x.op"]), ready());
        router.unbind("q");
        assert_eq!(provider_of(&router, "x.op"), "p");
    }

    #[test]
    fn reverse_index_tracks_exactly_what_each_provider_claims() {
        let router = Router::default();
        router.register("p", ops(&["a.two", "a.one"]), ready());
        router.register("q", ops(&["a.one"]), ready());
        assert_eq!(router.operations_of("p"), ["a.one", "a.two"]);
        assert_eq!(router.operations_of("q"), ["a.one"]);
        router.register("p", ops(&["a.two"]), ready());
        assert_eq!(router.operations_of("p"), ["a.two"]);
        assert_eq!(provider_of(&router, "a.one"), "q");
        router.register("p", [], ready());
        assert!(router.operations_of("p").is_empty());
    }

    #[test]
    fn registering_the_same_claim_again_does_not_swap_the_table() {
        let router = Router::default();
        let state = ready();
        router.register("p", ops(&["a.one"]), state.clone());
        let before = router.snapshot();
        router.register("p", ops(&["a.one"]), state);
        assert!(Arc::ptr_eq(&before, &router.snapshot()));
        router.unbind("absent");
        assert!(Arc::ptr_eq(&before, &router.snapshot()));
    }
}
