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
use big_dispatch_core::Body;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::NotReadyReason;
use big_dispatch_core::Readiness;
use big_dispatch_core::Route;

use method_core::Encoding;
use method_lexicon::MethodContract;
use serde_json::Value;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use tokio::sync::watch;

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
    /// The scope of the minted handle presented, `None` for the operator's root capability.
    pub scope: Option<DispatchScope>,
}

/// A native service registered as the provider of operations. Its input and output are what
/// dispatch exchanges with its caller: JSON, the edge a dynamic by-NSID call pays anyway, or a
/// byte stream for a method that declares a byte encoding.
#[async_trait]
pub trait Provider: Send + Sync {
    /// Runs `operation`. One provider serves all of a nodule's operations, so the NSID arrives
    /// per call (a Wasm provider builds `/xrpc/<operation>` from it). `request` is the
    /// caller's request context: its remaining budget, request id and trace travel on to
    /// whatever the provider calls.
    ///
    /// `input` is the JSON the method's typed request is built from: the query's parameters, a
    /// JSON procedure's body, or the parameters of a method whose body is bytes. `body` is that
    /// byte body, present exactly when the method's input encoding is bytes (dispatch has
    /// checked), and the stream the provider reads. The answer is a [`Body`] of the case the
    /// method's output encoding declares, which dispatch checks.
    async fn call(
        &self,
        caller: &Caller,
        operation: &str,
        request: &RequestContext,
        context: &InvocationContext,
        input: Value,
        body: Option<Body>,
    ) -> Result<Body, ProviderError>;
}

/// How a ready provider is reached. A kind is only constructible once its provider can be
/// called, which is why [`ProviderState::Ready`] carries it instead of an entry holding a kind
/// beside a readiness flag.
#[derive(Clone)]
pub enum ProviderKind {
    /// Native code registered in-process.
    BuiltIn(Arc<dyn Provider>),
    /// A nodule handler hosted by an executor. `kind` is the executor's provider type tag
    /// (`NoduleExecutor::kind` in `big_nodule`), which dispatch needs for type-dependent grants.
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

/// The Lexicon files a method's contract is compiled from: the method's own, and the files it
/// may reference (a nodule's cartridge carries copies of every Lexicon it depends on, ADR 028,
/// so datum-typed fields resolve).
#[derive(Debug, Clone, PartialEq)]
pub struct LexiconSource {
    pub file: Value,
    pub carried: Vec<Value>,
}

/// What a provider claims for one NSID: its kind and, when the provider is ready and the method's
/// Lexicon is known, the Lexicon source its calls are validated against. Without a source the
/// call is passed through unchecked: a foreign operation whose contract no Lexicon here states
/// (ADR 027 §2), or a provider that is not serving, which is never called.
#[derive(Debug, Clone, PartialEq)]
pub struct Operation {
    pub kind: Option<OperationKind>,
    pub lexicon: Option<LexiconSource>,
    /// The method's declaration says it is privileged (`XrpcMethod::privileged`): a scope
    /// without `DispatchScope::privileged` cannot invoke it. Only a built-in declares one.
    pub privileged: bool,
    /// The method's declared body encodings, which dispatch holds every call and answer to.
    pub input: Encoding,
    pub output: Encoding,
    /// The media type a Lexicon-declared byte side names (a nodule's, which is owned data), which
    /// wins over the static one in `input`/`output`'s `Encoding::Bytes`.
    pub input_mime: Option<Arc<str>>,
    pub output_mime: Option<Arc<str>>,
    /// The method's declaration says it is local-only (`XrpcMethod::local_only`): dispatch
    /// refuses a call whose origin is not local.
    pub local_only: bool,
}

impl Operation {
    /// An operation of `kind` with no Lexicon to check calls against, taking and answering JSON.
    pub fn unchecked(kind: Option<OperationKind>) -> Self {
        Self {
            kind,
            lexicon: None,
            privileged: false,
            input: Encoding::Json,
            output: Encoding::Json,
            input_mime: None,
            output_mime: None,
            local_only: false,
        }
    }

    /// An operation of `kind` checked against the Lexicon `lexicon`, whose `input` and `output`
    /// encodings are the body encodings the operation takes and answers (`*/*` is bytes), so a
    /// provider with no compiled-in method, such as a nodule, declares byte bodies in its file.
    pub fn declared(kind: Option<OperationKind>, lexicon: LexiconSource) -> Self {
        let (input, output) = method_lexicon::body_encodings(&lexicon.file);
        Self {
            input: input.encoding,
            output: output.encoding,
            input_mime: input.mime.map(Into::into),
            output_mime: output.mime.map(Into::into),
            lexicon: Some(lexicon),
            ..Self::unchecked(kind)
        }
    }
}

/// A method's Lexicon refused by the validator compiler when its provider registered.
#[derive(Debug, thiserror::Error)]
#[error("the Lexicon of `{nsid}` does not compile: {source}")]
pub struct ContractError {
    pub nsid: String,
    #[source]
    pub source: big_cabinet_core::datum::LexiconAdmissionError,
}

/// A compiled method contract with the source it came from, so re-registering an unchanged claim
/// is recognised without compiling again.
pub struct Contract {
    source: LexiconSource,
    compiled: MethodContract,
}

impl Contract {
    pub fn compiled(&self) -> &MethodContract {
        &self.compiled
    }
}

impl PartialEq for Contract {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl fmt::Debug for Contract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Contract({})", self.source.file["id"])
    }
}

/// One provider's claim on one operation. `operation_kind` is `None` when the contract is
/// foreign and unresolved (ADR 027 §2). `contract` is compiled once at registration, so a call
/// is checked without touching the Lexicon files.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderEntry {
    pub provider_id: String,
    pub operation_kind: Option<OperationKind>,
    pub privileged: bool,
    pub input: Encoding,
    pub output: Encoding,
    pub input_mime: Option<Arc<str>>,
    pub output_mime: Option<Arc<str>>,
    pub local_only: bool,
    pub state: ProviderState,
    pub contract: Option<Arc<Contract>>,
}

#[derive(Default)]
struct Table {
    /// Claims per NSID, sorted by provider id. More than one is an ambiguity.
    routes: BTreeMap<String, Vec<Arc<ProviderEntry>>>,
    /// Reverse index: the NSIDs each provider currently claims.
    providers: BTreeMap<String, Vec<String>>,
    /// The operator's routes (ADR 045): an exact NSID or an authority prefix, mapped to the id of
    /// the provider that serves it. Provider ids are opaque here, so a built-in and a nodule are
    /// routed alike.
    routes_to: BTreeMap<String, Route>,
}

pub struct Router {
    table: RwLock<Arc<Table>>,
    /// The claims revision: bumped by every swap that changes what a provider claims, so a
    /// follower that reads claims (the nodule service resolving its requests against them)
    /// learns when its answer may be stale. A watch coalesces: a burst of registrations is one
    /// notification, and a follower busy with the last one sees a single pending change.
    /// `None` once [`Self::retire_claims`] closed it.
    claims: Mutex<Option<watch::Sender<u64>>>,
}

impl Default for Router {
    fn default() -> Self {
        Self {
            table: RwLock::default(),
            claims: Mutex::new(Some(watch::channel(0).0)),
        }
    }
}

impl Router {
    /// Follows the claims revision. Every change of what a provider claims (a registration that
    /// swaps the table, an unbind that removes claims) moves it; routes do not, they are the
    /// follower's own to set. A receiver that sees the sender closed has been told the node is
    /// coming down ([`Self::retire_claims`]) and stops following.
    ///
    /// # Panics
    ///
    /// When the claims were already retired: nothing starts following a router that is down.
    pub fn follow_claims(&self) -> watch::Receiver<u64> {
        self.claims
            .lock()
            .expect("claims signal lock")
            .as_ref()
            .expect("claims are followed only while the router is live")
            .subscribe()
    }

    /// Ends claims notification for good: followers' receivers close. Registrations and unbinds
    /// still apply but no longer say so. Called when the node begins to stop, before the
    /// providers it unbinds, so a follower does not re-resolve against claims that are being
    /// withdrawn while the services behind them are stopping. Idempotent.
    pub fn retire_claims(&self) {
        self.claims.lock().expect("claims signal lock").take();
    }

    fn snapshot(&self) -> Arc<Table> {
        Arc::clone(&self.table.read().expect("routing table lock"))
    }

    /// The provider serving `nsid`: unknown when nothing claims it, not ready when the claim is
    /// ambiguous or its provider is not serving. Ambiguity is never resolved by order.
    ///
    /// The longest route matching `nsid` (an exact NSID, else the longest authority prefix, whole
    /// segments only) decides first: its provider's claim is returned whatever its state, and a
    /// target that does not claim `nsid` is `RouteTargetMissing`, never a fall back to another
    /// claimant. Without a matching route the claims decide.
    pub fn resolve(&self, nsid: &str) -> Result<Arc<ProviderEntry>, DispatchError> {
        let table = self.snapshot();
        match DispatchScope::route_target(&table.routes_to, nsid) {
            Some(Route::Invalid(reason)) => {
                return Err(DispatchError::NotReady {
                    reason: NotReadyReason::RouteInvalid {
                        reason: reason.clone(),
                    },
                });
            }
            Some(Route::Provider(provider)) => {
                return table
                    .routes
                    .get(nsid)
                    .and_then(|claims| claims.iter().find(|entry| entry.provider_id == *provider))
                    .map(Arc::clone)
                    .ok_or_else(|| DispatchError::NotReady {
                        reason: NotReadyReason::RouteTargetMissing {
                            provider: provider.clone(),
                        },
                    });
            }
            None => {}
        }
        match table.routes.get(nsid).map(Vec::as_slice) {
            None | Some([]) => Err(DispatchError::UnknownOperation),
            Some([entry]) => Ok(Arc::clone(entry)),
            Some(_) => Err(DispatchError::NotReady {
                reason: NotReadyReason::Ambiguous,
            }),
        }
    }

    /// The ids of every provider claiming `nsid`, in order: what a request for that contract
    /// can be resolved to, ready or not.
    pub fn claimants(&self, nsid: &str) -> Vec<String> {
        self.snapshot()
            .routes
            .get(nsid)
            .map(|entries| {
                entries
                    .iter()
                    .map(|entry| entry.provider_id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Replaces every route with `routes` (pattern to provider id) in one swap; setting what is
    /// already set swaps nothing. Routes are independent of claims: one may name a provider
    /// that has not registered yet, which callers see as `RouteTargetMissing` until it does.
    pub fn set_routes(&self, routes: BTreeMap<String, Route>) {
        let mut slot = self.table.write().expect("routing table lock");
        if slot.routes_to == routes {
            return;
        }
        *slot = Arc::new(Table {
            routes: slot.routes.clone(),
            providers: slot.providers.clone(),
            routes_to: routes,
        });
    }

    /// The routes in force, pattern to provider id.
    pub fn routes(&self) -> BTreeMap<String, Route> {
        self.snapshot().routes_to.clone()
    }

    /// The NSIDs `provider_id` currently claims, in order.
    pub fn operations_of(&self, provider_id: &str) -> Vec<String> {
        self.snapshot()
            .providers
            .get(provider_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Replace everything `provider_id` claims with `operations`, all in `state`, in one swap.
    /// Registering what is already registered swaps nothing. A ready provider's Lexicons are
    /// compiled here, once, outside the table lock; if one does not compile nothing changes.
    pub fn register(
        &self,
        provider_id: &str,
        operations: impl IntoIterator<Item = (String, Operation)>,
        state: ProviderState,
    ) -> Result<(), ContractError> {
        assert!(
            !matches!(&state, ProviderState::NotReady(readiness) if readiness.is_ready()),
            "a provider that is ready is registered with the way to call it"
        );
        let operations: BTreeMap<String, Operation> = operations.into_iter().collect();
        if self.is_registered(provider_id, &operations, &state) {
            return Ok(());
        }
        let mut contracts = BTreeMap::new();
        if matches!(state, ProviderState::Ready(_)) {
            for (nsid, operation) in &operations {
                let Some(source) = &operation.lexicon else {
                    continue;
                };
                // The node's own type libraries resolve for every operation, a nodule's or a
                // built-in's, whatever the provider carried: a provider's copy of one is
                // replaced by the node's (admission refused a differing one), so the library is
                // carried exactly once.
                let libraries = big_cabinet_core::reference::node_type_libraries();
                let carried: Vec<&Value> = source
                    .carried
                    .iter()
                    .filter(|file| !libraries.iter().any(|library| library["id"] == file["id"]))
                    .chain(libraries)
                    .collect();
                let compiled =
                    MethodContract::compile(&source.file, &carried).map_err(|source| {
                        ContractError {
                            nsid: nsid.clone(),
                            source,
                        }
                    })?;
                assert_eq!(
                    Some(compiled.kind()),
                    operation.kind,
                    "`{nsid}` is claimed as a different kind than its Lexicon declares"
                );
                contracts.insert(
                    nsid.clone(),
                    Arc::new(Contract {
                        source: source.clone(),
                        compiled,
                    }),
                );
            }
        }
        let mut slot = self.table.write().expect("routing table lock");
        let current = &**slot;
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
            for (nsid, operation) in &operations {
                let claims = routes.entry(nsid.clone()).or_default();
                let at = claims.partition_point(|entry| entry.provider_id.as_str() < provider_id);
                claims.insert(
                    at,
                    Arc::new(ProviderEntry {
                        provider_id: provider_id.to_owned(),
                        operation_kind: operation.kind,
                        privileged: operation.privileged,
                        input: operation.input,
                        output: operation.output,
                        input_mime: operation.input_mime.clone(),
                        output_mime: operation.output_mime.clone(),
                        local_only: operation.local_only,
                        state: state.clone(),
                        contract: contracts.remove(nsid),
                    }),
                );
            }
            providers.insert(provider_id.to_owned(), operations.into_keys().collect());
        }
        *slot = Arc::new(Table {
            routes,
            providers,
            routes_to: current.routes_to.clone(),
        });
        if let Some(claims) = self.claims.lock().expect("claims signal lock").as_ref() {
            claims.send_modify(|revision| *revision += 1);
        }
        Ok(())
    }

    /// Whether `provider_id` already claims exactly `operations` in `state`.
    fn is_registered(
        &self,
        provider_id: &str,
        operations: &BTreeMap<String, Operation>,
        state: &ProviderState,
    ) -> bool {
        let table = self.snapshot();
        match table.providers.get(provider_id) {
            None => operations.is_empty(),
            Some(claimed) => {
                claimed.len() == operations.len()
                    && claimed.iter().all(|nsid| {
                        let held = table.routes[nsid]
                            .iter()
                            .find(|entry| entry.provider_id == provider_id)
                            .expect("reverse index names a claimed route");
                        operations.get(nsid).is_some_and(|operation| {
                            operation.kind == held.operation_kind
                                && operation.privileged == held.privileged
                                && operation.input == held.input
                                && operation.output == held.output
                                && operation.input_mime == held.input_mime
                                && operation.output_mime == held.output_mime
                                && operation.local_only == held.local_only
                                && *state == held.state
                                // Lexicons only matter to a provider that is called.
                                && (!matches!(state, ProviderState::Ready(_))
                                    || operation.lexicon.as_ref()
                                        == held.contract.as_ref().map(|held| &held.source))
                        })
                    })
            }
        }
    }

    /// Drop everything `provider_id` claims. Unbinding an unknown provider is a no-op.
    pub fn unbind(&self, provider_id: &str) {
        self.register(provider_id, [], ProviderState::NotReady(Readiness::Refused))
            .expect("claiming nothing compiles nothing");
    }
}

#[cfg(test)]
mod tests;
