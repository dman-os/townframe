//! The native test harness for nodule authors: test a [`big_ndk::Nodule`]'s handlers against a
//! real booted node, natively, without building wasm.
//!
//! [`node`] starts a builder; each registered nodule is a cartridge manifest and the Rust value that
//! serves it. [`TestNodeBuilder::boot`] boots a real in-memory `BigNode`, hosting the nodules
//! with an executor that serves them in-process in place of the wasm component, and admits their
//! cartridges. Everything else is the production path: enabling, grants, dispatch authorization and
//! the nodule's own outgoing calls (see [`executor`]). It is for tests only (cartridge authoring
//! principle 6): no node outside a test is built with it.
//!
//! ```ignore
//! let cartridge = my_cartridge_manifest(); // declares a nodule with a `handler` artifact
//! let node = big_ndk_testing::node()
//!     .with_nodule(cartridge.clone(), MyNodule::default())
//!     .boot()
//!     .await;
//! node.enable(&cartridge.id).await;
//! let reply = node.call(CreateReview { /* .. */ }).await.unwrap();
//! node.stop().await;
//! ```
//!
//! The cartridge needs a `handler` artifact entry for the node to host the nodule, but the harness
//! never reads its bytes, so a placeholder digest is fine.

#![recursion_limit = "256"]

mod executor;

#[cfg(test)]
mod smoke;

use big_cabinet::SchemaRepo;
use big_cabinet_core::authority::Authorization;
use big_cartridges_core::Cartridge;
use big_cartridges_core::CartridgeId;
use big_dispatch_core::DispatchError;
use big_dispatch_core::DispatchScope;
use big_dispatch_core::InvocationContext;
use big_dispatch_core::Invoke;
use big_node::BigNode;
use big_node::Config;
use big_node::StopToken;
use big_nodule::executor::NoduleExecutor;
use big_nodule_core::protocol::AdmitNodule;
use big_nodule_core::protocol::NoduleRecord;
use big_nodule_core::protocol::NodulesProtocol;
use big_nodule_core::protocol::SetNoduleEnabled;
use big_nodule_core::protocol::SetNoduleOperationGrants;
use executor::Hosting;
use executor::NativeExecutor;
use executor::Registered;
use method_core::Unary;
use secrets_rs::SecretStore;
use serde_json::Value;
use service_core_rs::xrpc::DOMAIN_ERROR_FIELD;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use sqlite_rs::Database;
use sqlite_rs::SqliteRuntime;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

/// Starts a [`TestNodeBuilder`].
pub fn node() -> TestNodeBuilder {
    TestNodeBuilder {
        nodules: Vec::new(),
        ceiling: DispatchScope::default(),
    }
}

pub struct TestNodeBuilder {
    nodules: Vec<(Cartridge, Box<dyn Registered>)>,
    ceiling: DispatchScope,
}

impl TestNodeBuilder {
    /// Registers a nodule: `cartridge` is its manifest, which must declare a nodule contribution with
    /// a handler, and `nodule` is what serves it. Every request is served by a clone of
    /// `nodule`, as the wasm component serves each from a fresh value, so state does not
    /// carry from one call to the next.
    pub fn with_nodule<N>(mut self, cartridge: Cartridge, nodule: N) -> Self
    where
        N: big_ndk::Nodule + Clone + Send + 'static,
    {
        self.nodules
            .push((cartridge, Box::new(Hosting(Mutex::new(nodule)))));
        self
    }

    /// Widens the ceiling of what a nodule can be granted to invoke beyond the authorities of
    /// the registered cartridges, which it always covers.
    pub fn with_operation_ceiling(mut self, ceiling: DispatchScope) -> Self {
        self.ceiling.operations.extend(ceiling.operations);
        self.ceiling.authorities.extend(ceiling.authorities);
        self
    }

    /// Boots the node and admits every registered cartridge. The nodules are not enabled: grants are
    /// set and [`TestNode::enable`] called by the test, in whatever order it is exercising.
    pub async fn boot(self) -> TestNode {
        let Self {
            nodules,
            mut ceiling,
        } = self;
        let mut cartridges = Vec::new();
        let mut registered = BTreeMap::new();
        for (cartridge, nodule) in nodules {
            ceiling
                .authorities
                .extend(cartridge.authority.iter().cloned());
            registered.insert(cartridge.id.clone(), nodule);
            cartridges.push(cartridge);
        }
        let secrets = secrets().await;
        let runtime = Arc::new(SqliteRuntime::new(Default::default()));
        let sql = runtime.ephemeral(&[]).expect("an ephemeral database");
        let config = config(
            secrets,
            sql,
            big_repo::Storage::memory(&runtime).expect("in-memory repo storage"),
            ceiling,
        );
        let (node, stop) = BigNode::boot_with(config, move |deps| async move {
            let executor = NativeExecutor::new(registered, deps.dispatch);
            let (stop, spawner) = service_utils_rs::StopToken::new();
            // The executor stops what a test left enabled when the node is stopped, after the
            // nodule service that calls it and before the services it calls.
            let stopping = Arc::clone(&executor);
            spawner
                .spawn({
                    let spawner = spawner.clone();
                    async move {
                        spawner.cancelled().await;
                        stopping.stop_all().await;
                    }
                })
                .expect("a fresh stop token accepts tasks");
            Ok((executor as Arc<dyn NoduleExecutor>, stop))
        })
        .await
        .expect("booting the test node");
        let node = TestNode {
            node,
            stop,
            _runtime: runtime,
        };
        for cartridge in cartridges {
            node.nodules(AdmitNodule {
                cartridge,
                drawers: None,
            })
            .await;
        }
        node
    }
}

/// Why [`TestNode::call`] failed.
#[derive(Debug, PartialEq)]
pub enum CallError<E> {
    /// The method's own typed error, from the handler.
    Method(E),
    /// Dispatch refused or could not complete the call (denied, not ready, a provider failure
    /// that is not the method's error, ..).
    Dispatch(DispatchError),
}

/// A real in-memory node hosting the registered nodules natively. Stop it with
/// [`TestNode::stop`].
pub struct TestNode {
    node: BigNode,
    stop: StopToken,
    _runtime: Arc<SqliteRuntime>,
}

impl TestNode {
    /// The booted node, for what the wrappers here do not cover.
    pub fn big_node(&self) -> &BigNode {
        &self.node
    }

    /// Calls method `M` through dispatch as the operator, with no budget. The handler's own
    /// error comes back typed as [`CallError::Method`], as it does for a typed client of the
    /// operation; anything dispatch itself refused is [`CallError::Dispatch`].
    pub async fn call<M: Unary>(&self, input: M) -> Result<M::Output, CallError<M::Error>> {
        let output = self
            .invoke(
                M::XRPC.nsid,
                serde_json::to_value(input).expect("a method input serializes"),
                InvocationContext::default(),
                RequestContext::root(),
            )
            .await
            .map_err(|error| match error {
                // The provider's own error is the XRPC domain envelope the handler wrote.
                DispatchError::Provider { status, body } => body
                    .get(DOMAIN_ERROR_FIELD)
                    .and_then(|typed| serde_json::from_value(typed.clone()).ok())
                    .map_or(
                        CallError::Dispatch(DispatchError::Provider { status, body }),
                        CallError::Method,
                    ),
                other => CallError::Dispatch(other),
            })?;
        Ok(serde_json::from_value(output).unwrap_or_else(|error| {
            panic!("{} answered an unexpected shape: {error}", M::XRPC.nsid)
        }))
    }

    /// Invokes `operation` through dispatch as the operator, under `request` (its budget, request
    /// id and trace) and `context`.
    pub async fn invoke(
        &self,
        operation: &str,
        input: Value,
        context: InvocationContext,
        request: RequestContext,
    ) -> Result<Value, DispatchError> {
        self.node
            .dispatch
            .client
            .call_with(
                request,
                Authorization::identity(self.node.authority.root).metadata(),
                Invoke {
                    operation: operation.to_owned(),
                    input,
                    context,
                },
            )
            .await
            .map(|output| output.output)
            .map_err(|error| match error {
                ClientError::Method(error) => error,
                ClientError::Transport { message, .. } => panic!("transport failed: {message}"),
            })
    }

    /// Admits `cartridge` after boot. For a cartridge whose nodule only holds records, which no
    /// executor hosts, so [`TestNodeBuilder::with_nodule`] has nothing to register for it; a
    /// cartridge with a handler is registered there instead.
    pub async fn admit(&self, cartridge: Cartridge) -> NoduleRecord {
        self.nodules(AdmitNodule {
            cartridge,
            drawers: None,
        })
        .await
    }

    /// Enables the nodule of `cartridge_id`, which starts hosting it.
    pub async fn enable(&self, cartridge_id: &CartridgeId) -> NoduleRecord {
        self.set_enabled(cartridge_id, true).await
    }

    pub async fn set_enabled(&self, cartridge_id: &CartridgeId, enabled: bool) -> NoduleRecord {
        self.nodules(SetNoduleEnabled {
            cartridge_id: cartridge_id.clone(),
            enabled,
        })
        .await
    }

    /// Sets the operations the nodule of `cartridge_id` may invoke, which it holds as a dispatch
    /// capability minted from them. The grant is bounded by what the nodule's cartridge requested and
    /// by the node's ceiling, and the record says whether it was applied.
    pub async fn grant(
        &self,
        cartridge_id: &CartridgeId,
        operations: DispatchScope,
    ) -> NoduleRecord {
        self.nodules(SetNoduleOperationGrants {
            cartridge_id: cartridge_id.clone(),
            operations,
        })
        .await
    }

    /// Stops the node: the nodules, then the services behind them.
    pub async fn stop(self) {
        let Self { node, stop, .. } = self;
        drop(node);
        stop.stop().await.expect("stopping the test node");
    }

    async fn nodules<M>(&self, method: M) -> M::Output
    where
        M: Unary,
        M::Error: std::fmt::Debug,
        service_client_native_rs::Transport<NodulesProtocol>:
            service_client_core_rs::UnaryTransport<M>,
    {
        self.node
            .nodules
            .client
            .call_with(
                RequestContext::root(),
                Authorization::identity(self.node.authority.root).metadata(),
                method,
            )
            .await
            .expect("a nodule service call as the operator")
    }
}

// The secrets and node config below copy the private helpers of `big_node`'s e2e module
// (`src/big_node/e2e/mod.rs`): the duplication is accepted and logged in the nodule runtime
// ledger.

async fn secrets() -> Arc<SecretStore> {
    let keyring = keyring_core::mock::Store::new().unwrap() as Arc<keyring_core::CredentialStore>;
    Arc::new(SecretStore::boot_with_store(keyring).await.unwrap())
}

fn config(
    secrets: Arc<SecretStore>,
    sql: Database,
    storage: big_repo::Storage,
    nodule_operations: DispatchScope,
) -> Config {
    Config {
        repo: big_repo::Config {
            node_identity_seed: [47; 32],
            storage,
            scope_key: "big-ndk-testing".into(),
            hidden_parts: Default::default(),
            automerge_frontier_group_scope: Default::default(),
            causal_checkpoint_group_scope: Default::default(),
            group_part_group_scope: Default::default(),
        },
        sql,
        schemas: Arc::new(SchemaRepo::open([]).expect("an empty schema repo")),
        secrets,
        index_concurrency: NonZeroUsize::new(2).unwrap(),
        blob_storage: big_blobs::Storage::memory().unwrap(),
        nodule_operations,
        listeners: Default::default(),
        peers: None,
    }
}
