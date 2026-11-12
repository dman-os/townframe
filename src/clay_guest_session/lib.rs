use clay_docs::DocumentAccess;
use clay_docs::DocumentError;
use clay_docs::DocumentRef;
use clay_docs::DocumentSnapshot;
use clay_docs::DocumentWatch;
use clay_docs::DatumMutation;
use clay_docs::DatumRoute;
use clay_docs::Operation as DataOperation;
use clay_docs::OperationReceipt;
use clay_docs::WatchSelection;
use clay_host::AccessError;
use clay_host::Host;
use clay_host::Instance;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::sync::watch;

#[cfg(test)]
mod tests;

pub mod capabilities;
pub mod collections;
#[cfg(feature = "test-utils")]
pub mod lifecycle_conformance;
pub mod presentation;
pub use presentation::Placement;
pub use presentation::PresentationHost;
pub use presentation::Surface;
pub use presentation::ingress_budget;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub handles: usize,
    pub pending_deliveries: usize,
}

/// Everything one guest incarnation's `micro-lifecycle.initialize` receives (ADR 030 §5), in
/// host terms. Engines grant each entry into the guest's resource table under its local
/// binding name; `requirements` are the provisions the incarnation's resolved providers
/// published, minted into recipient-local capability references for this incarnation.
#[derive(Default)]
pub struct InitialContext {
    /// Opaque, configuration-supplied CBOR.
    pub arguments: Vec<u8>,
    pub configs: BTreeMap<String, DocumentRef>,
    pub collections: BTreeMap<String, collections::BytesProjection>,
    pub registrations: BTreeMap<String, clay_host::Grant<collections::BytesRegistrations>>,
    pub requirements: BTreeMap<String, capabilities::CapabilityPublication>,
    /// The publications the package's manifest declares, by name. What `initialize` returns is
    /// admitted against this (see [`capabilities::admit_publications`]).
    pub publications: BTreeMap<String, capabilities::DeclaredPublication>,
    /// The micro this incarnation belongs to; with `publications` it lets a row the guest
    /// writes attach one of its own publications by name (see
    /// [`capabilities::CapabilitySession::own_endpoint`]). `None` where no micro identity
    /// exists (engine tests), and every such attachment is then invalid.
    pub micro: Option<clay_host::MicroId>,
    /// The supervisor's demand handle, behind the lazy references this incarnation mints from
    /// stored endpoints (see [`capabilities::CapabilitySession::mint_lazy`]). `None` where no
    /// supervisor runs (engine tests); minting a lazy reference then crashes.
    pub activator: Option<clay_host::Activator<capabilities::CapabilityPublication>>,
}

/// A guest's own refusal to initialize, as opposed to a trap.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum InitializationFailure {
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("rejected: {0}")]
    Rejected(String),
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            handles: 1024,
            pending_deliveries: 1024,
        }
    }
}

struct State {
    host: Host,
    instance: Instance,
    limits: Limits,
    handles: Mutex<usize>,
    next_watch: AtomicU64,
}

struct Lease(Arc<State>);

impl State {
    fn admit(self: &Arc<Self>) -> Result<Lease, AccessError> {
        self.host.check(&self.instance)?;
        let mut handles = self.handles.lock().unwrap();
        if *handles >= self.limits.handles {
            return Err(AccessError::Denied);
        }
        *handles += 1;
        Ok(Lease(Arc::clone(self)))
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut handles = self.0.handles.lock().unwrap();
        *handles = handles.checked_sub(1).unwrap();
    }
}

pub struct DocumentSession {
    state: Arc<State>,
    work: mpsc::Sender<Work>,
}

impl DocumentSession {
    pub fn new(
        host: Host,
        instance: Instance,
        limits: Limits,
    ) -> Result<(Self, mpsc::Receiver<Work>), AccessError> {
        host.check(&instance)?;
        let (work, incoming) = mpsc::channel(limits.pending_deliveries);
        Ok((
            Self {
                state: Arc::new(State {
                    host,
                    instance,
                    limits,
                    handles: Mutex::new(0),
                    next_watch: AtomicU64::new(0),
                }),
                work,
            },
            incoming,
        ))
    }

    pub fn grant(&self, access: DocumentAccess) -> Result<Access, AccessError> {
        Ok(Access {
            lease: self.state.admit()?,
            access,
            work: self.work.clone(),
        })
    }

    pub fn check(&self) -> Result<(), AccessError> {
        self.state.host.check(&self.state.instance)
    }

    pub fn limits(&self) -> Limits {
        self.state.limits
    }

    pub fn handle_count(&self) -> usize {
        *self.state.handles.lock().unwrap()
    }
}

pub struct Access {
    access: DocumentAccess,
    work: mpsc::Sender<Work>,
    lease: Lease,
}

impl Access {
    pub fn watch(&self, selection: WatchSelection) -> Result<Watch, AccessError> {
        let lease = self.lease.0.admit()?;
        let permit = self.work.try_reserve().map_err(|_| AccessError::Denied)?;
        let watch = self.access.watch(selection)?;
        let revision = watch.current()?.revision;
        let id = self
            .lease
            .0
            .next_watch
            .fetch_add(1, Ordering::Relaxed)
            .checked_add(1)
            .expect("document watch identity overflow");
        let (cancel, cancelled) = watch::channel(());
        permit.send(Work {
            state: Arc::clone(&self.lease.0),
            cancel: cancelled,
            source: Source::Watch {
                id,
                watch: watch.clone(),
                revision,
            },
        });
        Ok(Watch {
            lease,
            id,
            watch,
            _cancel: cancel,
        })
    }

    pub fn mutate(
        &self,
        route: DatumRoute,
        mutation: DatumMutation,
    ) -> Result<Operation, AccessError> {
        let lease = self.lease.0.admit()?;
        let permit = self.work.try_reserve().map_err(|_| AccessError::Denied)?;
        let operation = self.access.mutate(route, mutation)?;
        let (cancel, cancelled) = watch::channel(());
        permit.send(Work {
            state: Arc::clone(&self.lease.0),
            cancel: cancelled,
            source: Source::Operation {
                operation: operation.clone(),
                revision: 0,
            },
        });
        Ok(Operation {
            lease,
            operation,
            _cancel: cancel,
        })
    }
}

pub struct Watch {
    _cancel: watch::Sender<()>,
    id: u64,
    watch: DocumentWatch,
    lease: Lease,
}

impl Watch {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn current(&self) -> Result<DocumentSnapshot, AccessError> {
        self.lease.0.host.check(&self.lease.0.instance)?;
        self.watch.current()
    }

    pub fn select(&self, selection: WatchSelection) -> Result<(), AccessError> {
        self.lease.0.host.check(&self.lease.0.instance)?;
        self.watch.select(selection)
    }

    pub fn refresh(&self) -> Result<(), AccessError> {
        self.lease.0.host.check(&self.lease.0.instance)?;
        self.watch.refresh().map(|_| ())
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        self.watch.close();
    }
}

pub struct Operation {
    _cancel: watch::Sender<()>,
    operation: DataOperation,
    lease: Lease,
}

impl Operation {
    pub fn current(&self) -> Result<OperationReceipt, AccessError> {
        self.lease.0.host.check(&self.lease.0.instance)?;
        Ok(self.operation.current())
    }
}

pub struct Work {
    state: Arc<State>,
    cancel: watch::Receiver<()>,
    source: Source,
}

enum Source {
    Watch {
        id: u64,
        watch: DocumentWatch,
        revision: u64,
    },
    Operation {
        operation: DataOperation,
        revision: u64,
    },
}

#[derive(Debug, PartialEq)]
pub enum Delivery {
    Changed { id: u64, snapshot: DocumentSnapshot },
    Lagged(u64),
    Completed(OperationReceipt),
}

impl Work {
    pub async fn wait(mut self) -> Option<(Self, Delivery)> {
        let delivery = tokio::select! {
            biased;
            _ = self.state.host.await_close(&self.state.instance) => return None,
            _ = self.cancel.changed() => return None,
            delivery = self.source.next() => delivery?,
        };
        if self.released() {
            return None;
        }
        Some((self, delivery))
    }

    pub fn released(&self) -> bool {
        self.cancel.has_changed().is_err() || self.state.host.check(&self.state.instance).is_err()
    }
}

/// Outcome of one delivery attempt, reported by an engine adapter to [`drive`].
#[derive(Debug, PartialEq)]
pub enum DeliveryOutcome {
    /// The guest accepted the delivery; watch work is rescheduled.
    Delivered,
    /// The work was already released; do not reschedule it.
    Skipped,
    /// The incarnation is gone; stop driving.
    Stopped,
    /// The guest trapped, with this cause; record it as the instance's fault, close the instance
    /// and stop driving.
    Trapped(String),
}

/// Drive document deliveries into a guest until the instance closes, the work
/// channel ends, or the guest traps (ADR 018 §2: document delivery is
/// engine-neutral). The adapter's `deliver` closure performs the engine-specific
/// call — lock the incarnation, refuel, invoke — and reports the outcome; every
/// scheduling, rescheduling, and close decision lives here so both engines
/// deliver identically.
pub async fn drive(
    host: Host,
    instance: Instance,
    mut incoming: mpsc::Receiver<Work>,
    pending_limit: usize,
    mut deliver: impl FnMut(&Work, Delivery) -> DeliveryOutcome,
) {
    use futures::StreamExt;
    let mut pending = futures::stream::FuturesUnordered::new();
    loop {
        let ready = tokio::select! {
            _ = host.await_close(&instance) => return,
            work = incoming.recv(), if pending.len() < pending_limit => {
                let Some(work) = work else { return };
                pending.push(work.wait());
                continue;
            }
            ready = pending.next(), if !pending.is_empty() => ready.unwrap(),
        };
        let Some((work, delivery)) = ready else {
            continue;
        };
        let repeat = !matches!(delivery, Delivery::Completed(_));
        match deliver(&work, delivery) {
            DeliveryOutcome::Delivered => {
                if repeat {
                    pending.push(work.wait());
                }
            }
            DeliveryOutcome::Skipped => {}
            DeliveryOutcome::Stopped => return,
            DeliveryOutcome::Trapped(cause) => {
                host.record_fault(&instance, cause);
                host.close_instance(&instance).unwrap();
                return;
            }
        }
    }
}

impl Source {
    async fn next(&mut self) -> Option<Delivery> {
        Some(match self {
            Self::Watch {
                id,
                watch,
                revision,
            } => match watch.next(*revision).await {
                Ok(snapshot) => {
                    *revision = snapshot.revision;
                    Delivery::Changed { id: *id, snapshot }
                }
                Err(DocumentError::SubscriptionLagged) => {
                    *revision = match watch.current() {
                        Ok(snapshot) => snapshot.revision,
                        Err(AccessError::Revoked) => return None,
                        Err(AccessError::Denied) => {
                            panic!("issued document watch lost its authority")
                        }
                    };
                    Delivery::Lagged(*id)
                }
                Err(DocumentError::Access(AccessError::Revoked)) => return None,
                Err(DocumentError::Access(AccessError::Denied)) => {
                    panic!("issued document watch lost its authority")
                }
            },
            Self::Operation {
                operation,
                revision,
            } => {
                let receipt = operation.next(*revision).await.unwrap()?;
                *revision = receipt.revision;
                Delivery::Completed(receipt)
            }
        })
    }
}
