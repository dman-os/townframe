use clay_docs::DocumentAccess;
use clay_docs::DocumentError;
use clay_docs::DocumentSnapshot;
use clay_docs::DocumentWatch;
use clay_docs::FacetMutation;
use clay_docs::FacetRoute;
use clay_docs::Operation as DataOperation;
use clay_docs::OperationReceipt;
use clay_docs::WatchSelection;
use clay_host::AccessError;
use clay_host::Host;
use clay_host::Instance;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::sync::watch;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub handles: usize,
    pub pending_deliveries: usize,
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
        Ok(Lease(self.clone()))
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
            state: self.lease.0.clone(),
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
        route: FacetRoute,
        mutation: FacetMutation,
    ) -> Result<Operation, AccessError> {
        let lease = self.lease.0.admit()?;
        let permit = self.work.try_reserve().map_err(|_| AccessError::Denied)?;
        let operation = self.access.mutate(route, mutation)?;
        let (cancel, cancelled) = watch::channel(());
        permit.send(Work {
            state: self.lease.0.clone(),
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
