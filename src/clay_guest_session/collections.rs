use clay_collection::AcquireError;
use clay_collection::Change;
use clay_collection::Cursor;
use clay_collection::Page;
use clay_collection::Projection;
use clay_collection::ProjectionLease;
use clay_collection::ReadError;
use clay_collection::ReadLimits;
use clay_collection::Source;
use clay_host::AccessError;
use clay_host::Host;
use clay_host::Instance;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

pub type Bytes = Vec<u8>;
pub type BytesProjection = Projection<Bytes, Bytes, BytesSource>;
pub type BytesPage = Page<Bytes, Bytes>;

#[derive(Clone, Debug)]
pub struct BytesSource;

impl Source<Bytes, Bytes> for BytesSource {
    type Selector = Bytes;
    type Interest = ();

    fn acquire(&self, selector: Bytes) -> Result<(Bytes, ()), AcquireError> {
        Ok((selector, ()))
    }

    fn matches(&self, _selector: &Bytes, _key: &Bytes, _value: &Bytes) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectionLimits {
    pub readers: usize,
    pub selector_bytes: usize,
    pub cursor_bytes: usize,
    pub page_bytes: usize,
    pub max_transactions: u32,
    pub max_changes: u32,
    pub max_poll_timeout: Duration,
}

impl Default for CollectionLimits {
    fn default() -> Self {
        Self {
            readers: 64,
            selector_bytes: 64 * 1024,
            cursor_bytes: 56,
            page_bytes: 1024 * 1024,
            max_transactions: 256,
            max_changes: 1024,
            max_poll_timeout: Duration::from_secs(30),
        }
    }
}

struct State {
    host: Host,
    instance: Instance,
    limits: CollectionLimits,
    readers: Mutex<usize>,
}

impl State {
    fn admit(self: &Arc<Self>) -> Result<Permit, AccessError> {
        self.host.check(&self.instance)?;
        let mut readers = self.readers.lock().unwrap();
        if *readers >= self.limits.readers {
            return Err(AccessError::Denied);
        }
        *readers += 1;
        Ok(Permit(Arc::clone(self)))
    }
}

struct Permit(Arc<State>);

impl Drop for Permit {
    fn drop(&mut self) {
        let mut readers = self.0.readers.lock().unwrap();
        *readers = readers.checked_sub(1).unwrap();
    }
}

#[derive(Clone)]
pub struct CollectionSession {
    state: Arc<State>,
}

impl CollectionSession {
    pub fn new(host: Host, instance: Instance, limits: CollectionLimits) -> Self {
        Self {
            state: Arc::new(State {
                host,
                instance,
                limits,
                readers: Mutex::new(0),
            }),
        }
    }

    pub fn grant(&self, projection: BytesProjection) -> Result<CollectionReader, AccessError> {
        Ok(CollectionReader {
            _permit: Arc::new(self.state.admit()?),
            projection,
            session: self.clone(),
        })
    }

    pub fn reader_count(&self) -> usize {
        *self.state.readers.lock().unwrap()
    }
}

#[derive(Clone)]
pub struct CollectionReader {
    _permit: Arc<Permit>,
    projection: BytesProjection,
    session: CollectionSession,
}

impl CollectionReader {
    pub fn acquire(
        &self,
        selector: Bytes,
        resume: Option<Cursor>,
    ) -> Result<ProjectionReader, AcquireError> {
        let state = &self.session.state;
        state
            .host
            .check(&state.instance)
            .map_err(|error| match error {
                AccessError::Denied => AcquireError::UnauthorizedSelector,
                AccessError::Revoked => AcquireError::Revoked,
            })?;
        if selector.len() > state.limits.selector_bytes {
            return Err(AcquireError::InvalidSelector);
        }
        if resume
            .as_ref()
            .is_some_and(|cursor| cursor.0.len() > state.limits.cursor_bytes)
        {
            return Err(AcquireError::ResetRequired);
        }
        let permit = state.admit().map_err(|error| match error {
            AccessError::Denied => AcquireError::UnauthorizedSelector,
            AccessError::Revoked => AcquireError::Revoked,
        })?;
        let lease = self.projection.acquire(selector, resume)?;
        let (cancel, cancelled) = watch::channel(false);
        Ok(ProjectionReader {
            inner: Arc::new(LeaseState {
                lease: Mutex::new(Some(lease)),
                cancelled,
                cancel: Mutex::new(Some(cancel)),
                _permit: permit,
            }),
        })
    }
}

#[derive(Clone)]
pub struct ProjectionReader {
    inner: Arc<LeaseState>,
}

struct LeaseState {
    lease: Mutex<Option<ProjectionLease<Bytes, Bytes, BytesSource>>>,
    cancelled: watch::Receiver<bool>,
    cancel: Mutex<Option<watch::Sender<bool>>>,
    _permit: Permit,
}

impl Drop for LeaseState {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.get_mut().unwrap().take() {
            cancel.send(true).unwrap();
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadFailure {
    Read(ReadError),
    Busy,
    PageLimit,
}

impl From<ReadError> for ReadFailure {
    fn from(error: ReadError) -> Self {
        Self::Read(error)
    }
}

impl ProjectionReader {
    pub fn cancel(&self) {
        if let Some(cancel) = self.inner.cancel.lock().unwrap().take() {
            cancel.send(true).unwrap();
        }
    }

    pub fn poll_timeout(&self, requested: Duration) -> Duration {
        requested.min(self.inner._permit.0.limits.max_poll_timeout)
    }

    pub async fn read(
        &self,
        committed: Option<Cursor>,
        limits: ReadLimits,
        timeout: impl Future<Output = ()>,
    ) -> Result<BytesPage, ReadFailure> {
        if committed
            .as_ref()
            .is_some_and(|cursor| cursor.0.len() > self.inner._permit.0.limits.cursor_bytes)
        {
            return Err(ReadError::ResetRequired.into());
        }
        let limits = clamp_limits(limits, self.inner._permit.0.limits);
        let mut lease = self
            .inner
            .lease
            .lock()
            .unwrap()
            .take()
            .ok_or(ReadFailure::Busy)?;
        let mut cancelled = self.inner.cancelled.clone();
        enum Outcome {
            Complete(Result<BytesPage, ReadError>),
            Timeout,
            Revoked,
        }
        let outcome = {
            let read = lease.read(committed.clone(), limits);
            tokio::pin!(read);
            tokio::select! {
                result = &mut read => Outcome::Complete(result),
                _ = self.inner._permit.0.host.await_close(&self.inner._permit.0.instance) => Outcome::Revoked,
                _ = cancelled.changed() => Outcome::Revoked,
                _ = timeout => Outcome::Timeout,
            }
        };
        let result = match outcome {
            Outcome::Complete(result) => result,
            Outcome::Timeout => lease.read(committed, limits).complete_timeout(),
            Outcome::Revoked => Err(ReadError::Revoked),
        };
        *self.inner.lease.lock().unwrap() = Some(lease);
        let page = result.map_err(ReadFailure::Read)?;
        if page_bytes(&page) > self.inner._permit.0.limits.page_bytes {
            return Err(ReadFailure::PageLimit);
        }
        Ok(page)
    }
}

fn clamp_limits(limits: ReadLimits, ceiling: CollectionLimits) -> ReadLimits {
    ReadLimits {
        max_transactions: Some(
            limits
                .max_transactions
                .unwrap_or(ceiling.max_transactions)
                .min(ceiling.max_transactions),
        ),
        max_changes: Some(
            limits
                .max_changes
                .unwrap_or(ceiling.max_changes)
                .min(ceiling.max_changes),
        ),
    }
}

fn page_bytes(page: &BytesPage) -> usize {
    page.through.0.len()
        + page
            .transactions
            .iter()
            .flat_map(|transaction| &transaction.changes)
            .map(|change| match change {
                Change::Upsert {
                    key,
                    value,
                    write_context,
                    ..
                } => key.len() + value.len() + write_context.as_ref().map_or(0, Vec::len),
                Change::Remove { key, .. } => key.len(),
            })
            .sum::<usize>()
}

#[cfg(test)]
mod tests;
