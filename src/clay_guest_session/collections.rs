use clay_collection::AcquireError;
use clay_collection::Change;
use clay_collection::Cursor;
use clay_collection::MutationError;
use clay_collection::Page;
use clay_collection::Projection;
use clay_collection::ProjectionLease;
use clay_collection::ReadError;
use clay_collection::ReadLimits;
use clay_collection::RegistrationCollection;
use clay_collection::Source;
use clay_collection::Version;
use clay_collection::WriterId;
use clay_host::AccessError;
use clay_host::Host;
use clay_host::Instance;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

pub type Bytes = Vec<u8>;
pub type BytesProjection = Projection<Bytes, Bytes, BytesSource>;
pub type BytesPage = Page<Bytes, Bytes>;
pub type BytesRegistrations = RegistrationCollection<Bytes, Bytes, Bytes, BytesSource>;

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

pub fn bytes_registrations(
    replay_capacity: NonZeroUsize,
    runtime_epoch: [u64; 2],
) -> BytesRegistrations {
    RegistrationCollection::with_source(BytesSource, replay_capacity, runtime_epoch, physical_key)
}

fn physical_key(writer: WriterId, local_key: &Bytes) -> Bytes {
    fn head(out: &mut Bytes, major: u8, value: u64) {
        let major = major << 5;
        match value {
            0..24 => out.push(major | value as u8),
            24..=0xff => out.extend([major | 24, value as u8]),
            0x100..=0xffff => {
                out.push(major | 25);
                out.extend((value as u16).to_be_bytes());
            }
            0x1_0000..=0xffff_ffff => {
                out.push(major | 26);
                out.extend((value as u32).to_be_bytes());
            }
            _ => {
                out.push(major | 27);
                out.extend(value.to_be_bytes());
            }
        }
    }
    let mut key = Vec::with_capacity(local_key.len() + 19);
    head(&mut key, 4, 2);
    head(&mut key, 0, writer.0);
    head(&mut key, 2, local_key.len() as u64);
    key.extend_from_slice(local_key);
    key
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectionLimits {
    pub readers: usize,
    pub writers: usize,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub rows_per_writer: usize,
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
            writers: 64,
            key_bytes: 1024,
            value_bytes: 64 * 1024,
            rows_per_writer: 1024,
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
    writers: Mutex<usize>,
}

#[derive(Clone, Copy)]
enum Budget {
    Readers,
    Writers,
}

impl State {
    fn admit(self: &Arc<Self>, budget: Budget) -> Result<Permit, AccessError> {
        self.host.check(&self.instance)?;
        let (count, limit) = self.budget(budget);
        let mut count = count.lock().unwrap();
        if *count >= limit {
            return Err(AccessError::Denied);
        }
        *count += 1;
        Ok(Permit(Arc::clone(self), budget))
    }

    fn budget(&self, budget: Budget) -> (&Mutex<usize>, usize) {
        match budget {
            Budget::Readers => (&self.readers, self.limits.readers),
            Budget::Writers => (&self.writers, self.limits.writers),
        }
    }
}

struct Permit(Arc<State>, Budget);

impl Drop for Permit {
    fn drop(&mut self) {
        let mut count = self.0.budget(self.1).0.lock().unwrap();
        *count = count.checked_sub(1).unwrap();
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
                writers: Mutex::new(0),
            }),
        }
    }

    pub fn grant(&self, projection: BytesProjection) -> Result<CollectionReader, AccessError> {
        Ok(CollectionReader {
            _permit: Arc::new(self.state.admit(Budget::Readers)?),
            projection,
            session: self.clone(),
        })
    }

    pub fn reader_count(&self) -> usize {
        *self.state.readers.lock().unwrap()
    }

    pub fn grant_writer(
        &self,
        registrations: &BytesRegistrations,
    ) -> Result<RegistrationWriter, AccessError> {
        let permit = self.state.admit(Budget::Writers)?;
        let slot = Arc::new(Mutex::new(Some(registrations.writer())));
        let revoked = Arc::downgrade(&slot);
        self.state.host.own(
            &self.state.instance,
            Box::new(move || {
                if let Some(slot) = revoked.upgrade() {
                    let writer = slot.lock().unwrap().take();
                    drop(writer);
                }
            }),
        )?;
        Ok(RegistrationWriter {
            slot,
            _permit: permit,
        })
    }

    pub fn writer_count(&self) -> usize {
        *self.state.writers.lock().unwrap()
    }
}

type SubstrateWriter = clay_collection::RegistrationWriter<Bytes, Bytes, Bytes, BytesSource>;

pub struct RegistrationWriter {
    slot: Arc<Mutex<Option<SubstrateWriter>>>,
    _permit: Permit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteFailure {
    Conflict { current: Option<Version> },
    Revoked,
    KeyTooLarge,
    ValueTooLarge,
    RowLimit,
}

impl From<MutationError> for WriteFailure {
    fn from(error: MutationError) -> Self {
        match error {
            MutationError::Conflict { current } => Self::Conflict { current },
        }
    }
}

impl RegistrationWriter {
    const MAX_PHYSICAL_KEY_OVERHEAD: usize = 1 + 9 + 9;

    pub fn put(
        &self,
        key: Bytes,
        based_on: Option<Version>,
        value: Bytes,
    ) -> Result<Version, WriteFailure> {
        let limits = self.admit(&key)?;
        if value.len() > limits.value_bytes
            || key
                .len()
                .saturating_add(Self::MAX_PHYSICAL_KEY_OVERHEAD)
                .saturating_add(value.len())
                > limits.page_bytes.saturating_sub(limits.cursor_bytes)
        {
            return Err(WriteFailure::ValueTooLarge);
        }
        let (version, notifications) = {
            let mut slot = self.slot.lock().unwrap();
            let writer = slot.as_mut().ok_or(WriteFailure::Revoked)?;
            if based_on.is_none()
                && writer.version(&key).is_none()
                && writer.row_count()
                    >= limits.rows_per_writer.min(
                        limits.page_bytes.saturating_sub(limits.cursor_bytes)
                            / limits
                                .key_bytes
                                .saturating_add(Self::MAX_PHYSICAL_KEY_OVERHEAD),
                    )
            {
                return Err(WriteFailure::RowLimit);
            }
            writer.put_deferred(key, based_on, value)?
        };
        notifications.dispatch();
        Ok(version)
    }

    pub fn remove(
        &self,
        key: Bytes,
        based_on: Option<Version>,
    ) -> Result<Option<Version>, WriteFailure> {
        self.admit(&key)?;
        let (version, notifications) = {
            let mut slot = self.slot.lock().unwrap();
            let writer = slot.as_mut().ok_or(WriteFailure::Revoked)?;
            writer.remove_deferred(key, based_on)?
        };
        notifications.dispatch();
        Ok(version)
    }

    fn admit(&self, key: &Bytes) -> Result<CollectionLimits, WriteFailure> {
        let state = &self._permit.0;
        state
            .host
            .check(&state.instance)
            .map_err(|_| WriteFailure::Revoked)?;
        if key.len() > state.limits.key_bytes {
            return Err(WriteFailure::KeyTooLarge);
        }
        Ok(state.limits)
    }
}

impl Drop for RegistrationWriter {
    fn drop(&mut self) {
        let writer = self.slot.lock().unwrap().take();
        drop(writer);
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
        let permit = state.admit(Budget::Readers).map_err(|error| match error {
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

#[cfg(test)]
mod registration_tests;
