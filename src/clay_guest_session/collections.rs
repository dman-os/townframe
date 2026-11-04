use crate::capabilities::ContractIdentity;
use crate::capabilities::PublicationEndpoint;
use crate::capabilities::RowAttachment;
use clay_collection::AcquireError;
use clay_collection::Change;
use clay_collection::Cursor;
use clay_collection::DurableCodec;
use clay_collection::MutationError;
use clay_collection::Page;
use clay_collection::Projection;
use clay_collection::ProjectionLease;
use clay_collection::ReadError;
use clay_collection::ReadLimits;
use clay_collection::RegistrationCollection;
use clay_collection::RegistrationDispatch;
use clay_collection::RegistrationScope;
use clay_collection::Source;
use clay_collection::Stamp;
use clay_collection::Stamped;
use clay_collection::WriteBasis;
use clay_collection::WriterId;
use clay_host::AccessError;
use clay_host::Host;
use clay_host::Instance;
use clay_host::Lifecycle;
use clay_host::MicroId;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::watch;

pub type Bytes = Vec<u8>;

/// One stored collection value: portable CBOR body plus host-retained
/// attachments (ADR 023 §6): live publications, or endpoints that name a micro's publication
/// (ADR 025 §7). The body names attachments by index under [`CAPABILITY_ATTACHMENT_TAG`]; the
/// body alone conveys no authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionValue {
    pub body: Bytes,
    pub attachments: Vec<RowAttachment>,
}

impl CollectionValue {
    pub fn plain(body: Bytes) -> Self {
        Self {
            body,
            attachments: Vec::new(),
        }
    }
    pub fn byte_len(&self) -> usize {
        self.body
            .len()
            .saturating_add(self.attachments.len().saturating_mul(CAPABILITY_PAGE_BYTES))
    }
}

/// The durable form of a [`CollectionValue`]: CBOR `[body, [[micro, publication, namespace,
/// name, major, minor, patch], ...]]`. A durable row persists endpoints and never references
/// (ADR 025 §7), so encoding a bound attachment is a programming error; a row that does not
/// decode is corrupt storage. Both crash.
pub fn durable_codec() -> DurableCodec<Bytes, CollectionValue> {
    use ciborium::Value;
    fn encode_value(value: &CollectionValue) -> Vec<u8> {
        let attachments = value
            .attachments
            .iter()
            .map(|attachment| {
                let RowAttachment::Endpoint(endpoint) = attachment else {
                    panic!("a durable row can only attach endpoints, never live publications");
                };
                let (major, minor, patch) = endpoint.contract.version;
                Value::Array(vec![
                    Value::Text(endpoint.micro.as_str().to_owned()),
                    Value::Text(endpoint.publication.clone()),
                    Value::Text(endpoint.contract.namespace.clone()),
                    Value::Text(endpoint.contract.name.clone()),
                    Value::Integer(major.into()),
                    Value::Integer(minor.into()),
                    Value::Integer(patch.into()),
                ])
            })
            .collect();
        let mut out = Vec::new();
        ciborium::into_writer(
            &Value::Array(vec![
                Value::Bytes(value.body.clone()),
                Value::Array(attachments),
            ]),
            &mut out,
        )
        .expect("CBOR encoding of an in-memory value");
        out
    }
    fn decode_value(bytes: &[u8]) -> CollectionValue {
        fn text(value: Value) -> String {
            value.into_text().expect("stored endpoint field is text")
        }
        fn number(value: Value) -> u32 {
            let number = value
                .into_integer()
                .expect("stored contract version is an integer");
            u32::try_from(number).expect("stored contract version fits u32")
        }
        let Value::Array(parts) =
            ciborium::from_reader(bytes).expect("stored collection value is CBOR")
        else {
            panic!("stored collection value is not an array");
        };
        let [body, attachments] = <[Value; 2]>::try_from(parts)
            .unwrap_or_else(|_| panic!("stored collection value has the wrong arity"));
        let attachments = attachments
            .into_array()
            .expect("stored attachments are an array")
            .into_iter()
            .map(|attachment| {
                let fields = attachment
                    .into_array()
                    .expect("stored attachment is an array");
                let [micro, publication, namespace, name, major, minor, patch] =
                    <[Value; 7]>::try_from(fields)
                        .unwrap_or_else(|_| panic!("stored endpoint has the wrong arity"));
                RowAttachment::Endpoint(PublicationEndpoint {
                    micro: MicroId::new(text(micro)),
                    publication: text(publication),
                    contract: ContractIdentity::new(
                        text(namespace),
                        text(name),
                        (number(major), number(minor), number(patch)),
                    ),
                })
            })
            .collect();
        CollectionValue {
            body: body.into_bytes().expect("stored body is bytes"),
            attachments,
        }
    }
    DurableCodec {
        encode_key: Bytes::clone,
        decode_key: <[u8]>::to_vec,
        encode_value,
        decode_value,
    }
}

/// Page-accounting charge per capability attachment.
pub const CAPABILITY_PAGE_BYTES: usize = 32;
/// Reserved CBOR tag whose unsigned-integer content is an index into the
/// value's capability attachment list.
pub const CAPABILITY_ATTACHMENT_TAG: u64 = 43000;

/// Attachment indices named by `body`, or `None` when the body is not
/// well-formed CBOR (opaque bodies name no attachments).
pub fn attachment_indices(body: &[u8]) -> Option<Vec<usize>> {
    struct Scanner<'a> {
        bytes: &'a [u8],
        pos: usize,
        indices: Vec<usize>,
    }
    impl Scanner<'_> {
        fn head(&mut self) -> Option<(u8, u64, bool)> {
            let byte = *self.bytes.get(self.pos)?;
            self.pos += 1;
            let (major, info) = (byte >> 5, byte & 31);
            let (value, indefinite) = match info {
                0..=23 => (u64::from(info), false),
                24 => (u64::from(*self.bytes.get(self.pos)?), false),
                25 if major == 7 => (0, false),
                25 => {
                    let raw = self.bytes.get(self.pos..self.pos + 2)?;
                    self.pos += 2;
                    (
                        u64::from(u16::from_be_bytes(raw.try_into().unwrap())),
                        false,
                    )
                }
                26 if major == 7 => (0, false),
                26 => {
                    let raw = self.bytes.get(self.pos..self.pos + 4)?;
                    self.pos += 4;
                    (
                        u64::from(u32::from_be_bytes(raw.try_into().unwrap())),
                        false,
                    )
                }
                27 if major == 7 => (0, false),
                27 => {
                    let raw = self.bytes.get(self.pos..self.pos + 8)?;
                    self.pos += 8;
                    (u64::from_be_bytes(raw.try_into().unwrap()), false)
                }
                31 => (0, true),
                _ => return None,
            };
            // Float payloads (major 7) carry bytes the head match skipped counting.
            if major == 7 && matches!(info, 25..=27) {
                self.pos += match info {
                    25 => 2,
                    26 => 4,
                    _ => 8,
                };
                if self.pos > self.bytes.len() {
                    return None;
                }
            }
            Some((major, value, indefinite))
        }
        fn item(&mut self, depth: usize) -> Option<()> {
            if depth > 64 {
                return None;
            }
            let (major, value, indefinite) = self.head()?;
            match major {
                0 | 1 | 7 => Some(()),
                2 | 3 => {
                    if indefinite {
                        loop {
                            if self.bytes.get(self.pos) == Some(&0xff) {
                                self.pos += 1;
                                return Some(());
                            }
                            self.item(depth + 1)?;
                        }
                    } else {
                        let len = usize::try_from(value).ok()?;
                        self.pos = self.pos.checked_add(len)?;
                        if self.pos > self.bytes.len() {
                            return None;
                        }
                        Some(())
                    }
                }
                4 => self.items(depth, value, indefinite, 1),
                5 => self.items(depth, value, indefinite, 2),
                6 => {
                    if value == CAPABILITY_ATTACHMENT_TAG {
                        // Peek the operand: an unsigned int is an attachment index.
                        let saved = self.pos;
                        match self.head() {
                            Some((0, index, false)) => {
                                self.indices.push(usize::try_from(index).ok()?);
                                Some(())
                            }
                            _ => {
                                self.pos = saved;
                                self.item(depth + 1)
                            }
                        }
                    } else {
                        self.item(depth + 1)
                    }
                }
                _ => None,
            }
        }
        fn items(&mut self, depth: usize, count: u64, indefinite: bool, each: u64) -> Option<()> {
            if indefinite {
                loop {
                    if self.bytes.get(self.pos) == Some(&0xff) {
                        self.pos += 1;
                        return Some(());
                    }
                    self.item(depth + 1)?;
                }
            }
            for _ in 0..count.checked_mul(each)? {
                self.item(depth + 1)?;
            }
            Some(())
        }
    }
    let mut scanner = Scanner {
        bytes: body,
        pos: 0,
        indices: Vec::new(),
    };
    scanner.item(0)?;
    if scanner.pos != body.len() {
        return None;
    }
    Some(scanner.indices)
}

/// Readers see each row with the stamp of the write that produced it, so a guest can pass
/// the stamp back as a [`WriteBasis`].
pub type BytesProjection = Projection<Bytes, Stamped<Option<CollectionValue>>, BytesSource>;
type BytesLease = ProjectionLease<Bytes, Stamped<Option<CollectionValue>>, BytesSource>;
pub type BytesPage = Page<Bytes, Stamped<Option<CollectionValue>>>;
pub type BytesScope = RegistrationScope<Bytes, CollectionValue, BytesSource>;

type BytesDispatch = RegistrationDispatch<Bytes, CollectionValue, BytesSource>;

pub type BytesRegistrations = RegistrationCollection<Bytes, CollectionValue, BytesSource>;

#[derive(Clone, Debug)]
pub struct BytesSource;

impl Source<Bytes, Stamped<Option<CollectionValue>>> for BytesSource {
    type Selector = Bytes;
    type Interest = ();

    fn acquire(&self, selector: Bytes) -> Result<(Bytes, ()), AcquireError> {
        Ok((selector, ()))
    }

    fn matches(
        &self,
        _selector: &Bytes,
        _key: &Bytes,
        _value: &Stamped<Option<CollectionValue>>,
    ) -> bool {
        true
    }
}

pub fn bytes_registrations(
    replay_capacity: NonZeroUsize,
    runtime_epoch: [u64; 2],
) -> BytesRegistrations {
    RegistrationCollection::with_source(BytesSource, replay_capacity, runtime_epoch)
}

/// A registration collection backed by `store`: installation scopes persist there and are
/// rebuilt on construction (ADR 025 §2), with [`durable_codec`] as the row encoding.
pub async fn durable_bytes_registrations<St: clay_store::OrderedStore + Send + Sync + 'static>(
    store: Arc<St>,
    collection_id: Bytes,
    replay_capacity: NonZeroUsize,
    runtime_epoch: [u64; 2],
) -> Result<BytesRegistrations, clay_store::StoreError> {
    RegistrationCollection::with_durable_source(
        store,
        collection_id,
        durable_codec(),
        BytesSource,
        replay_capacity,
        runtime_epoch,
    )
    .await
}

/// What a micro's registration binding is allowed to write into one collection: the
/// collection, the micro whose identity stamps its writes, and the key prefixes it may
/// write (ADR 024 §13). An empty prefix grants every key. Enforced here, in the session,
/// not in `clay_collection`.
#[derive(Clone)]
pub struct RegistrationGrant {
    pub registrations: BytesRegistrations,
    /// Singleton micros are their own stable writer identity: it breaks ties between
    /// concurrent writes and survives restarts.
    pub micro: MicroId,
    pub prefixes: Vec<Bytes>,
}

impl From<clay_host::Grant<BytesRegistrations>> for RegistrationGrant {
    /// A grant the declared-collection registry issued, in the session's terms.
    fn from(grant: clay_host::Grant<BytesRegistrations>) -> Self {
        Self {
            registrations: grant.collection,
            micro: grant.micro,
            prefixes: grant.prefixes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectionLimits {
    pub readers: usize,
    pub writers: usize,
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub capabilities_per_value: usize,
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
            capabilities_per_value: 32,
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
    /// One scope per writer grant (ADR 025 §4). The incarnation owns them: a scope lives until
    /// the incarnation closes or fails initialization, however long its writer handle lives.
    /// Scopes start staged, so their rows are in the authoritative replica but not the
    /// projection until initialization commits.
    scopes: Arc<IncarnationScopes>,
}

/// The scope owners of one incarnation. The host's teardown effect and outward hook hold this
/// directly, not through [`State`]: the scopes must outlive the session handle (a guest store
/// dropping its [`CollectionSession`] does not close them) and end only at teardown.
#[derive(Default)]
struct IncarnationScopes(Mutex<Vec<OwnedScope>>);

struct OwnedScope {
    registrations: BytesRegistrations,
    scope: BytesScope,
}

impl IncarnationScopes {
    /// Initialization committed: publish every staged scope, one transaction per collection.
    /// Each collection commits atomically on its own; there is no cross-collection atomicity
    /// (ADR 017 §6). Wakes are held until every collection has committed, then fired with no
    /// host lock held. Scopes that already committed are skipped, so a late call is a no-op.
    fn commit(&self) {
        let owned = self.0.lock().unwrap();
        let dispatches = Self::per_collection(&owned, |registrations, scopes| {
            registrations.commit_scopes(scopes)
        });
        drop(owned);
        dispatches
            .into_iter()
            .for_each(RegistrationDispatch::dispatch);
    }

    /// The incarnation was torn down (closed after cleanup, or failed initialization): withdraw
    /// every scope's rows, one transaction per collection, firing the held wakes only after all collections closed.
    /// Scopes are drained, so only the first call does work.
    fn close(&self) {
        let owned = std::mem::take(&mut *self.0.lock().unwrap());
        let dispatches = Self::per_collection(&owned, |registrations, scopes| {
            registrations.close_scopes(scopes)
        });
        // Dropping an owner closes it again, which finds nothing left to withdraw.
        drop(owned);
        dispatches
            .into_iter()
            .for_each(RegistrationDispatch::dispatch);
    }

    /// Run `operation` once per distinct collection over that collection's scopes, in grant
    /// order.
    fn per_collection(
        owned: &[OwnedScope],
        operation: impl Fn(&BytesRegistrations, &[&BytesScope]) -> BytesDispatch,
    ) -> Vec<BytesDispatch> {
        let mut groups: Vec<(&BytesRegistrations, Vec<&BytesScope>)> = Vec::new();
        for OwnedScope {
            registrations,
            scope,
        } in owned
        {
            match groups
                .iter_mut()
                .find(|(collection, _)| *collection == registrations)
            {
                Some((_, scopes)) => scopes.push(scope),
                None => groups.push((registrations, vec![scope])),
            }
        }
        groups
            .into_iter()
            .map(|(registrations, scopes)| operation(registrations, &scopes))
            .collect()
    }
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
                scopes: Arc::default(),
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
        grant: RegistrationGrant,
    ) -> Result<RegistrationWriter, AccessError> {
        let RegistrationGrant {
            registrations,
            micro,
            prefixes,
        } = grant;
        let permit = self.state.admit(Budget::Writers)?;
        let writer_id = WriterId::new(micro.as_str());
        // Every grant mints a staged scope (ADR 024 §5): rows written while the incarnation is
        // still initializing must not reach readers unless initialization commits.
        let scope = registrations.create_staged_scope(writer_id.clone());
        let writer = scope.writer().expect("a freshly minted scope is open");
        // Teardown covers an incarnation that is closed without ever committing (failed
        // initialization) as well as one that closed after cleanup. It owns the scopes, so a
        // dropped session or writer never withdraws rows.
        let teardown = Arc::clone(&self.state.scopes);
        self.state
            .host
            .own(&self.state.instance, Box::new(move || teardown.close()))?;
        self.state.scopes.0.lock().unwrap().push(OwnedScope {
            registrations,
            scope,
        });
        let outward = Arc::clone(&self.state.scopes);
        let lifecycle = self
            .state
            .host
            .on_outward(&self.state.instance, move |lifecycle| match lifecycle {
                Lifecycle::Active => outward.commit(),
                // Closing withdraws outward visibility of new publications only. The scopes keep
                // their rows while `instance-root.close()` runs (ADR 024 §6) and are closed by
                // the teardown effect above, after that cleanup.
                Lifecycle::Closing => {}
                other => unreachable!("outward hooks are never told {other:?}"),
            })?;
        if lifecycle == Lifecycle::Active {
            self.state.scopes.commit();
        }
        Ok(RegistrationWriter {
            writer,
            prefixes,
            stamp_bytes: STAMP_LAMPORT_BYTES + writer_id.as_bytes().len(),
            _permit: permit,
        })
    }

    pub fn writer_count(&self) -> usize {
        *self.state.writers.lock().unwrap()
    }
}

type SubstrateWriter = clay_collection::RegistrationWriter<Bytes, CollectionValue, BytesSource>;

/// Page-accounting charge for the lamport of a row's stamp; the stamp's writer id is
/// charged by its length.
const STAMP_LAMPORT_BYTES: usize = 8;

/// Mutation authority over one scope. Dropping it releases the authority only; the scope, and
/// its rows, belong to the incarnation and are withdrawn when it closes.
pub struct RegistrationWriter {
    writer: SubstrateWriter,
    /// Key prefixes this writer may write; see [`RegistrationGrant`].
    prefixes: Vec<Bytes>,
    /// What each row's stamp adds to a page, fixed by the writer identity.
    stamp_bytes: usize,
    _permit: Permit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteFailure {
    Conflict {
        current: Option<Stamp>,
    },
    /// The key lies outside the writer's granted key prefixes.
    Forbidden,
    Revoked,
    KeyTooLarge,
    ValueTooLarge,
    RowLimit,
    InvalidAttachment,
    CapabilityLimit,
}

impl From<MutationError> for WriteFailure {
    fn from(error: MutationError) -> Self {
        match error {
            MutationError::Conflict { current } => Self::Conflict { current },
            MutationError::Closed => Self::Revoked,
        }
    }
}

impl RegistrationWriter {
    pub fn put(&self, key: Bytes, basis: WriteBasis, value: Bytes) -> Result<Stamp, WriteFailure> {
        self.put_value(key, basis, CollectionValue::plain(value))
    }

    pub fn put_value(
        &self,
        key: Bytes,
        basis: WriteBasis,
        value: CollectionValue,
    ) -> Result<Stamp, WriteFailure> {
        let limits = self.admit(&key)?;
        if value.attachments.len() > limits.capabilities_per_value {
            return Err(WriteFailure::CapabilityLimit);
        }
        // The attachment list is authoritative; the body may only name indices into it.
        if let Some(indices) = attachment_indices(&value.body)
            && indices
                .iter()
                .any(|index| *index >= value.attachments.len())
        {
            return Err(WriteFailure::InvalidAttachment);
        }
        if value.body.len() > limits.value_bytes
            || key
                .len()
                .saturating_add(self.stamp_bytes)
                .saturating_add(value.byte_len())
                > limits.page_bytes.saturating_sub(limits.cursor_bytes)
        {
            return Err(WriteFailure::ValueTooLarge);
        }
        self.admit_claim(&key, limits)?;
        Ok(self.writer.put(key, basis, value)?.dispatch())
    }

    /// Delete writes a tombstone, which holds a row of this writer's budget like a value does.
    pub fn delete(&self, key: Bytes, basis: WriteBasis) -> Result<Stamp, WriteFailure> {
        let limits = self.admit(&key)?;
        self.admit_claim(&key, limits)?;
        Ok(self.writer.delete(key, basis)?.dispatch())
    }

    fn admit(&self, key: &Bytes) -> Result<CollectionLimits, WriteFailure> {
        let state = &self._permit.0;
        state
            .host
            .check(&state.instance)
            .map_err(|_| WriteFailure::Revoked)?;
        if !self.prefixes.iter().any(|prefix| key.starts_with(prefix)) {
            return Err(WriteFailure::Forbidden);
        }
        if key.len() > state.limits.key_bytes {
            return Err(WriteFailure::KeyTooLarge);
        }
        Ok(state.limits)
    }

    /// Refuse a write that would claim a key beyond the writer's row budget: the configured
    /// cap, or what one page can carry at the largest key.
    fn admit_claim(&self, key: &Bytes, limits: CollectionLimits) -> Result<(), WriteFailure> {
        let page_rows = limits.page_bytes.saturating_sub(limits.cursor_bytes)
            / limits.key_bytes.saturating_add(self.stamp_bytes);
        if !self.writer.holds(key)
            && self.writer.held_keys() >= limits.rows_per_writer.min(page_rows)
        {
            return Err(WriteFailure::RowLimit);
        }
        Ok(())
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
    lease: Mutex<Option<BytesLease>>,
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
                } => {
                    key.len()
                        + value.value.as_ref().map_or(0, CollectionValue::byte_len)
                        + STAMP_LAMPORT_BYTES
                        + value.stamp.writer.as_bytes().len()
                        + write_context.as_ref().map_or(0, Vec::len)
                }
                Change::Remove { key, .. } => key.len(),
            })
            .sum::<usize>()
}

/// A grant of every key for the session tests; production mint sites pass the manifest's
/// prefixes.
#[cfg(test)]
fn full_grant(registrations: &BytesRegistrations, micro: &str) -> RegistrationGrant {
    RegistrationGrant {
        registrations: registrations.clone(),
        micro: MicroId::new(micro),
        prefixes: vec![Vec::new()],
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod capability_tests;

#[cfg(test)]
mod registration_tests;

#[cfg(test)]
mod staging_tests;
