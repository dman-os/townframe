//! Typed access to declared registration collections for in-process micros (ADR 017 §9,
//! ADR 031 §4). Rows are the same CBOR bytes a guest reads and writes: a native micro and a
//! guest see one collection through the same host sessions, so a native reader consumes rows a
//! guest wrote (attachments included) and the reverse.
//!
//! A [`Collection`] mirrors one collection into the micro's incarnation: a local task
//! long-polls the projection lease and folds pages into a [`Mirror`]. A [`Registration`] adds
//! a writer whose default basis is that mirror's stamp for the key (so a write supersedes what
//! this micro has seen, tombstones included).
use std::cell::RefCell;
use std::rc::Rc;

use clay_collection::Mirror;
use clay_collection::ReadError;
use clay_collection::ReadLimits;
use clay_collection::Stamp;
use clay_collection::Stamped;
use clay_collection::WriteBasis;
use clay_guest_session::capabilities::CapabilityLimits;
use clay_guest_session::capabilities::CapabilityPublication;
use clay_guest_session::capabilities::CapabilitySession;
use clay_guest_session::capabilities::RowAttachment;
use clay_guest_session::collections::BytesRegistrations;
use clay_guest_session::collections::CAPABILITY_ATTACHMENT_TAG;
use clay_guest_session::collections::CollectionLimits;
use clay_guest_session::collections::CollectionReader;
use clay_guest_session::collections::CollectionSession;
use clay_guest_session::collections::CollectionValue;
use clay_guest_session::collections::ReadFailure;
use clay_guest_session::collections::RegistrationWriter;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::watch;

use crate::Error;
use crate::context::Capability;
use clay_host::Activator;
use clay_host::Host;
use clay_host::Instance;

/// A value's reference to one of its row's attachments, written under the reserved CBOR tag.
/// The body alone conveys no authority: the index names an entry of the row's attachment list,
/// which the host mints into a recipient-local [`CapabilityRef`] on read.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Attachment(ciborium::tag::Required<u64, CAPABILITY_ATTACHMENT_TAG>);

impl Attachment {
    pub fn at(index: u64) -> Self {
        Self(ciborium::tag::Required(index))
    }

    pub fn index(self) -> usize {
        usize::try_from(self.0.0).expect("attachment index fits in memory")
    }
}

/// One live row of a collection: its decoded key and value, its writer's stamp, and the
/// recipient-local references its value attaches (indexed by [`Attachment`]).
pub struct Row<K, V> {
    pub key: K,
    pub value: V,
    pub stamp: Stamp,
    pub attachments: Vec<Capability>,
}

type RawMirror = Mirror<Vec<u8>, Stamped<Option<CollectionValue>>>;

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).expect("CBOR encoding of an in-memory value");
    bytes
}

/// A row that does not decode as the contract's types is a defect in whoever wrote it: the
/// collection's admission is supposed to have refused it, so this crashes rather than hiding it.
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> T {
    ciborium::from_reader(bytes).expect("collection row violates its declared contract")
}

/// The rows of one revision, decoded once and shared by every reader of that revision.
type RowCache<K, V> = Rc<RefCell<Option<(u64, Rc<[Row<K, V>]>)>>>;

/// A read-only mirror of one declared collection, kept current by a task the incarnation owns.
/// Each clone keeps its own position in the change stream (see [`Self::changed`]) and shares
/// the mirror and the decoded-row cache.
pub struct Collection<K, V> {
    mirror: Rc<RefCell<RawMirror>>,
    revision: watch::Receiver<u64>,
    capabilities: CapabilitySession,
    decoded: RowCache<K, V>,
}

impl<K, V> Clone for Collection<K, V> {
    fn clone(&self) -> Self {
        Self {
            mirror: Rc::clone(&self.mirror),
            revision: self.revision.clone(),
            capabilities: self.capabilities.clone(),
            decoded: Rc::clone(&self.decoded),
        }
    }
}

impl<K: DeserializeOwned, V: DeserializeOwned> Collection<K, V> {
    /// Start mirroring `reader`. Returns the watch and the loop that feeds it, which the
    /// caller owns and must drive until the incarnation closes.
    pub(crate) fn start(
        reader: CollectionReader,
        capabilities: CapabilitySession,
    ) -> (Self, impl Future<Output = ()> + 'static) {
        let mirror = Rc::new(RefCell::new(Mirror::empty()));
        let (notify, revision) = watch::channel(0_u64);
        let feed = {
            let mirror = Rc::clone(&mirror);
            async move {
                loop {
                    // The empty selector is the whole collection.
                    let lease = reader
                        .acquire(Vec::new(), None)
                        .expect("a granted reader acquires its collection");
                    mirror.borrow_mut().begin_lease(None);
                    loop {
                        let ticket = mirror.borrow_mut().begin_read();
                        // No timeout: a caught-up lease parks until a change or revocation.
                        let read = lease
                            .read(
                                ticket.committed().cloned(),
                                ReadLimits::default(),
                                std::future::pending(),
                            )
                            .await;
                        match read {
                            Ok(page) => {
                                assert!(mirror.borrow_mut().apply(&ticket, &page));
                                notify.send_modify(|revision| *revision += 1);
                            }
                            Err(ReadFailure::Read(ReadError::ResetRequired)) => {
                                mirror.borrow_mut().clear();
                                notify.send_modify(|revision| *revision += 1);
                                break;
                            }
                            // The incarnation closed: nothing is left to mirror.
                            Err(ReadFailure::Read(ReadError::Revoked)) => return,
                            Err(ReadFailure::Read(ReadError::SourceUnavailable)) => {
                                unimplemented!("registration sources are always available")
                            }
                            Err(failure @ (ReadFailure::Busy | ReadFailure::PageLimit)) => {
                                panic!("mirror read failed: {failure:?}")
                            }
                        }
                    }
                }
            }
        };
        (
            Self {
                mirror,
                revision,
                capabilities,
                decoded: Rc::default(),
            },
            feed,
        )
    }

    /// Mirror `collection` for a participant that is not a micro, such as a shell: `instance`
    /// is the participant's own instance and `activator` is what its rows' lazy references
    /// activate through. Returns the watch and the feed loop, which the caller must drive
    /// until `instance` closes.
    pub fn observe(
        host: &Host,
        instance: &Instance,
        activator: Activator<CapabilityPublication>,
        collection: &BytesRegistrations,
    ) -> Result<(Self, impl Future<Output = ()> + 'static + use<K, V>), Error> {
        let capabilities =
            CapabilitySession::new(host.clone(), instance.clone(), CapabilityLimits::default());
        capabilities.attach_activator(activator);
        let sessions =
            CollectionSession::new(host.clone(), instance.clone(), CollectionLimits::default());
        let reader = sessions.grant(collection.projection())?;
        Ok(Self::start(reader, capabilities))
    }

    /// Every live row (tombstoned keys are hidden), attachments minted for this incarnation.
    /// Rows are decoded once per mirror revision: the feed task folds a page and bumps the
    /// revision with no await in between, so a revision read here names exactly the mirror
    /// state read next, and every reader of that revision shares the decoded rows.
    pub fn rows(&self) -> Result<Rc<[Row<K, V>]>, Error> {
        let revision = *self.revision.borrow();
        if let Some((cached, rows)) = &*self.decoded.borrow()
            && *cached == revision
        {
            return Ok(Rc::clone(rows));
        }
        let mirror = self.mirror.borrow();
        let mut rows = Vec::new();
        for (key, stamped) in mirror.rows() {
            let Some(value) = &stamped.value else {
                continue;
            };
            let attachments = value
                .attachments
                .iter()
                .map(|attachment| {
                    self.capabilities
                        .mint_attachment(attachment)
                        .map(Capability::new)
                })
                .collect::<Result<_, _>>()?;
            rows.push(Row {
                key: decode(&key),
                value: decode(&value.body),
                stamp: stamped.stamp.clone(),
                attachments,
            });
        }
        let rows: Rc<[Row<K, V>]> = rows.into();
        *self.decoded.borrow_mut() = Some((revision, Rc::clone(&rows)));
        Ok(rows)
    }

    /// Whether the mirror has folded in the collection's bootstrap.
    pub fn is_bootstrapped(&self) -> bool {
        self.mirror.borrow().bootstrap_complete()
    }

    /// Resolve when the mirror next changes after the last change this handle observed. Errors
    /// with [`Error::Revoked`] once the incarnation that feeds the mirror has closed.
    pub async fn changed(&mut self) -> Result<(), Error> {
        self.revision.changed().await.map_err(|_| Error::Revoked)
    }

    /// Resolve once the mirror has folded in the collection's bootstrap.
    pub async fn bootstrapped(&self) {
        let mut revision = self.revision.clone();
        while !self.is_bootstrapped() {
            // The feed loop owns the sender; it ending means the incarnation closed.
            if revision.changed().await.is_err() {
                return;
            }
        }
    }
}

/// A micro's write authority over a collection together with its mirror of that collection.
#[derive(Clone)]
pub struct Registration<K, V> {
    writer: Rc<RegistrationWriter>,
    collection: Collection<K, V>,
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> Registration<K, V> {
    pub(crate) fn new(writer: Rc<RegistrationWriter>, collection: Collection<K, V>) -> Self {
        Self { writer, collection }
    }

    pub fn collection(&self) -> &Collection<K, V> {
        &self.collection
    }

    fn basis(&self, key: &[u8]) -> WriteBasis {
        self.collection.mirror.borrow().write_basis(&key.to_vec())
    }

    /// Put `value` at `key`, superseding what this micro's mirror holds for it. `attachments`
    /// are the publications [`Attachment`] indices in `value` refer to. Async so the guest
    /// backend, whose writes cross the component boundary, shares this signature; the in-process
    /// write completes before the first poll returns.
    pub async fn put(
        &self,
        key: &K,
        value: &V,
        attachments: Vec<RowAttachment>,
    ) -> Result<Stamp, Error> {
        let key = encode(key);
        let basis = self.basis(&key);
        Ok(self.writer.put_value(
            key,
            basis,
            CollectionValue {
                body: encode(value),
                attachments,
            },
        )?)
    }

    /// Delete `key`, superseding what this micro's mirror holds for it.
    pub async fn delete(&self, key: &K) -> Result<Stamp, Error> {
        let key = encode(key);
        let basis = self.basis(&key);
        Ok(self.writer.delete(key, basis)?)
    }
}
