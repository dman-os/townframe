//! Typed access to declared registration collections, shared by every backend (ADR 017 §9,
//! ADR 025 §4). Rows are the same CBOR bytes a guest reads and writes: an in-process micro and
//! a guest see one collection through the same host sessions, so one reads rows the other wrote
//! (attachments included).
//!
//! A [`Collection`] mirrors one collection into the micro's incarnation: a task the backend
//! owns long-polls the projection lease and folds pages into a [`Mirror`], then reports the
//! change through [`Feed::applied`]. A [`Registration`] adds a writer whose default basis is
//! that mirror's stamp for the key, so a write supersedes what this micro has seen (tombstones
//! included). Everything here is backend-neutral; only how a page is fetched, how a row's
//! attachments become references, and how a write crosses to the host differ, and each is one
//! arm of [`Stored`], [`Minter`] and [`Writer`].
use std::cell::Cell;
use std::cell::RefCell;
use std::rc::Rc;
use std::task::Poll;
use std::task::Waker;

use clay_collection::Change;
use clay_collection::Mirror;
use clay_collection::Page;
use clay_collection::Stamp;
use clay_collection::Stamped;
use clay_collection::Transaction;
use clay_collection::WriteBasis;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::Capability;
use crate::Error;
use crate::RowAttachment;

/// The CBOR tag a value uses to name one of its row's attachments by index. Equal to
/// `clay_guest_session::collections::CAPABILITY_ATTACHMENT_TAG`, which the in-process backend
/// asserts at compile time.
pub const CAPABILITY_ATTACHMENT_TAG: u64 = 43000;

#[cfg(feature = "in_process")]
const _: () = assert!(
    CAPABILITY_ATTACHMENT_TAG == clay_guest_session::collections::CAPABILITY_ATTACHMENT_TAG
);

/// A value's reference to one of its row's attachments, written under the reserved CBOR tag.
/// The body alone conveys no authority: the index names an entry of the row's attachment list,
/// which the host mints into a recipient-local [`Capability`] on read.
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

/// One row value as its backend's page carried it, kept until a reader asks for the rows.
#[derive(Clone)]
pub(crate) enum Stored {
    #[cfg(feature = "in_process")]
    InProcess(clay_guest_session::collections::CollectionValue),
    #[cfg(feature = "guest")]
    Guest(Rc<crate::guest::GuestValue>),
}

/// What turns a [`Stored`] row's attachments into references recipient-local to this
/// incarnation: the in-process session mints them from the host's records, while a guest's
/// arrive from the host already minted.
pub(crate) enum Minter {
    #[cfg(feature = "in_process")]
    InProcess(clay_guest_session::capabilities::CapabilitySession),
    #[cfg(feature = "guest")]
    Guest,
}

impl Minter {
    fn body<'a>(&self, stored: &'a Stored) -> &'a [u8] {
        match stored {
            #[cfg(feature = "in_process")]
            Stored::InProcess(value) => &value.body,
            #[cfg(feature = "guest")]
            Stored::Guest(value) => &value.body,
        }
    }

    fn mint(&self, stored: &Stored) -> Result<Vec<Capability>, Error> {
        match (self, stored) {
            #[cfg(feature = "in_process")]
            (Self::InProcess(session), Stored::InProcess(value)) => value
                .attachments
                .iter()
                .map(|attachment| Ok(Capability::in_process(session.mint_attachment(attachment)?)))
                .collect(),
            #[cfg(feature = "guest")]
            (Self::Guest, Stored::Guest(value)) => Ok(value
                .capabilities
                .iter()
                .map(|reference| Capability::guest(Rc::clone(reference)))
                .collect()),
            #[cfg(all(feature = "in_process", feature = "guest"))]
            _ => unreachable!("a collection's rows come from its own backend"),
        }
    }
}

pub(crate) type RawMirror = Mirror<Vec<u8>, Stamped<Option<Stored>>>;

/// A backend's page of its own row values, as the shared mirror stores them.
pub(crate) fn stored_page<V>(
    page: Page<Vec<u8>, Stamped<Option<V>>>,
    wrap: impl Fn(V) -> Stored,
) -> Page<Vec<u8>, Stamped<Option<Stored>>> {
    Page {
        transactions: page
            .transactions
            .into_iter()
            .map(|transaction| Transaction {
                changes: transaction
                    .changes
                    .into_iter()
                    .map(|change| match change {
                        Change::Upsert {
                            key,
                            row_version,
                            value,
                            evidence,
                            write_context,
                        } => Change::Upsert {
                            key,
                            row_version,
                            value: Stamped {
                                stamp: value.stamp,
                                value: value.value.map(&wrap),
                            },
                            evidence,
                            write_context,
                        },
                        Change::Remove { key, row_version } => Change::Remove { key, row_version },
                    })
                    .collect(),
            })
            .collect(),
        through: page.through,
        bootstrap_complete: page.bootstrap_complete,
    }
}

/// The change counter every clone of one [`Collection`] observes. The feed bumps it after each
/// page it folds in and closes it when the feed ends, which is how readers learn the
/// incarnation did.
#[derive(Default)]
struct Changes {
    revision: Cell<u64>,
    closed: Cell<bool>,
    wakers: RefCell<Vec<Waker>>,
}

impl Changes {
    fn wake(&self) {
        for waker in std::mem::take(&mut *self.wakers.borrow_mut()) {
            waker.wake();
        }
    }
}

/// The feeding side of one [`Collection`], owned by the backend's feed task. Dropping it
/// closes the collection's change stream.
pub(crate) struct Feed {
    pub(crate) mirror: Rc<RefCell<RawMirror>>,
    changes: Rc<Changes>,
}

impl Feed {
    /// The mirror changed: wake everything waiting in [`Collection::changed`].
    pub(crate) fn applied(&self) {
        self.changes.revision.set(self.changes.revision.get() + 1);
        self.changes.wake();
    }
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.changes.closed.set(true);
        self.changes.wake();
    }
}

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
    observer: Observer,
    minter: Rc<Minter>,
    decoded: RowCache<K, V>,
}

impl<K, V> Clone for Collection<K, V> {
    fn clone(&self) -> Self {
        Self {
            mirror: Rc::clone(&self.mirror),
            observer: self.observer.clone(),
            minter: Rc::clone(&self.minter),
            decoded: Rc::clone(&self.decoded),
        }
    }
}

/// One reader's position in a collection's change stream.
#[derive(Clone)]
pub(crate) struct Observer {
    changes: Rc<Changes>,
    seen: u64,
}

impl Observer {
    /// Resolve when the stream next changes after the last change this observer saw. Errors
    /// with [`Error::Revoked`] once the feed has ended.
    pub(crate) async fn changed(&mut self) -> Result<(), Error> {
        std::future::poll_fn(|cx| {
            let revision = self.changes.revision.get();
            if self.seen < revision {
                self.seen = revision;
                return Poll::Ready(Ok(()));
            }
            if self.changes.closed.get() {
                return Poll::Ready(Err(Error::Revoked));
            }
            let mut wakers = self.changes.wakers.borrow_mut();
            if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                wakers.push(cx.waker().clone());
            }
            Poll::Pending
        })
        .await
    }
}

impl<K: DeserializeOwned, V: DeserializeOwned> Collection<K, V> {
    /// A collection and the [`Feed`] its backend's task must keep current until the
    /// incarnation closes.
    pub(crate) fn new(minter: Minter) -> (Self, Feed) {
        let mirror = Rc::new(RefCell::new(Mirror::empty()));
        let changes = Rc::new(Changes::default());
        (
            Self {
                mirror: Rc::clone(&mirror),
                observer: Observer {
                    changes: Rc::clone(&changes),
                    seen: 0,
                },
                minter: Rc::new(minter),
                decoded: Rc::default(),
            },
            Feed { mirror, changes },
        )
    }

    /// Every live row (tombstoned keys are hidden), attachments minted for this incarnation.
    /// Rows are decoded once per mirror revision: the feed folds a page and bumps the revision
    /// with no await in between, so a revision read here names exactly the mirror state read
    /// next, and every reader of that revision shares the decoded rows.
    pub fn rows(&self) -> Result<Rc<[Row<K, V>]>, Error> {
        let revision = self.observer.changes.revision.get();
        if let Some((cached, rows)) = &*self.decoded.borrow()
            && *cached == revision
        {
            return Ok(Rc::clone(rows));
        }
        let mirror = self.mirror.borrow();
        let mut rows = Vec::new();
        for (key, stamped) in mirror.rows() {
            let Some(stored) = &stamped.value else {
                continue;
            };
            rows.push(Row {
                key: decode(&key),
                value: decode(self.minter.body(stored)),
                stamp: stamped.stamp.clone(),
                attachments: self.minter.mint(stored)?,
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
        self.observer.changed().await
    }

    /// Resolve once the mirror has folded in the collection's bootstrap, or the incarnation
    /// that feeds it has closed.
    pub async fn bootstrapped(&self) {
        let mut observer = self.observer.clone();
        while !self.is_bootstrapped() {
            if observer.changed().await.is_err() {
                return;
            }
        }
    }

    /// The stream of this collection's changes, for a hook to turn into re-renders.
    #[cfg(any(feature = "view", feature = "guest"))]
    pub(crate) fn changes(&self) -> crate::hooks::Changes {
        match &*self.minter {
            #[cfg(feature = "in_process")]
            Minter::InProcess(_) => crate::hooks::Changes::Rows(self.observer.clone()),
            #[cfg(feature = "guest")]
            Minter::Guest => crate::hooks::Changes::Pushed,
        }
    }
}

/// How a registration's writes cross to the host.
pub(crate) enum Writer {
    #[cfg(feature = "in_process")]
    InProcess(Rc<clay_guest_session::collections::RegistrationWriter>),
    #[cfg(feature = "guest")]
    Guest(Rc<crate::collection_registration::RegistrationWriter>),
}

/// A micro's write authority over a collection together with its mirror of that collection.
pub struct Registration<K, V> {
    writer: Rc<Writer>,
    collection: Collection<K, V>,
}

impl<K, V> Clone for Registration<K, V> {
    fn clone(&self) -> Self {
        Self {
            writer: Rc::clone(&self.writer),
            collection: self.collection.clone(),
        }
    }
}

impl<K: Serialize + DeserializeOwned, V: Serialize + DeserializeOwned> Registration<K, V> {
    pub(crate) fn new(writer: Rc<Writer>, collection: Collection<K, V>) -> Self {
        Self { writer, collection }
    }

    pub fn collection(&self) -> &Collection<K, V> {
        &self.collection
    }

    /// What this micro's mirror holds for `key`, as the basis a write supersedes: its stamp when
    /// the mirror has seen the key (a tombstone included), `Concurrent` when it has not.
    fn basis(&self, key: &[u8]) -> WriteBasis {
        self.collection.mirror.borrow().write_basis(&key.to_vec())
    }

    /// Put `value` at `key`, superseding what this micro's mirror holds for it. `attachments`
    /// are the publications [`Attachment`] indices in `value` refer to. The write is complete
    /// when the future resolves: in-process it already is by the first poll, while a guest's
    /// crosses the component boundary.
    pub async fn put(
        &self,
        key: &K,
        value: &V,
        attachments: Vec<RowAttachment>,
    ) -> Result<Stamp, Error> {
        let key = encode(key);
        let basis = self.basis(&key);
        let body = encode(value);
        match &*self.writer {
            #[cfg(feature = "in_process")]
            Writer::InProcess(writer) => {
                let Minter::InProcess(capabilities) = &*self.collection.minter else {
                    unreachable!("an in-process writer belongs to an in-process collection")
                };
                let attachments = attachments
                    .into_iter()
                    .map(|attachment| attachment.into_in_process(capabilities))
                    .collect::<Result<_, _>>()?;
                Ok(writer.put_value(
                    key,
                    basis,
                    clay_guest_session::collections::CollectionValue { body, attachments },
                )?)
            }
            #[cfg(feature = "guest")]
            Writer::Guest(writer) => crate::guest::put(writer, key, basis, body, attachments).await,
        }
    }

    /// Delete `key`, superseding what this micro's mirror holds for it.
    pub async fn delete(&self, key: &K) -> Result<Stamp, Error> {
        let key = encode(key);
        let basis = self.basis(&key);
        match &*self.writer {
            #[cfg(feature = "in_process")]
            Writer::InProcess(writer) => Ok(writer.delete(key, basis)?),
            #[cfg(feature = "guest")]
            Writer::Guest(writer) => crate::guest::delete(writer, key, basis).await,
        }
    }
}
