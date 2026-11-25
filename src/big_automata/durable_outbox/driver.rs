//! The outbox driver: one loop that follows the wants table, owns the [`Outbox`] machine and
//! runs its commands against the backend. Runtime-agnostic as [`crate::driver`] is: it spawns
//! nothing and puts no `Send` bound on the backend's futures; [`OutboxHandle::start`] returns
//! the loop for the embedder to spawn (`tokio::spawn` natively, `spawn_local` in the browser).
//!
//! The loop's inputs are one at a time, and the machine is never borrowed across an await:
//!
//! - **the table**: a [`RevStream`] over the whole table from rev 0 (law 4). Its arm is always
//!   polled; only the machine's budgets gate dispatch. After the machine has taken a page in,
//!   the loop reports the page's cursor to the table (law 14) and, every `prune_every` revs
//!   of progress, prunes.
//! - **connections**: [`OutboxHandle::peer_connected`] and `peer_disconnected`.
//! - **calls**: a fetch or a landing the machine issued, polled in a set of futures whose type
//!   is inferred, never boxed. A fetch is cancelled by aborting its future, which is dropped
//!   before the loop is told, so the call's `Cancelled` report is the truth that the work
//!   stopped; a landing is never aborted (law 13).
//! - **time**: the machine's next backoff.
//!
//! The loop ends, cancelling every fetch, when the last handle drops. A panic in it is a bug,
//! which the embedder's spawn surfaces: a store error crashes it (law 15), a fetch error is
//! logged by [`OutboxBackend`]'s caller side and becomes a failed call.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use futures::FutureExt;
use futures::StreamExt;
use futures::future::AbortHandle;
use futures::future::AbortRegistration;
use futures::future::Abortable;
use futures::stream::FuturesUnordered;

use crate::Clock;
use crate::interlude::*;
use crate::okv::OkvStore;
use crate::okv::Selector;
use crate::rev::Limits;
use crate::rev::Page;
use crate::rev::Rev;
use crate::rev::RevStream;

use super::CallId;
use super::Command;
use super::Config;
use super::Ended;
use super::Fetched;
use super::Job;
use super::Outbox;
use super::Version;
use super::Want;

/// What the outbox fetches with. No `Send` bound on the futures, as for
/// [`SyncBackend`](crate::driver::SyncBackend).
pub trait OutboxBackend {
    /// Fetch the job's object from its provider and make the content durable. May be long;
    /// cancel-safe (the driver drops the future to cancel it) and idempotent (a crash after
    /// the content is durable and before the landing fetches again). An error is a network
    /// error: the call fails and the machine backs off. When the content lands in the want
    /// table's own database the backend writes it in the transaction that also calls
    /// [`WantsTable::land`](super::WantsTable::land), and answers [`Fetched::Landed`].
    fn fetch(&self, job: &Job) -> impl Future<Output = Res<Fetched>>;

    /// Land the want: [`WantsTable::land`](super::WantsTable::land) in a transaction the
    /// backend begins and commits, retrying a conflict. Whether it deleted the row. Never
    /// cancelled. An error is a store error, which crashes the loop.
    fn land(&self, obj: &ObjKey, version: Version) -> impl Future<Output = Res<bool>>;
}

#[derive(Debug, Clone)]
pub struct DriverConfig {
    pub outbox: Config,
    /// The name the outbox registers under as an okv consumer of the table. One per outbox:
    /// two outboxes on one table with the same name would share a cursor.
    pub consumer: String,
    /// The most rows one page of the table holds (law 4's scan reads the whole table in pages).
    pub page_entries: NonZeroUsize,
    /// Prune the table's tombstones after this many revs of progress (law 14).
    pub prune_every: Rev,
}

impl DriverConfig {
    pub fn new(consumer: impl Into<String>) -> Self {
        Self {
            outbox: Config::default(),
            consumer: consumer.into(),
            page_entries: NonZeroUsize::new(256).expect(ERROR_IMPOSSIBLE),
            prune_every: 1024,
        }
    }
}

enum Input {
    Connected(PeerKey),
    Disconnected(PeerKey),
}

/// The handle of a running outbox. Clones share the driver; the loop ends when the last drops.
#[derive(Clone)]
pub struct OutboxHandle {
    inputs: async_channel::Sender<Input>,
}

impl OutboxHandle {
    /// The handle and the loop, which the embedder spawns; nothing runs until it is polled. The
    /// loop is `Send` exactly when the store, the backend and the backend's futures are.
    pub fn start<S, B>(
        config: DriverConfig,
        clock: Clock,
        store: S,
        backend: B,
    ) -> (Self, impl Future<Output = ()> + use<S, B>)
    where
        S: OkvStore<Key = Vec<u8>, Value = Vec<u8>> + Clone + Sync + 'static,
        B: OutboxBackend,
    {
        let (inputs, receiver) = async_channel::unbounded();
        let driver = Driver {
            machine: Outbox::new(config.outbox.clone()),
            config,
            clock,
            store,
            backend: Arc::new(backend),
            aborts: default(),
        };
        (Self { inputs }, driver.run(receiver))
    }

    /// A peer is connected: wants it advertises may be fetched from it.
    pub fn peer_connected(&self, peer: PeerKey) {
        self.send(Input::Connected(peer));
    }

    /// A peer is gone: fetches running against it are cancelled (law 9).
    pub fn peer_disconnected(&self, peer: PeerKey) {
        self.send(Input::Disconnected(peer));
    }

    fn send(&self, input: Input) {
        // this handle keeps the channel open, so the loop only goes away by panicking
        self.inputs
            .try_send(input)
            .expect("the outbox loop outlives its handles");
    }
}

/// What a call came back with.
enum Done {
    Fetch(CallId, Ended),
    Land {
        obj: ObjKey,
        version: Version,
        deleted: bool,
    },
}

enum Event {
    Input(Option<Input>),
    Done(Done),
    /// The page the stream filled.
    Page,
    Timer,
}

struct Driver<S, B> {
    config: DriverConfig,
    clock: Clock,
    store: S,
    backend: Arc<B>,
    machine: Outbox,
    aborts: BTreeMap<CallId, AbortHandle>,
}

impl<S, B> Driver<S, B>
where
    S: OkvStore<Key = Vec<u8>, Value = Vec<u8>> + Clone + Sync + 'static,
    B: OutboxBackend,
{
    async fn run(mut self, inputs: async_channel::Receiver<Input>) {
        // Registering holds the table's tombstones from the first moment; the stream still
        // starts at 0, because the index is rebuilt from the live rows (law 4).
        self.store
            .register_consumer(&self.config.consumer)
            .await
            .expect("the wants table failed to register the outbox");
        let limits = Limits {
            max_entries: self.config.page_entries,
            max_bytes: None,
        };
        let mut stream = RevStream::new(self.store.clone(), Selector::All, 0);
        let mut page = Page::default();
        let mut pruned_at = 0;
        let mut calls = FuturesUnordered::new();
        loop {
            calls.extend(self.flush());
            let due = self.machine.next_due();
            let now = (self.clock)();
            let event = {
                let call = async {
                    if calls.is_empty() {
                        std::future::pending().await
                    } else {
                        calls.next().await.expect("a non-empty set yields")
                    }
                }
                .fuse();
                let input = inputs.recv().fuse();
                // Law 4: always polled; the machine's budgets gate dispatch, not this.
                let rows = stream.next_into(&mut page, limits).fuse();
                let timer = async {
                    match due {
                        Some(at) => n0_future::time::sleep(at.saturating_duration_since(now)).await,
                        None => std::future::pending().await,
                    }
                }
                .fuse();
                futures::pin_mut!(call, input, rows, timer);
                futures::select_biased! {
                    done = call => Event::Done(done),
                    input = input => Event::Input(input.ok()),
                    rows = rows => {
                        rows.expect("the wants table failed a read");
                        Event::Page
                    }
                    () = timer => Event::Timer,
                }
            };
            match event {
                Event::Input(Some(Input::Connected(peer))) => self.machine.peer_connected(peer),
                Event::Input(Some(Input::Disconnected(peer))) => {
                    self.machine.peer_disconnected(&peer);
                }
                Event::Input(None) => break,
                Event::Done(done) => self.feed(done),
                Event::Page => {
                    self.take_page(&mut page, &mut pruned_at).await;
                }
                Event::Timer => {}
            }
        }
    }

    /// Feed the machine a page of the table, then tell the table how far this consumer has
    /// gone, and prune now and then (law 14).
    async fn take_page(&mut self, page: &mut Page<Vec<u8>, Vec<u8>>, pruned_at: &mut Rev) {
        self.machine.rows(page.entries.drain(..).map(|entry| {
            (
                ObjKey::new(&entry.key),
                entry.value.map(|row| Want::decode(&row)),
            )
        }));
        self.store
            .report_cursor(&self.config.consumer, page.through)
            .await
            .expect("the wants table failed to take a cursor");
        if page.through - *pruned_at >= self.config.prune_every {
            self.store
                .prune()
                .await
                .expect("the wants table failed to prune");
            *pruned_at = page.through;
        }
    }

    /// An outcome goes to the machine once.
    fn feed(&mut self, done: Done) {
        let now = (self.clock)();
        match done {
            Done::Fetch(call, ended) => {
                self.aborts
                    .remove(&call)
                    .expect("a call's outcome is reported once");
                self.machine.fetch_ended(now, call, ended);
            }
            Done::Land {
                obj,
                version,
                deleted,
            } => self.machine.landed(&obj, version, deleted),
        }
    }

    /// The commands the machine owes after every input: the futures of the calls it issued.
    fn flush(&mut self) -> Vec<impl Future<Output = Done> + use<S, B>> {
        let mut cmds = vec![];
        self.machine.flush((self.clock)(), &mut cmds);
        let mut issued = vec![];
        for cmd in cmds {
            match cmd {
                Command::Cancel { call } => self.aborts[&call].abort(),
                Command::Fetch { call, job } => {
                    let (handle, registration) = AbortHandle::new_pair();
                    self.aborts.insert(call, handle);
                    issued.push(run_call(
                        Arc::clone(&self.backend),
                        Command::Fetch { call, job },
                        Some(registration),
                    ));
                }
                land @ Command::Land { .. } => {
                    issued.push(run_call(Arc::clone(&self.backend), land, None));
                }
            }
        }
        issued
    }
}

/// Run a fetch or a landing to its outcome. Only a fetch has an abort registration.
async fn run_call<B: OutboxBackend>(
    backend: Arc<B>,
    cmd: Command,
    abort: Option<AbortRegistration>,
) -> Done {
    match (cmd, abort) {
        (Command::Fetch { call, job }, Some(registration)) => {
            let fetch = Abortable::new(backend.fetch(&job), registration).await;
            Done::Fetch(
                call,
                match fetch {
                    Ok(Ok(fetched)) => Ended::Fetched(fetched),
                    Ok(Err(error)) => {
                        tracing::warn!(?error, obj = %job.obj, "an outbox fetch failed; the machine retries it");
                        Ended::Failed
                    }
                    Err(_aborted) => Ended::Cancelled,
                },
            )
        }
        (Command::Land { obj, version }, None) => {
            let deleted = backend
                .land(&obj, version)
                .await
                .expect("the wants table failed a landing");
            Done::Land {
                obj,
                version,
                deleted,
            }
        }
        (cmd, _) => unreachable!("not a call: {cmd:?}"),
    }
}
