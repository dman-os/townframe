//! Test-only driver for `PollDatums`.
//!
//! The service keeps no state between polls (ADR 025 §6), so a test that wants streaming
//! semantics keeps the cursor and the un-consumed page itself. Both the doc e2e and the drawer
//! e2e drive the same operation through the same client, so the driver lives here rather than
//! in either test module.

use big_cabinet_core::DatumKey;
use big_cabinet_core::DatumTag;
use big_cabinet_core::DocId;
use big_cabinet_core::authority::CapId;
use big_cabinet_core::doc::DatumEvent;
use big_cabinet_core::doc::DatumPage;
use big_cabinet_core::doc::DocsError;
use big_cabinet_core::doc::DocsProtocol;
use big_cabinet_core::doc::ObservedDatum;
use big_cabinet_core::doc::PollDatums;
use big_cabinet_core::query::DatumSelection;
use big_cabinet_core::query::Predicate;
use service_client_native_rs::Client;
use service_utils_rs::ClientError;
use service_utils_rs::RequestContext;
use std::collections::BTreeMap;
use std::collections::VecDeque;

/// How long a waiting poll is held open when a test asks for the next event.
const HOLD_MS: u32 = 5_000;

pub(crate) struct Poller {
    client: Client<DocsProtocol>,
    capability: CapId,
    predicate: Predicate,
    datums: DatumSelection,
    after: u64,
    replayed: bool,
    pending: VecDeque<DatumEvent>,
}

impl Poller {
    pub(crate) fn new(client: &Client<DocsProtocol>, request: PollDatums) -> Self {
        Self {
            client: client.clone(),
            capability: request.capability,
            predicate: request.predicate,
            datums: request.datums,
            after: request.after,
            replayed: false,
            pending: VecDeque::new(),
        }
    }

    pub(crate) fn doc(client: &Client<DocsProtocol>, capability: CapId, doc_id: DocId) -> Self {
        Self::new(
            client,
            PollDatums {
                capability,
                predicate: Predicate::IsDoc { doc_id },
                datums: DatumSelection::All,
                after: 0,
                limit: 0,
                hold_ms: 0,
            },
        )
    }

    pub(crate) fn tag(client: &Client<DocsProtocol>, capability: CapId, tag: DatumTag) -> Self {
        Self::new(
            client,
            PollDatums {
                capability,
                predicate: Predicate::HasTag { tag },
                datums: DatumSelection::All,
                after: 0,
                limit: 0,
                hold_ms: 0,
            },
        )
    }

    fn request(&self, hold_ms: u32) -> PollDatums {
        PollDatums {
            capability: self.capability,
            predicate: self.predicate.clone(),
            datums: self.datums.clone(),
            after: self.after,
            limit: 0,
            hold_ms,
        }
    }

    /// The cursor the caller last consumed.
    pub(crate) fn cursor(&self) -> u64 {
        self.after
    }

    /// One poll. The cursor advances to `through` even when the page has no events.
    pub(crate) async fn page(&mut self, hold_ms: u32) -> Result<DatumPage, ClientError<DocsError>> {
        let page = self
            .client
            .call(RequestContext::root(), self.request(hold_ms))
            .await?;
        self.after = self.after.max(page.through);
        Ok(page)
    }

    /// The next event, replaying from the cursor before holding for live changes.
    ///
    /// `None` means a held poll expired with nothing to report; a caller that wants to keep
    /// watching polls again.
    pub(crate) async fn next(&mut self) -> Result<Option<DatumEvent>, ClientError<DocsError>> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Ok(Some(event));
            }
            if !self.replayed {
                let page = self.page(0).await?;
                self.pending.extend(page.events);
                self.replayed = page.caught_up;
                if !self.pending.is_empty() || !self.replayed {
                    continue;
                }
            }
            let page = self.page(HOLD_MS).await?;
            self.pending.extend(page.events);
            if self.pending.is_empty() {
                return Ok(None);
            }
        }
    }

    /// The next row fact, skipping a `leave`.
    pub(crate) async fn next_datum(&mut self) -> (DocId, DatumKey, ObservedDatum) {
        loop {
            match self.next().await.expect("poll failed").expect("poll ended") {
                DatumEvent::Datum {
                    doc_id,
                    datum_key,
                    datum,
                    ..
                } => return (doc_id, datum_key, datum),
                DatumEvent::Leave { .. } => {}
            }
        }
    }
}

/// A current-state query: replay from zero to `caught_up` and fold the events into the docs that
/// still match, each with the selected datums still present.
pub(crate) async fn query(
    client: &Client<DocsProtocol>,
    capability: CapId,
    predicate: Predicate,
    datums: DatumSelection,
) -> Result<BTreeMap<DocId, BTreeMap<DatumKey, ObservedDatum>>, ClientError<DocsError>> {
    let mut poller = Poller::new(
        client,
        PollDatums {
            capability,
            predicate,
            datums,
            after: 0,
            limit: 0,
            hold_ms: 0,
        },
    );
    let mut state: BTreeMap<DocId, BTreeMap<DatumKey, ObservedDatum>> = BTreeMap::new();
    loop {
        let page = poller.page(0).await?;
        for event in page.events {
            match event {
                DatumEvent::Datum {
                    doc_id,
                    datum_key,
                    datum,
                    ..
                } => match datum {
                    ObservedDatum::Present { .. } => {
                        state.entry(doc_id).or_default().insert(datum_key, datum);
                    }
                    ObservedDatum::Removed { .. } => {
                        if let Some(datums) = state.get_mut(&doc_id) {
                            datums.remove(&datum_key);
                        }
                    }
                },
                DatumEvent::Leave { doc_id, .. } => {
                    state.remove(&doc_id);
                }
            }
        }
        if page.caught_up {
            return Ok(state);
        }
    }
}
