use super::*;
use proptest::prelude::*;
use std::collections::BTreeMap;

const INVENTORY_COUNT: usize = 2;
const DIGEST_COUNT: usize = 2;
const PEER_COUNT: usize = 2;

#[derive(Clone, Copy, Debug, Default)]
struct Source {
    seen: bool,
    present: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct PairModel {
    created: bool,
    pinned: bool,
    observed: bool,
    bytes: bool,
    fetching: bool,
}

#[derive(Clone, Debug)]
struct AdmissionModel {
    selected_by: [[bool; 2]; INVENTORY_COUNT],
    readable: [bool; INVENTORY_COUNT],
    pin_revision: [u64; INVENTORY_COUNT],
    pins_complete: [bool; INVENTORY_COUNT],
    pairs: [[PairModel; DIGEST_COUNT]; INVENTORY_COUNT],
    sources: [[[Source; PEER_COUNT]; DIGEST_COUNT]; INVENTORY_COUNT],
}

impl Default for AdmissionModel {
    fn default() -> Self {
        Self {
            selected_by: [[false; 2]; INVENTORY_COUNT],
            readable: [false; INVENTORY_COUNT],
            pin_revision: [0; INVENTORY_COUNT],
            pins_complete: [false; INVENTORY_COUNT],
            pairs: [[PairModel::default(); DIGEST_COUNT]; INVENTORY_COUNT],
            sources: [[[Source::default(); PEER_COUNT]; DIGEST_COUNT]; INVENTORY_COUNT],
        }
    }
}

impl AdmissionModel {
    fn selected(&self, inventory: usize) -> bool {
        self.selected_by[inventory]
            .into_iter()
            .any(|selected| selected)
    }

    fn observed(&self, inventory: usize, digest: usize) -> bool {
        self.sources[inventory][digest]
            .into_iter()
            .any(|source| source.seen && source.present)
    }

    fn invalidate_fetches(&mut self, inventory: usize) {
        for pair in &mut self.pairs[inventory] {
            pair.fetching = false;
        }
    }

    fn select(&mut self, inventory: usize, owner: usize, selected: bool) {
        if self.selected_by[inventory][owner] != selected {
            self.selected_by[inventory][owner] = selected;
            self.invalidate_fetches(inventory);
        }
    }

    fn authorize(&mut self, inventory: usize, readable: bool) {
        if self.readable[inventory] != readable {
            self.readable[inventory] = readable;
            self.invalidate_fetches(inventory);
        }
    }

    fn pins(&mut self, inventory: usize, revision: u64, digest: usize, pin: bool, complete: bool) {
        let previous_revision = self.pin_revision[inventory];
        let previous_complete = self.pins_complete[inventory];
        if revision < previous_revision
            || (revision == previous_revision && (previous_complete || !complete))
        {
            return;
        }

        let mut changed = false;
        if complete {
            for (index, pair) in self.pairs[inventory].iter_mut().enumerate() {
                let pinned = index == digest && pin;
                if pair.created && pair.pinned != pinned {
                    pair.pinned = pinned;
                    changed = true;
                }
            }
        }
        if pin {
            let pair = &mut self.pairs[inventory][digest];
            pair.created = true;
            if !pair.pinned {
                pair.pinned = true;
                changed = true;
            }
        }
        if changed {
            self.invalidate_fetches(inventory);
        }
        self.pin_revision[inventory] = revision;
        self.pins_complete[inventory] = complete;
    }

    fn observe(
        &mut self,
        inventory: usize,
        digest: usize,
        peer: usize,
        _revision: u64,
        present: bool,
    ) {
        // Big Sync delivers observations in cursor order and this backend treats
        // the durable (inventory, digest, peer) row as the idempotency key. The
        // compatibility revision argument is therefore intentionally ignored.
        let source = &mut self.sources[inventory][digest][peer];
        source.seen = true;
        source.present = present;
        let observed = self.observed(inventory, digest);
        let pair = &mut self.pairs[inventory][digest];
        pair.created = true;
        pair.observed = observed;
        self.invalidate_fetches(inventory);
    }

    fn available(&mut self, inventory: usize, digest: usize, available: bool) {
        if self.pairs[inventory][digest].created {
            self.pairs[inventory][digest].bytes = available;
        }
    }

    fn restart(&mut self) {
        self.readable = [false; INVENTORY_COUNT];
        for inventory in 0..INVENTORY_COUNT {
            self.invalidate_fetches(inventory);
        }
    }

    fn refresh_observed(&mut self) {
        for inventory in 0..INVENTORY_COUNT {
            for digest in 0..DIGEST_COUNT {
                self.pairs[inventory][digest].observed = self.observed(inventory, digest);
            }
        }
    }
}

#[derive(Clone, Debug)]
enum Command {
    Select {
        inventory: usize,
        owner: usize,
        selected: bool,
    },
    Authorize {
        inventory: usize,
        readable: bool,
    },
    Pins {
        inventory: usize,
        revision: u64,
        digest: usize,
        pin: bool,
        complete: bool,
    },
    Observe {
        inventory: usize,
        digest: usize,
        peer: usize,
        revision: u64,
        present: bool,
    },
    Available {
        inventory: usize,
        digest: usize,
        available: bool,
    },
    Begin {
        inventory: usize,
        digest: usize,
    },
    Complete {
        authorized: bool,
    },
    Fail,
    Restart,
}

fn command_strategy() -> impl Strategy<Value = Command> {
    prop_oneof![
        (0usize..INVENTORY_COUNT, 0usize..2, any::<bool>()).prop_map(
            |(inventory, owner, selected)| Command::Select {
                inventory,
                owner,
                selected,
            }
        ),
        (0usize..INVENTORY_COUNT, any::<bool>()).prop_map(|(inventory, readable)| {
            Command::Authorize {
                inventory,
                readable,
            }
        }),
        (
            0usize..INVENTORY_COUNT,
            0u64..8,
            0usize..DIGEST_COUNT,
            any::<bool>(),
            any::<bool>(),
        )
            .prop_map(
                |(inventory, revision, digest, pin, complete)| Command::Pins {
                    inventory,
                    revision,
                    digest,
                    pin,
                    complete,
                }
            ),
        (
            0usize..INVENTORY_COUNT,
            0usize..DIGEST_COUNT,
            0usize..PEER_COUNT,
            0u64..8,
            any::<bool>(),
        )
            .prop_map(
                |(inventory, digest, peer, revision, present)| Command::Observe {
                    inventory,
                    digest,
                    peer,
                    revision,
                    present,
                }
            ),
        (0usize..INVENTORY_COUNT, 0usize..DIGEST_COUNT, any::<bool>()).prop_map(
            |(inventory, digest, available)| Command::Available {
                inventory,
                digest,
                available,
            },
        ),
        (0usize..INVENTORY_COUNT, 0usize..DIGEST_COUNT)
            .prop_map(|(inventory, digest)| Command::Begin { inventory, digest }),
        any::<bool>().prop_map(|authorized| Command::Complete { authorized }),
        Just(Command::Fail),
        Just(Command::Restart),
    ]
}

fn database() -> Database {
    sqlite_rs::SqliteRuntime::new(Default::default())
        .ephemeral(&[])
        .expect("sqlite database")
}

fn inventory_id(index: usize) -> DocId {
    DocId::new([83 + index as u8; 32])
}

fn digest(index: usize) -> BlobId {
    BlobId::new([84 + index as u8; 32])
}

fn peer(index: usize) -> PeerKey {
    PeerKey::new([85 + index as u8; 32])
}

async fn assert_model(admission: &Admission, model: &AdmissionModel) {
    for inventory_index in 0..INVENTORY_COUNT {
        for digest_index in 0..DIGEST_COUNT {
            let expected = model.pairs[inventory_index][digest_index];
            let actual = admission
                .state(&inventory_id(inventory_index), digest(digest_index))
                .await
                .expect("read admission state");
            if !expected.created {
                assert!(actual.is_none(), "unexpected pair row for model {model:?}");
                continue;
            }
            let actual = actual.expect("missing pair row");
            assert_eq!(actual.selected, model.selected(inventory_index), "selected");
            assert_eq!(actual.pinned, expected.pinned, "pinned");
            assert_eq!(
                actual.observed,
                model.observed(inventory_index, digest_index),
                "observed"
            );
            assert_eq!(actual.readable, model.readable[inventory_index], "readable");
            assert_eq!(actual.bytes, expected.bytes, "bytes");
            assert_eq!(actual.fetching, expected.fetching, "fetching");
            assert_eq!(
                actual.fetch_eligible(),
                model.selected(inventory_index)
                    && expected.pinned
                    && model.observed(inventory_index, digest_index)
                    && model.readable[inventory_index]
            );
            assert_eq!(
                actual.advertisable(),
                actual.selected
                    && actual.pinned
                    && actual.observed
                    && actual.readable
                    && actual.bytes
            );
        }
    }
}

async fn apply_authorize(
    admission: &Admission,
    model: &mut AdmissionModel,
    inventory: usize,
    readable: bool,
) {
    model.authorize(inventory, readable);
    admission
        .authorize(&inventory_id(inventory), readable)
        .await
        .expect("authorize inventory");
}

async fn run_script(script: Vec<Command>) {
    let sql = database();
    let (mut admission, mut stop) = Admission::boot(sql.clone()).await.expect("boot admission");
    let mut model = AdmissionModel::default();
    let mut last_attempt = None;
    let mut attempt_live = false;
    let mut last_target = None;

    for command in script {
        match command {
            Command::Select {
                inventory,
                owner,
                selected,
            } => {
                model.select(inventory, owner, selected);
                admission
                    .select(
                        &inventory_id(inventory),
                        &format!("owner-{owner}"),
                        selected,
                    )
                    .await
                    .expect("select inventory");
            }

            Command::Authorize { inventory, readable } => {
                apply_authorize(&admission, &mut model, inventory, readable).await;
            }
            Command::Pins {
                inventory,
                revision,
                digest: digest_index,
                pin,
                complete,
            } => {
                model.pins(inventory, revision, digest_index, pin, complete);
                let pins = if pin {
                    BTreeMap::from([(digest(digest_index), 42)])
                } else {
                    BTreeMap::new()
                };
                admission
                    .pins(&inventory_id(inventory), revision, &pins, complete)
                    .await
                    .expect("observe pins");
            }
            Command::Observe {
                inventory,
                digest: digest_index,
                peer: peer_index,
                revision,
                present,
            } => {
                model.observe(inventory, digest_index, peer_index, revision, present);
                admission
                    .observe(
                        &inventory_id(inventory),
                        digest(digest_index),
                        &peer(peer_index),
                        revision,
                        present,
                        None,
                    )
                    .await
                    .expect("observe object");
            }
            Command::Available {
                inventory,
                digest: digest_index,
                available,
            } => {
                model.available(inventory, digest_index, available);
                admission
                    .available(&inventory_id(inventory), digest(digest_index), available)
                    .await
                    .expect("set availability");
            }
            Command::Begin {
                inventory,
                digest: digest_index,
            } => {
                let pair = model.pairs[inventory][digest_index];
                let expected = pair.created
                    && model.selected(inventory)
                    && pair.pinned
                    && model.observed(inventory, digest_index)
                    && model.readable[inventory]
                    && !pair.bytes
                    && !pair.fetching;
                let actual = admission
                    .begin(&inventory_id(inventory), digest(digest_index))
                    .await
                    .expect("begin fetch");
                assert_eq!(actual.is_some(), expected, "begin eligibility");
                if let Some(attempt) = actual {
                    assert!(expected);
                    last_target = Some((inventory, digest_index));
                    last_attempt = Some(attempt);
                    attempt_live = true;
                    model.pairs[inventory][digest_index].fetching = true;
                }
            }
            Command::Complete { authorized } => {
                let Some(attempt) = last_attempt.as_ref() else {
                    continue;
                };
                let Some((inventory_index, digest_index)) = last_target else {
                    continue;
                };
                let expected = attempt_live
                    && authorized
                    && model.selected(inventory_index)
                    && model.pairs[inventory_index][digest_index].pinned
                    && model.observed(inventory_index, digest_index)
                    && model.readable[inventory_index]
                    && model.pairs[inventory_index][digest_index].fetching;
                let actual = admission
                    .complete(attempt, authorized)
                    .await
                    .expect("complete fetch");
                assert_eq!(actual, expected, "completion fencing");
                attempt_live = false;
                model.pairs[inventory_index][digest_index].fetching = false;
                if expected {
                    model.pairs[inventory_index][digest_index].bytes = true;
                }
            }
            Command::Fail => {
                let Some(attempt) = last_attempt.as_ref() else {
                    continue;
                };
                admission.fail(attempt).await.expect("fail fetch");
                if attempt_live {
                    let Some((inventory_index, digest_index)) = last_target else {
                        continue;
                    };
                    model.pairs[inventory_index][digest_index].fetching = false;
                }
                attempt_live = false;
            }
            Command::Restart => {
                stop.stop().await;
                drop(admission);
                let (next, next_stop) = Admission::boot(sql.clone())
                    .await
                    .expect("reboot admission");
                next.recover().await.expect("recover admission");
                admission = next;
                stop = next_stop;
                model.restart();
                attempt_live = false;
            }
        }
        model.refresh_observed();
        assert_model(&admission, &model).await;
    }

    stop.stop().await;
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        max_shrink_iters: 256,
        .. ProptestConfig::default()
    })]

    #[test]
    fn generated_admission_commands_are_equivalent_to_the_reference_model(
        script in prop::collection::vec(command_strategy(), 1..48)
    ) {
        tokio::runtime::Runtime::new()
            .expect("tokio runtime")
            .block_on(run_script(script));
    }
}
