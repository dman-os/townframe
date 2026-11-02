use std::collections::BTreeMap;

use proptest::prelude::*;
use proptest::test_runner::RngSeed;

use super::*;

struct Client {
    selector: Selector,
    lease: Lease,
    mirror: Mirror<u8, Row>,
}

impl Client {
    fn new(source: &TestProjection, selector: Selector) -> Self {
        Self {
            selector,
            lease: source.acquire(selector, None).unwrap(),
            mirror: Mirror::empty(),
        }
    }

    fn reopen(&mut self, source: &TestProjection) {
        let resume = self.mirror.cursor().cloned();
        self.lease = match source.acquire(self.selector, resume.clone()) {
            Ok(lease) => {
                self.mirror.begin_lease(resume.as_ref());
                lease
            }
            Err(AcquireError::ResetRequired) => {
                self.mirror.clear();
                source.acquire(self.selector, None).unwrap()
            }
            Err(error) => panic!("unexpected resume error: {error:?}"),
        };
        assert_eq!(self.mirror.committed(), None);
        assert!(!self.mirror.bootstrap_complete());
    }

    fn settle(&mut self, source: &TestProjection, limits: ReadLimits, replay: bool) {
        for _ in 0..32 {
            let ticket = self.mirror.begin_read();
            let committed = ticket.committed().cloned();
            let result = poll(
                &mut self.lease.read(committed.clone(), limits),
                Waker::noop(),
            );
            let page = match result {
                Poll::Pending => {
                    assert!(self.mirror.cancel_read(&ticket));
                    return;
                }
                Poll::Ready(Err(ReadError::ResetRequired)) => {
                    self.mirror.clear();
                    self.lease = source.acquire(self.selector, None).unwrap();
                    continue;
                }
                Poll::Ready(Err(error)) => panic!("unexpected read error: {error:?}"),
                Poll::Ready(Ok(page)) => page,
            };
            assert_ne!(committed.as_ref(), Some(&page.through));
            assert!(!self.mirror.bootstrap_complete() || page.bootstrap_complete);
            if replay {
                assert!(self.mirror.cancel_read(&ticket));
                let wide_ticket = self.mirror.begin_read();
                let wide = read(&mut self.lease, committed.clone(), ReadLimits::default()).unwrap();
                assert!(wide.transactions.starts_with(&page.transactions));
                assert!(self.mirror.cancel_read(&wide_ticket));
                let retry = self.mirror.begin_read();
                let retried = read(&mut self.lease, committed, limits).unwrap();
                assert_eq!(retried, page);
                assert!(!self.mirror.apply(&ticket, &page));
                assert!(!self.mirror.apply(&wide_ticket, &wide));
                assert!(self.mirror.apply(&retry, &retried));
                assert!(!self.mirror.apply(&retry, &retried));
            } else {
                assert!(self.mirror.apply(&ticket, &page));
            }
        }
        panic!("bounded history failed to reach a pending read");
    }

    fn assert_matches(&self, authority: &BTreeMap<u8, Entry<Row>>) {
        let selected: BTreeMap<_, _> = authority
            .iter()
            .filter(|(_, entry)| self.selector == Selector::All || entry.value.group == Group::X)
            .map(|(key, entry)| (*key, entry.clone()))
            .collect();
        let actual: BTreeMap<_, _> = self
            .mirror
            .rows()
            .into_iter()
            .map(|(key, _)| (key, self.mirror.row(&key).unwrap().clone()))
            .collect();
        assert_eq!(actual, selected);
        assert!(self.mirror.bootstrap_complete());
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 48,
        max_shrink_iters: 512,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(0x0170_0240),
        .. ProptestConfig::default()
    })]

    #[test]
    fn selected_mirrors_converge_to_an_independent_current_state_model(
        capacity in 1usize..6,
        rounds in prop::collection::vec((
            prop::collection::vec(prop::collection::btree_map(0u8..3, 0u8..3, 0..4), 0..5),
            0u32..4,
            0u32..4,
            any::<bool>(),
            any::<bool>(),
        ), 1..9),
    ) {
        let (source, interests) = projection(capacity);
        let mut clients = [Client::new(&source, Selector::X), Client::new(&source, Selector::All)];
        let mut authority = BTreeMap::new();
        let mut seq = 0;
        for (burst, transactions, changes, replay, resume) in rounds {
            for transaction in burst {
                let mut published = Vec::new();
                for (key, operation) in transaction {
                    seq += 1;
                    if operation == 0 {
                        authority.remove(&key);
                        published.push(remove(key, seq));
                    } else {
                        let group = if operation == 1 { Group::X } else { Group::Y };
                        authority.insert(key, Entry {
                            version: Version::new(1, seq),
                            value: Row { group, value: seq },
                            evidence: if seq.is_multiple_of(2) { RowEvidence::Authoritative } else { RowEvidence::Pending },
                            write_context: Some(vec![key, 255]),
                        });
                        published.push(upsert(key, seq, group));
                    }
                }
                source.publish(published);
            }
            let limits = ReadLimits {
                max_transactions: (transactions != 0).then_some(transactions),
                max_changes: (changes != 0).then_some(changes),
            };
            for client in &mut clients {
                if resume {
                    client.reopen(&source);
                }
                client.settle(&source, limits, replay);
                client.assert_matches(&authority);
            }
            prop_assert_eq!(interests.load(Ordering::SeqCst), 2);
        }
        drop(clients);
        prop_assert_eq!(interests.load(Ordering::SeqCst), 0);
    }
}
