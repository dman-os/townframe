//! Synchronous keyed batch accumulation with caller-supplied time.
//!
//! [`KeyedBatcher`] accumulates values per key and lets the surrounding actor
//! decide when to flush. It owns no tasks, no clocks, and no delivery: the
//! caller passes `now` and performs the send. Two policies are provided:
//!
//! - [`DebouncePolicy`]: trailing-edge delivery — a quiet window after the last
//!   push, capped by a hard maximum latency so continuous traffic cannot
//!   starve delivery.
//! - [`BatchPolicy`]: first-item deadline with size thresholds — the batch is
//!   due `max_latency` after the first push, or immediately once it reaches
//!   `max_items`/`max_bytes`.
//!
//! Both share the same accumulation machinery; only the flush decision
//! differs. The primitive knows nothing about Keyhive, BigSync, RPC, Tokio, or
//! the concrete batch representation.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Decides when a keyed batch is due.
pub trait FlushPolicy {
    /// Effective deadline for a batch whose first push was `first_push` and
    /// whose most recent push was `last_push`.
    fn deadline(&self, first_push: Instant, last_push: Instant) -> Instant;

    /// Whether a push should make the batch due immediately (size thresholds).
    ///
    /// `pushes` is the number of pushes accumulated for the key; `bytes` is
    /// the accumulated value's size as reported by the batcher's `size_of`
    /// closure. The default is `false` (deadline-driven only).
    fn flush_immediately(&self, _pushes: usize, _bytes: usize) -> bool {
        false
    }
}

/// Trailing-edge debounce: deliver `quiet_window` after the last push, but no
/// later than `max_latency` after the first push.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DebouncePolicy {
    pub quiet_window: Duration,
    pub max_latency: Duration,
}

impl FlushPolicy for DebouncePolicy {
    fn deadline(&self, first_push: Instant, last_push: Instant) -> Instant {
        (last_push + self.quiet_window).min(first_push + self.max_latency)
    }
}

/// First-item deadline with size thresholds: due `max_latency` after the first
/// push, or immediately once `max_items` pushes or `max_bytes` accumulate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchPolicy {
    pub max_latency: Duration,
    pub max_items: usize,
    pub max_bytes: usize,
}

impl FlushPolicy for BatchPolicy {
    fn deadline(&self, first_push: Instant, _last_push: Instant) -> Instant {
        first_push + self.max_latency
    }

    fn flush_immediately(&self, pushes: usize, bytes: usize) -> bool {
        pushes >= self.max_items || bytes >= self.max_bytes
    }
}

struct PendingEntry<V> {
    value: V,
    first_push: Instant,
    last_push: Instant,
    deadline: Instant,
    pushes: usize,
    bytes: usize,
}

/// A synchronous keyed batch state machine.
///
/// Values accumulate per key; the policy decides when each key's batch is due.
/// The owner drives time (`push_with`/`take_due` take `now`) and performs
/// delivery, so the batcher can live inside an actor without owning tasks.
///
/// Deadlines are indexed by `(deadline, key)`, so `next_deadline` is O(1) and
/// `take_due` returns batches in deterministic order — equal deadlines are
/// ordered by key.
pub type SizeFn<V> = Box<dyn Fn(&V) -> usize + Send + Sync>;
pub type ReduceFn<V> = Box<dyn Fn(&mut V, V) + Send + Sync>;

pub struct KeyedBatcher<K, V, P> {
    policy: P,
    size_of: SizeFn<V>,
    reduce: ReduceFn<V>,
    pending: BTreeMap<K, PendingEntry<V>>,
    deadlines: BTreeMap<(Instant, K), ()>,
}

impl<K, V, P> KeyedBatcher<K, V, P>
where
    K: Ord + Clone,
    P: FlushPolicy,
{
    /// Create a batcher with the given policy.
    ///
    /// `size_of` reports the accumulated value's byte size; it is consulted
    /// after every merge and feeds `FlushPolicy::flush_immediately` (only
    /// [`BatchPolicy`] uses it today).
    ///
    /// `reduce` combines newly pushed values into an existing pending value.
    pub fn new(
        policy: P,
        size_of: impl Fn(&V) -> usize + Send + Sync + 'static,
        reduce: impl Fn(&mut V, V) + Send + Sync + 'static,
    ) -> Self {
        Self {
            policy,
            size_of: Box::new(size_of),
            reduce: Box::new(reduce),
            pending: BTreeMap::new(),
            deadlines: BTreeMap::new(),
        }
    }

    /// Push `value` for `key`, merging into any pending batch using the
    /// configured reducer.
    ///
    /// For a new key the value is inserted as-is; for an existing key `reduce`
    /// combines it with the accumulated value. The batch's deadline is
    /// recomputed from the policy; a policy that reports an immediate flush
    /// (size thresholds) makes the batch due at `now`.
    pub fn push(&mut self, now: Instant, key: K, value: V) {
        let bytes = (self.size_of)(&value);
        match self.pending.get_mut(&key) {
            Some(entry) => {
                (self.reduce)(&mut entry.value, value);
                entry.last_push = now;
                entry.pushes += 1;
                entry.bytes = (self.size_of)(&entry.value);
                self.deadlines.remove(&(entry.deadline, key.clone()));
                entry.deadline = self.policy.deadline(entry.first_push, now);
                if self.policy.flush_immediately(entry.pushes, entry.bytes) {
                    entry.deadline = now;
                }
                self.deadlines.insert((entry.deadline, key), ());
            }
            None => {
                let mut deadline = self.policy.deadline(now, now);
                if self.policy.flush_immediately(1, bytes) {
                    deadline = now;
                }
                self.pending.insert(
                    key.clone(),
                    PendingEntry {
                        value,
                        first_push: now,
                        last_push: now,
                        deadline,
                        pushes: 1,
                        bytes,
                    },
                );
                self.deadlines.insert((deadline, key), ());
            }
        }
    }

    /// The earliest deadline across all pending batches, or `None` when empty.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines
            .first_key_value()
            .map(|((deadline, _), _)| *deadline)
    }

    /// Take and return every batch whose deadline is at or before `now`, in
    /// deterministic order (equal deadlines ordered by key).
    pub fn take_due(&mut self, now: Instant) -> Vec<(K, V)> {
        let mut due = Vec::new();
        loop {
            let Some((deadline, key)) = self
                .deadlines
                .first_key_value()
                .map(|((deadline, key), _)| (*deadline, key.clone()))
            else {
                break;
            };
            if deadline > now {
                break;
            }
            self.deadlines.remove(&(deadline, key.clone()));
            let entry = self
                .pending
                .remove(&key)
                .expect("deadline index and pending map must agree");
            due.push((key, entry.value));
        }
        due
    }

    /// Drop the pending batch for `key`, returning its accumulated value.
    ///
    /// Used on disconnect/unsubscribe: only that key's item is removed.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self.pending.remove(key)?;
        self.deadlines.remove(&(entry.deadline, key.clone()));
        Some(entry.value)
    }

    /// Take every pending batch regardless of deadline — the owner's explicit
    /// shutdown choice (drain) as opposed to dropping the batcher (discard).
    pub fn drain(&mut self) -> Vec<(K, V)> {
        let mut out = Vec::with_capacity(self.pending.len());
        for (key, entry) in std::mem::take(&mut self.pending) {
            out.push((key, entry.value));
        }
        self.deadlines.clear();
        out
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(base: Instant, millis: u64) -> Instant {
        base + Duration::from_millis(millis)
    }

    fn debounce() -> DebouncePolicy {
        DebouncePolicy {
            quiet_window: Duration::from_millis(50),
            max_latency: Duration::from_millis(200),
        }
    }

    fn push_u32<P: FlushPolicy>(
        b: &mut KeyedBatcher<&'static str, u32, P>,
        now: Instant,
        k: &'static str,
        v: u32,
    ) {
        b.push(now, k, v);
    }

    #[test]
    fn burst_emits_once() {
        let mut b = KeyedBatcher::new(debounce(), |_: &u32| 4, |acc, v| *acc += v);
        let base = Instant::now();
        for i in 0..10 {
            push_u32(&mut b, at(base, 0), "k", i);
        }
        // Within the quiet window nothing is due.
        assert!(b.take_due(at(base, 40)).is_empty());
        // 50ms after the last push the single merged batch is due.
        let due = b.take_due(at(base, 50));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0, "k");
        assert_eq!(due[0].1, (0..10u32).sum::<u32>());
        assert!(b.is_empty());
    }

    #[test]
    fn continuous_traffic_emits_at_max_latency_and_cannot_starve() {
        let mut b = KeyedBatcher::new(debounce(), |_: &u32| 4, |acc, v| *acc += v);
        let base = Instant::now();
        // Continuous pushes every 10ms, each extending the quiet window.
        for i in 0..=20u64 {
            push_u32(&mut b, at(base, i * 10), "k", i as u32);
        }
        // The quiet window keeps resetting, but max_latency (200ms after the
        // first push) caps the deadline: the batch is due at t=200.
        assert_eq!(b.next_deadline(), Some(at(base, 200)));
        assert!(b.take_due(at(base, 199)).is_empty());
        let due = b.take_due(at(base, 200));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1, (0..=20u32).sum::<u32>());
    }

    #[test]
    fn different_keys_have_independent_deadlines() {
        let mut b = KeyedBatcher::new(debounce(), |_: &u32| 4, |acc, v| *acc += v);
        let base = Instant::now();
        push_u32(&mut b, at(base, 0), "a", 1);
        push_u32(&mut b, at(base, 30), "b", 2);
        // a: min(0+50, 0+200) = 50; b: min(30+50, 30+200) = 80.
        assert_eq!(b.next_deadline(), Some(at(base, 50)));
        let due = b.take_due(at(base, 50));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0, "a");
        assert!(b.take_due(at(base, 70)).is_empty());
        let due = b.take_due(at(base, 80));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0, "b");
    }

    #[test]
    fn values_merge_without_loss() {
        let mut b = KeyedBatcher::new(debounce(), |_: &u32| 4, |acc, v| *acc += v);
        let base = Instant::now();
        for i in 0..5 {
            push_u32(&mut b, at(base, 0), "k", i);
        }
        let due = b.take_due(at(base, 50));
        assert_eq!(due[0].1, (0..5u32).sum::<u32>());
    }

    #[test]
    fn merge_can_replace_latest_wins() {
        let mut b = KeyedBatcher::new(debounce(), |_: &u32| 4, |acc, v| *acc = v);
        let base = Instant::now();
        b.push(at(base, 0), "k", 1);
        b.push(at(base, 10), "k", 2);
        let due = b.take_due(at(base, 60));
        assert_eq!(due[0].1, 2);
    }

    #[test]
    fn push_after_take_due_creates_a_new_generation() {
        let mut b = KeyedBatcher::new(debounce(), |_: &u32| 4, |acc, v| *acc += v);
        let base = Instant::now();
        push_u32(&mut b, at(base, 0), "k", 1);
        let due = b.take_due(at(base, 50));
        assert_eq!(due.len(), 1);
        // A fresh push starts a new generation with a fresh deadline.
        push_u32(&mut b, at(base, 60), "k", 2);
        assert_eq!(b.next_deadline(), Some(at(base, 110)));
        assert!(b.take_due(at(base, 100)).is_empty());
        let due = b.take_due(at(base, 110));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1, 2);
    }

    #[test]
    fn removal_drops_only_that_key() {
        let mut b = KeyedBatcher::new(debounce(), |_: &u32| 4, |acc, v| *acc += v);
        let base = Instant::now();
        push_u32(&mut b, at(base, 0), "a", 1);
        push_u32(&mut b, at(base, 0), "b", 2);
        assert_eq!(b.remove(&"a"), Some(1));
        assert_eq!(b.len(), 1);
        let due = b.take_due(at(base, 50));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].0, "b");
        assert_eq!(due[0].1, 2);
        assert!(b.is_empty());
    }

    #[test]
    fn drain_takes_everything_regardless_of_deadline() {
        let mut b = KeyedBatcher::new(debounce(), |_: &u32| 4, |acc, v| *acc += v);
        let base = Instant::now();
        push_u32(&mut b, at(base, 0), "a", 1);
        push_u32(&mut b, at(base, 0), "b", 2);
        let drained = b.drain();
        assert_eq!(drained.len(), 2);
        assert!(b.is_empty());
        assert_eq!(b.next_deadline(), None);
    }

    #[test]
    fn equal_deadlines_have_deterministic_ordering() {
        let mut b = KeyedBatcher::new(debounce(), |_: &u32| 4, |acc, v| *acc += v);
        let base = Instant::now();
        push_u32(&mut b, at(base, 0), "b", 1);
        push_u32(&mut b, at(base, 0), "a", 2);
        let due = b.take_due(at(base, 50));
        let keys: Vec<_> = due.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn batch_policy_flushes_immediately_on_max_items() {
        let mut b = KeyedBatcher::new(
            BatchPolicy {
                max_latency: Duration::from_millis(200),
                max_items: 3,
                max_bytes: usize::MAX,
            },
            |v: &Vec<u32>| v.len() * 4,
            |acc, v| acc.extend(v),
        );
        let base = Instant::now();
        for i in 0..2 {
            b.push(at(base, 0), "k", vec![i]);
        }
        assert!(b.take_due(at(base, 0)).is_empty());
        b.push(at(base, 0), "k", vec![2]);
        let due = b.take_due(at(base, 0));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1, vec![0, 1, 2]);
    }

    #[test]
    fn batch_policy_flushes_immediately_on_max_bytes() {
        let mut b = KeyedBatcher::new(
            BatchPolicy {
                max_latency: Duration::from_millis(200),
                max_items: usize::MAX,
                max_bytes: 10,
            },
            |v: &Vec<u32>| v.len() * 4,
            |acc, v| acc.extend(v),
        );
        let base = Instant::now();
        for i in 0..2 {
            b.push(at(base, 0), "k", vec![i]);
        }
        assert!(b.take_due(at(base, 0)).is_empty());
        b.push(at(base, 0), "k", vec![2]);
        let due = b.take_due(at(base, 0));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1, vec![0, 1, 2]);
    }

    #[test]
    fn batch_policy_preserves_first_item_deadline() {
        let mut b = KeyedBatcher::new(
            BatchPolicy {
                max_latency: Duration::from_millis(100),
                max_items: usize::MAX,
                max_bytes: usize::MAX,
            },
            |_: &u32| 4,
            |acc, v| *acc += v,
        );
        let base = Instant::now();
        push_u32(&mut b, at(base, 0), "k", 1);
        push_u32(&mut b, at(base, 50), "k", 2);
        // Deadline is fixed at first push + max_latency, not extended by later
        // pushes.
        assert_eq!(b.next_deadline(), Some(at(base, 100)));
        assert!(b.take_due(at(base, 99)).is_empty());
        let due = b.take_due(at(base, 100));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].1, 3);
    }

    #[test]
    fn empty_batcher_has_no_deadline() {
        let mut b: KeyedBatcher<&'static str, u32, DebouncePolicy> =
            KeyedBatcher::new(debounce(), |_: &u32| 4, |acc, v| *acc += v);
        assert!(b.is_empty());
        assert_eq!(b.next_deadline(), None);
        assert!(b.take_due(Instant::now()).is_empty());
    }

    struct XorShift64(u64);
    impl XorShift64 {
        fn next_u64(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next_u64() % n
        }
    }

    /// Randomized consistency: after every `take_due(now)`, no pending batch
    /// may be due before its deadline, and every returned batch must contain
    /// exactly the pushes accumulated for its key.
    #[test]
    fn randomized_push_take_due_consistency() {
        let mut rng = XorShift64(0x1234_5678_9abc_def0);
        let mut b = KeyedBatcher::new(
            debounce(),
            |v: &Vec<u32>| v.len() * 4,
            |acc, v| acc.extend(v),
        );
        let base = Instant::now();
        let mut expected: BTreeMap<u8, Vec<u32>> = BTreeMap::new();
        let mut now_ms = 0u64;
        for _ in 0..2000 {
            now_ms += rng.below(10);
            let now = at(base, now_ms);
            if rng.below(3) == 0 {
                let due = b.take_due(now);
                for (k, v) in due {
                    let exp = expected.remove(&k).expect("due key must be pending");
                    assert_eq!(v, exp, "merged value must equal all pushes");
                }
                // After a flush, nothing may be due before its deadline.
                if let Some(deadline) = b.next_deadline() {
                    assert!(deadline > now, "next deadline must be in the future");
                }
            } else {
                let k = rng.below(8) as u8;
                let v = rng.below(1000) as u32;
                b.push(now, k, vec![v]);
                expected.entry(k).or_default().push(v);
            }
        }
        let drained = b.drain();
        assert_eq!(drained.len(), expected.len());
        for (k, v) in drained {
            assert_eq!(
                v,
                expected.remove(&k).expect("drained key must be expected")
            );
        }
        assert!(expected.is_empty());
    }
}
