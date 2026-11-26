//! Evaluated closures kept between queries, a few at a time, each valid at one version.
//!
//! Evaluating a closure is the expensive step, and several questions about one subject (its
//! members, a power, its closure) would each repeat it. A query reads the version in its own
//! snapshot before anything else, and uses an entry only if it was evaluated at exactly that
//! version, so an entry is never stale for the snapshot that takes it: a commit by this process or
//! another moves the version, and the next query evaluates again. It is bounded in entries, and
//! emptied when full; it holds no state a query could not rebuild from the tables.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use keyhive_crypto::identifier::Identifier;
use keyline_memory::state::State;

/// Closures held before the cache empties itself.
const CAPACITY: usize = 64;

const ERROR_MUTEX: &str = "the evaluations mutex is not poisoned: nothing panics under it";

/// A closure's evaluation and the version it was evaluated at.
type Held<W> = (i64, Arc<State<W>>);

pub struct Evaluations<W> {
    entries: Mutex<HashMap<Identifier, Held<W>>>,
}

impl<W> Default for Evaluations<W> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

impl<W> Evaluations<W> {
    /// The closure of `subject` evaluated at `version`, if this cache holds it.
    pub fn at(&self, subject: Identifier, version: i64) -> Option<Arc<State<W>>> {
        self.entries
            .lock()
            .expect(ERROR_MUTEX)
            .get(&subject)
            .filter(|(held, _)| *held == version)
            .map(|(_, state)| Arc::clone(state))
    }

    /// Keep `state` as the closure of `subject` at `version`, unless a newer one is held.
    pub fn keep(&self, subject: Identifier, version: i64, state: Arc<State<W>>) {
        let mut entries = self.entries.lock().expect(ERROR_MUTEX);
        if entries.len() >= CAPACITY && !entries.contains_key(&subject) {
            entries.clear();
        }
        match entries.get(&subject) {
            Some((held, _)) if *held > version => {}
            _ => {
                entries.insert(subject, (version, state));
            }
        }
    }
}
