//! The evaluation cache: the certificate set as a [`State`], at the version it was read.

use std::sync::Arc;
use std::sync::Mutex;

use keyhive_codec::traits::Decode;
use keyhive_codec::traits::Encode;
use keyline_memory::state::State;
use sqlite_rs::QueryExecutor;

use crate::error::Error;
use crate::rows;
use crate::sqlite::queries;
use crate::write::Staged;

/// The set at one version, and the evaluation `State` caches over it.
pub struct Cached<W> {
    pub version: i64,
    pub state: State<W>,
}

/// The newest [`Cached`] this handle's queries and commits have seen. Clones of a store share
/// it.
///
/// It is a cache of the tables, never a source of truth: a query takes it only when its version
/// is the one the query's snapshot read, and otherwise rebuilds from the snapshot's rows.
pub struct Cache<W> {
    slot: Mutex<Option<Arc<Cached<W>>>>,
}

impl<W> Default for Cache<W> {
    fn default() -> Self {
        Self {
            slot: Mutex::new(None),
        }
    }
}

impl<W: Encode + Decode> Cache<W> {
    /// The set as `snapshot` sees it, at `version`, which is the version `snapshot` read.
    ///
    /// A miss reads every row of the snapshot and verifies each again (see
    /// [`Error::Stored`]). Two queries that miss together both rebuild; the newer set wins the
    /// slot.
    pub async fn at(
        &self,
        snapshot: &impl QueryExecutor,
        version: i64,
    ) -> Result<Arc<Cached<W>>, Error> {
        if let Some(cached) = self.peek(version) {
            return Ok(cached);
        }
        let mut batch = Vec::new();
        for row in queries::all_certificates(snapshot).await? {
            batch.push(rows::verified_from_sql(row.kind, &row.digest, &row.signed)?);
        }
        let mut state = State::new();
        state.insert_all(batch);
        let cached = Arc::new(Cached { version, state });
        let mut slot = self.slot.lock().expect(ERROR_MUTEX);
        if slot.as_ref().is_none_or(|held| held.version < version) {
            *slot = Some(Arc::clone(&cached));
        }
        Ok(cached)
    }

    fn peek(&self, version: i64) -> Option<Arc<Cached<W>>> {
        self.slot
            .lock()
            .expect(ERROR_MUTEX)
            .as_ref()
            .filter(|held| held.version == version)
            .map(Arc::clone)
    }

    /// Carry a committed transaction's batch into the cache.
    ///
    /// The cache moves only from the exact version the transaction started at, so it never
    /// holds a set some commit between them is missing. From a newer version it is left alone
    /// (a later commit got here first); from any other it is emptied and the next query
    /// rebuilds from the tables. An empty cache moves only from version 0, where the set it
    /// would have to load is known to be empty, which spares a database's first writer a load
    /// that has nothing to read.
    pub fn committed(&self, staged: Staged<W>) {
        let mut slot = self.slot.lock().expect(ERROR_MUTEX);
        let state = match slot.take() {
            Some(held) if held.version >= staged.version => {
                *slot = Some(held);
                return;
            }
            // Mutate in place when nothing else holds the old set; otherwise `State::clone`
            // copies pointers.
            Some(held) if held.version == staged.base => match Arc::try_unwrap(held) {
                Ok(held) => held.state,
                Err(held) => held.state.clone(),
            },
            None if staged.base == 0 => State::new(),
            _ => return,
        };
        let mut state = state;
        state.insert_all(staged.added);
        *slot = Some(Arc::new(Cached {
            version: staged.version,
            state,
        }));
    }
}

const ERROR_MUTEX: &str = "the cache mutex is not poisoned: nothing panics under it";
