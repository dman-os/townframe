//! Pauperfuse: the vtree bridge core.
//!
//! This crate implements the core of `docs/adrs/010-vtree-store.md`: a bridge
//! that observes N **backends**, records each one's latest-known state as a
//! **rep** (rows keyed by path), and brokers transfers between them.
//!
//! Three ideas hold it together.
//!
//! **Identity is provenance, and it is the backend's to name.** An entry
//! carries a [`Token`]: a scheme tag and some bytes, whose owner promises the
//! token is a deterministic function of the content. A blob store names bytes by
//! hash; a checkout by digest, or by a stat-derived marker where it declines to
//! read a large file; a deployment that *produces* content by the recipe it would
//! produce from. Nothing has to be rendered, hashed, or read in order to say that
//! something changed, which is what keeps a doc edit from costing a full render
//! and what keeps a photo library from being read to notice an mtime moved.
//!
//! **The core never answers "are these the same bytes".** Two identities that
//! differ might stand for the same content, and the same disagreement means
//! "nothing to do" for one pair of backends and "produce it again" for another.
//! Only the backend holding the path can tell those apart, so it is asked
//! ([`Backend::accept`]); the bridge orders work and writes down what was
//! written.
//!
//! **Order is the substrate.** Every rep is a path-ordered row set, so a walk is
//! an index scan, a comparison is a merge join, and bulk work resumes at a path
//! cursor. There is no tree hash, no path copying, and nothing to garbage
//! collect: a rep is a state, not a version.
//!
//! Deliberately **not** here (ADR 010 §5): history, an op log, VCS semantics,
//! blob bytes, lenses, the reconcile policy, and any reading of an identity's
//! bytes. Backends own their own truth;
//! reps are caches; recovery is a fresh change report.
//!
//! Errors are **one concrete type** ([`Error`], ADR 010 §4.6): both traits are
//! implemented by embedders, so the error type has to be nameable by everyone.
//! Its variants separate the environment's failures from the caller's input,
//! from a recorded row that cannot be read back, from an implementor's own
//! error, which is boxed with its source chain kept.
//!
//! ```
//! use pauperfuse::prelude::*;
//! use utils_rs::prelude::futures;
//!
//! let roundtrip = async {
//!     let store: std::sync::Arc<dyn VtreeStore> = std::sync::Arc::new(MemVtreeStore::new());
//!     let rep = BackendId::new("fs");
//!     assert_eq!(store.generation(&rep).await?, None);
//!
//!     let path = RelPath::try_new(vec!["a.txt".into()])?;
//!     store.put_entry(&rep, &path, &Entry::dir(None)).await?;
//!     assert_eq!(store.generation(&rep).await?, Some(1));
//!     assert_eq!(store.entry(&rep, &path).await?, Some(Entry::dir(None)));
//!     Ok::<(), Error>(())
//! };
//! futures::executor::block_on(roundtrip).expect("the store round trips");
//! ```

mod interlude {
    pub use std::cmp::Ordering;
    pub use std::collections::{BTreeMap, VecDeque};
    pub use std::ffi::{OsStr, OsString};
    pub use std::fmt;
    pub use std::ops::{Bound, Range};
    pub use std::path::{Path, PathBuf};
    pub use std::sync::{Arc, RwLock};
    pub use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub use utils_rs::prelude::*;

    pub use crate::error::{Error, FsOp, Result};
}

/// The stored row encoding. A store needs it; nothing else does.
#[cfg(feature = "sqlite")]
mod codec;

pub mod backend;
pub mod bridge;
pub mod delta;
pub mod entry;
pub mod error;
pub mod fs;
pub mod path;
pub mod store;

#[cfg(test)]
mod e2e;

#[cfg(test)]
mod test_support;

/// The commonly needed names of this crate.
pub mod prelude {
    pub use crate::backend::{Accepted, Backend, BackendId, Capabilities, DeltaSink, Report};
    pub use crate::bridge::{Outcome, reconcile};
    pub use crate::delta::{Delta, DiffWalk};
    pub use crate::entry::{
        Avail, Entry, Kind, Payload, StatFingerprint, TimeStamp, Token, TokenScheme,
    };
    #[cfg(feature = "sqlite")]
    pub use crate::error::StoredError;
    pub use crate::error::{Error, FsOp, Result};
    pub use crate::fs::TokioFs;
    pub use crate::path::{PathError, RelPath};
    pub use crate::store::mem::MemVtreeStore;
    #[cfg(feature = "sqlite")]
    pub use crate::store::sqlite::SqliteVtreeStore;
    pub use crate::store::{RepScan, VtreeStore};
}
