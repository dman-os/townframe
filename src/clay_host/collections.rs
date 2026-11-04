//! The declared-collection registry (ADR 025 §1, §11): a collection exists independently of any
//! micro incarnation, is named by its contract id, and authority over it is issued as grants.
//!
//! Knowing a collection id conveys no authority; a [`Grant`] does. The registry is engine- and
//! encoding-neutral: it stores one handle `C` per collection (the bytes registration collection
//! guests write, in practice) and the contract it was declared under. Manifest admission
//! declares collections here; nothing in this module is tied to an incarnation, so a collection
//! outlives every micro that reads or writes it.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use crate::supervisor::MicroId;

/// What a collection was declared as: its id, and the schema ids its keys and values follow.
/// The registry only names the schemas; the collection's own contract interprets them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionContract {
    pub id: String,
    pub key_schema: String,
    pub value_schema: String,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DeclareError {
    #[error("collection {0} is already declared")]
    AlreadyDeclared(String),
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum GrantError {
    #[error("collection {0} is not declared")]
    Undeclared(String),
}

/// Write authority over one declared collection for one singleton micro: the collection, the
/// micro whose stable identity stamps its writes, and the key prefixes it may write. An empty
/// prefix is every key; manifest admission narrows it (ADR 024 §13).
#[derive(Clone)]
pub struct Grant<C> {
    pub collection: C,
    pub micro: MicroId,
    pub prefixes: Vec<Vec<u8>>,
}

struct Declared<C> {
    contract: CollectionContract,
    collection: C,
}

/// Shared handle to the declared collections of one host. Cloning shares the registry.
pub struct CollectionRegistry<C> {
    declared: Arc<Mutex<BTreeMap<String, Declared<C>>>>,
}

impl<C> Clone for CollectionRegistry<C> {
    fn clone(&self) -> Self {
        Self {
            declared: Arc::clone(&self.declared),
        }
    }
}

impl<C> Default for CollectionRegistry<C> {
    fn default() -> Self {
        Self {
            declared: Arc::default(),
        }
    }
}

impl<C: Clone> CollectionRegistry<C> {
    /// Declare `collection` under `contract.id`. The id is unique: two manifests naming one
    /// collection is a conflict the admitting layer reports, not a merge.
    pub fn declare(
        &self,
        contract: CollectionContract,
        collection: C,
    ) -> Result<(), DeclareError> {
        let mut declared = self.declared.lock().unwrap();
        if declared.contains_key(&contract.id) {
            return Err(DeclareError::AlreadyDeclared(contract.id));
        }
        declared.insert(
            contract.id.clone(),
            Declared {
                contract,
                collection,
            },
        );
        Ok(())
    }

    pub fn contract(&self, id: &str) -> Option<CollectionContract> {
        let declared = self.declared.lock().unwrap();
        Some(declared.get(id)?.contract.clone())
    }

    /// The collection itself, for read-side wiring done by the host (projections, readers).
    /// Writing needs a [`Grant`].
    pub fn collection(&self, id: &str) -> Option<C> {
        let declared = self.declared.lock().unwrap();
        Some(declared.get(id)?.collection.clone())
    }

    /// Issue `micro` write authority over the full key space of collection `id`. Narrower
    /// prefixes come with manifest admission.
    pub fn grant(&self, id: &str, micro: &MicroId) -> Result<Grant<C>, GrantError> {
        let collection = self
            .collection(id)
            .ok_or_else(|| GrantError::Undeclared(id.to_owned()))?;
        Ok(Grant {
            collection,
            micro: micro.clone(),
            prefixes: vec![Vec::new()],
        })
    }
}
