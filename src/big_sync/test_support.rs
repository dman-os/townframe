use crate::interlude::*;
use crate::part_store::HostPartStore;

use big_sync_core::part_store::CursorIndex;
use big_sync_core::{ObjId, PartId, PeerId};

use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedObjSnapshot {
    pub payload: Option<serde_json::Value>,
    pub parts: BTreeSet<PartId>,
}

#[derive(Debug, Clone)]
pub(crate) struct ObservedStoreSnapshot {
    pub objs: BTreeMap<ObjId, ObservedObjSnapshot>,
    pub peer_part_cursors: BTreeMap<(PeerId, PartId), CursorIndex>,
}

impl PartialEq for ObservedStoreSnapshot {
    fn eq(&self, other: &Self) -> bool {
        self.objs == other.objs
    }
}

impl Eq for ObservedStoreSnapshot {}

#[async_trait]
pub(crate) trait ObservedStore: HostPartStore {
    async fn observed_snapshot(&self) -> Res<ObservedStoreSnapshot>;
}
