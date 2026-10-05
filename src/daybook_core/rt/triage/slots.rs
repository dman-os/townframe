use crate::interlude::*;
use crate::tasks::storage::RegisterStore;
use big_repo::SharedPartStore;
use big_sync_core::ObjKey;
use big_sync_core::encrypted_register::LaneState;
use daybook_types::doc::{BranchPathBuf, ChangeHashSet, DocId};
use std::collections::BTreeMap;

pub(crate) const LOCAL_SLOT_PART: &str = "processor-slots/v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessorSlotKey {
    pub document_id: DocId,
    pub branch_path: BranchPathBuf,
    pub processor_full_id: String,
}

impl ProcessorSlotKey {
    pub fn id(&self) -> [u8; 32] {
        let mut hash = blake3::Hasher::new();
        hash.update(b"daybook/processor-slot/v1\0");
        for field in [
            self.document_id.as_bytes(),
            self.branch_path.as_str().as_bytes(),
            self.processor_full_id.as_bytes(),
        ] {
            hash.update(&(field.len() as u64).to_be_bytes());
            hash.update(field);
        }
        *hash.finalize().as_bytes()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessorCapture {
    pub generation: [u8; 32],
    pub heads: ChangeHashSet,
    pub execution_baseline: Option<ChangeHashSet>,
    pub processor_generation: [u8; 32],
    pub configuration_generation: [u8; 32],
}

impl ProcessorCapture {
    pub fn new(
        mut heads: ChangeHashSet,
        mut execution_baseline: Option<ChangeHashSet>,
        processor_generation: [u8; 32],
        configuration_generation: [u8; 32],
    ) -> Self {
        fn normalize(heads: &mut ChangeHashSet) {
            if heads.windows(2).any(|pair| pair[0] >= pair[1]) {
                let mut ordered = heads.to_vec();
                ordered.sort_unstable();
                ordered.dedup();
                heads.0 = ordered.into();
            }
        }
        normalize(&mut heads);
        if let Some(baseline) = &mut execution_baseline {
            normalize(baseline);
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"daybook/processor-work/v1\0");
        hash.update(&processor_generation);
        hash.update(&configuration_generation);
        let mut append_heads = |heads: &ChangeHashSet| {
            hash.update(&(heads.len() as u64).to_be_bytes());
            for head in heads.iter() {
                hash.update(&head.0);
            }
        };
        append_heads(&heads);
        // Absence is distinct from a captured empty baseline.
        if let Some(baseline) = &execution_baseline {
            append_heads(baseline);
            hash.update(&[1]);
        } else {
            hash.update(&[0]);
        }
        Self {
            generation: *hash.finalize().as_bytes(),
            heads,
            execution_baseline,
            processor_generation,
            configuration_generation,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProcessorDesired {
    pub capture: ProcessorCapture,
    pub matches: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ProcessorSettlement {
    pub capture: ProcessorCapture,
    pub attempt_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
struct ProcessorCell {
    // Evaluation clocks advance only on evaluation, not when an older attempt
    // settles. Otherwise late completion would supersede a concurrent desire.
    evaluation: u64,
    observed: Vec<([u8; 32], u64)>,
    desired: Option<ProcessorDesired>,
    settlement: Option<ProcessorSettlement>,
    desired_settlements: Vec<ProcessorSettlement>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ProcessorSlot {
    cells: BTreeMap<[u8; 32], ProcessorCell>,
}

impl ProcessorSlot {
    pub fn desired(&self) -> impl Iterator<Item = &ProcessorDesired> {
        self.cells.iter().filter_map(|(writer, cell)| {
            if self.cells.values().any(|other| {
                other
                    .observed
                    .iter()
                    .any(|(seen, evaluation)| seen == writer && *evaluation >= cell.evaluation)
            }) {
                None
            } else {
                cell.desired.as_ref()
            }
        })
    }

    pub fn settled(&self, generation: &[u8; 32]) -> bool {
        self.cells.values().any(|cell| {
            cell.settlement
                .iter()
                .chain(&cell.desired_settlements)
                .any(|settlement| &settlement.capture.generation == generation)
        })
    }

    pub fn settlements(&self) -> impl Iterator<Item = &ProcessorSettlement> {
        self.cells
            .values()
            .filter_map(|cell| cell.settlement.as_ref())
    }

    pub fn execution_baseline(&self) -> Option<ChangeHashSet> {
        let mut settlements = self.settlements().peekable();
        settlements.peek()?;
        let mut heads: Vec<_> = settlements
            .flat_map(|item| item.capture.heads.iter().copied())
            .collect();
        heads.sort_unstable();
        heads.dedup();
        Some(ChangeHashSet(heads.into()))
    }

    fn evaluate(&mut self, writer: [u8; 32], desired: ProcessorDesired) -> bool {
        if self.cells.get(&writer).is_some_and(|cell| {
            cell.desired.as_ref() == Some(&desired)
                && self.desired().all(|current| current == &desired)
        }) {
            return false;
        }
        let observed = self
            .cells
            .iter()
            .filter_map(|(peer, cell)| {
                (*peer != writer && cell.evaluation != 0).then_some((*peer, cell.evaluation))
            })
            .collect();
        let cell = self.cells.entry(writer).or_default();
        cell.evaluation = cell
            .evaluation
            .checked_add(1)
            .expect("processor evaluation clock exhausted");
        cell.observed = observed;
        cell.desired_settlements
            .retain(|item| item.capture.generation == desired.capture.generation);
        cell.desired = Some(desired);
        true
    }

    fn settle(&mut self, writer: [u8; 32], settlement: ProcessorSettlement) -> bool {
        let desired: Vec<_> = self
            .desired()
            .map(|state| state.capture.generation)
            .collect();
        let cell = self.cells.entry(writer).or_default();
        if cell.settlement.as_ref() == Some(&settlement) {
            return false;
        }
        cell.desired_settlements
            .retain(|item| desired.contains(&item.capture.generation));
        if let Some(previous) = cell.settlement.take()
            && desired.contains(&previous.capture.generation)
            && !cell
                .desired_settlements
                .iter()
                .any(|item| item.capture.generation == previous.capture.generation)
        {
            cell.desired_settlements.push(previous);
        }
        // A late older completion must not erase proof that a still-desired
        // generation already settled. Retention is bounded by live desires,
        // plus the exact last incorporated target for this writer.
        cell.settlement = Some(settlement);
        true
    }
}

#[expect(
    clippy::large_enum_variant,
    reason = "One long-lived register is stored inline rather than heap-allocated during configuration"
)]
enum SlotBackend {
    Local {
        store: SharedPartStore,
        writer: [u8; 32],
    },
    Distributed(RegisterStore),
}

pub(crate) struct ProcessorSlotStore {
    backend: SlotBackend,
    publication: tokio::sync::Mutex<()>,
}

impl ProcessorSlotStore {
    pub fn local(store: SharedPartStore, writer: [u8; 32]) -> Self {
        Self {
            backend: SlotBackend::Local { store, writer },
            publication: tokio::sync::Mutex::new(()),
        }
    }

    pub fn distributed(register: RegisterStore) -> Self {
        Self {
            backend: SlotBackend::Distributed(register),
            publication: tokio::sync::Mutex::new(()),
        }
    }

    pub(crate) fn register(&self) -> &RegisterStore {
        let SlotBackend::Distributed(register) = &self.backend else {
            unreachable!("per-node slots have no replication register");
        };
        register
    }

    async fn writer(&self) -> Res<[u8; 32]> {
        match &self.backend {
            SlotBackend::Local { writer, .. } => Ok(*writer),
            SlotBackend::Distributed(register) => register.local_writer().await,
        }
    }

    pub async fn slot(&self, key: &ProcessorSlotKey) -> Res<ProcessorSlot> {
        let mut result = ProcessorSlot::default();
        match &self.backend {
            SlotBackend::Local { store, writer } => {
                if let Some(payload) = store.obj_payload(ObjKey::new(key.id())).await? {
                    result
                        .cells
                        .insert(*writer, serde_json::from_value(payload)?);
                }
            }
            SlotBackend::Distributed(register) => {
                if let Some(snapshot) = register.current(&key.id()).await? {
                    for (writer, lane) in snapshot.lanes {
                        let LaneState::Current { representation } = lane else {
                            eyre::bail!("processor slot writer equivocated");
                        };
                        let original = register.open_original(&representation).await?;
                        result
                            .cells
                            .insert(writer, serde_json::from_slice(&original.body)?);
                    }
                }
            }
        }
        Ok(result)
    }

    async fn publish(
        &self,
        key: &ProcessorSlotKey,
        writer: [u8; 32],
        slot: &ProcessorSlot,
    ) -> Res<()> {
        let cell = &slot.cells[&writer];
        match &self.backend {
            SlotBackend::Local { store, .. } => {
                store
                    .set_obj_payload(ObjKey::new(key.id()), serde_json::to_value(cell)?)
                    .await?;
                store
                    .add_obj_to_parts(
                        ObjKey::new(key.id()),
                        vec![crate::part_id_from_label(LOCAL_SLOT_PART)],
                    )
                    .await?;
            }
            SlotBackend::Distributed(register) => {
                // Semantic evaluation observations live in the cell. Settlement
                // publication must not invent an observation of a newer desire.
                register
                    .publish_local(&key.id(), Vec::new(), serde_json::to_vec(cell)?)
                    .await?;
            }
        }
        Ok(())
    }

    pub async fn evaluate(
        &self,
        key: &ProcessorSlotKey,
        desired: ProcessorDesired,
    ) -> Res<ProcessorSlot> {
        let _publication = self.publication.lock().await;
        let writer = self.writer().await?;
        let mut slot = self.slot(key).await?;
        if slot.evaluate(writer, desired) {
            self.publish(key, writer, &slot).await?;
        }
        Ok(slot)
    }

    pub async fn settle(&self, key: &ProcessorSlotKey, settlement: ProcessorSettlement) -> Res<()> {
        let _publication = self.publication.lock().await;
        let writer = self.writer().await?;
        let mut slot = self.slot(key).await?;
        if slot.settle(writer, settlement) {
            self.publish(key, writer, &slot).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capture(head: u8) -> ProcessorCapture {
        ProcessorCapture::new(
            ChangeHashSet(vec![automerge::ChangeHash([head; 32])].into()),
            None,
            [1; 32],
            [2; 32],
        )
    }

    #[tokio::test]
    async fn admission_does_not_settle_and_old_completion_keeps_new_desire() -> Res<()> {
        let sql = crate::app::open_sql_ctx(crate::app::SqlConfig::memory()).await?;
        let parts: SharedPartStore = Arc::new(
            big_sync::SqlitePartStore::new(
                sql,
                "slot-transition",
                big_sync_core::BuckId::MAX_LEVEL,
            )
            .await?,
        );
        let store = ProcessorSlotStore::local(Arc::clone(&parts), [3; 32]);
        let key = ProcessorSlotKey {
            document_id: "doc".into(),
            branch_path: "main".into(),
            processor_full_id: "@test/processor".into(),
        };
        let old = capture(4);
        let newer = capture(5);
        store
            .evaluate(
                &key,
                ProcessorDesired {
                    capture: old.clone(),
                    matches: true,
                },
            )
            .await?;
        store
            .evaluate(
                &key,
                ProcessorDesired {
                    capture: newer.clone(),
                    matches: true,
                },
            )
            .await?;
        let admitted = store.slot(&key).await?;
        assert_eq!(admitted.execution_baseline(), None);
        assert!(!admitted.settled(&old.generation));
        assert!(!admitted.settled(&newer.generation));
        store
            .settle(
                &key,
                ProcessorSettlement {
                    capture: old.clone(),
                    attempt_id: "old-attempt".into(),
                },
            )
            .await?;
        let settled = store.slot(&key).await?;
        assert_eq!(settled.execution_baseline(), Some(old.heads.clone()));
        assert!(settled.settled(&old.generation));
        assert!(!settled.settled(&newer.generation));
        assert_eq!(
            settled.desired().collect::<Vec<_>>(),
            vec![&ProcessorDesired {
                capture: newer.clone(),
                matches: true
            }]
        );
        store
            .settle(
                &key,
                ProcessorSettlement {
                    capture: newer.clone(),
                    attempt_id: "new-attempt".into(),
                },
            )
            .await?;
        store
            .settle(
                &key,
                ProcessorSettlement {
                    capture: old,
                    attempt_id: "late-old-attempt".into(),
                },
            )
            .await?;
        // Reopening the wrapper exercises the durable projection, not a cache.
        let reopened = ProcessorSlotStore::local(parts, [3; 32]);
        assert!(reopened.slot(&key).await?.settled(&newer.generation));
        Ok(())
    }

    #[test]
    fn settlement_does_not_supersede_unobserved_concurrent_desire() {
        let mut a = ProcessorSlot::default();
        let mut b = ProcessorSlot::default();
        let first = capture(4);
        let second = capture(5);
        a.evaluate(
            [1; 32],
            ProcessorDesired {
                capture: first.clone(),
                matches: true,
            },
        );
        b.evaluate(
            [2; 32],
            ProcessorDesired {
                capture: second.clone(),
                matches: true,
            },
        );
        a.cells.extend(b.cells);
        a.settle(
            [1; 32],
            ProcessorSettlement {
                capture: first.clone(),
                attempt_id: "first".into(),
            },
        );
        assert!(a.desired().any(|desired| desired.capture == first));
        assert!(a.desired().any(|desired| desired.capture == second));
        assert!(!a.settled(&second.generation));
        let next = capture(6);
        a.evaluate(
            [1; 32],
            ProcessorDesired {
                capture: next.clone(),
                matches: false,
            },
        );
        assert_eq!(
            a.desired().collect::<Vec<_>>(),
            vec![&ProcessorDesired {
                capture: next,
                matches: false
            }]
        );
    }
    #[test]
    fn concurrent_settlements_are_a_baseline_not_merged_target_proof() -> Res<()> {
        use automerge::transaction::Transactable;
        let mut left = automerge::Automerge::new();
        let mut tx = left.transaction();
        tx.put(automerge::ROOT, "content", "base")?;
        tx.commit();
        let mut right = left.fork();
        let mut tx = left.transaction();
        tx.put(automerge::ROOT, "content", "left")?;
        tx.commit();
        let mut tx = right.transaction();
        tx.put(automerge::ROOT, "content", "right")?;
        tx.commit();
        let left_capture = ProcessorCapture::new(
            ChangeHashSet(left.get_heads().into()),
            None,
            [3; 32],
            [4; 32],
        );
        let right_capture = ProcessorCapture::new(
            ChangeHashSet(right.get_heads().into()),
            None,
            [3; 32],
            [4; 32],
        );
        let mut slot = ProcessorSlot::default();
        slot.settle(
            [1; 32],
            ProcessorSettlement {
                capture: left_capture.clone(),
                attempt_id: "left".into(),
            },
        );
        slot.settle(
            [2; 32],
            ProcessorSettlement {
                capture: right_capture.clone(),
                attempt_id: "right".into(),
            },
        );
        left.merge(&mut right)?;
        let merged = ProcessorCapture::new(
            ChangeHashSet(left.get_heads().into()),
            slot.execution_baseline(),
            [3; 32],
            [4; 32],
        );
        assert_eq!(slot.execution_baseline(), Some(merged.heads.clone()));
        assert!(slot.settled(&left_capture.generation));
        assert!(slot.settled(&right_capture.generation));
        assert!(!slot.settled(&merged.generation));
        slot.evaluate(
            [1; 32],
            ProcessorDesired {
                capture: merged.clone(),
                matches: true,
            },
        );
        slot.settle(
            [2; 32],
            ProcessorSettlement {
                capture: right_capture,
                attempt_id: "late-right".into(),
            },
        );
        assert_eq!(
            slot.desired()
                .map(|desired| &desired.capture)
                .collect::<Vec<_>>(),
            vec![&merged]
        );
        assert!(!slot.settled(&merged.generation));
        slot.settle(
            [1; 32],
            ProcessorSettlement {
                capture: merged.clone(),
                attempt_id: "merged".into(),
            },
        );
        assert!(slot.settled(&merged.generation));
        Ok(())
    }
}
