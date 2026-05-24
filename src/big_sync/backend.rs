use crate::interlude::*;
use big_sync_core::part_store::ObjPayload;
use big_sync_core::{ObjId, PartId, PeerId};

#[async_trait]
pub trait SyncBackend: Send + Sync + 'static {
    async fn sync_obj(
        &self,
        peer_id: PeerId,
        obj_id: ObjId,
        part_hints: Vec<PartId>,
        remote_payload: Option<ObjPayload>,
    ) -> Res<crate::SyncTaskRunOutcome>;
}

pub mod contract {
    use super::*;
    use big_sync_core::SyncCompletionDeets;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum SyncBackendLeaseKind {
        Fresh,
        Stale,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum SyncBackendOutcome {
        Completion(SyncCompletionDeets),
        Stale,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum SyncBackendLeaseMutation {
        RemoveObjFromPart {
            part_id: PartId,
        },
        AddObjToParts {
            parts: Vec<PartId>,
        },
        UpsertObj {
            payload: ObjPayload,
            parts: Vec<PartId>,
        },
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SyncBackendScenario {
        pub name: &'static str,
        pub peer_id: PeerId,
        pub obj_id: ObjId,
        pub initial_payload: Option<ObjPayload>,
        pub initial_parts: Vec<PartId>,
        pub sync_part_hints: Vec<PartId>,
        pub remote_payload: Option<ObjPayload>,
        pub lease_kind: SyncBackendLeaseKind,
        pub lease_mutation: Option<SyncBackendLeaseMutation>,
        pub expected_outcome: SyncBackendOutcome,
        pub expected_payload: Option<ObjPayload>,
        pub expected_parts: Vec<PartId>,
    }

    impl SyncBackendScenario {
        pub fn with_remote_payload(mut self, remote_payload: Option<ObjPayload>) -> Self {
            self.remote_payload = remote_payload;
            self
        }

        pub fn with_sync_part_hints(mut self, sync_part_hints: Vec<PartId>) -> Self {
            self.sync_part_hints = sync_part_hints;
            self
        }

        pub fn with_lease_mutation(
            mut self,
            lease_mutation: Option<SyncBackendLeaseMutation>,
        ) -> Self {
            self.lease_mutation = lease_mutation;
            self
        }

        pub fn noop(
            name: &'static str,
            peer_id: PeerId,
            obj_id: ObjId,
            payload: ObjPayload,
            parts: Vec<PartId>,
        ) -> Self {
            Self {
                name,
                peer_id,
                obj_id,
                initial_payload: Some(payload.clone()),
                initial_parts: parts.clone(),
                sync_part_hints: parts.clone(),
                remote_payload: Some(payload.clone()),
                lease_kind: SyncBackendLeaseKind::Fresh,
                lease_mutation: None,
                expected_outcome: SyncBackendOutcome::Completion(SyncCompletionDeets::Noop),
                expected_payload: Some(payload),
                expected_parts: parts,
            }
        }

        pub fn changed_object(
            name: &'static str,
            peer_id: PeerId,
            obj_id: ObjId,
            initial_payload: ObjPayload,
            remote_payload: ObjPayload,
            parts: Vec<PartId>,
        ) -> Self {
            Self {
                name,
                peer_id,
                obj_id,
                initial_payload: Some(initial_payload),
                initial_parts: parts.clone(),
                sync_part_hints: parts.clone(),
                remote_payload: Some(remote_payload.clone()),
                lease_kind: SyncBackendLeaseKind::Fresh,
                lease_mutation: None,
                expected_outcome: SyncBackendOutcome::Completion(
                    SyncCompletionDeets::ChangedObject,
                ),
                expected_payload: Some(remote_payload),
                expected_parts: parts,
            }
        }

        pub fn added_member(
            name: &'static str,
            peer_id: PeerId,
            obj_id: ObjId,
            remote_payload: ObjPayload,
            parts: Vec<PartId>,
        ) -> Self {
            Self {
                name,
                peer_id,
                obj_id,
                initial_payload: None,
                initial_parts: vec![],
                sync_part_hints: parts.clone(),
                remote_payload: Some(remote_payload.clone()),
                lease_kind: SyncBackendLeaseKind::Fresh,
                lease_mutation: None,
                expected_outcome: SyncBackendOutcome::Completion(SyncCompletionDeets::AddedMember),
                expected_payload: Some(remote_payload),
                expected_parts: parts,
            }
        }

        pub fn stale(
            name: &'static str,
            peer_id: PeerId,
            obj_id: ObjId,
            initial_payload: ObjPayload,
            parts: Vec<PartId>,
            remote_payload: ObjPayload,
        ) -> Self {
            Self {
                name,
                peer_id,
                obj_id,
                initial_payload: Some(initial_payload.clone()),
                initial_parts: parts.clone(),
                sync_part_hints: parts.clone(),
                remote_payload: Some(remote_payload),
                lease_kind: SyncBackendLeaseKind::Stale,
                lease_mutation: None,
                expected_outcome: SyncBackendOutcome::Stale,
                expected_payload: Some(initial_payload),
                expected_parts: parts,
            }
        }

        pub fn stale_after_remove_from_part(
            name: &'static str,
            peer_id: PeerId,
            obj_id: ObjId,
            payload: ObjPayload,
            part_id: PartId,
            remote_payload: ObjPayload,
        ) -> Self {
            Self {
                name,
                peer_id,
                obj_id,
                initial_payload: Some(payload.clone()),
                initial_parts: vec![part_id],
                sync_part_hints: vec![part_id],
                remote_payload: Some(remote_payload),
                lease_kind: SyncBackendLeaseKind::Fresh,
                lease_mutation: Some(SyncBackendLeaseMutation::RemoveObjFromPart { part_id }),
                expected_outcome: SyncBackendOutcome::Stale,
                expected_payload: Some(payload),
                expected_parts: vec![],
            }
        }

        pub fn stale_after_add_obj_to_parts(
            name: &'static str,
            peer_id: PeerId,
            obj_id: ObjId,
            payload: ObjPayload,
            initial_parts: Vec<PartId>,
            added_parts: Vec<PartId>,
            remote_payload: ObjPayload,
        ) -> Self {
            let mut expected_parts = initial_parts.clone();
            expected_parts.extend(added_parts.clone());
            Self {
                name,
                peer_id,
                obj_id,
                initial_payload: Some(payload.clone()),
                initial_parts,
                sync_part_hints: expected_parts.clone(),
                remote_payload: Some(remote_payload),
                lease_kind: SyncBackendLeaseKind::Fresh,
                lease_mutation: Some(SyncBackendLeaseMutation::AddObjToParts {
                    parts: added_parts,
                }),
                expected_outcome: SyncBackendOutcome::Stale,
                expected_payload: Some(payload),
                expected_parts,
            }
        }

        pub fn stale_after_upsert_obj(
            name: &'static str,
            peer_id: PeerId,
            obj_id: ObjId,
            initial_payload: ObjPayload,
            initial_parts: Vec<PartId>,
            updated_payload: ObjPayload,
            updated_parts: Vec<PartId>,
            remote_payload: ObjPayload,
        ) -> Self {
            Self {
                name,
                peer_id,
                obj_id,
                initial_payload: Some(initial_payload.clone()),
                initial_parts,
                sync_part_hints: updated_parts.clone(),
                remote_payload: Some(remote_payload),
                lease_kind: SyncBackendLeaseKind::Fresh,
                lease_mutation: Some(SyncBackendLeaseMutation::UpsertObj {
                    payload: updated_payload.clone(),
                    parts: updated_parts.clone(),
                }),
                expected_outcome: SyncBackendOutcome::Stale,
                expected_payload: Some(updated_payload),
                expected_parts: updated_parts,
            }
        }
    }

    #[async_trait]
    pub trait SyncBackendHarness {
        fn backend(&self) -> &dyn SyncBackend;
        fn store(&self) -> &dyn crate::HostPartStore;

        async fn prepare_case(&self, _case: &SyncBackendScenario) -> Res<()> {
            Ok(())
        }

        async fn assert_case(&self, _case: &SyncBackendScenario) -> Res<()> {
            Ok(())
        }
    }

    pub async fn assert_sync_backend_case<H>(harness: &H, case: &SyncBackendScenario) -> Res<()>
    where
        H: SyncBackendHarness + Sync,
    {
        harness.prepare_case(case).await?;
        let store = harness.store();

        match &case.initial_payload {
            Some(payload) => {
                store
                    .set_obj_payload(
                        case.obj_id,
                        payload.clone(),
                        case.initial_parts.clone(),
                        None,
                    )
                    .await?;
            }
            None if !case.initial_parts.is_empty() => {
                store
                    .add_obj_to_parts(case.obj_id, case.initial_parts.clone(), None)
                    .await?;
            }
            None => {}
        }

        let lease = match case.lease_kind {
            SyncBackendLeaseKind::Fresh => store.get_obj_lease(case.obj_id).await?,
            SyncBackendLeaseKind::Stale => {
                let stale_lease = store.get_obj_lease(case.obj_id).await?;
                let _fresh_lease = store.get_obj_lease(case.obj_id).await?;
                stale_lease
            }
        };

        if let Some(lease_mutation) = &case.lease_mutation {
            match lease_mutation {
                SyncBackendLeaseMutation::RemoveObjFromPart { part_id } => {
                    store
                        .remove_obj_from_part(case.obj_id, *part_id, Some(lease))
                        .await?;
                }
                SyncBackendLeaseMutation::AddObjToParts { parts } => {
                    store
                        .add_obj_to_parts(case.obj_id, parts.clone(), Some(lease))
                        .await?;
                }
                SyncBackendLeaseMutation::UpsertObj { payload, parts } => {
                    store
                        .set_obj_payload(case.obj_id, payload.clone(), parts.clone(), Some(lease))
                        .await?;
                }
            }
        }

        let outcome = harness
            .backend()
            .sync_obj(
                case.peer_id,
                lease,
                case.obj_id,
                case.sync_part_hints.clone(),
                case.remote_payload.clone(),
            )
            .await?;

        match (&case.expected_outcome, outcome) {
            (
                SyncBackendOutcome::Completion(expected_deets),
                crate::SyncTaskRunOutcome::Completion(completion),
            ) => {
                assert_eq!(
                    completion.deets, *expected_deets,
                    "unexpected sync completion outcome for case {}",
                    case.name
                );
            }
            (SyncBackendOutcome::Stale, crate::SyncTaskRunOutcome::Stale) => {}
            (expected, got) => {
                panic!(
                    "unexpected sync outcome for case {}: expected {:?}, got {:?}",
                    case.name, expected, got
                );
            }
        }

        assert_eq!(
            store.obj_payload(case.obj_id).await?,
            case.expected_payload,
            "unexpected payload after sync case {}",
            case.name
        );
        assert_eq!(
            store.obj_parts(case.obj_id).await?,
            case.expected_parts,
            "unexpected parts after sync case {}",
            case.name
        );

        harness.assert_case(case).await?;
        Ok(())
    }

    pub async fn assert_sync_backend_scenarios<H>(
        harness: &H,
        cases: &[SyncBackendScenario],
    ) -> Res<()>
    where
        H: SyncBackendHarness + Sync,
    {
        for case in cases {
            assert_sync_backend_case(harness, case).await?;
        }
        Ok(())
    }

    pub fn assert_sync_backend_stale(outcome: &crate::SyncTaskRunOutcome) {
        assert!(
            matches!(outcome, crate::SyncTaskRunOutcome::Stale),
            "expected stale sync outcome"
        );
    }

    pub fn assert_sync_backend_completion(
        outcome: &crate::SyncTaskRunOutcome,
        had_local_state: bool,
        had_remote_state: bool,
    ) {
        match (had_local_state, had_remote_state) {
            (false, false) => {
                assert!(
                    matches!(
                        outcome,
                        crate::SyncTaskRunOutcome::Completion(big_sync_core::SyncTaskCompletion {
                            deets: SyncCompletionDeets::Noop,
                            ..
                        })
                    ),
                    "expected no-op sync outcome when local and remote state already match"
                );
            }
            (true, false) | (false, true) | (true, true) => {
                assert!(
                    matches!(
                        outcome,
                        crate::SyncTaskRunOutcome::Completion(big_sync_core::SyncTaskCompletion {
                            deets: SyncCompletionDeets::ChangedObject
                                | SyncCompletionDeets::AddedMember,
                            ..
                        })
                    ),
                    "expected a mutating sync outcome when either side diverged"
                );
            }
        }
    }

    pub fn assert_sync_backend_completion_deets(
        outcome: &crate::SyncTaskRunOutcome,
        expected_deets: SyncCompletionDeets,
    ) {
        match outcome {
            crate::SyncTaskRunOutcome::Completion(completion) => {
                assert_eq!(
                    completion.deets, expected_deets,
                    "unexpected sync completion outcome"
                );
            }
            crate::SyncTaskRunOutcome::Stale => {
                panic!("unexpected stale sync outcome")
            }
        }
    }
}
