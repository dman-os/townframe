use crate::interlude::*;
use big_sync_core::part_store::ObjPayload;
use big_sync_core::{ObjId, PartId, PeerId};

#[async_trait]
pub trait SyncBackend: Send + Sync + 'static {
    async fn sync_obj(
        &self,
        peer_id: PeerId,
        obj_id: ObjId,
        remote_payload: Option<ObjPayload>,
    ) -> Res<crate::SyncTaskRunOutcome>;
}

pub mod contract {
    use super::*;
    use big_sync_core::SyncCompletionDeets;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum SyncBackendOutcome {
        Completion(SyncCompletionDeets),
        Stale,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SyncBackendScenario {
        pub name: &'static str,
        pub peer_id: PeerId,
        pub obj_id: ObjId,
        pub initial_payload: Option<ObjPayload>,
        pub initial_parts: Vec<PartId>,
        pub remote_payload: Option<ObjPayload>,
        pub expected_outcome: SyncBackendOutcome,
        pub expected_payload: Option<ObjPayload>,
        pub expected_parts: Vec<PartId>,
    }

    impl SyncBackendScenario {
        pub fn with_remote_payload(mut self, remote_payload: Option<ObjPayload>) -> Self {
            self.remote_payload = remote_payload;
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
                remote_payload: Some(payload.clone()),
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
                remote_payload: Some(remote_payload.clone()),
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
                initial_parts: parts.clone(),
                remote_payload: Some(remote_payload.clone()),
                expected_outcome: SyncBackendOutcome::Completion(SyncCompletionDeets::AddedMember),
                expected_payload: Some(remote_payload),
                expected_parts: parts,
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
                store.set_obj_payload(case.obj_id, payload.clone()).await?;
                if !case.initial_parts.is_empty() {
                    store
                        .add_obj_to_parts(case.obj_id, case.initial_parts.clone())
                        .await?;
                }
            }
            None if !case.initial_parts.is_empty() => {
                store
                    .add_obj_to_parts(case.obj_id, case.initial_parts.clone())
                    .await?;
            }
            None => {}
        }

        let outcome = harness
            .backend()
            .sync_obj(case.peer_id, case.obj_id, case.remote_payload.clone())
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
        let mut actual_parts = store.obj_parts(case.obj_id).await?;
        let mut expected_parts = case.expected_parts.clone();
        actual_parts.sort();
        actual_parts.dedup();
        expected_parts.sort();
        expected_parts.dedup();
        assert_eq!(
            actual_parts, expected_parts,
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
