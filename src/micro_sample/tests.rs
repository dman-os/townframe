use super::*;
use clay_dioxus::NativeComponents;
use clay_guest_session::collections::bytes_registrations;
use clay_host::CollectionContract;
use clay_host::ConfiguredMicro;
use clay_host::Host;
use clay_host::MicroState;
use clay_host::Supervisor;
use clay_mdk::native::CollectionBindings;
use clay_mdk::native::CollectionRegistry;
use clay_mdk::native::RendererRegistry;
use clay_mdk::native::Resources;
use clay_mdk::native::factory;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

// The manifest is wire data (ADR 015 §9); the snapshot follows the current shape so identity,
// publication, or ACL drift is a reviewable diff.
#[test]
fn manifest_wire_shape_is_reviewable() {
    insta::assert_yaml_snapshot!(manifest());
}

#[tokio::test(flavor = "current_thread")]
async fn the_sample_section_lives_exactly_as_long_as_its_incarnation() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let host = Host::new();
            let resources = Resources {
                registry: NativeComponents::for_host(&host),
                documents: DocumentService::new(16),
                collections: CollectionRegistry::default(),
            };
            let sections = bytes_registrations(NonZeroUsize::new(64).unwrap(), [0, 0]);
            resources
                .collections
                .declare(
                    CollectionContract {
                        id: SECTIONS.into(),
                        key_schema: micro_settings_manifest::SECTION_KEY_SCHEMA.into(),
                        value_schema: micro_settings_manifest::SECTION_VALUE_SCHEMA.into(),
                    },
                    sections.clone(),
                )
                .unwrap();
            resources.documents.mock_documents().bind_config(
                CONFIG_NAME.into(),
                "sample-config".into(),
                "main".into(),
            );
            seed_mock_defaults(&resources.documents);
            let sample = ConfiguredMicro {
                identity: clay_host::MicroId::new(ID),
                eager: true,
                enabled: true,
                requirements: BTreeMap::new(),
                factory: factory(
                    resources,
                    manifest(),
                    CollectionBindings {
                        read: vec![],
                        write: vec![SECTIONS.into()],
                    },
                    activate,
                ),
            };
            let supervisor = Supervisor::new(host.clone(), [sample]);
            let id = clay_host::MicroId::new(ID);
            let mut key = Vec::new();
            ciborium::into_writer(ID, &mut key).unwrap();

            assert!(sections.get(&key).is_none());
            assert!(supervisor.reconcile().await.is_empty());
            let row = sections.get(&key).unwrap().value.unwrap();
            let section: Section = ciborium::from_reader(row.body.as_slice()).unwrap();
            assert_eq!(section.title, "Sample micro");
            assert_eq!(section.detail, Attachment::at(0));
            let [RowAttachment::Bound(detail)] = row.attachments.as_slice() else {
                panic!("the runtime row attaches its live presentation");
            };
            assert_eq!(detail.name(), DETAIL);

            supervisor.disable(&id).await.unwrap();
            assert_eq!(supervisor.state(&id), MicroState::Disabled);
            assert!(sections.get(&key).is_none());
            host.close();
        })
        .await;
}
