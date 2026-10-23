use super::*;
use clay_dioxus::NativeComponents;
use clay_mdk::native::FacetRoute;
use clay_mdk::native::output;
use clay_mdk::native::provider;
use clay_micro_core::application::ApplicationRuntime;
use clay_micro_core::micros::MicroId;
use clay_micro_core::micros::composition::ConfiguredMicro;
use clay_micro_core::micros::composition::Enablement;
use clay_micro_core::micros::composition::InstanceState;
use micro_settings::SettingsMicro;
use std::collections::BTreeMap;

type Application = ApplicationRuntime<NativeComponents>;

// ── manifest wire shape: the manifest is wire data (ADR 015 §9); the snapshot follows
//    the current shape so identity, requirement, ACL, or provision drift is a reviewable diff ──

#[test]
fn manifest_wire_shape_is_reviewable() {
    insta::assert_yaml_snapshot!(manifest());
}

fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

#[test]
fn configured_binding_selects_a_live_settings_instance_not_a_captured_controller() {
    let mut application = Application::new();
    assert!(application.roots().snapshot().unwrap().is_empty());
    let settings_provider = provider(
        &application,
        micro_settings_manifest::manifest(),
        micro_settings::activate,
    );
    application.register_provider(settings_provider).unwrap();
    let sample_provider = provider(&application, manifest(), activate);
    application.register_provider(sample_provider).unwrap();
    let host = application.host();
    let documents = application.documents();
    documents
        .mock_documents()
        .bind_config("sample".into(), "configuration".into(), "main".into());
    documents.mock_documents().seed(
        FacetRoute {
            document: "configuration".into(),
            branch: "main".into(),
            tag: CONFIG_TAG.into(),
            key_id: CONFIG_KEY.into(),
        },
        serde_json::to_value(SampleConfig {
            enabled: true,
            greeting: "Bound by configuration".into(),
        })
        .unwrap(),
    );
    let mut configuration = BTreeMap::from([
        (
            "first".into(),
            ConfiguredMicro {
                provider: MicroId::new(micro_settings_manifest::ID),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::new(),
            },
        ),
        (
            "second".into(),
            ConfiguredMicro {
                provider: MicroId::new(micro_settings_manifest::ID),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::new(),
            },
        ),
        (
            "sample".into(),
            ConfiguredMicro {
                provider: MicroId::new(ID),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::from([("settings.sections".into(), "second".into())]),
            },
        ),
    ]);
    assert_eq!(
        block_on(application.configure(&configuration)).unwrap(),
        BTreeMap::from([
            ("first".into(), InstanceState::Active),
            ("second".into(), InstanceState::Active),
            ("sample".into(), InstanceState::Active),
        ])
    );
    let first = output::<SettingsMicro, _>(&application, "first")
        .unwrap()
        .instance;
    let sample = output::<SampleMicro, _>(&application, "sample")
        .unwrap()
        .instance;
    let first_settings = output::<SettingsMicro, _>(&application, "first").unwrap();
    let second_settings = output::<SettingsMicro, _>(&application, "second").unwrap();
    assert!(first_settings.sections.snapshot().unwrap().is_empty());
    assert_eq!(second_settings.sections.snapshot().unwrap().len(), 1);
    configuration.get_mut("second").unwrap().enablement = Enablement::Disabled;
    assert_eq!(
        block_on(application.configure(&configuration)).unwrap(),
        BTreeMap::from([
            ("first".into(), InstanceState::Active),
            ("second".into(), InstanceState::Disabled),
            (
                "sample".into(),
                InstanceState::Blocked {
                    requirements: vec!["settings.sections".into()]
                }
            ),
        ])
    );
    assert_eq!(host.check(&sample), Err(AccessError::Revoked));
    assert!(matches!(
        output::<SampleMicro, _>(&application, "sample"),
        Err(AccessError::Revoked)
    ));
    assert_eq!(host.check(&first), Ok(()));
    assert_eq!(application.roots().snapshot().unwrap().len(), 1);
    configuration.get_mut("second").unwrap().enablement = Enablement::Enabled;
    assert_eq!(
        block_on(application.configure(&configuration)).unwrap(),
        BTreeMap::from([
            ("first".into(), InstanceState::Active),
            ("second".into(), InstanceState::Active),
            ("sample".into(), InstanceState::Active),
        ])
    );
    assert!(
        output::<SettingsMicro, _>(&application, "first")
            .unwrap()
            .instance
            == first
    );
    assert!(
        output::<SettingsMicro, _>(&application, "second")
            .unwrap()
            .instance
            != second_settings.instance
    );
    assert!(
        output::<SampleMicro, _>(&application, "sample")
            .unwrap()
            .instance
            != sample
    );
    assert_eq!(
        output::<SettingsMicro, _>(&application, "second")
            .unwrap()
            .sections
            .snapshot()
            .unwrap()
            .len(),
        1
    );
    let sample = output::<SampleMicro, _>(&application, "sample").unwrap();
    application.host().close_instance(&sample.instance).unwrap();
    assert_eq!(sample.watch().current().unwrap_err(), AccessError::Revoked);
    assert!(
        output::<SettingsMicro, _>(&application, "second")
            .unwrap()
            .sections
            .snapshot()
            .unwrap()
            .is_empty()
    );
    assert_eq!(host.check(&first), Ok(()));
    assert_eq!(application.roots().snapshot().unwrap().len(), 2);
    host.close();
}

#[test]
fn sample_micro_distinguishes_loading_absent_present_and_refreshing() {
    let mut application = Application::new();
    let settings_provider = provider(
        &application,
        micro_settings_manifest::manifest(),
        micro_settings::activate,
    );
    application.register_provider(settings_provider).unwrap();
    let sample_provider = provider(&application, manifest(), activate);
    application.register_provider(sample_provider).unwrap();

    let documents = application.documents();
    documents
        .mock_documents()
        .bind_config("sample".into(), "configuration".into(), "main".into());
    documents.mock_documents().hold_refreshes(true);

    let configuration = BTreeMap::from([
        (
            "settings".into(),
            ConfiguredMicro {
                provider: MicroId::new(micro_settings_manifest::ID),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::new(),
            },
        ),
        (
            "sample".into(),
            ConfiguredMicro {
                provider: MicroId::new(ID),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::from([("settings.sections".into(), "settings".into())]),
            },
        ),
    ]);
    assert_eq!(
        block_on(application.configure(&configuration)).unwrap(),
        BTreeMap::from([
            ("settings".into(), InstanceState::Active),
            ("sample".into(), InstanceState::Active),
        ])
    );

    let sample = output::<SampleMicro, _>(&application, "sample").unwrap();
    let loading = sample.watch().current().unwrap();
    assert_eq!(
        sample.state(&loading),
        clay_mdk::native::FacetValue::Loading
    );
    assert_eq!(loading.freshness, clay_mdk::native::Freshness::Refreshing);

    documents.mock_documents().complete_refreshes();
    let absent = sample.watch().current().unwrap();
    assert_eq!(sample.state(&absent), clay_mdk::native::FacetValue::Absent);
    assert_eq!(absent.freshness, clay_mdk::native::Freshness::Current);

    documents.mock_documents().seed(
        FacetRoute {
            document: "configuration".into(),
            branch: "main".into(),
            tag: CONFIG_TAG.into(),
            key_id: CONFIG_KEY.into(),
        },
        serde_json::to_value(SampleConfig {
            enabled: true,
            greeting: "Now present".into(),
        })
        .unwrap(),
    );
    sample.watch().refresh().unwrap();
    let refreshing = sample.watch().current().unwrap();
    assert_eq!(
        sample.state(&refreshing),
        clay_mdk::native::FacetValue::Absent
    );
    assert_eq!(
        refreshing.freshness,
        clay_mdk::native::Freshness::Refreshing
    );

    documents.mock_documents().complete_refreshes();
    let present = sample.watch().current().unwrap();
    assert_eq!(
        sample.state(&present),
        clay_mdk::native::FacetValue::Present(SampleConfig {
            enabled: true,
            greeting: "Now present".into(),
        })
    );
    assert_eq!(present.freshness, clay_mdk::native::Freshness::Current);

    application.host().close();
}
