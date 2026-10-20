use super::*;
use clay_compose::micros::{
    MicroId,
    composition::{ConfiguredMicro, Enablement, InstanceState},
};
use clay_dioxus::application::ApplicationRuntime;
use clay_mdk::native::{FacetRoute, output, provider};
use micro_settings::SettingsMicro;
use std::collections::BTreeMap;

fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

#[test]
fn configured_binding_selects_a_live_settings_instance_not_a_captured_controller() {
    let mut application = ApplicationRuntime::new();
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
    let first = output::<SettingsMicro>(&application, "first")
        .unwrap()
        .instance;
    let sample = output::<SampleMicro>(&application, "sample")
        .unwrap()
        .instance;
    let first_settings = output::<SettingsMicro>(&application, "first").unwrap();
    let second_settings = output::<SettingsMicro>(&application, "second").unwrap();
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
        output::<SampleMicro>(&application, "sample"),
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
        output::<SettingsMicro>(&application, "first")
            .unwrap()
            .instance
            == first
    );
    assert!(
        output::<SettingsMicro>(&application, "second")
            .unwrap()
            .instance
            != second_settings.instance
    );
    assert!(
        output::<SampleMicro>(&application, "sample")
            .unwrap()
            .instance
            != sample
    );
    assert_eq!(
        output::<SettingsMicro>(&application, "second")
            .unwrap()
            .sections
            .snapshot()
            .unwrap()
            .len(),
        1
    );
    let sample = output::<SampleMicro>(&application, "sample").unwrap();
    application.host().close_instance(&sample.instance).unwrap();
    assert_eq!(sample.watch().current().unwrap_err(), AccessError::Revoked);
    assert!(
        output::<SettingsMicro>(&application, "second")
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
