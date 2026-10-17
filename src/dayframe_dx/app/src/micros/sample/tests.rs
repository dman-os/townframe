use super::*;
use crate::{
    application::ApplicationRuntime,
    documents::FacetRoute,
    micros::{
        self, MicroId,
        composition::{ConfiguredMicro, Enablement, InstanceState},
        settings::SettingsMicro,
    },
};
use std::collections::BTreeMap;

#[test]
fn configured_binding_selects_a_live_settings_instance_not_a_captured_controller() {
    let mut application = ApplicationRuntime::new();
    assert!(application.roots().snapshot().unwrap().is_empty());
    micros::register_native(&mut application).unwrap();
    let host = application.host();
    host.mock_documents()
        .bind_config("sample".into(), "configuration".into(), "main".into());
    host.mock_documents().seed(
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
                provider: MicroId::new("dayframe.settings"),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::new(),
            },
        ),
        (
            "second".into(),
            ConfiguredMicro {
                provider: MicroId::new("dayframe.settings"),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::new(),
            },
        ),
        (
            "sample".into(),
            ConfiguredMicro {
                provider: MicroId::new("dayframe.sample"),
                enablement: Enablement::Enabled,
                bindings: BTreeMap::from([("settings.sections".into(), "second".into())]),
            },
        ),
    ]);
    assert_eq!(
        application.configure(&configuration).unwrap(),
        BTreeMap::from([
            ("first".into(), InstanceState::Active),
            ("second".into(), InstanceState::Active),
            ("sample".into(), InstanceState::Active),
        ])
    );
    let first = application
        .output::<SettingsMicro>("first")
        .unwrap()
        .instance;
    let sample = application
        .output::<SampleMicro>("sample")
        .unwrap()
        .instance;
    let first_settings = application.output::<SettingsMicro>("first").unwrap();
    let second_settings = application.output::<SettingsMicro>("second").unwrap();
    assert!(first_settings.sections.snapshot().unwrap().is_empty());
    assert_eq!(second_settings.sections.snapshot().unwrap().len(), 1);
    configuration.get_mut("second").unwrap().enablement = Enablement::Disabled;
    assert_eq!(
        application.configure(&configuration).unwrap(),
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
    assert_eq!(sample.check(), Err(AccessError::Revoked));
    assert!(matches!(
        application.output::<SampleMicro>("sample"),
        Err(AccessError::Revoked)
    ));
    assert_eq!(first.check(), Ok(()));
    assert_eq!(application.roots().snapshot().unwrap().len(), 1);
    configuration.get_mut("second").unwrap().enablement = Enablement::Enabled;
    assert_eq!(
        application.configure(&configuration).unwrap(),
        BTreeMap::from([
            ("first".into(), InstanceState::Active),
            ("second".into(), InstanceState::Active),
            ("sample".into(), InstanceState::Active),
        ])
    );
    assert!(
        application
            .output::<SettingsMicro>("first")
            .unwrap()
            .instance
            == first
    );
    assert!(
        application
            .output::<SettingsMicro>("second")
            .unwrap()
            .instance
            != second_settings.instance
    );
    assert!(
        application
            .output::<SampleMicro>("sample")
            .unwrap()
            .instance
            != sample
    );
    assert_eq!(
        application
            .output::<SettingsMicro>("second")
            .unwrap()
            .sections
            .snapshot()
            .unwrap()
            .len(),
        1
    );
    host.close();
}
