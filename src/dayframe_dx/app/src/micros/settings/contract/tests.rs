use super::*;
use crate::host::{AccessError, Host};

#[test]
fn output_binds_independent_consumers_and_cannot_outlive_provider() {
    let host = Host::new();
    let provider = host.start("settings");
    let first = host.start("first");
    let second = host.start("second");
    let output = SettingsSections {
        collection: HostedCollection::new(&host, &provider),
        provider: provider.clone(),
    };
    let reader = output.collection.reader(&provider);
    let first_grant = output.bind(&first).unwrap();
    let second_grant = output.bind(&second).unwrap();
    let detail = host.publish_component(&first).unwrap();
    let first_entry = first_grant
        .insert(SettingsSection {
            title: "First".into(),
            detail,
        })
        .unwrap();
    let second_detail = host.publish_component(&second).unwrap();
    let second_entry = second_grant
        .insert(SettingsSection {
            title: "Second".into(),
            detail: second_detail,
        })
        .unwrap();
    first.close();
    assert_eq!(
        reader.snapshot().unwrap().into_values().collect::<Vec<_>>(),
        vec![SettingsSection {
            title: "Second".into(),
            detail: second_detail
        }]
    );
    second_entry
        .update(SettingsSection {
            title: "Updated".into(),
            detail: second_detail,
        })
        .unwrap();
    assert_eq!(
        first_entry.update(SettingsSection {
            title: "Stale".into(),
            detail
        }),
        Err(AccessError::Revoked)
    );
    provider.close();
    assert!(matches!(
        second_grant.insert(SettingsSection {
            title: "Stale".into(),
            detail
        }),
        Err(AccessError::Revoked)
    ));
    let late = host.start("late");
    assert!(matches!(output.bind(&late), Err(AccessError::Revoked)));
    host.close();
}
