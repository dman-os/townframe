use super::*;
use crate::repos::{Repo, SubscribeOpts};
#[tokio::test(flavor = "multi_thread")]
async fn inspect_test_plug_oci_layout() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_inspect_test_plug_oci_layout").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let artifact_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/oci")
        .join("@daybook/test");
    eyre::ensure!(
        artifact_path.exists(),
        "missing OCI plug artifact at '{}'. Build it first with: cargo run -p xtask -- build-plug-oci --plug-root ./src/plug_test",
        artifact_path.display()
    );

    let manifest = repo.inspect_oci_layout(&artifact_path).await?;
    assert_eq!(manifest.id(), "@daybook/test");
    assert_eq!(manifest.title, "Daybook Test Plug");
    assert_eq!(
        manifest.desc,
        "Internal e2e test plug for command invocation"
    );
    assert_eq!(manifest.version.to_string(), "0.0.1");
    assert!(!manifest.commands.is_empty());
    assert!(!manifest.facets.is_empty());
    assert!(!manifest.views.is_empty());
    assert!(!manifest.routines.is_empty());
    assert!(!manifest.processors.is_empty());
    ctx.stop().await?;
    Ok(())
}

fn mock_plug(name: &str) -> manifest::PlugManifest {
    manifest::PlugManifest {
        namespace: "test".into(),
        name: name.into(),
        version: "0.1.0".parse().unwrap(),
        title: format!("Test Plug {}", name),
        desc: "A test plug".into(),
        facets: vec![],
        local_states: default(),
        dependencies: default(),
        views: default(),
        routines: default(),
        wflow_bundles: default(),
        commands: default(),
        inits: default(),
        processors: default(),
    }
}

async fn temp_component_url() -> Res<(tempfile::TempDir, url::Url)> {
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path().join("component.wasm");
    tokio::fs::write(&temp_path, b"dummy wasm").await?;
    let file_url = url::Url::from_file_path(&temp_path).unwrap();
    Ok((temp_dir, file_url))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_add_success() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_plug_add_success").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let plug = mock_plug("plug1");

    repo.add(plug).await?;

    // Authoring makes the plug known; it is not active until enabled.
    let saved = repo.get_known("@test/plug1").await.unwrap();
    assert_eq!(saved.name, "plug1");
    assert!(repo.get("@test/plug1").await.is_none());
    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_add_emits_no_event() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_plug_add_emits_no_event").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let listener = repo.subscribe(SubscribeOpts::new(16));

    repo.add(mock_plug("plug-single-event")).await?;

    // ADR 007 §7: events are enabled-only. Authoring a manifest doc makes
    // the plug known (and writes the config facet's known_manifests, which
    // surfaces as PlugsConfigChanged via the switch), but emits no
    // enablement event until the plug is enabled.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let mut saw_enablement = false;
    while let Ok(event) = listener.try_recv() {
        if matches!(
            event.as_ref(),
            PlugsEvent::PlugEnabled { .. }
                | PlugsEvent::PlugDisabled { .. }
                | PlugsEvent::EnabledPlugUpdated { .. }
        ) {
            saw_enablement = true;
        }
    }
    assert!(
        !saw_enablement,
        "expected no enablement PlugsEvent for known-but-disabled manifest"
    );
    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_tag_clash() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_plug_tag_clash").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    // Add first plug with a tag
    let mut p1 = mock_plug("plug1");
    p1.facets.push(manifest::FacetManifest {
        key_tag: "org.test.tag".into(),
        value_schema: schemars::schema_for!(String),
        display_config: default(),
        references: default(),
    });
    repo.add(p1).await?;

    // Try to add second plug with same tag
    let mut p2 = mock_plug("plug2");
    p2.facets.push(manifest::FacetManifest {
        key_tag: "org.test.tag".into(),
        value_schema: schemars::schema_for!(String),
        display_config: default(),
        references: default(),
    });

    let res = repo.add(p2).await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("Tag clash"));

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_dependency_resolution() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_plug_dependency_resolution").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    // Add provider plug
    let mut provider = mock_plug("provider");
    provider.facets.push(manifest::FacetManifest {
        key_tag: "org.test.shared".into(),
        value_schema: schemars::schema_for!(String),
        display_config: default(),
        references: default(),
    });
    repo.add(provider).await?;

    // Add consumer plug that depends on provider
    let mut consumer = mock_plug("consumer");
    consumer.dependencies.insert(
        "@test/provider".into(),
        manifest::PlugDependencyManifest {
            keys: vec![manifest::FacetDependencyManifest {
                key_tag: "org.test.shared".into(),
                value_schema: schemars::schema_for!(String),
            }],
            local_states: vec![],
        }
        .into(),
    );

    repo.add(consumer).await?;
    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_missing_dependency() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_plug_missing_dependency").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let mut consumer = mock_plug("consumer");
    consumer.dependencies.insert(
        "@test/missing".into(),
        manifest::PlugDependencyManifest {
            keys: vec![],
            local_states: vec![],
        }
        .into(),
    );

    let res = repo.add(consumer).await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Dependency not found")
    );
    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_version_breaking_change() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_plug_version_breaking_change").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    // Create a temporary file for the component (keep it alive)
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path().join("component.wasm");
    tokio::fs::write(&temp_path, b"dummy wasm content").await?;
    let file_url = url::Url::from_file_path(&temp_path).unwrap();

    // Initial version
    let mut p1_v1 = mock_plug("plug1");
    p1_v1.version = "0.1.0".parse().unwrap();
    p1_v1.commands.insert(
        "cmd1".into(),
        manifest::CommandManifest {
            desc: "First command".into(),
            deets: manifest::CommandDeets::DocCommand {
                routine_name: "routine1".into(),
            },
        }
        .into(),
    );
    p1_v1.routines.insert(
        "routine1".into(),
        manifest::RoutineManifest {
            r#impl: manifest::RoutineImpl::Wflow {
                key: "wflow1".into(),
                bundle: "bundle1".into(),
            },
            doc_acls: vec![],
            query_acls: vec![],
            config_facet_acl: vec![],
            local_state_acl: vec![],
            command_invoke_acl: vec![],
        }
        .into(),
    );
    p1_v1.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec!["wflow1".into()],
            component_urls: vec![file_url],
        }
        .into(),
    );
    repo.add(p1_v1).await?;

    // Update version (patch) with command removed -> should fail
    let mut p1_v2 = mock_plug("plug1");
    p1_v2.version = "0.1.1".parse().unwrap();
    // cmd1 is missing

    let res = repo.add(p1_v2).await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("Breaking change"));

    // Update version (major) with command removed -> should succeed
    let mut p1_v3 = mock_plug("plug1");
    p1_v3.version = "1.0.0".parse().unwrap(); // major bump from 0.1 to 1.0 (in standard semver terms)

    repo.add(p1_v3).await?;
    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_version_must_increase() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_plug_version_must_increase").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    // Add initial version
    let mut p1_v1 = mock_plug("plug1");
    p1_v1.version = "0.1.0".parse().unwrap();
    repo.add(p1_v1).await?;

    // Try to add same version -> should fail
    let mut p1_same = mock_plug("plug1");
    p1_same.version = "0.1.0".parse().unwrap();
    let res = repo.add(p1_same).await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Version must be greater")
    );

    // Try to add lower version -> should fail
    let mut p1_lower = mock_plug("plug1");
    p1_lower.version = "0.0.9".parse().unwrap();
    let res = repo.add(p1_lower).await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Version must be greater")
    );

    // Add higher version -> should succeed
    let mut p1_v2 = mock_plug("plug1");
    p1_v2.version = "0.1.1".parse().unwrap();
    repo.add(p1_v2).await?;

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_bundle_key_validation() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_plug_bundle_key_validation").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let (_temp_dir, file_url) = temp_component_url().await?;

    // Create plug with routine referencing non-existent bundle
    let mut plug = mock_plug("plug1");
    plug.routines.insert(
        "routine1".into(),
        manifest::RoutineManifest {
            r#impl: manifest::RoutineImpl::Wflow {
                key: "wflow1".into(),
                bundle: "missing_bundle".into(),
            },
            doc_acls: vec![],
            query_acls: vec![],
            config_facet_acl: vec![],
            local_state_acl: vec![],
            command_invoke_acl: vec![],
        }
        .into(),
    );
    plug.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec!["wflow1".into()],
            component_urls: vec![file_url.clone()],
        }
        .into(),
    );

    let res = repo.add(plug).await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("wflow bundle 'missing_bundle' not found")
    );

    // Create plug with routine referencing non-existent key in bundle
    let mut plug2 = mock_plug("plug2");
    plug2.routines.insert(
        "routine1".into(),
        manifest::RoutineManifest {
            r#impl: manifest::RoutineImpl::Wflow {
                key: "missing_key".into(),
                bundle: "bundle1".into(),
            },
            doc_acls: vec![],
            query_acls: vec![],
            config_facet_acl: vec![],
            local_state_acl: vec![],
            command_invoke_acl: vec![],
        }
        .into(),
    );
    plug2.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec!["wflow1".into()],
            component_urls: vec![file_url],
        }
        .into(),
    );

    let res = repo.add(plug2).await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("key 'missing_key' not found")
    );

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_view_provider_bundle_must_exist() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_view_provider_bundle_must_exist").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let mut plug = mock_plug("missing-view-bundle");
    plug.views.insert(
        "summary".into(),
        Arc::new(manifest::ViewManifest {
            title: "Summary".into(),
            desc: "Summary view".into(),
            provider: manifest::ViewProviderManifest::StatelessWasm {
                bundle: "missing-bundle".into(),
                export: "render-facet-view".into(),
            },
        }),
    );

    let result = repo.add(plug).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("wflow bundle 'missing-bundle' not found")
    );

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_view_local_reference_must_exist() -> Res<()> {
    let ctx =
        crate::test_support::test_cx("plugs_test_custom_view_local_reference_must_exist").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let (_view_temp_dir, file_url) = temp_component_url().await?;

    let mut plug = mock_plug("custom-view-local");
    plug.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec![],
            component_urls: vec![file_url],
        }
        .into(),
    );
    plug.views.insert(
        "present-view".into(),
        Arc::new(manifest::ViewManifest {
            title: "Present".into(),
            desc: "Present view".into(),
            provider: manifest::ViewProviderManifest::StatelessWasm {
                bundle: "bundle1".into(),
                export: "render-facet-view".into(),
            },
        }),
    );
    plug.facets.push(manifest::FacetManifest {
        key_tag: "org.test.customview".into(),
        value_schema: schemars::schema_for!(serde_json::Value),
        display_config: manifest::FacetDisplayHint {
            deets: manifest::FacetDisplayDeets::CustomView {
                view: manifest::ViewRef {
                    plug_id: None,
                    view_key: "missing-view".into(),
                },
                mode: manifest::FacetViewMode::Display,
                priority: 0,
            },
            ..default()
        },
        references: vec![],
    });

    let result = repo.add(plug).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("view 'missing-view' not found in this plug")
    );

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_view_dependency_requires_declared_dependency() -> Res<()> {
    let ctx = crate::test_support::test_cx(
        "plugs_test_custom_view_dependency_requires_declared_dependency",
    )
    .await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let mut plug = mock_plug("custom-view-dependency-missing");
    plug.facets.push(manifest::FacetManifest {
        key_tag: "org.test.customview".into(),
        value_schema: schemars::schema_for!(serde_json::Value),
        display_config: manifest::FacetDisplayHint {
            deets: manifest::FacetDisplayDeets::CustomView {
                view: manifest::ViewRef {
                    plug_id: Some("@test/provider".into()),
                    view_key: "provider-view".into(),
                },
                mode: manifest::FacetViewMode::Display,
                priority: 0,
            },
            ..default()
        },
        references: vec![],
    });

    let result = repo.add(plug).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("neither this plug nor a declared dependency")
    );

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_custom_view_dependency_requires_target_view() -> Res<()> {
    let ctx =
        crate::test_support::test_cx("plugs_test_custom_view_dependency_requires_target_view")
            .await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let (_provider_temp_dir, file_url) = temp_component_url().await?;

    let mut provider = mock_plug("provider");
    provider.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec![],
            component_urls: vec![file_url.clone()],
        }
        .into(),
    );
    provider.views.insert(
        "provider-view".into(),
        Arc::new(manifest::ViewManifest {
            title: "Provider".into(),
            desc: "Provider view".into(),
            provider: manifest::ViewProviderManifest::StatelessWasm {
                bundle: "bundle1".into(),
                export: "render-facet-view".into(),
            },
        }),
    );
    repo.add(provider).await?;

    let mut caller = mock_plug("custom-view-dependency-missing-view");
    caller.dependencies.insert(
        "@test/provider".into(),
        manifest::PlugDependencyManifest {
            keys: vec![],
            local_states: vec![],
        }
        .into(),
    );
    caller.facets.push(manifest::FacetManifest {
        key_tag: "org.test.customview".into(),
        value_schema: schemars::schema_for!(serde_json::Value),
        display_config: manifest::FacetDisplayHint {
            deets: manifest::FacetDisplayDeets::CustomView {
                view: manifest::ViewRef {
                    plug_id: Some("@test/provider".into()),
                    view_key: "missing-view".into(),
                },
                mode: manifest::FacetViewMode::Display,
                priority: 0,
            },
            ..default()
        },
        references: vec![],
    });

    let result = repo.add(caller).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("not found in view provider plug '@test/provider'")
    );

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_component_url_validation() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_plug_component_url_validation").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    // Test with non-existent file URL
    let mut plug = mock_plug("plug1");
    plug.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec![],
            component_urls: vec!["file:///nonexistent/path".parse().unwrap()],
        }
        .into(),
    );

    let res = repo.add(plug).await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Component file not found")
    );

    // Test with non-existent blob URL
    let mut plug2 = mock_plug("plug2");
    plug2.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec![],
            component_urls: vec![
                format!("{}:///nonexistent_hash", crate::blobs::BLOB_SCHEME)
                    .parse()
                    .unwrap(),
            ],
        }
        .into(),
    );

    let res = repo.add(plug2).await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("Blob not found"));

    // Test with unsupported scheme
    let mut plug3 = mock_plug("plug3");
    plug3.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec![],
            component_urls: vec!["http://example.com/wasm.wasm".parse().unwrap()],
        }
        .into(),
    );

    let res = repo.add(plug3).await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("Unsupported URL scheme")
    );

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_reference_json_path_must_exist_in_schema() -> Res<()> {
    let ctx =
        crate::test_support::test_cx("plugs_test_plug_reference_json_path_must_exist_in_schema")
            .await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let mut plug = mock_plug("ref-path");
    plug.facets.push(manifest::FacetManifest {
        key_tag: "org.test.image".into(),
        value_schema: schemars::schema_for!(daybook_types::doc::ImageMetadata),
        display_config: default(),
        references: vec![manifest::FacetReferenceManifest::UrlStringSplit {
            json_path: "/doesNotExist".into(),
            at_commit_json_path: "/refHeads".into(),
        }],
    });

    let result = repo.add(plug).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("path does not exist in schema")
    );

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_at_commit_json_path_type_must_be_array_of_strings() -> Res<()> {
    let ctx = crate::test_support::test_cx(
        "plugs_test_plug_at_commit_json_path_type_must_be_array_of_strings",
    )
    .await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let mut plug = mock_plug("bad-at-commit");
    plug.facets.push(manifest::FacetManifest {
        key_tag: "org.test.image".into(),
        value_schema: schemars::schema_for!(daybook_types::doc::ImageMetadata),
        display_config: default(),
        references: vec![manifest::FacetReferenceManifest::UrlStringSplit {
            json_path: "/facetRef".into(),
            at_commit_json_path: "/mime".into(),
        }],
    });

    let result = repo.add(plug).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("must allow an array of commit hashes")
    );

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_processor_routine_must_exist() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_processor_routine_must_exist").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let mut plug = mock_plug("processor-routine");
    plug.processors.insert(
        "proc1".into(),
        manifest::ProcessorManifest {
            desc: "Processor".into(),
            deets: manifest::ProcessorDeets::DocProcessor {
                event_predicate: default(),
                predicate: manifest::DocPredicateClause::HasTag("org.test.tag".into()),
                routine_name: "missing-routine".into(),
            },
        }
        .into(),
    );

    let result = repo.add(plug).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid processor deets")
    );

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_processor_predicate_tags_must_be_in_scope() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_processor_predicate_tags_must_be_in_scope")
        .await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path().join("component.wasm");
    tokio::fs::write(&temp_path, b"dummy wasm").await?;
    let file_url = url::Url::from_file_path(&temp_path).unwrap();

    let mut plug = mock_plug("processor-predicate-scope");
    plug.routines.insert(
        "routine1".into(),
        manifest::RoutineManifest {
            r#impl: manifest::RoutineImpl::Wflow {
                key: "wflow1".into(),
                bundle: "bundle1".into(),
            },
            doc_acls: vec![],
            query_acls: vec![],
            config_facet_acl: vec![],
            local_state_acl: vec![],
            command_invoke_acl: vec![],
        }
        .into(),
    );
    plug.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec!["wflow1".into()],
            component_urls: vec![file_url],
        }
        .into(),
    );
    plug.processors.insert(
        "proc1".into(),
        manifest::ProcessorManifest {
            desc: "Processor".into(),
            deets: manifest::ProcessorDeets::DocProcessor {
                event_predicate: default(),
                predicate: manifest::DocPredicateClause::HasTag("org.test.missing".into()),
                routine_name: "routine1".into(),
            },
        }
        .into(),
    );

    let result = repo.add(plug).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Invalid processor predicate")
    );

    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_command_invoke_acl_rejects_target_without_dependency() -> Res<()> {
    let ctx = crate::test_support::test_cx(
        "plugs_test_command_invoke_acl_rejects_target_without_dependency",
    )
    .await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path().join("component.wasm");
    tokio::fs::write(&temp_path, b"dummy wasm").await?;
    let file_url = url::Url::from_file_path(&temp_path).unwrap();

    let mut target = mock_plug("target");
    target.routines.insert(
        "routine1".into(),
        manifest::RoutineManifest {
            r#impl: manifest::RoutineImpl::Wflow {
                key: "wflow1".into(),
                bundle: "bundle1".into(),
            },
            doc_acls: vec![],
            query_acls: vec![],
            config_facet_acl: vec![],
            local_state_acl: vec![],
            command_invoke_acl: vec![],
        }
        .into(),
    );
    target.commands.insert(
        "cmd1".into(),
        manifest::CommandManifest {
            desc: "target command".into(),
            deets: manifest::CommandDeets::DocCommand {
                routine_name: "routine1".into(),
            },
        }
        .into(),
    );
    target.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec!["wflow1".into()],
            component_urls: vec![file_url.clone()],
        }
        .into(),
    );
    repo.add(target).await?;

    let mut caller = mock_plug("caller");
    caller.routines.insert(
        "routine1".into(),
        manifest::RoutineManifest {
            r#impl: manifest::RoutineImpl::Wflow {
                key: "wflow1".into(),
                bundle: "bundle1".into(),
            },
            doc_acls: vec![],
            query_acls: vec![],
            config_facet_acl: vec![],
            local_state_acl: vec![],
            command_invoke_acl: vec!["db+command:///@test/target/cmd1".parse().unwrap()],
        }
        .into(),
    );
    caller.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec!["wflow1".into()],
            component_urls: vec![file_url.clone()],
        }
        .into(),
    );

    let result = repo.add(caller).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("declared dependency")
    );
    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_command_invoke_acl_rejects_missing_command() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_command_invoke_acl_rejects_missing_command")
        .await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path().join("component.wasm");
    tokio::fs::write(&temp_path, b"dummy wasm").await?;
    let file_url = url::Url::from_file_path(&temp_path).unwrap();

    let mut provider = mock_plug("provider");
    provider.routines.insert(
        "routine1".into(),
        manifest::RoutineManifest {
            r#impl: manifest::RoutineImpl::Wflow {
                key: "wflow1".into(),
                bundle: "bundle1".into(),
            },
            doc_acls: vec![],
            query_acls: vec![],
            config_facet_acl: vec![],
            local_state_acl: vec![],
            command_invoke_acl: vec![],
        }
        .into(),
    );
    provider.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec!["wflow1".into()],
            component_urls: vec![file_url.clone()],
        }
        .into(),
    );
    repo.add(provider).await?;

    let mut caller = mock_plug("caller");
    caller.dependencies.insert(
        "@test/provider".into(),
        manifest::PlugDependencyManifest {
            keys: vec![],
            local_states: vec![],
        }
        .into(),
    );
    caller.routines.insert(
        "routine1".into(),
        manifest::RoutineManifest {
            r#impl: manifest::RoutineImpl::Wflow {
                key: "wflow1".into(),
                bundle: "bundle1".into(),
            },
            doc_acls: vec![],
            query_acls: vec![],
            config_facet_acl: vec![],
            local_state_acl: vec![],
            command_invoke_acl: vec!["db+command:///@test/provider/nope".parse().unwrap()],
        }
        .into(),
    );
    caller.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec!["wflow1".into()],
            component_urls: vec![file_url],
        }
        .into(),
    );

    let result = repo.add(caller).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("target command"));
    ctx.stop().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_plug_file_to_blob_conversion() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_plug_file_to_blob_conversion").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    // Create a temporary file with wasm content (keep it alive)
    let temp_dir = tempfile::tempdir()?;
    let temp_path = temp_dir.path().join("component.wasm");
    let wasm_content = b"fake wasm binary content";
    tokio::fs::write(&temp_path, wasm_content).await?;
    let file_url = url::Url::from_file_path(&temp_path).unwrap();

    // Create plug with file:// URL
    let mut plug = mock_plug("plug1");
    plug.wflow_bundles.insert(
        "bundle1".into(),
        manifest::WflowBundleManifest {
            keys: vec![],
            component_urls: vec![file_url],
        }
        .into(),
    );

    // Add plug - should convert file:// to db+blob://
    repo.add(plug.clone()).await?;

    // Retrieve the plug and verify URL was converted
    let saved = repo.get_known("@test/plug1").await.unwrap();
    let bundle = saved.wflow_bundles.get("bundle1").unwrap();
    assert_eq!(bundle.component_urls.len(), 1);
    let converted_url = &bundle.component_urls[0];
    assert_eq!(converted_url.scheme(), crate::blobs::BLOB_SCHEME);

    // Verify the blob exists and contains the correct content
    let hash = converted_url.path().trim_start_matches('/');
    let blob_path = repo
        .blobs
        .get_path(hash.parse::<crate::blobs::BlobId>()?)
        .await?;
    let blob_content = tokio::fs::read(&blob_path).await?;
    assert_eq!(blob_content, wasm_content);

    ctx.stop().await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 1: config-delta events, gates, and the known_plugs track (ADR 007 §5/§7).
// Local paths go through the public mutators; remote paths are driven by writing
// the config / manifest facets directly through the drawer (the switch's Doc
// events always carry origin = Remote; the notif loop's local-writer-actor
// filter is what distinguishes local writes).
// ---------------------------------------------------------------------------

fn mock_plug_at(name: &str, version: &str) -> manifest::PlugManifest {
    let mut plug = mock_plug(name);
    plug.version = version.parse().unwrap();
    plug
}

fn mock_plug_with_facet(
    name: &str,
    version: &str,
    tag: &str,
    schema: schemars::Schema,
) -> manifest::PlugManifest {
    let mut plug = mock_plug_at(name, version);
    plug.facets.push(manifest::FacetManifest {
        key_tag: tag.into(),
        value_schema: schema,
        display_config: default(),
        references: default(),
    });
    plug
}

async fn read_config(ctx: &crate::test_support::DaybookTestContext) -> Res<PlugsConfig> {
    let config_doc_id = ctx.rt.rcx.doc_config.document_id().to_string();
    let facet_key =
        daybook_types::doc::FacetKey::from(daybook_types::doc::WellKnownFacetTag::PlugsConfig);
    let doc = ctx
        .rt
        .drawer
        .get_doc_with_facets_at_branch(
            &config_doc_id,
            daybook_types::doc::BranchPath::new("main"),
            Some(vec![facet_key.clone()]),
        )
        .await?
        .ok_or_eyre("config doc missing")?;
    let raw = doc
        .facets
        .get(&facet_key)
        .ok_or_eyre("config facet missing")?;
    Ok(serde_json::from_value(raw.clone())?)
}

/// Simulate a remote manifest change: write a new manifest version to an
/// existing manifest doc through the drawer.
async fn write_manifest_via_drawer(
    ctx: &crate::test_support::DaybookTestContext,
    doc_id: &daybook_types::doc::DocId,
    manifest: &manifest::PlugManifest,
) -> Res<()> {
    ctx.rt
        .drawer
        .update_at_heads(
            daybook_types::doc::DocPatch {
                id: doc_id.clone(),
                facets_set: [(
                    PlugsRepo::plug_manifest_facet_key(),
                    daybook_types::doc::WellKnownFacet::PlugManifest(manifest.clone()).into(),
                )]
                .into(),
                facets_remove: vec![],
                user_path: None,
            },
            daybook_types::doc::BranchPath::new("main"),
            None,
        )
        .await?;
    Ok(())
}

async fn doc_heads(
    ctx: &crate::test_support::DaybookTestContext,
    doc_id: &daybook_types::doc::DocId,
) -> Res<ChangeHashSet> {
    ctx.rt
        .drawer
        .get_doc_branches(doc_id)
        .await?
        .ok_or_eyre("doc missing")?
        .branches
        .get("main")
        .cloned()
        .ok_or_eyre("doc missing main branch")
}

async fn wait_for_event(
    listener: &crate::repos::ListenerHandle<PlugsEvent>,
    pred: impl Fn(&PlugsEvent) -> bool,
    what: &str,
) -> Res<Arc<PlugsEvent>> {
    let start = std::time::Instant::now();
    loop {
        while let Ok(event) = listener.try_recv() {
            if pred(event.as_ref()) {
                return Ok(event);
            }
        }
        if start.elapsed() > std::time::Duration::from_secs(5) {
            eyre::bail!("timed out waiting for {what}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

fn drain_events(listener: &crate::repos::ListenerHandle<PlugsEvent>) -> Vec<Arc<PlugsEvent>> {
    let mut events = vec![];
    while let Ok(event) = listener.try_recv() {
        events.push(event);
    }
    events
}

async fn wait_until<F, Fut>(mut f: F, what: &str) -> Res<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    loop {
        if f().await {
            return Ok(());
        }
        if start.elapsed() > std::time::Duration::from_secs(5) {
            eyre::bail!("timed out waiting for {what}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// ADR 007 §7: enabling a plug emits `PlugEnabled` (local origin), makes it
/// active, and records `last_enabled_version` in the track.
#[tokio::test(flavor = "multi_thread")]
async fn test_enable_plug_emits_plug_enabled() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_enable_plug_emits_plug_enabled").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let listener = repo.subscribe(SubscribeOpts::new(32));

    let doc_id = repo.add(mock_plug("plug1")).await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let ref_url = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;

    repo.enable_plug(&ref_url).await?;

    let event = wait_for_event(
        &listener,
        |e| matches!(e, PlugsEvent::PlugEnabled { .. }),
        "PlugEnabled",
    )
    .await?;
    let PlugsEvent::PlugEnabled {
        id,
        heads: ev_heads,
        origin,
    } = event.as_ref()
    else {
        unreachable!()
    };
    assert_eq!(id, "@test/plug1");
    assert_eq!(ev_heads, &heads);
    assert!(matches!(
        origin,
        crate::event_origin::SwitchEventOrigin::Local { .. }
    ));

    // Active now.
    assert!(repo.get("@test/plug1").await.is_some());

    // Track: last_enabled_version recorded.
    let config = read_config(&ctx).await?;
    let track = config
        .known_plugs
        .get("@test/plug1")
        .ok_or_eyre("track missing")?;
    assert_eq!(track.last_enabled_version.as_deref(), Some("0.1.0"));

    ctx.stop().await?;
    Ok(())
}

/// ADR 007 §7: re-enabling the same ref is a no-op — no duplicate
/// `PlugEnabled` (the notif loop's local-writer-actor filter also prevents
/// double-processing of the mutator's own config write).
#[tokio::test(flavor = "multi_thread")]
async fn test_re_enable_same_ref_no_duplicate_event() -> Res<()> {
    let ctx =
        crate::test_support::test_cx("plugs_test_re_enable_same_ref_no_duplicate_event").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let listener = repo.subscribe(SubscribeOpts::new(32));

    let doc_id = repo.add(mock_plug("plug1")).await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let ref_url = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;
    repo.enable_plug(&ref_url).await?;
    wait_for_event(
        &listener,
        |e| matches!(e, PlugsEvent::PlugEnabled { .. }),
        "first PlugEnabled",
    )
    .await?;
    drain_events(&listener);

    repo.enable_plug(&ref_url).await?;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let events = drain_events(&listener);
    assert!(
        !events
            .iter()
            .any(|e| matches!(e.as_ref(), PlugsEvent::PlugEnabled { .. })),
        "re-enable of the same ref must not emit PlugEnabled"
    );

    ctx.stop().await?;
    Ok(())
}

/// ADR 007 §7: disabling a plug emits `PlugDisabled` and clears the active
/// entry.
#[tokio::test(flavor = "multi_thread")]
async fn test_disable_plug_emits_plug_disabled() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_disable_plug_emits_plug_disabled").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let listener = repo.subscribe(SubscribeOpts::new(32));

    let doc_id = repo.add(mock_plug("plug1")).await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let ref_url = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;
    repo.enable_plug(&ref_url).await?;
    wait_for_event(
        &listener,
        |e| matches!(e, PlugsEvent::PlugEnabled { .. }),
        "PlugEnabled",
    )
    .await?;
    drain_events(&listener);

    repo.disable_plug("@test/plug1").await?;

    let event = wait_for_event(
        &listener,
        |e| matches!(e, PlugsEvent::PlugDisabled { .. }),
        "PlugDisabled",
    )
    .await?;
    let PlugsEvent::PlugDisabled { id, origin } = event.as_ref() else {
        unreachable!()
    };
    assert_eq!(id, "@test/plug1");
    assert!(matches!(
        origin,
        crate::event_origin::SwitchEventOrigin::Local { .. }
    ));
    assert!(repo.get("@test/plug1").await.is_none());

    ctx.stop().await?;
    Ok(())
}

/// ADR 007 §7: re-pinning to a newer version of the same manifest doc emits
/// `EnabledPlugUpdated` and the active manifest becomes the new version.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_plug_emits_enabled_plug_updated() -> Res<()> {
    let ctx =
        crate::test_support::test_cx("plugs_test_update_plug_emits_enabled_plug_updated").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let listener = repo.subscribe(SubscribeOpts::new(32));

    let doc_id = repo.add(mock_plug_at("plug1", "0.1.0")).await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let ref_url = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;
    repo.enable_plug(&ref_url).await?;
    wait_for_event(
        &listener,
        |e| matches!(e, PlugsEvent::PlugEnabled { .. }),
        "PlugEnabled",
    )
    .await?;

    // Remote republish: v0.2.0 on the same doc (valid, no breaking change).
    write_manifest_via_drawer(&ctx, &doc_id, &mock_plug_at("plug1", "0.2.0")).await?;
    wait_until(
        || async {
            read_config(&ctx)
                .await
                .map(|c| {
                    c.known_plugs
                        .get("@test/plug1")
                        .is_some_and(|t| t.latest_version == "0.2.0")
                })
                .unwrap_or(false)
        },
        "record of v0.2.0",
    )
    .await?;
    drain_events(&listener);

    repo.update_plug("@test/plug1").await?;

    let event = wait_for_event(
        &listener,
        |e| matches!(e, PlugsEvent::EnabledPlugUpdated { .. }),
        "EnabledPlugUpdated",
    )
    .await?;
    let PlugsEvent::EnabledPlugUpdated { id, .. } = event.as_ref() else {
        unreachable!()
    };
    assert_eq!(id, "@test/plug1");
    let active = repo
        .get("@test/plug1")
        .await
        .ok_or_eyre("plug not active")?;
    assert_eq!(active.version.to_string(), "0.2.0");

    ctx.stop().await?;
    Ok(())
}

/// ADR 007 §5: activating the same version that was rejected is blocked —
/// the durable `latest_rejection` blocks "update to latest".
#[tokio::test(flavor = "multi_thread")]
async fn test_same_version_activation_blocked_when_latest_rejected() -> Res<()> {
    let ctx = crate::test_support::test_cx(
        "plugs_test_same_version_activation_blocked_when_latest_rejected",
    )
    .await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let doc_id = repo
        .add(mock_plug_with_facet(
            "plug1",
            "0.1.0",
            "org.test.prop1",
            schemars::schema_for!(String),
        ))
        .await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let v1_ref = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;
    repo.enable_plug(&v1_ref).await?;

    // Remote republish: v0.2.0 with an incompatible schema for the same tag
    // (breaking in a non-major update) — the record gate rejects it.
    write_manifest_via_drawer(
        &ctx,
        &doc_id,
        &mock_plug_with_facet(
            "plug1",
            "0.2.0",
            "org.test.prop1",
            schemars::schema_for!(i64),
        ),
    )
    .await?;
    wait_until(
        || async {
            read_config(&ctx)
                .await
                .map(|c| {
                    c.known_plugs
                        .get("@test/plug1")
                        .is_some_and(|t| t.latest_rejection.is_some())
                })
                .unwrap_or(false)
        },
        "rejection of v0.2.0",
    )
    .await?;

    // Explicit activation of the rejected version is blocked.
    let v2_heads = doc_heads(&ctx, &doc_id).await?;
    let v2_ref = PlugsRepo::build_enabled_ref(&doc_id, "main", &v2_heads)?;
    let res = repo.enable_plug(&v2_ref).await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("was rejected"));

    ctx.stop().await?;
    Ok(())
}

/// ADR 007 §5: `update_plug` (jump to latest) is blocked when the latest
/// version was rejected.
#[tokio::test(flavor = "multi_thread")]
async fn test_update_plug_blocked_when_latest_rejected() -> Res<()> {
    let ctx =
        crate::test_support::test_cx("plugs_test_update_plug_blocked_when_latest_rejected").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let doc_id = repo
        .add(mock_plug_with_facet(
            "plug1",
            "0.1.0",
            "org.test.prop1",
            schemars::schema_for!(String),
        ))
        .await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let v1_ref = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;
    repo.enable_plug(&v1_ref).await?;

    write_manifest_via_drawer(
        &ctx,
        &doc_id,
        &mock_plug_with_facet(
            "plug1",
            "0.2.0",
            "org.test.prop1",
            schemars::schema_for!(i64),
        ),
    )
    .await?;
    wait_until(
        || async {
            read_config(&ctx)
                .await
                .map(|c| {
                    c.known_plugs
                        .get("@test/plug1")
                        .is_some_and(|t| t.latest_rejection.is_some())
                })
                .unwrap_or(false)
        },
        "rejection of v0.2.0",
    )
    .await?;

    let res = repo.update_plug("@test/plug1").await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("rejected"));

    ctx.stop().await?;
    Ok(())
}

/// ADR 007 §5: rolling back to the previously enabled version after a
/// disable is allowed (it was validated when first enabled).
#[tokio::test(flavor = "multi_thread")]
async fn test_rollback_to_last_enabled_allowed() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_rollback_to_last_enabled_allowed").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let doc_id = repo.add(mock_plug_at("plug1", "0.1.0")).await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let v1_ref = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;
    repo.enable_plug(&v1_ref).await?;

    // A newer valid version is recorded (but never enabled).
    write_manifest_via_drawer(&ctx, &doc_id, &mock_plug_at("plug1", "0.2.0")).await?;
    wait_until(
        || async {
            read_config(&ctx)
                .await
                .map(|c| {
                    c.known_plugs
                        .get("@test/plug1")
                        .is_some_and(|t| t.latest_version == "0.2.0")
                })
                .unwrap_or(false)
        },
        "record of v0.2.0",
    )
    .await?;

    repo.disable_plug("@test/plug1").await?;
    // Rollback to the last enabled version (0.1.0) is allowed.
    repo.enable_plug(&v1_ref).await?;
    let active = repo
        .get("@test/plug1")
        .await
        .ok_or_eyre("plug not active")?;
    assert_eq!(active.version.to_string(), "0.1.0");

    ctx.stop().await?;
    Ok(())
}

/// ADR 007 §5: a downgrade to a version that was never enabled is blocked.
#[tokio::test(flavor = "multi_thread")]
async fn test_downgrade_blocked() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_downgrade_blocked").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let doc_id = repo.add(mock_plug_at("plug1", "0.1.0")).await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let v1_ref = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;
    repo.enable_plug(&v1_ref).await?;

    // Record + enable v0.2.0: last_enabled becomes 0.2.0.
    write_manifest_via_drawer(&ctx, &doc_id, &mock_plug_at("plug1", "0.2.0")).await?;
    wait_until(
        || async {
            read_config(&ctx)
                .await
                .map(|c| {
                    c.known_plugs
                        .get("@test/plug1")
                        .is_some_and(|t| t.latest_version == "0.2.0")
                })
                .unwrap_or(false)
        },
        "record of v0.2.0",
    )
    .await?;
    let v2_heads = doc_heads(&ctx, &doc_id).await?;
    let v2_ref = PlugsRepo::build_enabled_ref(&doc_id, "main", &v2_heads)?;
    repo.enable_plug(&v2_ref).await?;

    repo.disable_plug("@test/plug1").await?;
    // Re-enabling 0.1.0 is a downgrade (last enabled was 0.2.0) — blocked.
    let res = repo.enable_plug(&v1_ref).await;
    assert!(res.is_err());
    assert!(res.unwrap_err().to_string().contains("downgrade rejected"));

    ctx.stop().await?;
    Ok(())
}

/// ADR 007 §5: the per-plug track records latest (valid or rejected), last
/// valid, and last enabled — the durable state that gates updates.
#[tokio::test(flavor = "multi_thread")]
async fn test_known_plugs_track_fields() -> Res<()> {
    let ctx = crate::test_support::test_cx("plugs_test_known_plugs_track_fields").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);

    let doc_id = repo
        .add(mock_plug_with_facet(
            "plug1",
            "0.1.0",
            "org.test.prop1",
            schemars::schema_for!(String),
        ))
        .await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let v1_ref = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;

    // After add: latest == last_valid == v0.1.0, never enabled.
    let config = read_config(&ctx).await?;
    let track = config
        .known_plugs
        .get("@test/plug1")
        .ok_or_eyre("track missing")?;
    assert_eq!(track.latest, v1_ref);
    assert_eq!(track.latest_version, "0.1.0");
    assert_eq!(track.latest_rejection, None);
    assert_eq!(track.last_valid, v1_ref);
    assert_eq!(track.last_valid_version, "0.1.0");
    assert_eq!(track.last_enabled_version, None);

    // After enable: last_enabled_version recorded.
    repo.enable_plug(&v1_ref).await?;
    let config = read_config(&ctx).await?;
    let track = config
        .known_plugs
        .get("@test/plug1")
        .ok_or_eyre("track missing")?;
    assert_eq!(track.last_enabled_version.as_deref(), Some("0.1.0"));

    // Remote republish v0.2.0 with an incompatible schema: rejected. The
    // track keeps latest = v0.2.0 + rejection, last_valid stays v0.1.0.
    write_manifest_via_drawer(
        &ctx,
        &doc_id,
        &mock_plug_with_facet(
            "plug1",
            "0.2.0",
            "org.test.prop1",
            schemars::schema_for!(i64),
        ),
    )
    .await?;
    wait_until(
        || async {
            read_config(&ctx)
                .await
                .map(|c| {
                    c.known_plugs
                        .get("@test/plug1")
                        .is_some_and(|t| t.latest_rejection.is_some())
                })
                .unwrap_or(false)
        },
        "rejection of v0.2.0",
    )
    .await?;
    let v2_heads = doc_heads(&ctx, &doc_id).await?;
    let v2_ref = PlugsRepo::build_enabled_ref(&doc_id, "main", &v2_heads)?;
    let config = read_config(&ctx).await?;
    let track = config
        .known_plugs
        .get("@test/plug1")
        .ok_or_eyre("track missing")?;
    assert_eq!(track.latest, v2_ref);
    assert_eq!(track.latest_version, "0.2.0");
    assert!(
        track
            .latest_rejection
            .as_deref()
            .unwrap_or("")
            .contains("Incompatible schema")
    );
    assert_eq!(track.last_valid, v1_ref);
    assert_eq!(track.last_valid_version, "0.1.0");
    assert_eq!(track.last_enabled_version.as_deref(), Some("0.1.0"));

    ctx.stop().await?;
    Ok(())
}

/// ADR 007 §5: a remote manifest change that fails the record gate emits
/// `ManifestRejected` with the rejection reason and a remote origin.
#[tokio::test(flavor = "multi_thread")]
async fn test_remote_manifest_rejection_emits_manifest_rejected() -> Res<()> {
    let ctx = crate::test_support::test_cx(
        "plugs_test_remote_manifest_rejection_emits_manifest_rejected",
    )
    .await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let listener = repo.subscribe(SubscribeOpts::new(32));

    let doc_id = repo
        .add(mock_plug_with_facet(
            "plug1",
            "0.1.0",
            "org.test.prop1",
            schemars::schema_for!(String),
        ))
        .await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let v1_ref = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;
    repo.enable_plug(&v1_ref).await?;
    wait_for_event(
        &listener,
        |e| matches!(e, PlugsEvent::PlugEnabled { .. }),
        "PlugEnabled",
    )
    .await?;
    drain_events(&listener);

    write_manifest_via_drawer(
        &ctx,
        &doc_id,
        &mock_plug_with_facet(
            "plug1",
            "0.2.0",
            "org.test.prop1",
            schemars::schema_for!(i64),
        ),
    )
    .await?;

    let event = wait_for_event(
        &listener,
        |e| matches!(e, PlugsEvent::ManifestRejected { .. }),
        "ManifestRejected",
    )
    .await?;
    let PlugsEvent::ManifestRejected {
        id,
        version,
        reason,
        origin,
    } = event.as_ref()
    else {
        unreachable!()
    };
    assert_eq!(id, "@test/plug1");
    assert_eq!(version, "0.2.0");
    assert!(reason.contains("Incompatible schema"));
    assert!(matches!(
        origin,
        crate::event_origin::SwitchEventOrigin::Remote { .. }
    ));

    ctx.stop().await?;
    Ok(())
}

/// ADR 007 §7: the notif loop's local-writer-actor filter prevents
/// double-processing — a local enable emits exactly one `PlugEnabled`
/// (from the mutator), never a second one from the loop.
#[tokio::test(flavor = "multi_thread")]
async fn test_local_config_write_not_double_processed() -> Res<()> {
    let ctx =
        crate::test_support::test_cx("plugs_test_local_config_write_not_double_processed").await?;
    let repo = Arc::clone(&ctx.rt.plugs_repo);
    let listener = repo.subscribe(SubscribeOpts::new(32));

    let doc_id = repo.add(mock_plug("plug1")).await?;
    let heads = doc_heads(&ctx, &doc_id).await?;
    let ref_url = PlugsRepo::build_enabled_ref(&doc_id, "main", &heads)?;
    repo.enable_plug(&ref_url).await?;

    wait_for_event(
        &listener,
        |e| matches!(e, PlugsEvent::PlugEnabled { .. }),
        "PlugEnabled",
    )
    .await?;
    // Let the notif loop drain any in-flight config diffs.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let events = drain_events(&listener);
    let enabled_count = events
        .iter()
        .filter(|e| matches!(e.as_ref(), PlugsEvent::PlugEnabled { .. }))
        .count();
    assert_eq!(
        enabled_count, 0,
        "no duplicate PlugEnabled from the notif loop"
    );

    ctx.stop().await?;
    Ok(())
}
