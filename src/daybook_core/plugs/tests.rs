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
