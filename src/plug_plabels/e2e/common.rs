use utils_rs::prelude::*;

/// Import and enable the plabels OCI, plus the `@daybook/test` plug whose
/// `embed-image`/`embed-text` processors produce the `Embedding` facets the
/// plabels `label-image`/`label-note` processors consume (plabels declares a
/// dependency on the `Embedding` facet but does not write it itself).
pub async fn import_plabels_oci(
    test_cx: &daybook_core::test_support::DaybookTestContext,
) -> Res<()> {
    import_plug_oci(test_cx, crate::plug_manifest().id(), "plug_plabels").await?;
    import_plug_oci(test_cx, "@daybook/test".into(), "plug_test").await?;
    Ok(())
}

async fn import_plug_oci(
    test_cx: &daybook_core::test_support::DaybookTestContext,
    plug_id: String,
    plug_root: &str,
) -> Res<()> {
    let artifact_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/oci")
        .join(&plug_id);
    eyre::ensure!(
        artifact_path.exists(),
        "missing OCI plug artifact at '{}'. Build it first with: cargo run -p xtask -- build-plug-oci --plug-root ./src/{plug_root}",
        artifact_path.display()
    );

    let imported = test_cx
        .rt
        .plugs_repo
        .import_from_oci_layout(
            &artifact_path,
            daybook_core::plugs::OciImportOptions::default(),
        )
        .await?;
    // ADR 007: dispatch behavior requires the plug to be enabled; the config
    // doc is created at enablement (and retained across disablement).
    let doc_id = imported
        .doc_id
        .ok_or_eyre("imported plug missing manifest doc id")?;
    let ref_url: Url =
        format!("db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch=main")
            .parse()?;
    let target = test_cx.rt.plugs_repo.enable_plug(&ref_url).await?;
    let status = test_cx.rt.triage_worker
        .wait_for_activation(&test_cx.rt.plugs_repo, target).await?;
    eyre::ensure!(
        matches!(status, daybook_core::rt::triage::ActivationStatus::Active(_)),
        "plug activation failed: {status:?}",
    );
    Ok(())
}
