use std::path::PathBuf;

use utils_rs::prelude::*;

pub async fn import_plabels_oci(
    test_cx: &daybook_core::test_support::DaybookTestContext,
) -> Res<()> {
    let artifact_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/oci")
        .join(crate::plug_manifest().id());
    eyre::ensure!(
        artifact_path.exists(),
        "missing OCI plug artifact at '{}'. Build it first with: cargo run -p xtask -- build-plug-oci --plug-root ./src/plug_plabels",
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
        .ok_or_eyre("imported plabels plug missing manifest doc id")?;
    let ref_url: url::Url = format!(
        "db+facet:///{doc_id}/org.example.daybook.plugManifest/main?branch=main"
    )
    .parse()?;
    test_cx.rt.plugs_repo.enable_plug(&ref_url).await?;
    Ok(())
}
