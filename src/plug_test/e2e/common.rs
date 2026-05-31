use std::path::PathBuf;

use api_utils_rs::prelude::*;

pub async fn import_test_plug_oci(
    test_cx: &daybook_core::test_support::DaybookTestContext,
) -> Res<()> {
    let artifact_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/oci")
        .join(crate::plug_manifest().id());
    eyre::ensure!(
        artifact_path.exists(),
        "missing OCI plug artifact at '{}'. Build it first with: cargo run -p xtask -- build-plug-oci --plug-root ./src/plug_test",
        artifact_path.display()
    );

    test_cx
        .rt
        .plugs_repo
        .import_from_oci_layout(
            &artifact_path,
            daybook_core::plugs::OciImportOptions::default(),
        )
        .await?;
    Ok(())
}
