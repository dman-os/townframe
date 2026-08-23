use super::*;

pub const OCI_PLUG_ARTIFACT_TYPE: &str = "application/vnd.daybook.plug.v1";
pub const OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE: &str =
    "application/vnd.daybook.plug.manifest.v1+json";

#[derive(Debug, Clone, Copy)]
pub struct OciImportOptions {
    pub strict: bool,
}

impl Default for OciImportOptions {
    fn default() -> Self {
        Self { strict: true }
    }
}



// oci support
impl PlugsRepo {
    /// ADR 007 §8: authoring import from a local OCI layout. Pulls the
    /// artifact layers into the blob store, writes a manifest doc (drawer
    /// `add`, validated), leaving the plug known but not enabled.
    pub async fn import_from_oci_layout(
        &self,
        layout_root: &std::path::Path,
        opts: OciImportOptions,
    ) -> Res<ImportedPlug> {
        let (image_manifest, selected_manifest_sha) =
            Self::load_oci_layout_image_manifest(layout_root).await?;

        self.import_from_oci_image_manifest(
            image_manifest,
            Some(selected_manifest_sha),
            opts,
            |digest| async move {
                let sha = Self::sha256_hex_from_digest_str(&digest)?;
                Self::read_oci_layout_blob_by_sha(layout_root, &sha).await
            },
        )
        .await
    }

    pub async fn inspect_oci_layout(
        &self,
        layout_root: &std::path::Path,
    ) -> Res<manifest::PlugManifest> {
        let (image_manifest, _) = Self::load_oci_layout_image_manifest(layout_root).await?;
        Self::inspect_oci_image_manifest(&image_manifest, |digest| async move {
            let sha = Self::sha256_hex_from_digest_str(&digest)?;
            Self::read_oci_layout_blob_by_sha(layout_root, &sha).await
        })
        .await
    }

    pub async fn import_from_oci_registry(
        &self,
        reference: &str,
        auth: oci_client::secrets::RegistryAuth,
        opts: OciImportOptions,
    ) -> Res<ImportedPlug> {
        let reference: oci_client::Reference = reference.parse()?;
        let client_config = oci_client::client::ClientConfig {
            connect_timeout: Some(std::time::Duration::from_secs(15)),
            read_timeout: Some(std::time::Duration::from_secs(300)),
            ..Default::default()
        };
        let client = oci_client::Client::new(client_config);
        let (manifest, source_digest) = client.pull_manifest(&reference, &auth).await?;
        let (target_manifest, target_ref) = match manifest {
            oci_client::manifest::OciManifest::Image(manifest) => (manifest, reference.clone()),
            oci_client::manifest::OciManifest::ImageIndex(index_manifest) => {
                let desc = index_manifest
                    .manifests
                    .first()
                    .ok_or_eyre("oci image index has no manifests")?;
                let target_ref = reference.clone_with_digest(desc.digest.clone());
                let (nested, _) = client.pull_manifest(&target_ref, &auth).await?;
                let oci_client::manifest::OciManifest::Image(manifest) = nested else {
                    eyre::bail!("nested OCI manifest must resolve to an image manifest");
                };
                (manifest, target_ref)
            }
        };

        self.import_from_oci_image_manifest(target_manifest, Some(source_digest), opts, |digest| {
            let client = &client;
            let target_ref = &target_ref;
            async move {
                let mut out = Vec::new();
                client
                    .pull_blob(target_ref, digest.as_str(), &mut out)
                    .await?;
                eyre::Ok(out)
            }
        })
        .await
    }

    async fn import_from_oci_image_manifest<F, Fut>(
        &self,
        image_manifest: oci_client::manifest::OciImageManifest,
        source_digest: Option<String>,
        opts: OciImportOptions,
        mut pull_blob_by_digest: F,
    ) -> Res<ImportedPlug>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = Res<Vec<u8>>>,
    {
        let mut manifest_layer: Option<Vec<u8>> = None;
        let mut oci_digest_to_repo_hash: HashMap<String, String> = HashMap::new();
        let mut imported_blob_hashes = vec![];

        for layer in &image_manifest.layers {
            let layer_bytes = pull_blob_by_digest(layer.digest.clone())
                .await
                .wrap_err_with(|| format!("error pulling OCI layer blob '{}'", layer.digest))?;
            if opts.strict {
                Self::validate_sha256_digest(&layer.digest, &layer_bytes)?;
            }
            let repo_hash = self.blobs.put(&layer_bytes).await?;
            let repo_hash_str = crate::blobs::blob_hash_from_id(repo_hash);
            oci_digest_to_repo_hash.insert(layer.digest.clone(), repo_hash_str.clone());
            imported_blob_hashes.push(repo_hash_str);
            if layer.media_type == OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE {
                if manifest_layer.is_some() {
                    eyre::bail!(
                        "OCI artifact contains multiple '{}' layers",
                        OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE
                    );
                }
                manifest_layer = Some(layer_bytes);
            }
        }

        let manifest_layer = manifest_layer.ok_or_eyre(format!(
            "missing required '{}' layer",
            OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE
        ))?;

        let manifest_json: serde_json::Value = serde_json::from_slice(&manifest_layer)
            .wrap_err("error parsing plug manifest layer JSON")?;
        let rewritten_manifest_json =
            Self::rewrite_oci_component_urls(manifest_json, &oci_digest_to_repo_hash)?;
        let plug_manifest: manifest::PlugManifest = serde_json::from_value(rewritten_manifest_json)
            .wrap_err("error parsing rewritten plug manifest JSON into PlugManifest")?;
        let plug_id = plug_manifest.id();
        let plug_version = plug_manifest.version.clone();

        let doc_id = self.add(plug_manifest).await?;

        Ok(ImportedPlug {
            plug_id,
            version: plug_version,
            doc_id: Some(doc_id),
            imported_blob_hashes,
            source_digest,
        })
    }

    async fn inspect_oci_image_manifest<F, Fut>(
        image_manifest: &oci_client::manifest::OciImageManifest,
        mut pull_blob_by_digest: F,
    ) -> Res<manifest::PlugManifest>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = Res<Vec<u8>>>,
    {
        let Some(manifest_layer) = image_manifest
            .layers
            .iter()
            .find(|layer| layer.media_type == OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE)
        else {
            eyre::bail!(
                "missing required '{}' layer",
                OCI_PLUG_MANIFEST_LAYER_MEDIA_TYPE
            );
        };
        let layer_bytes = pull_blob_by_digest(manifest_layer.digest.clone())
            .await
            .wrap_err_with(|| {
                format!("error pulling OCI layer blob '{}'", manifest_layer.digest)
            })?;
        let manifest_json: serde_json::Value = serde_json::from_slice(&layer_bytes)
            .wrap_err("error parsing plug manifest layer JSON")?;
        let manifest_json = Self::scrub_oci_preview_manifest(manifest_json);
        let plug_manifest: manifest::PlugManifest = serde_json::from_value(manifest_json)
            .wrap_err("error parsing plug manifest JSON into PlugManifest")?;
        Ok(plug_manifest)
    }

    fn scrub_oci_preview_manifest(mut manifest_json: serde_json::Value) -> serde_json::Value {
        let Some(wflow_bundles) = manifest_json
            .get_mut("wflowBundles")
            .and_then(serde_json::Value::as_object_mut)
        else {
            return manifest_json;
        };
        for bundle in wflow_bundles.values_mut() {
            let Some(bundle_object) = bundle.as_object_mut() else {
                continue;
            };
            bundle_object.insert(
                "componentUrls".to_string(),
                serde_json::Value::Array(vec![]),
            );
        }
        manifest_json
    }

    async fn load_oci_layout_image_manifest(
        layout_root: &std::path::Path,
    ) -> Res<(oci_client::manifest::OciImageManifest, String)> {
        let _oci_layout = oci_spec::image::OciLayout::from_file(layout_root.join("oci-layout"))?;
        let index = oci_spec::image::ImageIndex::from_file(layout_root.join("index.json"))?;
        let selected_manifest_descriptor = index
            .manifests()
            .first()
            .cloned()
            .ok_or_eyre("oci index has no manifests")?;
        let selected_manifest_sha = selected_manifest_descriptor
            .as_digest_sha256()
            .ok_or_eyre("oci index manifest descriptor must use sha256 digest")?
            .to_string();
        let manifest_bytes = Self::read_oci_layout_blob_by_sha(layout_root, &selected_manifest_sha)
            .await
            .wrap_err("error reading selected OCI manifest blob from layout")?;
        let oci_manifest: oci_client::manifest::OciManifest =
            serde_json::from_slice(&manifest_bytes)?;

        match oci_manifest {
            oci_client::manifest::OciManifest::Image(manifest) => {
                Ok((manifest, selected_manifest_sha))
            }
            oci_client::manifest::OciManifest::ImageIndex(index_manifest) => {
                let nested_descriptor = index_manifest
                    .manifests
                    .first()
                    .ok_or_eyre("nested OCI image index has no manifests")?;
                let nested_sha = Self::sha256_hex_from_digest_str(&nested_descriptor.digest)?;
                let nested_bytes = Self::read_oci_layout_blob_by_sha(layout_root, &nested_sha)
                    .await
                    .wrap_err("error reading nested OCI manifest blob from layout")?;
                match serde_json::from_slice::<oci_client::manifest::OciManifest>(&nested_bytes)? {
                    oci_client::manifest::OciManifest::Image(manifest) => {
                        Ok((manifest, selected_manifest_sha))
                    }
                    oci_client::manifest::OciManifest::ImageIndex(_) => {
                        eyre::bail!("nested OCI manifest must resolve to an image manifest");
                    }
                }
            }
        }
    }

    fn rewrite_oci_component_urls(
        mut manifest_json: serde_json::Value,
        oci_digest_to_repo_hash: &HashMap<String, String>,
    ) -> Res<serde_json::Value> {
        let bundles = manifest_json
            .get_mut("wflowBundles")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_eyre("plug manifest JSON missing object at 'wflowBundles'")?;

        for bundle in bundles.values_mut() {
            let component_urls = bundle
                .get_mut("componentUrls")
                .and_then(serde_json::Value::as_array_mut)
                .ok_or_eyre("plug manifest JSON bundle missing array at 'componentUrls'")?;

            for url_value in component_urls.iter_mut() {
                let Some(url_str) = url_value.as_str() else {
                    eyre::bail!("componentUrls entries must be strings");
                };
                if !url_str.starts_with("oci://sha256:") {
                    eyre::bail!("componentUrls entries must be OCI digests: '{url_str}'");
                }
                let digest_hex = url_str.trim_start_matches("oci://sha256:");
                if digest_hex.is_empty() {
                    eyre::bail!("empty digest in OCI URL '{url_str}'");
                }
                let digest_key = format!("sha256:{digest_hex}");
                let Some(repo_hash) = oci_digest_to_repo_hash.get(&digest_key) else {
                    eyre::bail!(
                        "OCI URL '{url_str}' references missing layer digest '{digest_key}'"
                    );
                };
                *url_value = serde_json::Value::String(format!(
                    "{}:///{repo_hash}",
                    crate::blobs::BLOB_SCHEME
                ));
            }
        }

        Ok(manifest_json)
    }

    fn sha256_hex_from_digest_str(digest: &str) -> Res<String> {
        let Some((algo, hex)) = digest.split_once(':') else {
            eyre::bail!("invalid OCI digest '{digest}'");
        };
        eyre::ensure!(
            algo == "sha256",
            "unsupported OCI digest algorithm '{algo}'"
        );
        eyre::ensure!(!hex.is_empty(), "empty OCI digest hex");
        Ok(hex.to_string())
    }

    fn validate_sha256_digest(digest: &str, bytes: &[u8]) -> Res<()> {
        use sha2::{Digest as _, Sha256};
        let expected_hex = Self::sha256_hex_from_digest_str(digest)?;
        let actual_hex = format!("{:x}", Sha256::digest(bytes));
        eyre::ensure!(
            expected_hex.eq_ignore_ascii_case(&actual_hex),
            "OCI blob digest mismatch for '{digest}'"
        );
        Ok(())
    }

    async fn read_oci_layout_blob_by_sha(
        layout_root: &std::path::Path,
        sha_hex: &str,
    ) -> Res<Vec<u8>> {
        let path = layout_root.join("blobs").join("sha256").join(sha_hex);
        tokio::fs::read(&path)
            .await
            .wrap_err_with(|| format!("error reading OCI layout blob '{}'", path.display()))
    }

}
